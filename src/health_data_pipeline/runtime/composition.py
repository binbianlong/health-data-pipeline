"""Production composition roots for Cloud Run Service and Cloud Run Job."""

from __future__ import annotations

import base64
import json
import os
import threading
from collections.abc import Mapping
from contextlib import suppress
from typing import cast

import structlog
from google.cloud import tasks_v2

from health_data_pipeline.integrations.gcp.identity import GoogleOidcVerifier
from health_data_pipeline.integrations.gcp.secrets import (
    SecretManager,
    SecretRefreshTokenSink,
)
from health_data_pipeline.integrations.gcp.tasks import (
    CloudTaskDispatcher,
)
from health_data_pipeline.integrations.gcp.tasks import (
    CloudTasksClient as CloudTasksClientProtocol,
)
from health_data_pipeline.integrations.google_health.client import (
    GoogleHealthClient,
)
from health_data_pipeline.integrations.google_health.oauth import (
    GOOGLE_HEALTH_SCOPES,
    GoogleOAuthProvider,
    OAuthClientCredentials,
)
from health_data_pipeline.integrations.google_health.transport import (
    HttpxTransport,
    RefreshingAccessToken,
)
from health_data_pipeline.integrations.google_health.webhook import (
    GoogleHealthWebhookAuthenticator,
    VerifiedWebhookBatch,
    WebhookPayloadError,
    WebhookSignatureVerifier,
    WebhookVerification,
)
from health_data_pipeline.integrations.google_health.webhook_keys import GoogleHealthKeysetVerifier
from health_data_pipeline.integrations.notion import NotionDataSourceGateway
from health_data_pipeline.ops.alerts import FitbitSyncAlertService
from health_data_pipeline.ops.class_a import (
    ClassACountingObjectStore,
    ClassAUsageReporter,
)
from health_data_pipeline.runtime.config import AppConfig
from health_data_pipeline.runtime.service import WebDependencies
from health_data_pipeline.setup.oauth import (
    OWNER_PATH,
    AesGcmCipher,
    OAuthSessionService,
    OwnerRegistry,
    SetupCodeService,
)
from health_data_pipeline.state.commit import CommitCoordinator
from health_data_pipeline.state.gcs import GoogleCloudStorageObjectStore
from health_data_pipeline.state.head import HeadRepository
from health_data_pipeline.state.objects import ContentAddressedStore, ObjectNotFound, ObjectStore
from health_data_pipeline.state.snapshots import SnapshotRepository
from health_data_pipeline.sync.events import GcsWebhookEventSink
from health_data_pipeline.sync.runner import JobRunner
from health_data_pipeline.sync.service import IncrementalSyncPipeline
from health_data_pipeline.views import (
    NotionViewService,
    ViewApplicationService,
    read_notion_sync_state,
    request_notion_generation,
)
from health_data_pipeline.views.notion import DefaultNotionRenderer
from health_data_pipeline.views.snapshot_source import SnapshotViewSource
from health_data_pipeline.views.steps import StepsSvgService

HMAC_HEX_LENGTH = 64
SECRET_KEY_BYTES = 32


class DynamicOwnerWebhookAuthenticator:
    """Resolve the owner HMAC from GCS for every notification."""

    def __init__(
        self,
        *,
        store: ObjectStore,
        expected_authorization: str,
        signature_verifier: WebhookSignatureVerifier,
        application_hmac_key: bytes,
    ) -> None:
        self._store = store
        self._expected_authorization = expected_authorization
        self._signature_verifier = signature_verifier
        self._application_hmac_key = application_hmac_key

    def authenticate(
        self,
        *,
        authorization: str | None,
        signature_header: str | None,
        content_type: str | None,
        raw_body: bytes,
    ) -> VerifiedWebhookBatch | WebhookVerification:
        try:
            expected_owner_hmac = _owner_hmac(self._store)
        except WebhookPayloadError:
            # Subscriber endpoint verification happens before the owner grants
            # OAuth consent. The authenticator returns a verification result
            # before it evaluates this placeholder; real notifications remain
            # rejected until owner state exists.
            expected_owner_hmac = ""
        return GoogleHealthWebhookAuthenticator(
            expected_authorization=self._expected_authorization,
            signature_verifier=self._signature_verifier,
            application_hmac_key=self._application_hmac_key,
            expected_owner_hmac=expected_owner_hmac,
        ).authenticate(
            authorization=authorization,
            signature_header=signature_header,
            content_type=content_type,
            raw_body=raw_body,
        )


def build_web_dependencies() -> WebDependencies:
    """Build dependencies without starting the HTTP server."""

    _configure_structured_logging()
    config = AppConfig.from_env()
    raw_store = _store(config)
    store = ClassACountingObjectStore(raw_store)
    logger = structlog.get_logger("health_data_pipeline")
    class_a_usage = ClassAUsageReporter(
        store=raw_store,
        counter=store,
        emit=lambda event, payload: logger.warning(event, **payload),
    )
    class_a_flush_lock = threading.Lock()

    def flush_class_a_usage() -> None:
        with class_a_flush_lock:
            try:
                class_a_usage.flush()
            except Exception as exc:
                logger.error(  # noqa: TRY400 - deliberately omit exception details.
                    "class_a_state_update_failed",
                    error_type=type(exc).__name__,
                )

    secrets = SecretManager(project_id=config.project_id)
    oauth_client = _oauth_client(secrets)
    application_hmac_key = _key_secret(
        secrets,
        _required_env("APPLICATION_HMAC_SECRET_ID"),
    )
    cipher_key = _key_secret(
        secrets,
        _required_env("OAUTH_SESSION_AEAD_SECRET_ID"),
    )
    provider = GoogleOAuthProvider(
        oauth_client=oauth_client,
        redirect_uri=config.oauth_redirect_uri,
        authorization_url=config.oauth_authorization_url,
        token_url=config.oauth_token_url,
        health_api_base_url=config.health_api_base_url,
    )
    setup_codes = SetupCodeService(store)
    oauth_sessions = OAuthSessionService(
        store=store,
        setup_codes=setup_codes,
        cipher=AesGcmCipher(cipher_key),
        provider=provider,
        owner_registry=OwnerRegistry(store, application_hmac_key),
        refresh_tokens=SecretRefreshTokenSink(
            secrets,
            _required_env("GOOGLE_OAUTH_REFRESH_SECRET_ID"),
        ),
        application_hmac_key=application_hmac_key,
    )
    tasks = CloudTaskDispatcher(
        client=cast(
            CloudTasksClientProtocol,
            tasks_v2.CloudTasksClient(),
        ),
        config=config,
    )
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(ContentAddressedStore(store))
    source = SnapshotViewSource(heads=heads, snapshots=snapshots)
    notion = NotionViewService(
        store=store,
        source=source,
        notion=NotionDataSourceGateway(
            token=secrets.read_text(_required_env("NOTION_TOKEN_SECRET_ID")).strip(),
            data_source_id=config.notion_data_source_id,
        ),
        renderer=DefaultNotionRenderer(),
    )
    steps = StepsSvgService(store)
    return WebDependencies(
        config=config,
        webhook_authenticator=DynamicOwnerWebhookAuthenticator(
            store=store,
            expected_authorization=secrets.read_text(
                _required_env("WEBHOOK_AUTH_SECRET_ID")
            ).strip(),
            signature_verifier=GoogleHealthKeysetVerifier(),
            application_hmac_key=application_hmac_key,
        ),
        webhook_events=GcsWebhookEventSink(store),
        tasks=tasks,
        oauth_sessions=oauth_sessions,
        oidc_verifier=GoogleOidcVerifier(),
        expected_task_service_account=config.cloud_tasks_service_account,
        views=ViewApplicationService(
            notion=notion,
            steps=steps,
            step_samples=source,
        ),
        steps=steps,
        on_complete=flush_class_a_usage,
    )


def build_job_runner() -> JobRunner:
    """Build the one-process synchronization Job."""

    _configure_structured_logging()
    config = AppConfig.from_env()
    raw_store = _store(config)
    store = ClassACountingObjectStore(raw_store)
    content = ContentAddressedStore(store)
    heads = HeadRepository(store)
    snapshots = SnapshotRepository(content)
    secrets = SecretManager(project_id=config.project_id)
    oauth_client = _oauth_client(secrets)
    token = RefreshingAccessToken(
        oauth_client=oauth_client,
        refresh_token=secrets.read_text(_required_env("GOOGLE_OAUTH_REFRESH_SECRET_ID")).strip(),
        token_uri=config.oauth_token_url,
        scopes=GOOGLE_HEALTH_SCOPES,
    )
    logger = structlog.get_logger("health_data_pipeline")
    class_a_usage = ClassAUsageReporter(
        store=raw_store,
        counter=store,
        emit=lambda event, payload: logger.warning(event, **payload),
    )

    def flush_class_a_usage() -> None:
        try:
            class_a_usage.flush()
        except Exception as exc:
            # Accounting is an operational guardrail. A corrupt or contended
            # counter must never turn a completed health-data commit into a
            # failed synchronization, and the log must not include payloads.
            logger.error(  # noqa: TRY400 - deliberately omit exception details.
                "class_a_state_update_failed",
                error_type=type(exc).__name__,
            )

    sync_alert = FitbitSyncAlertService(
        store,
        emit=lambda event, payload: logger.warning(event, **payload),
    )
    pipeline = IncrementalSyncPipeline(
        store=store,
        content=content,
        heads=heads,
        snapshots=snapshots,
        commits=CommitCoordinator(heads=heads, snapshots=snapshots),
        health=GoogleHealthClient(
            transport=HttpxTransport(),
            access_token=token,
            base_url=config.health_api_base_url,
        ),
        owner_hmac=_owner_hmac(store),
        default_time_zone=os.environ.get("OWNER_TIME_ZONE", "Asia/Tokyo"),
        sync_alert=sync_alert,
        backfill_ready=lambda generation: (
            read_notion_sync_state(store)[0].applied_generation >= generation
        ),
    )
    source = SnapshotViewSource(heads=heads, snapshots=snapshots)
    task_dispatcher = CloudTaskDispatcher(
        client=cast(
            CloudTasksClientProtocol,
            tasks_v2.CloudTasksClient(),
        ),
        config=config,
    )
    return JobRunner(
        pipeline=pipeline,
        tasks=task_dispatcher,
        latest_generation=source.latest_generation,
        request_view_generation=lambda generation: request_notion_generation(
            store,
            generation,
        ),
        sync_tasks=task_dispatcher,
        sync_recovery_needed=pipeline.has_pending_webhooks,
        on_sync_recovery_error=lambda exc: logger.error(
            "sync_recovery_failed",
            error_type=type(exc).__name__,
        ),
        on_complete=flush_class_a_usage,
    )


def _store(config: AppConfig) -> GoogleCloudStorageObjectStore:
    return GoogleCloudStorageObjectStore.from_bucket_name(
        config.health_bucket,
        project=config.project_id,
    )


def _oauth_client(secrets: SecretManager) -> OAuthClientCredentials:
    return OAuthClientCredentials.from_json(
        secrets.read_text(_required_env("GOOGLE_OAUTH_CLIENT_SECRET_ID"))
    )


def _owner_hmac(store: ObjectStore) -> str:
    try:
        stored = store.get(OWNER_PATH)
    except ObjectNotFound as exc:
        raise WebhookPayloadError("pipeline owner has not completed OAuth setup") from exc
    try:
        value = json.loads(stored.data)
    except json.JSONDecodeError as exc:
        raise RuntimeError("stored owner state is invalid") from exc
    if not isinstance(value, Mapping):
        raise TypeError("stored owner state is not an object")
    result = value.get("health_user_id_hmac")
    if not isinstance(result, str) or len(result) != HMAC_HEX_LENGTH:
        raise RuntimeError("stored owner state has no health user HMAC")
    return result


def _key_secret(secrets: SecretManager, secret_id: str) -> bytes:
    raw = secrets.read_bytes(secret_id)
    if len(raw) == SECRET_KEY_BYTES:
        return raw
    try:
        text = raw.decode("ascii").strip()
    except UnicodeDecodeError as exc:
        raise RuntimeError(f"key secret must contain 32 bytes: {secret_id}") from exc
    candidates: list[bytes] = []
    with suppress(ValueError):
        candidates.append(base64.b64decode(text, validate=True))
    with suppress(ValueError):
        candidates.append(bytes.fromhex(text))
    for candidate in candidates:
        if len(candidate) == SECRET_KEY_BYTES:
            return candidate
    raise RuntimeError(f"key secret must decode to 32 bytes: {secret_id}")


def _required_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def _configure_structured_logging() -> None:
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.JSONRenderer(),
        ],
        cache_logger_on_first_use=True,
    )
