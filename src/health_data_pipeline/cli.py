"""Operator CLI for one-time setup actions."""

from __future__ import annotations

import argparse
import os
from collections.abc import Sequence

from health_data_pipeline.cloud import SecretManager
from health_data_pipeline.notion_adapter import NotionDataSourceGateway
from health_data_pipeline.oauth import SetupCodeService
from health_data_pipeline.storage import GoogleCloudStorageObjectStore
from health_data_pipeline.subscriber import GoogleHealthSubscriberProvisioner


def _required(value: str | None, label: str) -> str:
    if value is None or not value.strip():
        raise SystemExit(f"{label} is required")
    return value.strip()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="health-data-pipeline")
    commands = parser.add_subparsers(dest="command", required=True)

    setup = commands.add_parser("setup-code", help="issue a 10-minute one-time code")
    setup.add_argument("--project-id", default=os.environ.get("PROJECT_ID"))
    setup.add_argument("--bucket", default=os.environ.get("HEALTH_BUCKET"))

    subscriber = commands.add_parser(
        "subscriber-upsert",
        help="create or update the automatic Google Health webhook subscriber",
    )
    subscriber.add_argument("--project-id", default=os.environ.get("PROJECT_ID"))
    subscriber.add_argument(
        "--service-url",
        default=os.environ.get("PUBLIC_BASE_URL", os.environ.get("CLOUD_RUN_SERVICE_URL")),
    )
    subscriber.add_argument(
        "--authorization-secret-id",
        default=os.environ.get(
            "WEBHOOK_AUTH_SECRET_ID",
            "health-data-pipeline-webhook-authorization",
        ),
    )
    subscriber.add_argument(
        "--subscriber-id",
        default="health-data-pipeline",
    )
    notion = commands.add_parser(
        "notion-configure",
        help="create or repair the health data source properties",
    )
    notion.add_argument("--project-id", default=os.environ.get("PROJECT_ID"))
    notion.add_argument(
        "--data-source-id",
        default=os.environ.get("NOTION_DATA_SOURCE_ID"),
    )
    notion.add_argument(
        "--token-secret-id",
        default=os.environ.get(
            "NOTION_TOKEN_SECRET_ID",
            "health-data-pipeline-notion-token",
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    project_id = _required(args.project_id, "--project-id or PROJECT_ID")
    if args.command == "setup-code":
        bucket = _required(args.bucket, "--bucket or HEALTH_BUCKET")
        store = GoogleCloudStorageObjectStore.from_bucket_name(
            bucket,
            project=project_id,
        )
        issued = SetupCodeService(store).issue()
        print(f"Setup code: {issued.code}")  # noqa: T201 - intentional local secret output.
        print(f"Expires at: {issued.expires_at.isoformat()}")  # noqa: T201
        return 0

    if args.command == "subscriber-upsert":
        service_url = _required(
            args.service_url,
            "--service-url, PUBLIC_BASE_URL, or CLOUD_RUN_SERVICE_URL",
        ).rstrip("/")
        secret_id = _required(
            args.authorization_secret_id,
            "--authorization-secret-id",
        )
        authorization = SecretManager(project_id=project_id).read_text(secret_id).strip()
        name = GoogleHealthSubscriberProvisioner(
            project_id=project_id,
            endpoint_uri=f"{service_url}/webhooks/google-health",
            endpoint_authorization=authorization,
            subscriber_id=args.subscriber_id,
        ).upsert()
        print(f"Subscriber ready: {name}")  # noqa: T201
        return 0

    if args.command == "notion-configure":
        data_source_id = _required(
            args.data_source_id,
            "--data-source-id or NOTION_DATA_SOURCE_ID",
        )
        token_secret_id = _required(
            args.token_secret_id,
            "--token-secret-id",
        )
        token = SecretManager(project_id=project_id).read_text(token_secret_id).strip()
        changed = NotionDataSourceGateway(
            token=token,
            data_source_id=data_source_id,
        ).ensure_schema()
        status = "configured" if changed else "already configured"
        print(f"Notion data source {status}.")  # noqa: T201
        return 0

    raise AssertionError("unreachable command")


if __name__ == "__main__":
    raise SystemExit(main())
