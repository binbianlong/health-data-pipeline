"""Secret Manager adapter without exposing secret values in logs."""

from __future__ import annotations

from google.cloud import secretmanager


class SecretValueError(RuntimeError):
    """A required secret is missing or malformed."""


class SecretManager:
    """Read and append Secret Manager versions without exposing values in logs."""

    def __init__(
        self,
        *,
        project_id: str,
        client: secretmanager.SecretManagerServiceClient | None = None,
    ) -> None:
        self._project_id = project_id
        self._client = client or secretmanager.SecretManagerServiceClient()

    def read_bytes(self, secret_id: str) -> bytes:
        name = f"projects/{self._project_id}/secrets/{secret_id}/versions/latest"
        response = self._client.access_secret_version(request={"name": name})
        value = bytes(response.payload.data)
        if not value:
            raise SecretValueError(f"secret has no active value: {secret_id}")
        return value

    def read_text(self, secret_id: str) -> str:
        try:
            return self.read_bytes(secret_id).decode("utf-8")
        except UnicodeDecodeError as exc:
            raise SecretValueError(f"secret is not UTF-8 text: {secret_id}") from exc

    def add_text_version(self, secret_id: str, value: str) -> None:
        if not value:
            raise SecretValueError("refusing to store an empty secret version")
        parent = f"projects/{self._project_id}/secrets/{secret_id}"
        self._client.add_secret_version(
            request={
                "parent": parent,
                "payload": {"data": value.encode("utf-8")},
            }
        )


class SecretRefreshTokenSink:
    """Append a newly authorized refresh token as a Secret Manager version."""

    def __init__(self, secrets: SecretManager, secret_id: str) -> None:
        self._secrets = secrets
        self._secret_id = secret_id

    def store(self, refresh_token: str) -> None:
        self._secrets.add_text_version(self._secret_id, refresh_token)
