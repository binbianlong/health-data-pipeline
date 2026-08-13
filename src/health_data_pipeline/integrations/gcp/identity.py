"""Google-signed OIDC verification for Cloud Tasks calls."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any, cast


class GoogleOidcVerifier:
    """Verify Google-signed Cloud Tasks ID tokens."""

    def verify(self, token: str, *, audience: str) -> Mapping[str, Any]:
        from google.auth.transport import requests  # noqa: PLC0415
        from google.oauth2 import id_token  # noqa: PLC0415

        verifier = cast(Callable[..., Mapping[str, Any]], id_token.verify_oauth2_token)
        claims = verifier(token, requests.Request(), audience=audience)
        if claims.get("iss") not in {
            "accounts.google.com",
            "https://accounts.google.com",
        }:
            raise ValueError("unexpected OIDC issuer")
        return claims
