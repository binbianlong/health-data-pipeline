from __future__ import annotations

from urllib.parse import parse_qs, urlparse

import httpx
import pytest

from health_data_pipeline.integrations.google_health.oauth import (
    GOOGLE_HEALTH_SCOPES,
    GoogleOAuthExchangeError,
    GoogleOAuthProvider,
    OAuthClientCredentials,
)


def _provider(client: httpx.Client) -> GoogleOAuthProvider:
    return GoogleOAuthProvider(
        oauth_client=OAuthClientCredentials("client-id", "client-secret"),
        redirect_uri="https://service.example/setup/oauth/callback",
        authorization_url="https://accounts.example/authorize",
        token_url="https://accounts.example/token",
        health_api_base_url="https://health.example",
        client=client,
    )


def test_authorization_url_contains_pkce_nonce_and_offline_scopes() -> None:
    provider = _provider(httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(500))))

    url = provider.authorization_url(
        state="state-value",
        nonce="nonce-value",
        code_challenge="challenge-value",
    )
    query = parse_qs(urlparse(url).query)

    assert query["state"] == ["state-value"]
    assert query["nonce"] == ["nonce-value"]
    assert query["code_challenge_method"] == ["S256"]
    assert query["access_type"] == ["offline"]
    assert set(query["scope"][0].split()) == set(GOOGLE_HEALTH_SCOPES)


def test_exchange_code_verifies_oidc_and_fetches_health_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.url.path == "/token":
            return httpx.Response(
                200,
                json={
                    "access_token": "access-token",
                    "refresh_token": "refresh-token",
                    "id_token": "id-token",
                },
            )
        assert request.headers["authorization"] == "Bearer access-token"
        return httpx.Response(200, json={"healthUserId": "health-user"})

    monkeypatch.setattr(
        "health_data_pipeline.integrations.google_health.oauth.id_token.verify_oauth2_token",
        lambda token, request, audience: {
            "iss": "https://accounts.google.com",
            "nonce": "expected-nonce",
            "sub": "google-sub",
        },
    )
    client = httpx.Client(transport=httpx.MockTransport(handler))

    grant = _provider(client).exchange_code(
        code="authorization-code",
        code_verifier="pkce-verifier",
        expected_nonce="expected-nonce",
    )

    assert grant.sub == "google-sub"
    assert grant.health_user_id == "health-user"
    assert grant.refresh_token == "refresh-token"
    assert [request.url.path for request in requests] == ["/token", "/v4/users/me/identity"]
    token_form = parse_qs(requests[0].content.decode())
    assert token_form["code_verifier"] == ["pkce-verifier"]


@pytest.mark.parametrize(
    ("token_payload", "claims", "message"),
    [
        (
            {"access_token": "a", "refresh_token": "r"},
            {"iss": "https://accounts.google.com", "nonce": "n", "sub": "s"},
            "id_token",
        ),
        (
            {"access_token": "a", "refresh_token": "r", "id_token": "i"},
            {"iss": "https://accounts.google.com", "nonce": "wrong", "sub": "s"},
            "nonce",
        ),
        (
            {"access_token": "a", "refresh_token": "r", "id_token": "i"},
            {"iss": "untrusted.example", "nonce": "n", "sub": "s"},
            "issuer",
        ),
    ],
)
def test_exchange_rejects_missing_or_untrusted_oidc_fields(
    monkeypatch: pytest.MonkeyPatch,
    token_payload: dict[str, str],
    claims: dict[str, str],
    message: str,
) -> None:
    monkeypatch.setattr(
        "health_data_pipeline.integrations.google_health.oauth.id_token.verify_oauth2_token",
        lambda token, request, audience: claims,
    )
    client = httpx.Client(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, json=token_payload))
    )

    with pytest.raises(GoogleOAuthExchangeError, match=message):
        _provider(client).exchange_code(
            code="code",
            code_verifier="verifier",
            expected_nonce="n",
        )


def test_exchange_rejects_token_and_identity_http_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "health_data_pipeline.integrations.google_health.oauth.id_token.verify_oauth2_token",
        lambda token, request, audience: {
            "iss": "accounts.google.com",
            "nonce": "n",
            "sub": "s",
        },
    )
    token_failure = httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(400)))
    with pytest.raises(GoogleOAuthExchangeError, match="token exchange failed"):
        _provider(token_failure).exchange_code(
            code="code", code_verifier="verifier", expected_nonce="n"
        )

    def identity_failure(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/token":
            return httpx.Response(
                200,
                json={
                    "access_token": "a",
                    "refresh_token": "r",
                    "id_token": "i",
                },
            )
        return httpx.Response(503)

    client = httpx.Client(transport=httpx.MockTransport(identity_failure))
    with pytest.raises(GoogleOAuthExchangeError, match="identity lookup failed"):
        _provider(client).exchange_code(code="code", code_verifier="verifier", expected_nonce="n")
