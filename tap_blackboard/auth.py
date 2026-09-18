"""blackboard Authentication."""

from __future__ import annotations

import os

from hotglue_singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from typing_extensions import override


class blackboardAuthenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for blackboard.

    Blackboard uses the OAuth2 client-credentials grant with no refresh token.
    The application key and secret are sent as HTTP Basic auth on the token
    request; the body only carries ``grant_type=client_credentials``. Access
    tokens expire after about one hour; the SDK re-runs this exchange on expiry.
    """

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the blackboard API.

        Returns:
            A dict with the request body
        """
        return {
            "grant_type": "client_credentials",
        }

    @override
    def request_auth(self) -> tuple[str, str]:
        """Send the credentials as Basic auth on the token request.

        Returns:
            The (client_id, client_secret) pair used for the Basic header.
        """
        return self.client_id, self.client_secret

    @override
    def update_access_token(self) -> None:
        """Refresh via client_credentials locally when not running in Hotglue."""
        # Hotglue executor sets these; without them, fall through to local OAuth.
        if not os.environ.get("API_KEY"):
            self.update_access_token_locally()
            return
        super().update_access_token()
