""" Default token manager presented by OpenSkyApi """
import requests
from typing import Tuple
from datetime import datetime, timedelta
from airflow.sdk.bases.hook import BaseHook


# How many seconds before expiry to proactively refresh the token.
TOKEN_REFRESH_MARGIN = 30
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"


class TokenManager:
    def __init__(self, conn_id: str = 'opensky_api'):
        self.token = None
        self.expires_at = None
        self.conn_id = conn_id
        self.client_id, self.client_secret = self._get_credentials()

    def get_token(self):
        """Return a valid access token, refreshing automatically if needed."""
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _get_credentials(self) -> Tuple[str, str]:
        conn = BaseHook.get_connection(self.conn_id).extra_dejson
        return conn['client_id'], conn['client_secret']

    def _refresh(self):
        """Fetch a new access token from the OpenSky authentication server."""
        r = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
        )
        r.raise_for_status()

        data = r.json()
        self.token = data["access_token"]
        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(seconds=expires_in - TOKEN_REFRESH_MARGIN)
        return self.token

    def auth_headers(self):
        """Return request headers with a valid Bearer token."""
        return {"Authorization": f"Bearer {self.get_token()}"}
