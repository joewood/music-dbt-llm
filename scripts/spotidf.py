import json
import logging
import os
import time
from datetime import datetime
from itertools import groupby
from typing import Any, List, TypedDict

from spotipy import Spotify
from spotify_auth import build_pkce_spotify_client

# This is not a secret - don't worry
SPOTILAB_REDIRECT_URI = "http://127.0.0.1:8000/hub/oauth_callback"
CONFIG_ROOT = f"{os.environ.get('HOME',os.environ.get('HOMEPATH','.'))}{os.sep}.spotilab"


class TrackUriPositions(TypedDict):
    uri: str
    positions: List[int]


client_id = os.environ.get("SPOTIPY_CLIENT_ID", "")
redirect_uri = os.environ.get("SPOTIPY_REDIRECT_URI", SPOTILAB_REDIRECT_URI)
scope = [
    "user-read-private",
    "user-read-email",
    "playlist-modify-public",
    "playlist-modify-private",
    "user-library-read",
    "user-library-read",
    "user-library-modify",
    "playlist-read-private",
    "playlist-read-collaborative",
]


class SpotifyClient:
    def __init__(self) -> None:
        self.logger = logging.getLogger("spotidf.spotify_client")
        os.makedirs(CONFIG_ROOT, exist_ok=True)

        if not client_id:
            raise RuntimeError("SPOTIPY_CLIENT_ID must be set in the environment")

        self.cache_path = f"{CONFIG_ROOT}{os.sep}.spotilab_token.json"
        self._spotify = build_pkce_spotify_client(
            client_id=client_id,
            redirect_uri=redirect_uri,
            scopes=scope,
            cache_path=self.cache_path,
            logger=self.logger,
        )
        self._playlists_cache = self.fetch_playlists()
