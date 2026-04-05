import logging
import threading
import webbrowser
from queue import Empty, Queue
from typing import Iterable
from urllib.parse import urlparse

from flask import Flask, request
from spotipy import Spotify
from spotipy.cache_handler import CacheFileHandler
from spotipy.oauth2 import SpotifyPKCE
from werkzeug.serving import make_server


class SpotifyOAuthCallbackServer(threading.Thread):
    """Run a minimal Flask server to capture Spotify's OAuth callback."""

    def __init__(self, host: str, port: int, path: str, result_queue: Queue):
        super().__init__(daemon=True)
        self.host = host
        self.port = port
        self.path = path or "/callback"
        self._result_queue = result_queue
        self._ready_event = threading.Event()

        self.app = Flask(__name__)

        @self.app.route(self.path, methods=["GET"])
        def callback() -> str:
            error = request.args.get("error")
            if error:
                self._result_queue.put({"error": error})
            else:
                self._result_queue.put(
                    {
                        "code": request.args.get("code"),
                        "state": request.args.get("state"),
                    }
                )
            return (
                "<html><body><h1>Authorization complete</h1>"
                "<p>You can close this window and return to the application.</p>"
                "</body></html>"
            )

        self._server = make_server(self.host, self.port, self.app)
        self._ctx = self.app.app_context()
        self._ctx.push()

    def run(self) -> None:
        self._ready_event.set()
        self._server.serve_forever()

    def wait_until_ready(self, timeout: float = 5.0) -> None:
        self._ready_event.wait(timeout)

    def shutdown(self) -> None:
        self._server.shutdown()
        self._ctx.pop()


def _run_authorization_flow(
    auth_manager: SpotifyPKCE,
    redirect_uri: str,
    timeout: int,
    logger: logging.Logger | None = None,
) -> None:
    parsed_uri = urlparse(redirect_uri)
    host = parsed_uri.hostname or "127.0.0.1"
    port = parsed_uri.port or 8000
    path = parsed_uri.path or "/callback"

    result_queue: Queue = Queue()
    callback_server = SpotifyOAuthCallbackServer(host, port, path, result_queue)
    callback_server.start()
    callback_server.wait_until_ready()

    auth_url = auth_manager.get_authorize_url()
    print("Starting Spotify authorization flow...")
    print(f"If your browser doesn't open automatically, visit:\n{auth_url}\n")

    try:
        webbrowser.open(auth_url)
    except webbrowser.Error:
        if logger:
            logger.warning("Unable to open browser automatically")

    try:
        result = result_queue.get(timeout=timeout)
    except Empty as exc:
        raise TimeoutError("Spotify authorization timed out") from exc
    finally:
        callback_server.shutdown()

    error = result.get("error")
    if error:
        raise RuntimeError(f"Spotify authorization failed: {error}")

    code = result.get("code")
    if not code:
        raise RuntimeError("Spotify authorization failed: missing authorization code")

    auth_manager.get_access_token(code, check_cache=False)
    token_info = auth_manager.cache_handler.get_cached_token()
    if not token_info:
        raise RuntimeError("Spotify authorization failed to return token information")


def build_pkce_spotify_client(
    client_id: str,
    redirect_uri: str,
    scopes: str | Iterable[str],
    cache_path: str,
    timeout: int = 180,
    logger: logging.Logger | None = None,
) -> Spotify:
    """Create a Spotipy client and ensure a PKCE token is available via local callback."""
    if not client_id:
        raise RuntimeError("SPOTIPY_CLIENT_ID must be set in the environment")

    scope_str = " ".join(scopes) if not isinstance(scopes, str) else scopes

    auth_manager = SpotifyPKCE(
        client_id=client_id,
        redirect_uri=redirect_uri,
        scope=scope_str,
        cache_handler=CacheFileHandler(cache_path=cache_path),
        open_browser=False,
    )

    token_info = auth_manager.cache_handler.get_cached_token()
    token_info = auth_manager.validate_token(token_info)
    if token_info is None:
        _run_authorization_flow(auth_manager, redirect_uri, timeout, logger)

    return Spotify(auth_manager=auth_manager)
