"""
main.py — Real-time chat server built on FastAPI + native WebSockets.

Features
--------
* ConnectionManager that owns the lifecycle of every active socket
  (connect / disconnect / send_personal_message / broadcast).
* Strict, server-authoritative validation of every inbound frame.
* Session takeover: a reconnecting client always wins over a stale socket
  bearing the same display name.
* Per-connection sliding-window rate limiting.
* Clean shutdown paths for `WebSocketDisconnect` and unexpected errors.

Run with:
    uvicorn main:app --host 0.0.0.0 --port 8000
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, Final, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BASE_DIR: Final[Path] = Path(__file__).resolve().parent

MIN_USERNAME_LENGTH: Final[int] = 2
MAX_USERNAME_LENGTH: Final[int] = 24
MAX_MESSAGE_LENGTH: Final[int] = 2000

# A username must start with an alphanumeric character and may contain
# letters, digits, spaces, dots, underscores and hyphens.
USERNAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 ._-]{1,23}$")

# Sliding-window rate limit applied per connection.
RATE_LIMIT_BURST: Final[int] = 6
RATE_LIMIT_WINDOW_SECONDS: Final[float] = 2.0

# Hard cap on a single inbound frame, to protect the event loop.
MAX_FRAME_BYTES: Final[int] = 16_384

# Application-specific WebSocket close codes (must live in 4000-4999).
WS_CLOSE_REPLACED: Final[int] = 4000
WS_CLOSE_INVALID_NAME: Final[int] = 4001

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
)
logger = logging.getLogger("chat.server")


# --------------------------------------------------------------------------- #
# Payload helpers
# --------------------------------------------------------------------------- #


def utc_timestamp() -> str:
    """Return the current UTC time as an ISO-8601 string (e.g. 2024-05-01T12:00:00Z)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def system_payload(message: str) -> Dict[str, Any]:
    """Build a server-originated informational packet."""
    return {"type": "system", "message": message, "timestamp": utc_timestamp()}


def error_payload(message: str) -> Dict[str, Any]:
    """Build a server-originated error packet addressed to a single client."""
    return {"type": "error", "message": message, "timestamp": utc_timestamp()}


def sanitize_username(raw: Optional[str]) -> Optional[str]:
    """
    Normalise and validate a display name.

    Returns the cleaned name, or ``None`` when the input is unacceptable.
    Whitespace runs are collapsed and control characters are stripped, which
    also makes the name safe to echo back inside log lines and UI labels.
    """
    if not raw:
        return None

    candidate = " ".join(raw.split())
    if not (MIN_USERNAME_LENGTH <= len(candidate) <= MAX_USERNAME_LENGTH):
        return None
    if not USERNAME_RE.match(candidate):
        return None
    return candidate


def parse_client_packet(raw: str) -> Optional[Dict[str, str]]:
    """
    Decode and validate an inbound frame.

    Accepted shapes::

        {"type": "message", "user": "ada", "message": "hello"}
        {"type": "ping"}

    Note: the ``user`` field is accepted for schema compatibility but is
    deliberately ignored — the identity of a connection is decided by the
    server at handshake time and can never be spoofed mid-session.
    """
    try:
        data = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None

    if not isinstance(data, dict):
        return None

    packet_type = data.get("type", "message")
    if not isinstance(packet_type, str):
        return None
    packet_type = packet_type.strip().lower()

    if packet_type == "ping":
        return {"type": "ping", "message": ""}

    if packet_type != "message":
        return None

    text = data.get("message")
    if not isinstance(text, str):
        return None

    return {"type": "message", "message": text}


class SlidingWindowLimiter:
    """Allow at most ``burst`` events per ``window`` seconds."""

    def __init__(self, burst: int, window: float) -> None:
        self._burst = burst
        self._window = window
        self._events: Deque[float] = deque()

    def allow(self) -> bool:
        now = time.monotonic()
        while self._events and now - self._events[0] > self._window:
            self._events.popleft()
        if len(self._events) >= self._burst:
            return False
        self._events.append(now)
        return True


# --------------------------------------------------------------------------- #
# Connection manager
# --------------------------------------------------------------------------- #


class ConnectionManager:
    """
    Owns every active WebSocket and provides fan-out primitives.

    All mutations of the internal registry are guarded by an ``asyncio.Lock``
    because FastAPI may service many connections concurrently on one loop.
    """

    def __init__(self) -> None:
        self._clients: Dict[WebSocket, str] = {}
        self._lock = asyncio.Lock()

    # -- lifecycle ---------------------------------------------------------- #

    async def connect(self, websocket: WebSocket, username: str) -> None:
        """Accept the handshake and register the socket under ``username``."""
        await websocket.accept()
        async with self._lock:
            self._clients[websocket] = username
        logger.info("connected: %s (online=%d)", username, len(self._clients))

    async def disconnect(self, websocket: WebSocket) -> Optional[str]:
        """
        Deregister a socket.

        Returns the username that was associated with it, or ``None`` when the
        socket had already been removed (for example by a session takeover).
        """
        async with self._lock:
            username = self._clients.pop(websocket, None)
        if username is not None:
            logger.info("disconnected: %s (online=%d)", username, len(self._clients))
        return username

    async def evict(self, username: str, code: int, reason: str) -> None:
        """Close and deregister every socket currently holding ``username``."""
        async with self._lock:
            stale = [
                ws for ws, name in self._clients.items() if name.casefold() == username.casefold()
            ]

        for ws in stale:
            try:
                await ws.close(code=code, reason=reason)
            except Exception:  # noqa: BLE001 - closing must never raise upward
                logger.debug("evict: socket already gone for %s", username)
            await self.disconnect(ws)

    # -- delivery ----------------------------------------------------------- #

    async def send_personal_message(self, payload: Dict[str, Any], websocket: WebSocket) -> bool:
        """Send ``payload`` to one socket. Returns ``False`` if it is dead."""
        try:
            await websocket.send_json(payload)
            return True
        except (WebSocketDisconnect, RuntimeError):
            return False
        except Exception:  # noqa: BLE001 - a broken pipe must not kill the loop
            logger.exception("send_personal_message failed")
            return False

    async def broadcast(self, payload: Dict[str, Any], *, exclude: Optional[WebSocket] = None) -> None:
        """Fan ``payload`` out to every registered socket, pruning dead ones."""
        async with self._lock:
            targets = [ws for ws in self._clients if ws is not exclude]

        if not targets:
            return

        results = await asyncio.gather(
            *(self.send_personal_message(payload, ws) for ws in targets),
            return_exceptions=True,
        )

        for ws, delivered in zip(targets, results):
            if delivered is not True:
                await self.disconnect(ws)

    # -- introspection ------------------------------------------------------ #

    def usernames(self) -> List[str]:
        """Sorted, de-duplicated list of the display names currently online."""
        return sorted(set(self._clients.values()), key=str.casefold)

    @property
    def connection_count(self) -> int:
        return len(self._clients)


manager = ConnectionManager()

# --------------------------------------------------------------------------- #
# FastAPI application
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="Nexus Chat",
    description="Asynchronous real-time chat over native WebSockets.",
    version="1.0.0",
)


async def broadcast_presence() -> None:
    """Push the current roster to every connected client."""
    users = manager.usernames()
    await manager.broadcast(
        {
            "type": "presence",
            "users": users,
            "count": len(users),
            "timestamp": utc_timestamp(),
        }
    )


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    """
    Bi-directional chat channel.

    The client identifies itself once, at handshake time, via
    ``/ws?username=<display-name>``.
    """
    username = sanitize_username(websocket.query_params.get("username"))

    # Reject malformed identities before allocating any server-side state.
    if username is None:
        await websocket.accept()
        await websocket.send_json(
            error_payload(
                f"Invalid display name. Use {MIN_USERNAME_LENGTH}-{MAX_USERNAME_LENGTH} "
                "letters, digits, spaces, '.', '_' or '-'."
            )
        )
        await websocket.close(code=WS_CLOSE_INVALID_NAME, reason="Invalid username")
        return

    # A reconnecting client always takes priority over a stale socket that
    # still holds the same name (e.g. after an abrupt network drop).
    await manager.evict(username, code=WS_CLOSE_REPLACED, reason="Session replaced")

    await manager.connect(websocket, username)
    await broadcast_presence()
    await manager.broadcast(system_payload(f"{username} joined the chat."))

    await manager.send_personal_message(
        {
            "type": "welcome",
            "user": username,
            "message": f"Welcome, {username}!",
            "timestamp": utc_timestamp(),
            "users": manager.usernames(),
            "max_message_length": MAX_MESSAGE_LENGTH,
        },
        websocket,
    )

    limiter = SlidingWindowLimiter(RATE_LIMIT_BURST, RATE_LIMIT_WINDOW_SECONDS)

    try:
        while True:
            try:
                frame = await websocket.receive()
            except WebSocketDisconnect:
                break

            if frame.get("type") == "websocket.disconnect":
                break

            raw = frame.get("text")
            if raw is None:
                await manager.send_personal_message(
                    error_payload("Binary frames are not supported."), websocket
                )
                continue

            if len(raw) > MAX_FRAME_BYTES:
                await manager.send_personal_message(
                    error_payload("Payload too large."), websocket
                )
                continue

            packet = parse_client_packet(raw)
            if packet is None:
                await manager.send_personal_message(
                    error_payload(
                        "Malformed payload: expected a JSON object with a 'message' string."
                    ),
                    websocket,
                )
                continue

            # Keep-alive handshake — cheap, and never rate limited.
            if packet["type"] == "ping":
                await manager.send_personal_message(
                    {"type": "pong", "timestamp": utc_timestamp()}, websocket
                )
                continue

            if not limiter.allow():
                await manager.send_personal_message(
                    error_payload("You are sending messages too quickly. Please slow down."),
                    websocket,
                )
                continue

            text = packet["message"].strip()
            if not text:
                continue  # silently drop empty payloads

            if len(text) > MAX_MESSAGE_LENGTH:
                await manager.send_personal_message(
                    error_payload(f"Messages are limited to {MAX_MESSAGE_LENGTH} characters."),
                    websocket,
                )
                continue

            await manager.broadcast(
                {
                    "type": "message",
                    "user": username,
                    "message": text,
                    "timestamp": utc_timestamp(),
                }
            )

    except WebSocketDisconnect:
        logger.info("client disconnected abruptly: %s", username)
    except Exception:  # noqa: BLE001 - never let one socket take down the worker
        logger.exception("unhandled error on connection for %s", username)
    finally:
        removed = await manager.disconnect(websocket)
        # `removed is None` means the socket was already evicted by a newer
        # session, so we must not announce a spurious departure.
        if removed is not None:
            await broadcast_presence()
            await manager.broadcast(system_payload(f"{removed} left the chat."))


# --------------------------------------------------------------------------- #
# Static asset routes
# --------------------------------------------------------------------------- #
# NOTE: the assets are served through explicit routes rather than a
# StaticFiles mount on BASE_DIR, so that main.py itself is never downloadable.

_NO_STORE = {"Cache-Control": "no-cache"}


@app.get("/", include_in_schema=False)
async def serve_index() -> FileResponse:
    return FileResponse(BASE_DIR / "index.html", media_type="text/html", headers=_NO_STORE)


@app.get("/styles.css", include_in_schema=False)
async def serve_styles() -> FileResponse:
    return FileResponse(BASE_DIR / "styles.css", media_type="text/css", headers=_NO_STORE)


@app.get("/app.js", include_in_schema=False)
async def serve_script() -> FileResponse:
    return FileResponse(
        BASE_DIR / "app.js", media_type="application/javascript", headers=_NO_STORE
    )


@app.get("/health", include_in_schema=False)
async def health() -> Dict[str, Any]:
    """Liveness probe for load balancers and container orchestrators."""
    return {
        "status": "ok",
        "connections": manager.connection_count,
        "users": manager.usernames(),
        "timestamp": utc_timestamp(),
    }


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)
