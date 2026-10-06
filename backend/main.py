"""
Real-time chat server built on FastAPI + native WebSockets.
...
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Final, List, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

# --------------------------------------------------------------------------- #
# Configuration
# --------------------------------------------------------------------------- #

BASE_DIR: Final[Path] = Path(__file__).resolve().parent

MAX_USERNAME_LENGTH: Final[int] = 24
MIN_USERNAME_LENGTH: Final[int] = 2
MAX_MESSAGE_LENGTH: Final[int] = 2000
USERNAME_RE: Final[re.Pattern[str]] = re.compile(r"^[A-Za-z0-9][A-Za-z0-9 _.\-]{1,23}$")
RATE_LIMIT_WINDOW: Final[float] = 1.0     # seconds
RATE_LIMIT_BURST: Final[int] = 5          # messages per window
HEARTBEAT... (server doesn't need)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s")
logger = logging.getLogger("chat.server")
