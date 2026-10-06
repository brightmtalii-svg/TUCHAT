# TUCHAT
A realtime messaging platform built with javascript,HTML, and python
Real-Time Chat Mechanics & UI Design
The app pairs a clean WebSocket lifecycle with a responsive, themeable interface so you can test messaging immediately.

Connection manager – ConnectionManager tracks every socket, accepts new users, broadcasts messages and presence updates, and cleans up on disconnect. Session takeover means a reconnecting client always wins over a stale connection.

Validation & rate limiting – inbound frames are parsed as JSON, the message type is checked, and a sliding-window limiter prevents spam. Usernames must match a strict pattern and are sanitized before use.

Reconnection logic – the client automatically retries with exponential backoff and jitter, sends periodic heartbeats, and handles abnormal close codes by returning to the username gate.

UI behavior – a modal collects the display name, the chat log auto-scrolls when pinned to the bottom, and incoming vs. outgoing messages use distinct bubble styles. A theme toggle switches between dark and light modes via CSS variables.

Optimization Tip: You can adjust MAX_MESSAGE_LENGTH, RATE_LIMIT_BURST, and RATE_LIMIT_WINDOW_SECONDS in main.py to match your traffic needs. The frontend's CONFIG object in app.js also exposes heartbeatIntervalMs and reconnectMaxDelayMs for tuning connection resilience.
