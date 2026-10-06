/**
 * app.js — Nexus Chat client
 *
 * Responsibilities
 * ----------------
 *  1. Own the WebSocket lifecycle (connect, heartbeat, auto-reconnect).
 *  2. Hold all application state in one place.
 *  3. Render safely — every piece of remote data reaches the DOM through
 *     `textContent` or an explicitly constructed element, never `innerHTML`.
 *  4. Keep the message log pinned to the bottom when the user is at the bottom.
 *
 * No frameworks, no dependencies. ES2020+, strict mode.
 */

"use strict";

/* ==================================================================== */
/* 1. Configuration                                                     */
/* ==================================================================== */

const CONFIG = Object.freeze({
  // WebSocket endpoint is derived from the page origin so the app works
  // behind any host/port/proxy without code changes.
  wsPath: "/ws",

  /** Milliseconds between client-initiated pings. */
  heartbeatIntervalMs: 25000,
  /** If no server frame arrives within this window, force a reconnect. */
  heartbeatTimeoutMs: 45000,

  /** Exponential backoff bounds for reconnection attempts. */
  reconnectBaseDelayMs: 800,
  reconnectMaxDelayMs: 20000,
  /** Randomised jitter (0–1) applied to each backoff delay. */
  reconnectJitter: 0.3,
  /** Maximum number of consecutive reconnect attempts before giving up. */
  reconnectMaxAttempts: 12,

  /** Client-side mirror of the server limits. */
  maxMessageLength: 2000,
  maxUsernameLength: 24,
  minUsernameLength: 2,

  /** Auto-grow ceiling for the composer textarea (px). */
  composerMaxHeight: 180,

  /** How many rendered messages to keep in the DOM before pruning. */
  maxRenderedMessages: 400,

  /** Duration (ms) a "user is typing" signal stays visible without renewal. */
  typingIndicatorTtlMs: 3000,
  /** Throttle for outbound typing notifications. */
  typingSendThrottleMs: 1500,

  /** localStorage keys. */
  storageKeys: {
    theme: "nexus.theme",
    username: "nexus.username",
  },
});

/* ==================================================================== */
/* 2. DOM references                                                    */
/* ==================================================================== */

const $ = (id) => document.getElementById(id);

const dom = {
  // Banner
  banner: $("connection-banner"),
  bannerText: $("connection-banner-text"),

  // Username gate
  overlay: $("username-overlay"),
  usernameForm: $("username-form"),
  usernameInput: $("username-input"),
  usernameSubmit: $("username-submit"),
  usernameError: $("username-error"),

  // Shell
  app: $("app"),
  themeToggle: $("theme-toggle"),

  // Sidebar
  sidebar: $("sidebar"),
  sidebarToggle: $("sidebar-toggle"),
  sidebarClose: $("sidebar-close"),
  sidebarBackdrop: $("sidebar-backdrop"),
  sidebarCount: $("sidebar-count"),
  userList: $("user-list"),

  // Chat
  messageLog: $("message-log"),
  messageList: $("message-list"),
  presenceCount: $("presence-count"),

  // Scroll-to-bottom
  scrollBottom: $("scroll-bottom"),
  scrollBottomBadge: $("scroll-bottom-badge"),

  // Typing
  typingIndicator: $("typing-indicator"),
  typingText: $("typing-indicator-text"),

  // Composer
  composer: $("composer"),
  messageInput: $("message-input"),
  sendButton: $("send-button"),
  charCounter: $("char-counter"),
};

/* ==================================================================== */
/* 3. Application state                                                 */
/* ==================================================================== */

const state = {
  /** @type {WebSocket|null} */
  socket: null,
  /** "idle" | "connecting" | "open" | "reconnecting" | "closed" */
  status: "idle",
  /** @type {string} */
  username: "",
  /** @type {Set<string>} */
  users: new Set(),
  /** @type {string|null} ISO timestamp of the last rendered message (for day dividers). */
  lastMessageDay: null,
  /** @type {string|null} Username of the last rendered message (for grouping). */
  lastMessageAuthor: null,
  /** @type {boolean} Whether the user is at (or near) the bottom of the log. */
  pinnedToBottom: true,
  /** @type {number} Unread messages accrued while scrolled up. */
  unreadCount: 0,
  /** @type {number} Current reconnect attempt counter. */
  reconnectAttempts: 0,
  /** @type {number|null} */
  reconnectTimer: null,
  /** @type {number|null} */
  heartbeatTimer: null,
  /** @type {number|null} */
  heartbeatWatchdog: null,
  /** @type {number} Last time a pong/frame was received (ms epoch). */
  lastServerFrameAt: 0,
  /** @type {number} Last time we sent a typing notification (ms epoch). */
  lastTypingSentAt: 0,
  /** @type {Map<string, number>} username -> expiry timestamp for typing indicators. */
  typingUsers: new Map(),
  /** @type {number|null} */
  typingSweepTimer: null,
  /** @type {boolean} Set once the user explicitly leaves; suppresses reconnection. */
  intentionalClose: false,
};

/* ==================================================================== */
/* 4. Small utilities                                                   */
/* ==================================================================== */

/** Clamp a number to [min, max]. */
const clamp = (value, min, max) => Math.min(Math.max(value, min), max);

/** Promise-based delay. */
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Validate a display name exactly as the server does.
 * @returns {{ ok: true, value: string } | { ok: false, reason: string }}
 */
function validateUsername(raw) {
  const candidate = String(raw ?? "").replace(/\s+/g, " ").trim();

  if (!candidate) {
    return { ok: false, reason: "Please enter a display name." };
  }
  if (candidate.length < CONFIG.minUsernameLength) {
    return { ok: false, reason: `At least ${CONFIG.minUsernameLength} characters required.` };
  }
  if (candidate.length > CONFIG.maxUsernameLength) {
    return { ok: false, reason: `At most ${CONFIG.maxUsernameLength} characters allowed.` };
  }
  if (!/^[A-Za-z0-9][A-Za-z0-9 ._-]*$/.test(candidate)) {
    return {
      ok: false,
      reason: "Use letters, digits, spaces, '.', '_' or '-' only.",
    };
  }
  return { ok: true, value: candidate };
}

/**
 * Format an ISO timestamp as a short local time (HH:MM).
 * Falls back gracefully when the input is unparseable.
 */
function formatTime(iso) {
  const date = iso ? new Date(iso) : new Date();
  if (Number.isNaN(date.getTime())) return "";
  return date.toLocaleTimeString(undefined, { hour: "2-digit", minute: "2-digit" });
}

/**
 * Format an ISO timestamp as a day label ("Today", "Yesterday", or a date).
 */
function formatDayLabel(iso) {
  const date = iso ? new Date(iso) : new Date();
  if (Number.isNaN(date.getTime())) return "";

  const startOfDay = (d) => new Date(d.getFullYear(), d.getMonth(), d.getDate()).getTime();
  const today = startOfDay(new Date());
  const target = startOfDay(date);
  const dayMs = 86400000;

  if (target === today) return "Today";
  if (target === today - dayMs) return "Yesterday";

  const sameYear = date.getFullYear() === new Date().getFullYear();
  return date.toLocaleDateString(undefined, {
    weekday: "short",
    month: "short",
    day: "numeric",
    year: sameYear ? undefined : "numeric",
  });
}

/** Stable per-name hue in [0, 360). */
function hashHue(str) {
  let hash = 0;
  for (let i = 0; i < str.length; i += 1) {
    hash = (hash * 31 + str.charCodeAt(i)) >>> 0;
  }
  return hash % 360;
}

/** Deterministic avatar gradient for a display name. */
function avatarStyle(name) {
  const hue = hashHue(name);
  return `linear-gradient(135deg, hsl(${hue} 70% 55%), hsl(${(hue + 42) % 360} 70% 45%))`;
}

/** First grapheme-ish character of a name, uppercased. */
function initialOf(name) {
  const trimmed = String(name ?? "").trim();
  if (!trimmed) return "?";
  return [...trimmed][0].toUpperCase();
}

/* ==================================================================== */
/* 5. Theme management                                                  */
/* ==================================================================== */

const ThemeManager = {
  init() {
    let stored = null;
    try {
      stored = localStorage.getItem(CONFIG.storageKeys.theme);
    } catch {
      /* Storage may be unavailable (private mode); fall back to system. */
    }

    const prefersLight =
      window.matchMedia?.("(prefers-color-scheme: light)").matches ?? false;

    this.apply(stored === "light" || stored === "dark"
      ? stored
      : prefersLight ? "light" : "dark");

    dom.themeToggle.addEventListener("click", () => {
      const next =
        document.documentElement.getAttribute("data-theme") === "dark" ? "light" : "dark";
      this.apply(next);
      try {
        localStorage.setItem(CONFIG.storageKeys.theme, next);
      } catch { /* non-fatal */ }
    });
  },

  apply(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    document.documentElement.style.colorScheme = theme;
  },
};

/* ==================================================================== */
/* 6. Message rendering                                                 */
/* ==================================================================== */

const Renderer = {
  /** Cache of rendered message nodes for pruning. */
  _nodes: [],

  /** Clear the log and reset grouping/day state. */
  reset() {
    dom.messageList.replaceChildren();
    this._nodes = [];
    state.lastMessageDay = null;
    state.lastMessageAuthor = null;
  },

  /**
   * Append a normal chat message.
   * @param {{user: string, message: string, timestamp: string}} data
   * @param {boolean} outgoing — true when the local user sent it.
   */
  appendMessage(data, outgoing) {
    const iso = data.timestamp || new Date().toISOString();
    this._maybeAppendDayDivider(iso);

    const author = String(data.user ?? "unknown");
    const grouped = !outgoing && state.lastMessageAuthor === author;

    // --- Row ---------------------------------------------------------
    const row = document.createElement("div");
    row.className = `message message--${outgoing ? "out" : "in"}`;
    if (grouped) row.classList.add("message--grouped");

    // --- Avatar ------------------------------------------------------
    const avatar = document.createElement("div");
    avatar.className = "message__avatar";
    avatar.style.background = avatarStyle(author);
    avatar.setAttribute("aria-hidden", "true");
    avatar.textContent = initialOf(author);

    // --- Content -----------------------------------------------------
    const content = document.createElement("div");
    content.className = "message__content";

    const meta = document.createElement("div");
    meta.className = "message__meta";

    const authorEl = document.createElement("span");
    authorEl.className = "message__author";
    authorEl.textContent = outgoing ? "You" : author;

    const timeEl = document.createElement("time");
    timeEl.className = "message__time";
    timeEl.dateTime = iso;
    timeEl.textContent = formatTime(iso);

    meta.append(authorEl, timeEl);

    const bubble = document.createElement("div");
    bubble.className = "message__bubble";
    // textContent — never innerHTML — so markup in a message is inert.
    bubble.textContent = String(data.message ?? "");

    content.append(meta, bubble);
    row.append(avatar, content);

    this._commit(row);
    state.lastMessageAuthor = outgoing ? null : author;
  },

  /**
   * Append a server/system informational line.
   * @param {string} text
   * @param {string} [iso]
   */
  appendSystem(text, iso) {
    const el = document.createElement("div");
    el.className = "system-message";
    el.textContent = String(text ?? "");

    this._commit(el);
    // System lines break the grouping run.
    state.lastMessageAuthor = null;
  },

  /** Insert a day divider when the calendar day changes. */
  _maybeAppendDayDivider(iso) {
    const dayKey = formatDayLabel(iso);
    if (!dayKey || dayKey === state.lastMessageDay) return;

    const divider = document.createElement("div");
    divider.className = "day-divider";
    divider.textContent = dayKey;

    dom.messageList.append(divider);
    state.lastMessageDay = dayKey;
  },

  /** Attach a node, track it, and prune the backlog. */
  _commit(node) {
    dom.messageList.append(node);
    this._nodes.push(node);

    while (this._nodes.length > CONFIG.maxRenderedMessages) {
      const oldest = this._nodes.shift();
      oldest?.remove();
    }
  },
};

/* ==================================================================== */
/* 7. Scrolling behaviour                                               */
/* ==================================================================== */

const Scroller = {
  /** Distance from the bottom (px) still considered "at the bottom". */
  THRESHOLD: 80,

  init() {
    dom.messageLog.addEventListener("scroll", () => this._onScroll(), { passive: true });

    dom.scrollBottom.addEventListener("click", () => {
      this.scrollToBottom(true);
      this.clearUnread();
    });
  },

  /** True when the viewport is within THRESHOLD of the bottom. */
  isAtBottom() {
    const { scrollTop, scrollHeight, clientHeight } = dom.messageLog;
    return scrollHeight - scrollTop - clientHeight <= this.THRESHOLD;
  },

  /**
   * Pin the log to the bottom.
   * @param {boolean} smooth — animate the scroll.
   */
  scrollToBottom(smooth = false) {
    dom.messageLog.scrollTo({
      top: dom.messageLog.scrollHeight,
      behavior: smooth ? "smooth" : "auto",
    });
    state.pinnedToBottom = true;
  },

  /**
   * Called after appending a message. Auto-scrolls when pinned; otherwise
   * surfaces the unread counter.
   */
  afterAppend() {
    if (state.pinnedToBottom) {
      this.scrollToBottom(false);
      this.clearUnread();
    } else {
      state.unreadCount += 1;
      this.showUnread(state.unreadCount);
    }
  },

  _onScroll() {
    state.pinnedToBottom = this.isAtBottom();
    if (state.pinnedToBottom) {
      this.clearUnread();
    } else if (state.unreadCount === 0) {
      dom.scrollBottom.hidden = false;
    }
  },

  showUnread(count) {
    dom.scrollBottom.hidden = false;
    dom.scrollBottomBadge.hidden = count <= 0;
    dom.scrollBottomBadge.textContent = count > 99 ? "99+" : String(count);
  },

  clearUnread() {
    state.unreadCount = 0;
    dom.scrollBottomBadge.hidden = true;
    dom.scrollBottom.hidden = true;
  },
};

/* ==================================================================== */
/* 8. Connection banner                                                 */
/* ==================================================================== */

const Banner = {
  show(text, isError = false) {
    dom.bannerText.textContent = text;
    dom.banner.classList.toggle("connection-banner--error", isError);
    dom.banner.hidden = false;
  },
  hide() {
    dom.banner.hidden = true;
  },
};

/* ==================================================================== */
/* 9. Sidebar & presence rendering                                      */
/* ==================================================================== */

const Presence = {
  init() {
    dom.sidebarToggle.addEventListener("click", () => this.toggleSidebar());
    dom.sidebarClose.addEventListener("click", () => this.closeSidebar());
    dom.sidebarBackdrop.addEventListener("click", () => this.closeSidebar());

    document.addEventListener("keydown", (event) => {
      if (event.key === "Escape") this.closeSidebar();
    });
  },

  toggleSidebar() {
    if (dom.sidebar.classList.contains("sidebar--open")) {
      this.closeSidebar();
    } else {
      this.openSidebar();
    }
  },

  openSidebar() {
    dom.sidebar.classList.add("sidebar--open");
    dom.sidebarBackdrop.hidden = false;
    dom.sidebarToggle.setAttribute("aria-expanded", "true");
  },

  closeSidebar() {
    dom.sidebar.classList.remove("sidebar--open");
    dom.sidebarBackdrop.hidden = true;
    dom.sidebarToggle.setAttribute("aria-expanded", "false");
  },

  /**
   * Replace the participant list and update every count badge.
   * @param {string[]} users
   */
  render(users) {
    const roster = Array.isArray(users) ? users.map(String) : [];
    state.users = new Set(roster);

    const count = roster.length;
    dom.sidebarCount.textContent = String(count);
    dom.presenceCount.textContent = `${count} online`;

    dom.userList.replaceChildren();

    if (count === 0) {
      const empty = document.createElement("li");
      empty.className = "user-list__empty";
      empty.textContent = "No one else is here yet.";
      dom.userList.append(empty);
      return;
    }

    const fragment = document.createDocumentFragment();

    for (const name of roster) {
      const item = document.createElement("li");
      item.className = "user-list__item";

      const avatar = document.createElement("span");
      avatar.className = "user-list__avatar";
      avatar.style.background = avatarStyle(name);
      avatar.setAttribute("aria-hidden", "true");
      avatar.textContent = initialOf(name);

      const label = document.createElement("span");
      label.className = "user-list__name";
      label.textContent = name;

      item.append(avatar, label);

      if (name.toLowerCase() === state.username.toLowerCase()) {
        const you = document.createElement("span");
        you.className = "user-list__you";
        you.textContent = "You";
        item.append(you);
      }

      fragment.append(item);
    }

    dom.userList.append(fragment);
  },
};

/* ==================================================================== */
/* 10. Typing indicator                                                 */
/* ==================================================================== */

const Typing = {
  init() {
    // Sweep expired entries four times a second.
    state.typingSweepTimer = window.setInterval(() => this.render(), 400);
  },

  /** Record that `user` is typing right now. */
  note(user) {
    const name = String(user ?? "");
    if (!name || name.toLowerCase() === state.username.toLowerCase()) return;
    state.typingUsers.set(name, Date.now() + CONFIG.typingIndicatorTtlMs);
    this.render();
  },

  /** Drop a user's typing flag immediately (e.g. they just sent a message). */
  clear(user) {
    const name = String(user ?? "");
    if (state.typingUsers.delete(name)) this.render();
  },

  /** Notify the server that *we* are typing, throttled. */
  sendSignal() {
    const now = Date.now();
    if (now - state.lastTypingSentAt < CONFIG.typingSendThrottleMs) return;
    state.lastTypingSentAt = now;
    Socket.send({ type: "typing" });
  },

  render() {
    const now = Date.now();
    for (const [name, expiry] of state.typingUsers) {
      if (expiry <= now) state.typingUsers.delete(name);
    }

    const names = [...state.typingUsers.keys()];

    if (names.length === 0) {
      dom.typingIndicator.hidden = true;
      dom.typingText.textContent = "";
      return;
    }

    let label;
    if (names.length === 1) {
      label = `${names[0]} is typing…`;
    } else if (names.length === 2) {
      label = `${names[0]} and ${names[1]} are typing…`;
    } else {
      label = `${names.length} people are typing…`;
    }

    dom.typingText.textContent = label;
    dom.typingIndicator.hidden = false;
  },

  reset() {
    state.typingUsers.clear();
    this.render();
  },
};

/* ==================================================================== */
/* 11. WebSocket client                                                 */
/* ==================================================================== */

const Socket = {
  /** Build the absolute ws:// or wss:// URL for this page. */
  _url() {
    const scheme = window.location.protocol === "https:" ? "wss:" : "ws:";
    const query = new URLSearchParams({ username: state.username });
    return `${scheme}//${window.location.host}${CONFIG.wsPath}?${query}`;
  },

  /** Open a connection. Safe to call repeatedly. */
  connect() {
    if (state.socket && (state.socket.readyState === WebSocket.OPEN ||
                         state.socket.readyState === WebSocket.CONNECTING)) {
      return; // already connected or mid-handshake
    }

    this._clearTimers();
    state.intentionalClose = false;
    state.status = state.reconnectAttempts > 0 ? "reconnecting" : "connecting";

    if (state.status === "reconnecting") {
      Banner.show(`Reconnecting… (attempt ${state.reconnectAttempts})`);
    } else {
      Banner.show("Connecting…");
    }

    let socket;
    try {
      socket = new WebSocket(this._url());
    } catch (error) {
      console.error("[socket] construction failed:", error);
      this._scheduleReconnect();
      return;
    }

    state.socket = socket;

    socket.addEventListener("open", () => this._onOpen());
    socket.addEventListener("message", (event) => this._onMessage(event));
    socket.addEventListener("error", () => this._onError());
    socket.addEventListener("close", (event) => this._onClose(event));
  },

  /** Send a JSON packet if the socket is open. Returns success. */
  send(payload) {
    const socket = state.socket;
    if (!socket || socket.readyState !== WebSocket.OPEN) return false;
    try {
      socket.send(JSON.stringify(payload));
      return true;
    } catch (error) {
      console.error("[socket] send failed:", error);
      return false;
    }
  },

  /** Close deliberately and stop reconnecting. */
  close(reason = "client closed") {
    state.intentionalClose = true;
    this._clearTimers();
    if (state.socket) {
      try {
        state.socket.close(1000, reason);
      } catch { /* already closing */ }
    }
    state.socket = null;
    state.status = "closed";
  },

  /* ---------------------------------------------------------------- */
  /* Event handlers                                                    */
  /* ---------------------------------------------------------------- */

  _onOpen() {
    console.info("[socket] connected");
    state.status = "open";
    state.reconnectAttempts = 0;
    state.lastServerFrameAt = Date.now();
    Banner.hide();

    this._startHeartbeat();
    UI.setComposerEnabled(true);
    dom.messageInput.focus();
  },

  _onMessage(event) {
    state.lastServerFrameAt = Date.now();

    let packet;
    try {
      packet = JSON.parse(event.data);
    } catch {
      console.warn("[socket] dropped non-JSON frame");
      return;
    }

    if (!packet || typeof packet !== "object") return;

    switch (packet.type) {
      case "welcome":
        this._handleWelcome(packet);
        break;

      case "message":
        Typing.clear(packet.user);
        Renderer.appendMessage(packet, false);
        Scroller.afterAppend();
        break;

      case "presence":
        Presence.render(packet.users);
        break;

      case "system":
        Renderer.appendSystem(packet.message, packet.timestamp);
        Scroller.afterAppend();
        break;

      case "typing":
        Typing.note(packet.user);
        break;

      case "error":
        Renderer.appendSystem(`⚠ ${packet.message}`, packet.timestamp);
        Scroller.afterAppend();
        break;

      case "pong":
        // Liveness confirmed by lastServerFrameAt above.
        break;

      default:
        console.debug("[socket] unhandled packet type:", packet.type);
    }
  },

  _handleWelcome(packet) {
    if (Array.isArray(packet.users)) Presence.render(packet.users);

    if (typeof packet.max_message_length === "number") {
      // Trust the server's limit if it is stricter than our own.
      CONFIG.maxMessageLength = packet.max_message_length;
      dom.messageInput.maxLength = CONFIG.maxMessageLength;
    }

    Renderer.appendSystem(packet.message || "Connected.", packet.timestamp);
    Scroller.scrollToBottom(false);
  },

  _onError() {
    // The 'close' event always follows; reconnection is handled there.
    console.warn("[socket] transport error");
  },

  _onClose(event) {
    this._clearTimers();
    UI.setComposerEnabled(false);
    Typing.reset();

    if (state.intentionalClose) {
      state.status = "closed";
      return;
    }

    // 4000 = session replaced by a newer connection with the same name.
    if (event.code === 4000) {
      state.intentionalClose = true;
      Banner.show("This session was replaced by a newer connection.", true);
      UI.returnToGate("Your session was replaced. Please re-enter.");
      return;
    }

    // 4001 = server rejected the username.
    if (event.code === 4001) {
      state.intentionalClose = true;
      Banner.hide();
      UI.returnToGate("That display name was rejected. Please try another.");
      return;
    }

    // 1000 with a clean reason and no intent = server asked us to stop.
    if (event.code === 1000 && !event.wasClean) {
      console.warn("[socket] abnormal 1000 close");
    }

    console.warn(`[socket] closed (code=${event.code}) — will reconnect`);
    this._scheduleReconnect();
  },

  /* ---------------------------------------------------------------- */
  /* Heartbeat                                                         */
  /* ---------------------------------------------------------------- */

  _startHeartbeat() {
    this._clearHeartbeat();

    state.heartbeatTimer = window.setInterval(() => {
      if (!this.send({ type: "ping" })) return;

      // If the server has gone quiet for too long, force a reconnect.
      if (Date.now() - state.lastServerFrameAt > CONFIG.heartbeatTimeoutMs) {
        console.warn("[socket] heartbeat timeout");
        try {
          state.socket?.close(4000, "heartbeat timeout");
        } catch { /* ignore */ }
      }
    }, CONFIG.heartbeatIntervalMs);
  },

  _clearHeartbeat() {
    if (state.heartbeatTimer !== null) {
      clearInterval(state.heartbeatTimer);
      state.heartbeatTimer = null;
    }
    if (state.heartbeatWatchdog !== null) {
      clearTimeout(state.heartbeatWatchdog);
      state.heartbeatWatchdog = null;
    }
  },

  _clearTimers() {
    this._clearHeartbeat();
    if (state.reconnectTimer !== null) {
      clearTimeout(state.reconnectTimer);
      state.reconnectTimer = null;
    }
  },

  /* ---------------------------------------------------------------- */
  /* Reconnection                                                      */
  /* ---------------------------------------------------------------- */

  _scheduleReconnect() {
    if (state.intentionalClose) return;

    if (state.reconnectAttempts >= CONFIG.reconnectMaxAttempts) {
      state.status = "closed";
      Banner.show("Unable to reach the server. Reload the page to retry.", true);
      return;
    }

    state.reconnectAttempts += 1;
    state.status = "reconnecting";

    // Exponential backoff with jitter.
    const base = Math.min(
      CONFIG.reconnectBaseDelayMs * 2 ** (state.reconnectAttempts - 1),
      CONFIG.reconnectMaxDelayMs,
    );
    const jitter = base * CONFIG.reconnectJitter * Math.random();
    const delay = Math.round(base + jitter);

    Banner.show(
      `Connection lost. Retrying in ${(delay / 1000).toFixed(1)}s ` +
      `(attempt ${state.reconnectAttempts}/${CONFIG.reconnectMaxAttempts})…`
    );

    state.reconnectTimer = window.setTimeout(() => {
      state.reconnectTimer = null;
      this.connect();
    }, delay);
  },
};

/* ==================================================================== */
/* 12. UI controller (gate, composer, send)                             */
/* ==================================================================== */

const UI = {
  init() {
    this._bindUsernameGate();
    this._bindComposer();

    // Restore the last used name (but still require an explicit click).
    try {
      const saved = localStorage.getItem(CONFIG.storageKeys.username);
      if (saved) dom.usernameInput.value = saved;
    } catch { /* ignore */ }

    this._validateUsernameField();
    dom.usernameInput.focus();
  },

  /* ---------------------------------------------------------------- */
  /* Username gate                                                     */
  /* ---------------------------------------------------------------- */

  _bindUsernameGate() {
    dom.usernameInput.addEventListener("input", () => this._validateUsernameField());

    dom.usernameForm.addEventListener("submit", (event) => {
      event.preventDefault();
      this._submitUsername();
    });
  },

  _validateUsernameField() {
    const result = validateUsername(dom.usernameInput.value);
    dom.usernameSubmit.disabled = !result.ok;
    dom.usernameError.textContent = result.ok ? "" : result.reason;
    return result;
  },

  _submitUsername() {
    const result = this._validateUsernameField();
    if (!result.ok) {
      dom.usernameInput.focus();
      return;
    }

    state.username = result.value;

    try {
      localStorage.setItem(CONFIG.storageKeys.username, state.username);
    } catch { /* non-fatal */ }

    // Swap the gate for the app shell.
    dom.overlay.hidden = true;
    dom.app.hidden = false;

    Renderer.reset();
    Presence.render([]);
    Scroller.clearUnread();
    state.pinnedToBottom = true;

    Socket.connect();

    // Focus the composer once the socket reports open (see Socket._onOpen).
    dom.messageInput.focus();
  },

  /** Tear the chat down and return to the username gate. */
  returnToGate(message) {
    Socket.close("returning to gate");

    dom.app.hidden = true;
    dom.overlay.hidden = false;

    Renderer.reset();
    Presence.reset?.();
    Presence.render([]);
    Typing.reset();

    dom.usernameError.textContent = message ?? "";
    dom.usernameInput.focus();
    dom.usernameInput.select();
  },

  /* ---------------------------------------------------------------- */
  /* Composer                                                          */
  /* ---------------------------------------------------------------- */

  _bindComposer() {
    // Auto-grow the textarea up to the configured ceiling.
    dom.messageInput.addEventListener("input", () => {
      this._autoGrow();
      this._updateCounter();

      const hasText = dom.messageInput.value.trim().length > 0;
      dom.sendButton.disabled = !hasText || !Socket.isOpen?.();

      if (hasText) Typing.sendSignal();
    });

    // Enter sends; Shift+Enter inserts a newline.
    dom.messageInput.addEventListener("keydown", (event) => {
      if (event.key === "Enter" && !event.shiftKey && !event.isComposing) {
        event.preventDefault();
        this._send();
      }
    });

    dom.composer.addEventListener("submit", (event) => {
      event.preventDefault();
      this._send();
    });
  },

  _autoGrow() {
    const el = dom.messageInput;
    el.style.height = "auto";
    el.style.height = `${clamp(el.scrollHeight, 44, CONFIG.composerMaxHeight)}px`;
  },

  _updateCounter() {
    const used = dom.messageInput.value.length;
    const max = CONFIG.maxMessageLength;
    dom.charCounter.textContent = `${used} / ${max}`;

    dom.charCounter.classList.toggle("char-counter--warn", used > max * 0.8 && used <= max);
    dom.charCounter.classList.toggle("char-counter--danger", used > max);
  },

  _send() {
    const raw = dom.messageInput.value;
    const text = raw.trim();

    if (!text) return;
    if (text.length > CONFIG.maxMessageLength) {
      Renderer.appendSystem(`⚠ Message exceeds ${CONFIG.maxMessageLength} characters.`);
      Scroller.afterAppend();
      return;
    }

    const packet = {
      type: "message",
      message: text,
      // 'user' is sent for wire-schema compatibility; the server ignores it
      // and stamps the authoritative identity from the session.
      user: state.username,
    };

    const delivered = Socket.send(packet);

    if (delivered) {
      Renderer.appendMessage(
        { user: state.username, message: text, timestamp: new Date().toISOString() },
        true,
      );
      Scroller.afterAppend();
    } else {
      Renderer.appendSystem("⚠ Not connected — your message was not sent.");
      Scroller.afterAppend();
    }

    // Reset the composer.
    dom.messageInput.value = "";
    dom.sendButton.disabled = true;
    this._autoGrow();
    this._updateCounter();
    dom.messageInput.focus();
  },

  /** Enable or disable the composer based on socket state. */
  setComposerEnabled(enabled) {
    dom.messageInput.disabled = !enabled;
    dom.sendButton.disabled = !enabled || dom.messageInput.value.trim().length === 0;
    dom.messageInput.placeholder = enabled
      ? "Type a message…"
      : "Reconnecting…";
  },
};

/* ==================================================================== */
/* 13. Boot                                                             */
/* ==================================================================== */

document.addEventListener("DOMContentLoaded", () => {
  ThemeManager.init();
  Scroller.init();
  Presence.init();
  Typing.init();
  UI.init();

  // Graceful teardown: tell the server we are leaving.
  window.addEventListener("pagehide", () => Socket.close("page unload"));
  window.addEventListener("beforeunload", () => Socket.close("page unload"));

  // If the tab regains focus and the socket is dead, reconnect immediately.
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState !== "visible") return;
    if (state.intentionalClose) return;
    if (!state.socket || state.socket.readyState === WebSocket.CLOSED) {
      state.reconnectAttempts = 0;
      Socket.connect();
    }
  });

  // Expose a minimal debug surface without polluting the global namespace.
  Object.defineProperty(window, "__nexus", {
    value: Object.freeze({
      state,
      config: CONFIG,
      reconnect: () => {
        state.reconnectAttempts = 0;
        Socket.connect();
      },
    }),
    writable: false,
    configurable: false,
  });
});
