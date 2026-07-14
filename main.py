import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import time
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

try:
    from baleclient import Client, Dispatcher
    from baleclient.enums import ChatType
    from baleclient.types import Message

    # Enable BaleClient internal logging

    # Monkey-patch: fix BaleClient bug where string annotations
    # (from __future__ import annotations) cause AttributeError on __name__
    import inspect as _inspect
    from functools import partial as _partial
    from baleclient.dispatcher.event.handler import CallableObject as _CO
    _orig_call = _CO.call
    async def _patched_call(self, *args, **kwargs):
        callback = _inspect.unwrap(self.callback)
        sig = _inspect.signature(callback)
        filtered_kwargs = {}
        for name, param in sig.parameters.items():
            if name in kwargs:
                filtered_kwargs[name] = kwargs[name]
            else:
                ann = param.annotation
                ann_name = getattr(ann, '__name__', None)
                if ann_name is None and isinstance(ann, str):
                    ann_name = ann
                if ann_name == "Client" and "client" in kwargs:
                    filtered_kwargs[name] = kwargs["client"]
        wrapped = _partial(callback, *args, **filtered_kwargs)
        if self.awaitable:
            return await wrapped()
        import contextvars
        loop = asyncio.get_event_loop()
        context = contextvars.copy_context()
        wrapped = _partial(context.run, wrapped)
        return await loop.run_in_executor(None, wrapped)
    _CO.call = _patched_call

    # Monkey-patch: fix BaleClient bug where MessageContent._check_empty
    # forcibly strips text content from messages
    from baleclient.types.message_content import MessageContent as _MC
    from pydantic import model_validator as _mv
    @_mv(mode="before")
    @classmethod
    def _fixed_check_empty(cls, data):
        if isinstance(data, dict) and "5" in data:
            raw_val = data["5"]
            data["5"] = bool(raw_val) if raw_val is not None else True
        return data
    _MC._check_empty = _fixed_check_empty

except ModuleNotFoundError as exc:
    print(
        "Dependency is missing. Run: python -m pip install -r requirements.txt",
        file=sys.stderr,
    )
    raise SystemExit(2) from exc


APP_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = APP_DIR / "config.json"
DEFAULT_SESSION_PATH = APP_DIR / "account_session.bale"
DEFAULT_DB_PATH = APP_DIR / "relay_state.sqlite3"
DEFAULT_LOG_PATH = APP_DIR / "relay.log"

CHAT_TYPE_NAMES = {
    "UNKNOWN": ChatType.UNKNOWN,
    "PRIVATE": ChatType.PRIVATE,
    "GROUP": ChatType.GROUP,
    "CHANNEL": ChatType.CHANNEL,
    "BOT": ChatType.BOT,
    "SUPER_GROUP": ChatType.SUPER_GROUP,
}


class ConfigurationError(ValueError):
    pass


class UnsupportedMessageError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# Mutable runtime state (shared across tasks)
# ---------------------------------------------------------------------------
relay_state: dict[str, Any] = {
    "paused": False,
    "start_time": time.time(),
    "last_message_at": None,
    "message_count_since_summary": 0,
    "last_summary_time": time.time(),
}


@dataclass(frozen=True)
class RelayConfig:
    sources: tuple  # tuple of (chat_id, ChatType) pairs
    target_chat_id: int
    target_chat_type: ChatType
    mode: str
    allowed_sender_id: Optional[int]
    mark_as_read: bool
    copy_fallback_to_forward: bool
    delay_seconds: float
    max_retries: int
    retry_base_seconds: float
    dedupe_max_rows: int

    # --- New optional fields (features 1-10) ---
    # Feature 1: Multiple targets – tuple of (chat_id, ChatType)
    all_targets: tuple = ()
    # Feature 2: Keyword filter / exclude
    keyword_filter: Optional[tuple] = None   # tuple of lowercase keywords
    keyword_exclude: Optional[tuple] = None  # tuple of lowercase keywords
    # Feature 3: Message prefix / suffix
    message_prefix: Optional[str] = None
    message_suffix: Optional[str] = None
    # Feature 4: Admin chat
    admin_chat_id: Optional[int] = None
    admin_chat_type: Optional[ChatType] = None
    # Feature 5: Active hours (start_hour, end_hour) in UTC 0-23
    active_hours: Optional[tuple] = None  # (start, end)
    # Feature 6: stats are tracked in StateStore, no config field needed
    # Feature 7: Health check port
    health_port: Optional[int] = None
    # Feature 8: Silent mode
    silent: bool = False
    # Feature 9: Webhook URL
    webhook_url: Optional[str] = None
    # Feature 10: Log level
    log_level: str = "INFO"

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "RelayConfig":
        try:
            # --- Targets (Feature 1): support "targets" list and single "target" ---
            targets_raw = raw.get("targets")
            if targets_raw is not None and isinstance(targets_raw, list) and len(targets_raw) > 0:
                targets_list = [(int(t["id"]), parse_chat_type(t["type"])) for t in targets_raw]
            else:
                # Backward compat: single "target" key
                target = raw["target"]
                targets_list = [(int(target["id"]), parse_chat_type(target["type"]))]

            if not targets_list:
                raise ConfigurationError("At least one target is required.")

            # Primary target is the first one (keeps old fields working)
            target_id, target_type = targets_list[0]

            mode = str(raw.get("mode", "forward")).strip().lower()
            if mode not in {"forward", "copy"}:
                raise ConfigurationError("mode must be 'forward' or 'copy'.")

            allowed_sender_raw = raw.get("allowed_sender_id")
            allowed_sender_id = (
                None
                if allowed_sender_raw in (None, "", 0, "0")
                else int(allowed_sender_raw)
            )

            # Support both single "source" and multiple "sources"
            sources_raw = raw.get("sources")
            if sources_raw is None:
                # Backward compat: single "source" key
                src = raw["source"]
                sources_list = [(int(src["id"]), parse_chat_type(src["type"]))]
            else:
                sources_list = []
                for src in sources_raw:
                    sources_list.append((int(src["id"]), parse_chat_type(src["type"])))
            if not sources_list:
                raise ConfigurationError("At least one source is required.")

            for sid, stype in sources_list:
                for tid, ttype in targets_list:
                    if sid == tid and int(stype) == int(ttype):
                        raise ConfigurationError(
                            f"Source {sid}/{stype.name} cannot be the same as target {tid}/{ttype.name}."
                        )

            # Feature 2: keyword filter / exclude
            kf_raw = raw.get("keyword_filter")
            keyword_filter = None
            if kf_raw and isinstance(kf_raw, list):
                keyword_filter = tuple(str(k).lower() for k in kf_raw if k)

            ke_raw = raw.get("keyword_exclude")
            keyword_exclude = None
            if ke_raw and isinstance(ke_raw, list):
                keyword_exclude = tuple(str(k).lower() for k in ke_raw if k)

            # Feature 3: prefix / suffix
            message_prefix = raw.get("message_prefix") or None
            message_suffix = raw.get("message_suffix") or None

            # Feature 4: admin chat
            admin_chat_id_raw = raw.get("admin_chat_id")
            admin_chat_id = int(admin_chat_id_raw) if admin_chat_id_raw not in (None, "", 0, "0") else None
            admin_chat_type = None
            if admin_chat_id is not None:
                admin_chat_type = parse_chat_type(raw.get("admin_chat_type", "PRIVATE"))

            # Feature 5: active hours
            ah_raw = raw.get("active_hours")
            active_hours = None
            if ah_raw and isinstance(ah_raw, dict):
                start_h = int(ah_raw.get("start", 0))
                end_h = int(ah_raw.get("end", 24))
                active_hours = (start_h, end_h)

            # Feature 7: health port
            hp_raw = raw.get("health_port")
            health_port = int(hp_raw) if hp_raw not in (None, "", 0) else None

            # Feature 8: silent
            silent = bool(raw.get("silent", False))

            # Feature 9: webhook
            webhook_url = raw.get("webhook_url") or None

            # Feature 10: log level
            log_level = str(raw.get("log_level", "INFO")).upper()
            if log_level not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
                log_level = "INFO"

            config = cls(
                sources=tuple(sources_list),
                target_chat_id=target_id,
                target_chat_type=target_type,
                mode=mode,
                allowed_sender_id=allowed_sender_id,
                mark_as_read=bool(raw.get("mark_as_read", False)),
                copy_fallback_to_forward=bool(
                    raw.get("copy_fallback_to_forward", True)
                ),
                delay_seconds=max(0.0, float(raw.get("delay_seconds", 0.35))),
                max_retries=max(1, int(raw.get("max_retries", 4))),
                retry_base_seconds=max(
                    0.2, float(raw.get("retry_base_seconds", 1.5))
                ),
                dedupe_max_rows=max(1000, int(raw.get("dedupe_max_rows", 20000))),
                # New fields
                all_targets=tuple(targets_list),
                keyword_filter=keyword_filter,
                keyword_exclude=keyword_exclude,
                message_prefix=message_prefix,
                message_suffix=message_suffix,
                admin_chat_id=admin_chat_id,
                admin_chat_type=admin_chat_type,
                active_hours=active_hours,
                health_port=health_port,
                silent=silent,
                webhook_url=webhook_url,
                log_level=log_level,
            )
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, ConfigurationError):
                raise
            raise ConfigurationError(f"Invalid config.json: {exc}") from exc
        return config

    def as_dict(self) -> dict[str, Any]:
        sources_list = [{"id": sid, "type": st.name} for sid, st in self.sources]
        targets_list = [{"id": tid, "type": tt.name} for tid, tt in self.all_targets]
        result: dict[str, Any] = {
            "sources": sources_list,
            "targets": targets_list,
            "target": targets_list[0] if targets_list else None,  # backward compat
            "mode": self.mode,
            "allowed_sender_id": self.allowed_sender_id,
            "mark_as_read": self.mark_as_read,
            "copy_fallback_to_forward": self.copy_fallback_to_forward,
            "delay_seconds": self.delay_seconds,
            "max_retries": self.max_retries,
            "retry_base_seconds": self.retry_base_seconds,
            "dedupe_max_rows": self.dedupe_max_rows,
        }
        # New optional fields – only include when set
        if self.keyword_filter:
            result["keyword_filter"] = list(self.keyword_filter)
        if self.keyword_exclude:
            result["keyword_exclude"] = list(self.keyword_exclude)
        if self.message_prefix:
            result["message_prefix"] = self.message_prefix
        if self.message_suffix:
            result["message_suffix"] = self.message_suffix
        if self.admin_chat_id is not None:
            result["admin_chat_id"] = self.admin_chat_id
            result["admin_chat_type"] = self.admin_chat_type.name if self.admin_chat_type else "PRIVATE"
        if self.active_hours is not None:
            result["active_hours"] = {"start": self.active_hours[0], "end": self.active_hours[1]}
        if self.health_port is not None:
            result["health_port"] = self.health_port
        if self.silent:
            result["silent"] = True
        if self.webhook_url:
            result["webhook_url"] = self.webhook_url
        result["log_level"] = self.log_level
        return result


def parse_chat_type(value: Any) -> ChatType:
    if isinstance(value, ChatType):
        return value
    if isinstance(value, int) or (isinstance(value, str) and value.strip().isdigit()):
        try:
            return ChatType(int(value))
        except ValueError as exc:
            raise ConfigurationError(f"Unknown numeric chat type: {value}") from exc

    normalized = str(value).strip().upper().replace("-", "_").replace(" ", "_")
    if normalized == "SUPERGROUP":
        normalized = "SUPER_GROUP"
    try:
        return CHAT_TYPE_NAMES[normalized]
    except KeyError as exc:
        valid = ", ".join(CHAT_TYPE_NAMES)
        raise ConfigurationError(
            f"Unknown chat type '{value}'. Valid values: {valid}"
        ) from exc


def setup_logging(log_path: Path, log_level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger("bale-relay")
    level = getattr(logging, log_level.upper(), logging.INFO)
    logger.setLevel(level)
    logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Also capture BaleClient internal logs
    bale_logger = logging.getLogger("client")
    bale_logger.setLevel(logging.DEBUG)
    bale_logger.addHandler(console)
    bale_logger.addHandler(file_handler)

    return logger


class StateStore:
    """Persistent de-duplication store, so reconnects do not resend messages."""

    def __init__(self, path: Path) -> None:
        self.connection = sqlite3.connect(path)
        self.connection.execute("PRAGMA journal_mode=WAL")
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS processed_messages (
                chat_id INTEGER NOT NULL,
                chat_type INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                message_date INTEGER NOT NULL,
                processed_at INTEGER NOT NULL,
                PRIMARY KEY (chat_id, chat_type, message_id)
            )
            """
        )
        # Feature 6: Statistics table
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS relay_stats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp INTEGER NOT NULL,
                source_id INTEGER,
                action TEXT NOT NULL,
                hour INTEGER NOT NULL
            )
            """
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_ts ON relay_stats (timestamp)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_stats_action ON relay_stats (action)"
        )
        self.connection.commit()

    def claim(self, message: Message) -> bool:
        cursor = self.connection.execute(
            """
            INSERT OR IGNORE INTO processed_messages
                (chat_id, chat_type, message_id, message_date, processed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                int(message.chat.id),
                int(message.chat.type),
                int(message.message_id),
                int(message.date),
                int(time.time()),
            ),
        )
        self.connection.commit()
        return cursor.rowcount == 1

    def release(self, message: Message) -> None:
        self.connection.execute(
            """
            DELETE FROM processed_messages
            WHERE chat_id = ? AND chat_type = ? AND message_id = ?
            """,
            (int(message.chat.id), int(message.chat.type), int(message.message_id)),
        )
        self.connection.commit()

    def prune(self, max_rows: int) -> None:
        self.connection.execute(
            """
            DELETE FROM processed_messages
            WHERE rowid NOT IN (
                SELECT rowid FROM processed_messages
                ORDER BY processed_at DESC
                LIMIT ?
            )
            """,
            (max_rows,),
        )
        self.connection.commit()

    # --- Feature 6: Statistics methods ---

    def record_relay(self, source_id: int) -> None:
        """Record a successful relay event."""
        now = int(time.time())
        hour = datetime.fromtimestamp(now, tz=timezone.utc).hour
        self.connection.execute(
            "INSERT INTO relay_stats (timestamp, source_id, action, hour) VALUES (?, ?, 'relayed', ?)",
            (now, source_id, hour),
        )
        self.connection.commit()

    def record_error(self, source_id: int = 0) -> None:
        """Record an error event."""
        now = int(time.time())
        hour = datetime.fromtimestamp(now, tz=timezone.utc).hour
        self.connection.execute(
            "INSERT INTO relay_stats (timestamp, source_id, action, hour) VALUES (?, ?, 'error', ?)",
            (now, source_id, hour),
        )
        self.connection.commit()

    def get_stats(self) -> dict[str, Any]:
        """Return aggregate statistics."""
        total = self.connection.execute(
            "SELECT COUNT(*) FROM relay_stats WHERE action='relayed'"
        ).fetchone()[0]
        errors = self.connection.execute(
            "SELECT COUNT(*) FROM relay_stats WHERE action='error'"
        ).fetchone()[0]

        by_source = {}
        for row in self.connection.execute(
            "SELECT source_id, COUNT(*) FROM relay_stats WHERE action='relayed' GROUP BY source_id"
        ):
            by_source[str(row[0])] = row[1]

        by_hour = {}
        for row in self.connection.execute(
            "SELECT hour, COUNT(*) FROM relay_stats WHERE action='relayed' GROUP BY hour ORDER BY hour"
        ):
            by_hour[str(row[0])] = row[1]

        last_ts = self.connection.execute(
            "SELECT MAX(timestamp) FROM relay_stats WHERE action='relayed'"
        ).fetchone()[0]

        return {
            "total_relayed": total,
            "errors": errors,
            "by_source": by_source,
            "by_hour": by_hour,
            "last_relay_at": (
                datetime.fromtimestamp(last_ts, tz=timezone.utc).isoformat()
                if last_ts
                else None
            ),
        }

    def get_total_relayed(self) -> int:
        """Fast total relayed count."""
        return self.connection.execute(
            "SELECT COUNT(*) FROM relay_stats WHERE action='relayed'"
        ).fetchone()[0]

    def close(self) -> None:
        self.connection.close()


def prompt_int(label: str, *, optional: bool = False) -> Optional[int]:
    while True:
        value = input(label).strip()
        if optional and value == "":
            return None
        try:
            return int(value)
        except ValueError:
            print("Please enter a numeric ID.")


def prompt_chat_type(label: str, default: str = "PRIVATE") -> ChatType:
    print("Chat types: 1=PRIVATE, 2=GROUP, 3=CHANNEL, 4=BOT, 5=SUPER_GROUP")
    while True:
        value = input(f"{label} [{default}]: ").strip() or default
        try:
            return parse_chat_type(value)
        except ConfigurationError as exc:
            print(exc)


def prompt_yes_no(label: str, default: bool = False) -> bool:
    suffix = "Y/n" if default else "y/N"
    while True:
        value = input(f"{label} [{suffix}]: ").strip().lower()
        if not value:
            return default
        if value in {"y", "yes", "1", "true"}:
            return True
        if value in {"n", "no", "0", "false"}:
            return False
        print("Enter y or n.")


def run_setup(config_path: Path) -> RelayConfig:
    print("\n=== Bale relay setup ===")
    print("Use --inspect first if you do not know the chat IDs.\n")

    sources_list = []
    while True:
        source_id = prompt_int(f"Source chat ID #{len(sources_list)+1}: ")
        source_type = prompt_chat_type("Source chat type")
        sources_list.append((int(source_id), source_type))
        if prompt_yes_no("Add another source?", False):
            continue
        break

    target_id = prompt_int("Target chat ID: ")
    target_type = prompt_chat_type("Target chat type")

    for sid, stype in sources_list:
        if sid == target_id and int(stype) == int(target_type):
            print(f"Source {sid} is the same as target. Removing.")
            sources_list.remove((sid, stype))
    if not sources_list:
        raise ConfigurationError("No valid sources remaining.")

    while True:
        mode = (input("Transfer mode [forward/copy] (default forward): ").strip().lower() or "forward")
        if mode in {"forward", "copy"}:
            break
        print("Enter forward or copy.")

    allowed_sender = prompt_int(
        "Optional sender user ID (blank = every sender in source chat): ",
        optional=True,
    )
    mark_as_read = prompt_yes_no("Mark source chat as read after success?", False)

    config = RelayConfig(
        sources=tuple(sources_list),
        target_chat_id=int(target_id),
        target_chat_type=target_type,
        mode=mode,
        allowed_sender_id=allowed_sender,
        mark_as_read=mark_as_read,
        copy_fallback_to_forward=True,
        delay_seconds=0.35,
        max_retries=4,
        retry_base_seconds=1.5,
        dedupe_max_rows=20000,
        all_targets=((int(target_id), target_type),),
    )
    config_path.write_text(
        json.dumps(config.as_dict(), ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"Configuration saved to: {config_path}\n")
    return config


def load_config(config_path: Path) -> RelayConfig:
    if not config_path.exists():
        return run_setup(config_path)
    try:
        raw = json.loads(config_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ConfigurationError(f"config.json is not valid JSON: {exc}") from exc
    return RelayConfig.from_dict(raw)


def preview_message(message: Message) -> str:
    if message.text:
        compact = " ".join(message.text.split())
        return f"text={compact[:100]!r}"
    if message.document:
        return f"file={message.document.name!r}, mime={message.document.mime_type!r}"
    if message.gift:
        return "gift"
    if message.replied_to:
        return "forwarded/embedded message"
    return "unsupported/service content"


def get_caption(document: Any) -> Optional[str]:
    caption = getattr(document, "caption", None)
    content = getattr(caption, "content", None) if caption else None
    return content or None


# ---------------------------------------------------------------------------
# Feature 5: Active hours helper
# ---------------------------------------------------------------------------
def is_within_active_hours(config: RelayConfig) -> bool:
    """Return True if current UTC hour is within the configured active window."""
    if config.active_hours is None:
        return True  # No restriction
    start, end = config.active_hours
    current_hour = datetime.now(tz=timezone.utc).hour
    if start <= end:
        return start <= current_hour < end
    else:
        # Wraps midnight, e.g. start=22, end=6
        return current_hour >= start or current_hour < end


# ---------------------------------------------------------------------------
# Feature 2: Keyword filter helpers
# ---------------------------------------------------------------------------
def passes_keyword_filter(text: Optional[str], config: RelayConfig) -> bool:
    """Return True if message text passes keyword include/exclude filters."""
    if text is None:
        # Non-text messages pass if no filter is set; blocked if filter requires keywords
        return config.keyword_filter is None
    lower_text = text.lower()

    # Exclude filter: if any excluded keyword is found, reject
    if config.keyword_exclude:
        for kw in config.keyword_exclude:
            if kw in lower_text:
                return False

    # Include filter: at least one keyword must be present
    if config.keyword_filter:
        for kw in config.keyword_filter:
            if kw in lower_text:
                return True
        return False  # None of the required keywords found

    return True


# ---------------------------------------------------------------------------
# Feature 9: Webhook helper
# ---------------------------------------------------------------------------
async def send_webhook(
    webhook_url: str,
    message: Message,
    action: str,
    target_id: int,
    logger: logging.Logger,
) -> None:
    """POST message info to the configured webhook URL."""
    try:
        import aiohttp
        payload = {
            "chat_id": int(message.chat.id),
            "chat_type": getattr(message.chat.type, "name", str(message.chat.type)),
            "sender_id": message.sender_id,
            "message_id": message.message_id,
            "text": message.text,
            "timestamp": message.date,
            "action": action,
            "target_id": target_id,
            "relayed_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status >= 400:
                    logger.warning("Webhook returned HTTP %s", resp.status)
    except Exception as exc:
        logger.warning("Webhook failed: %s", exc)


# ---------------------------------------------------------------------------
# Feature 4: Admin notification helper
# ---------------------------------------------------------------------------
async def send_admin_message(
    client: Client,
    config: RelayConfig,
    text: str,
    logger: logging.Logger,
) -> None:
    """Send a text message to the admin chat, if configured."""
    if config.admin_chat_id is None or config.admin_chat_type is None:
        return
    try:
        await client.send_message(
            text=text,
            chat_id=config.admin_chat_id,
            chat_type=config.admin_chat_type,
        )
    except Exception as exc:
        logger.warning("Failed to send admin notification: %s", exc)


def format_stats_for_admin(store: StateStore, config: RelayConfig) -> str:
    """Build a human-readable stats summary for the admin."""
    stats = store.get_stats()
    uptime_s = int(time.time() - relay_state["start_time"])
    hours, remainder = divmod(uptime_s, 3600)
    minutes, seconds = divmod(remainder, 60)

    sources_desc = ", ".join(f"{sid}/{st.name}" for sid, st in config.sources)
    targets_desc = ", ".join(f"{tid}/{tt.name}" for tid, tt in config.all_targets)
    paused = "⏸ PAUSED" if relay_state["paused"] else "▶ RUNNING"

    lines = [
        f"📊 Relay Stats {paused}",
        f"⏱ Uptime: {hours}h {minutes}m {seconds}s",
        f"📨 Total relayed: {stats['total_relayed']}",
        f"❌ Errors: {stats['errors']}",
        f"📡 Sources: {sources_desc}",
        f"🎯 Targets: {targets_desc}",
        f"🔧 Mode: {config.mode}",
    ]
    if stats.get("last_relay_at"):
        lines.append(f"🕐 Last relay: {stats['last_relay_at']}")
    if stats.get("by_source"):
        lines.append("── By source ──")
        for sid, count in stats["by_source"].items():
            lines.append(f"  {sid}: {count}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Feature 4: Admin command handler
# ---------------------------------------------------------------------------
async def handle_admin_command(
    message: Message,
    client: Client,
    config: RelayConfig,
    store: StateStore,
    logger: logging.Logger,
) -> bool:
    """
    Handle admin commands. Returns True if the message was a command
    (and should NOT be relayed), False otherwise.
    """
    if config.admin_chat_id is None:
        return False

    # Only handle messages from the admin chat
    msg_chat_id = int(message.chat.id)
    if msg_chat_id != config.admin_chat_id:
        return False

    text = (message.text or "").strip()
    if not text.startswith("/"):
        return False

    command = text.split()[0].lower()

    if command == "/stats":
        summary = format_stats_for_admin(store, config)
        await send_admin_message(client, config, summary, logger)

    elif command == "/pause":
        relay_state["paused"] = True
        await send_admin_message(client, config, "⏸ Relay PAUSED.", logger)

    elif command == "/resume":
        relay_state["paused"] = False
        await send_admin_message(client, config, "▶ Relay RESUMED.", logger)

    elif command == "/sources":
        lines = ["📡 Configured sources:"]
        for sid, stype in config.sources:
            lines.append(f"  • {sid} ({stype.name})")
        lines.append("\n🎯 Configured targets:")
        for tid, ttype in config.all_targets:
            lines.append(f"  • {tid} ({ttype.name})")
        await send_admin_message(client, config, "\n".join(lines), logger)

    else:
        await send_admin_message(
            client, config,
            "Unknown command. Available: /stats, /pause, /resume, /sources",
            logger,
        )

    return True


# ---------------------------------------------------------------------------
# Feature 7: Health check HTTP server
# ---------------------------------------------------------------------------
async def start_health_server(
    port: int,
    store: StateStore,
    config: RelayConfig,
    logger: logging.Logger,
) -> asyncio.AbstractServer:
    """Start a minimal async HTTP server returning JSON status."""

    async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            # Read the HTTP request (we don't need to parse it deeply)
            data = await asyncio.wait_for(reader.readline(), timeout=5.0)
            # Consume remaining headers
            while True:
                line = await asyncio.wait_for(reader.readline(), timeout=2.0)
                if line == b"\r\n" or not line:
                    break

            stats = store.get_stats()
            uptime = int(time.time() - relay_state["start_time"])
            sources = [{"id": sid, "type": st.name} for sid, st in config.sources]
            body = json.dumps({
                "status": "paused" if relay_state["paused"] else "running",
                "uptime_seconds": uptime,
                "total_relayed": stats["total_relayed"],
                "errors": stats["errors"],
                "sources": sources,
                "last_message_at": relay_state["last_message_at"],
            }).encode("utf-8")

            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"Connection: close\r\n"
                b"\r\n" + body
            )
            writer.write(response)
            await writer.drain()
        except Exception as exc:
            logger.debug("Health check connection error: %s", exc)
        finally:
            writer.close()

    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    logger.info("Health check server listening on port %s", port)
    return server


# ---------------------------------------------------------------------------
# Core relay functions (modified for multi-target, prefix/suffix, silent)
# ---------------------------------------------------------------------------

async def copy_message(
    client: Client,
    message: Message,
    config: RelayConfig,
    target_id: int,
    target_type: ChatType,
) -> str:
    """
    Copy text as a new text message and media as a new media/document message.
    For forwarded/embedded messages, use the embedded original content when available.
    When content is not extractable (bot keyboards etc.), send a notification.
    """
    payload = message
    if not payload.text and not payload.document and payload.replied_to:
        payload = payload.replied_to

    if payload.text is not None and payload.text.strip():
        # Feature 3: Apply prefix/suffix
        text = payload.text
        if config.message_prefix or config.message_suffix:
            text = (config.message_prefix or "") + text + (config.message_suffix or "")

        # Feature 8: Silent mode — try passing silent kwarg
        send_kwargs: dict[str, Any] = {}
        if config.silent:
            send_kwargs["silent"] = True

        try:
            await client.send_message(
                text=text,
                chat_id=target_id,
                chat_type=target_type,
                **send_kwargs,
            )
        except TypeError:
            # BaleClient may not support 'silent' kwarg — fall back
            await client.send_message(
                text=text,
                chat_id=target_id,
                chat_type=target_type,
            )
        return "copied-text"

    if payload.document is not None:
        await client.send_document(
            file=payload.document,
            caption=get_caption(payload.document),
            chat_id=target_id,
            chat_type=target_type,
            use_own_content=True,
        )
        return "copied-media"

    # Content not extractable (bot keyboard, inline buttons, etc.)
    # Send a notification instead
    ts = datetime.fromtimestamp(message.date / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    notification = (
        f"📨 پیام جدید\n"
        f"از: {message.chat.id}\n"
        f"فرستنده: {message.sender_id}\n"
        f"شناسه: {message.message_id}\n"
        f"زمان: {ts}"
    )
    await client.send_message(
        text=notification,
        chat_id=target_id,
        chat_type=target_type,
    )
    return "notified"


async def transfer_message(
    client: Client,
    message: Message,
    config: RelayConfig,
    target_id: int,
    target_type: ChatType,
) -> str:
    if config.mode == "forward":
        await message.forward_to(
            chat_id=target_id,
            chat_type=target_type,
        )
        return "forwarded"
    return await copy_message(client, message, config, target_id, target_type)


def message_matches(message: Message, config: RelayConfig) -> bool:
    msg_chat_id = int(message.chat.id)
    for src_id, src_type in config.sources:
        if msg_chat_id == src_id:
            if (
                config.allowed_sender_id is not None
                and int(message.sender_id) != config.allowed_sender_id
            ):
                return False
            return True
    return False


async def transfer_with_retries(
    client: Client,
    message: Message,
    config: RelayConfig,
    logger: logging.Logger,
) -> str:
    """
    Transfer a message to ALL configured targets with retries per target.
    Returns a summary action string.
    """
    actions = []
    for target_id, target_type in config.all_targets:
        last_exc: Optional[Exception] = None
        for attempt in range(1, config.max_retries + 1):
            try:
                action = await transfer_message(client, message, config, target_id, target_type)
                actions.append(action)
                break
            except Exception as exc:
                last_exc = exc
                if attempt >= config.max_retries:
                    raise
                wait_seconds = min(
                    config.retry_base_seconds * (2 ** (attempt - 1)), 20.0
                )
                logger.warning(
                    "Transfer attempt %s/%s failed for message %s -> target %s: %s; retrying in %.1fs",
                    attempt,
                    config.max_retries,
                    message.message_id,
                    target_id,
                    exc,
                    wait_seconds,
                )
                await asyncio.sleep(wait_seconds)
    return "+".join(actions) if actions else "no-target"


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Forward or copy new Bale messages from one chat to another."
    )
    parser.add_argument(
        "--config",
        type=Path,
        default=DEFAULT_CONFIG_PATH,
        help="Path to config JSON.",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Run the interactive source/target setup again.",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Login and print chat_id/chat_type/sender_id for every received message; do not relay.",
    )
    parser.add_argument(
        "--reset-session",
        action="store_true",
        help="Delete the local login session and ask for phone/code again.",
    )
    return parser


def main() -> int:
    args = build_parser().parse_args()
    config_path = args.config.expanduser().resolve()
    session_path = config_path.parent / DEFAULT_SESSION_PATH.name
    db_path = config_path.parent / DEFAULT_DB_PATH.name
    log_path = config_path.parent / DEFAULT_LOG_PATH.name

    # Load config early to get log_level (if not inspect/setup mode)
    if args.inspect:
        config = None
        logger = setup_logging(log_path, "INFO")
    elif args.setup:
        config = run_setup(config_path)
        logger = setup_logging(log_path, config.log_level)
    else:
        try:
            config = load_config(config_path)
        except ConfigurationError as exc:
            logger_temp = setup_logging(log_path, "INFO")
            logger_temp.error("Configuration error: %s", exc)
            return 2
        logger = setup_logging(log_path, config.log_level)

    # Feature 10: inform about log level
    logger.debug("Log level set to %s", config.log_level if config else "INFO")

    if args.reset_session and session_path.exists():
        session_path.unlink()
        logger.info("Deleted session file: %s", session_path)

    store = StateStore(db_path)
    dispatcher = Dispatcher()
    client = Client(
        dispatcher=dispatcher,
        session_file=session_path,
        show_update_errors=True,
    )
    transfer_lock = asyncio.Lock()
    successful_count = 0

    @dispatcher.message()
    async def on_message(message: Message) -> None:
        nonlocal successful_count

        try:
            return await _handle_message_inner(message)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unexpected error in message handler for msg=%s", message.message_id)

    async def _handle_message_inner(message: Message) -> None:
        nonlocal successful_count

        if args.inspect:
            logger.info(
                "INSPECT | chat_id=%s | chat_type=%s(%s) | sender_id=%s | message_id=%s | %s",
                message.chat.id,
                getattr(message.chat.type, "name", str(message.chat.type)),
                int(message.chat.type),
                message.sender_id,
                message.message_id,
                preview_message(message),
            )
            return

        assert config is not None

        # Feature 4: Handle admin commands (before any relay logic)
        is_command = await handle_admin_command(message, client, config, store, logger)
        if is_command:
            return

        # Feature 5: Check active hours
        if not is_within_active_hours(config):
            logger.debug("Outside active hours, skipping message %s", message.message_id)
            return

        # Feature 4: Check if relay is paused
        if relay_state["paused"]:
            logger.debug("Relay is paused, skipping message %s", message.message_id)
            return

        if not message_matches(message, config):
            return

        # Feature 2: Keyword filter
        if not passes_keyword_filter(message.text, config):
            logger.debug("Message %s blocked by keyword filter", message.message_id)
            return

        if not store.claim(message):
            return

        async with transfer_lock:
            try:
                action = await transfer_with_retries(client, message, config, logger)
                successful_count += 1
                relay_state["last_message_at"] = datetime.now(tz=timezone.utc).isoformat()

                # Feature 6: Record stats
                store.record_relay(int(message.chat.id))

                logger.info(
                    "%s | source=%s/%s | sender=%s | message=%s | targets=%s",
                    action,
                    message.chat.id,
                    getattr(message.chat.type, "name", message.chat.type),
                    message.sender_id,
                    message.message_id,
                    ",".join(f"{tid}/{tt.name}" for tid, tt in config.all_targets),
                )

                if config.mark_as_read:
                    try:
                        await message.seen()
                    except Exception as exc:
                        logger.warning("Could not mark message as read: %s", exc)

                if successful_count % 100 == 0:
                    store.prune(config.dedupe_max_rows)

                # Feature 9: Webhook notification
                if config.webhook_url:
                    for tid, _ in config.all_targets:
                        await send_webhook(config.webhook_url, message, action, tid, logger)

                # Feature 4: Periodic admin stats summary (every 100 messages or every hour)
                relay_state["message_count_since_summary"] += 1
                now = time.time()
                should_summarize = (
                    relay_state["message_count_since_summary"] >= 100
                    or (now - relay_state["last_summary_time"]) >= 3600
                )
                if should_summarize and config.admin_chat_id is not None:
                    summary = format_stats_for_admin(store, config)
                    await send_admin_message(client, config, summary, logger)
                    relay_state["message_count_since_summary"] = 0
                    relay_state["last_summary_time"] = now

                if config.delay_seconds:
                    await asyncio.sleep(config.delay_seconds)
            except Exception:
                store.release(message)
                # Feature 6: Record error
                store.record_error(int(message.chat.id))
                logger.exception(
                    "Transfer failed permanently; message was released for a later retry. "
                    "chat=%s message=%s",
                    message.chat.id,
                    message.message_id,
                )
                # Feature 4: Notify admin on error
                if config.admin_chat_id is not None:
                    await send_admin_message(
                        client, config,
                        f"❌ Transfer failed permanently\n"
                        f"Chat: {message.chat.id}\n"
                        f"Message: {message.message_id}\n"
                        f"Sender: {message.sender_id}",
                        logger,
                    )

    async def poll_source(client: Client, src_id: int, src_type, config: RelayConfig) -> None:
        """Poll a single source chat for new messages via load_history."""
        nonlocal successful_count
        try:
            messages = await client.load_history(
                chat_id=src_id, chat_type=src_type, limit=5
            )
        except Exception as exc:
            logger.warning("Poll failed for %s/%s: %s", src_id, src_type.name, exc)
            return

        for message in reversed(messages):  # oldest first
            if not store.claim(message):
                continue

            # Feature 5: Check active hours for polled messages too
            if not is_within_active_hours(config):
                store.release(message)
                continue

            # Feature 4: Check if paused
            if relay_state["paused"]:
                store.release(message)
                continue

            # Feature 2: Keyword filter
            if not passes_keyword_filter(message.text, config):
                store.release(message)
                continue

            async with transfer_lock:
                try:
                    action = await transfer_with_retries(client, message, config, logger)
                    successful_count += 1
                    relay_state["last_message_at"] = datetime.now(tz=timezone.utc).isoformat()

                    # Feature 6: Record stats
                    store.record_relay(src_id)

                    logger.info(
                        "%s | POLL | source=%s/%s | sender=%s | message=%s | targets=%s",
                        action,
                        src_id,
                        src_type.name,
                        message.sender_id,
                        message.message_id,
                        ",".join(f"{tid}/{tt.name}" for tid, tt in config.all_targets),
                    )

                    # Feature 9: Webhook
                    if config.webhook_url:
                        for tid, _ in config.all_targets:
                            await send_webhook(config.webhook_url, message, action, tid, logger)

                    if config.delay_seconds:
                        await asyncio.sleep(config.delay_seconds)
                except Exception:
                    store.release(message)
                    # Feature 6: Record error
                    store.record_error(src_id)
                    logger.exception(
                        "Poll transfer failed; message released. chat=%s message=%s",
                        src_id,
                        message.message_id,
                    )

    async def poll_all_sources(client: Client, config: RelayConfig) -> None:
        """Periodically poll all source chats for new messages."""
        await asyncio.sleep(5)  # initial delay
        consecutive_errors = 0
        while True:
            try:
                for src_id, src_type in config.sources:
                    await poll_source(client, src_id, src_type, config)
                consecutive_errors = 0
            except asyncio.CancelledError:
                raise  # Don't suppress cancellation
            except Exception as exc:
                consecutive_errors += 1
                backoff = min(60, 10 * (2 ** min(consecutive_errors, 5)))
                logger.warning(
                    "Poll loop error #%d: %s — backing off %ds",
                    consecutive_errors, exc, backoff,
                )
                await asyncio.sleep(backoff)
                continue
            await asyncio.sleep(10)

    if args.inspect:
        logger.info(
            "Inspect mode is active. Send a message in the desired source/target chats, "
            "copy the printed IDs, then press Ctrl+C and run with --setup."
        )
    else:
        assert config is not None
        sources_desc = ", ".join(f"{sid}/{st.name}" for sid, st in config.sources)
        targets_desc = ", ".join(f"{tid}/{tt.name}" for tid, tt in config.all_targets)
        logger.info(
            "Relay active | sources=[%s] -> targets=[%s] | mode=%s | sender_filter=%s",
            sources_desc,
            targets_desc,
            config.mode,
            config.allowed_sender_id or "ALL",
        )
        # Log enabled features
        features = []
        if config.keyword_filter:
            features.append(f"keyword_filter={list(config.keyword_filter)}")
        if config.keyword_exclude:
            features.append(f"keyword_exclude={list(config.keyword_exclude)}")
        if config.message_prefix:
            features.append(f"prefix={config.message_prefix!r}")
        if config.message_suffix:
            features.append(f"suffix={config.message_suffix!r}")
        if config.active_hours:
            features.append(f"active_hours={config.active_hours}")
        if config.silent:
            features.append("silent=true")
        if config.webhook_url:
            features.append("webhook=on")
        if config.admin_chat_id:
            features.append(f"admin={config.admin_chat_id}")
        if config.health_port:
            features.append(f"health=:{config.health_port}")
        if features:
            logger.info("Enabled features: %s", ", ".join(features))

    logger.info(
        "If no saved session exists, enter the Bale phone number in international format "
        "when prompted (example: 98912..., without '+')."
    )

    async def run_with_polling():
        stop_event = asyncio.Event()

        # Install our own signal handlers to gracefully shut down
        import signal as _signal
        loop = asyncio.get_running_loop()
        for sig in (_signal.SIGTERM, _signal.SIGINT):
            loop.add_signal_handler(sig, stop_event.set)

        await client.start(run_in_background=True)

        health_server = None
        poll_task = None
        if not args.inspect and config is not None:
            poll_task = asyncio.create_task(poll_all_sources(client, config))
            logger.info("Polling started for all sources (every 10s)")

            # Feature 7: Start health check server if configured
            if config.health_port is not None:
                health_server = await start_health_server(
                    config.health_port, store, config, logger
                )

            # Feature 4: Notify admin that relay has started
            if config.admin_chat_id is not None:
                await send_admin_message(
                    client, config,
                    f"✅ Bale relay started\n"
                    f"Sources: {', '.join(f'{sid}/{st.name}' for sid, st in config.sources)}\n"
                    f"Targets: {', '.join(f'{tid}/{tt.name}' for tid, tt in config.all_targets)}\n"
                    f"Mode: {config.mode}",
                    logger,
                )

        # Wait until stop signal — no try/except needed, just wait
        await stop_event.wait()

        # Clean shutdown sequence
        logger.info("Shutdown signal received, cleaning up...")
        if poll_task is not None:
            poll_task.cancel()
            with suppress(asyncio.CancelledError):
                await poll_task
        if health_server is not None:
            health_server.close()
            await health_server.wait_closed()
        await client.stop()
        logger.info("Clean shutdown complete.")

    try:
        asyncio.run(run_with_polling())
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except RuntimeError as exc:
        if "Event loop stopped" in str(exc):
            logger.info("Shutdown complete (event loop closed).")
        else:
            logger.exception("Unexpected RuntimeError.")
            return 1
    except Exception:
        logger.exception("Client stopped because of an unexpected error.")
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
