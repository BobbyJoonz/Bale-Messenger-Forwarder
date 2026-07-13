import argparse
import asyncio
import json
import logging
import sqlite3
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

try:
    from baleclient import Client, Dispatcher
    from baleclient.enums import ChatType
    from baleclient.types import Message

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

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "RelayConfig":
        try:
            target = raw["target"]
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

            target_id = int(target["id"])
            target_type = parse_chat_type(target["type"])

            for sid, stype in sources_list:
                if sid == target_id and int(stype) == int(target_type):
                    raise ConfigurationError(
                        f"Source {sid}/{stype.name} cannot be the same as target."
                    )

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
            )
        except (KeyError, TypeError, ValueError) as exc:
            if isinstance(exc, ConfigurationError):
                raise
            raise ConfigurationError(f"Invalid config.json: {exc}") from exc
        return config

    def as_dict(self) -> dict[str, Any]:
        sources_list = [{"id": sid, "type": st.name} for sid, st in self.sources]
        return {
            "sources": sources_list,
            "target": {
                "id": self.target_chat_id,
                "type": self.target_chat_type.name,
            },
            "mode": self.mode,
            "allowed_sender_id": self.allowed_sender_id,
            "mark_as_read": self.mark_as_read,
            "copy_fallback_to_forward": self.copy_fallback_to_forward,
            "delay_seconds": self.delay_seconds,
            "max_retries": self.max_retries,
            "retry_base_seconds": self.retry_base_seconds,
            "dedupe_max_rows": self.dedupe_max_rows,
        }


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


def setup_logging(log_path: Path) -> logging.Logger:
    logger = logging.getLogger("bale-relay")
    logger.setLevel(logging.INFO)
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


async def copy_message(
    client: Client,
    message: Message,
    config: RelayConfig,
) -> str:
    """
    Copy text as a new text message and media as a new media/document message.
    For forwarded/embedded messages, use the embedded original content when available.
    """
    payload = message
    if not payload.text and not payload.document and payload.replied_to:
        payload = payload.replied_to

    if payload.text is not None:
        await client.send_message(
            text=payload.text,
            chat_id=config.target_chat_id,
            chat_type=config.target_chat_type,
        )
        return "copied-text"

    if payload.document is not None:
        await client.send_document(
            file=payload.document,
            caption=get_caption(payload.document),
            chat_id=config.target_chat_id,
            chat_type=config.target_chat_type,
            use_own_content=True,
        )
        return "copied-media"

    if config.copy_fallback_to_forward:
        await message.forward_to(
            chat_id=config.target_chat_id,
            chat_type=config.target_chat_type,
        )
        return "forwarded-fallback"

    raise UnsupportedMessageError("Message type cannot be copied.")


async def transfer_message(
    client: Client,
    message: Message,
    config: RelayConfig,
) -> str:
    if config.mode == "forward":
        await message.forward_to(
            chat_id=config.target_chat_id,
            chat_type=config.target_chat_type,
        )
        return "forwarded"
    return await copy_message(client, message, config)


def message_matches(message: Message, config: RelayConfig) -> bool:
    msg_chat_id = int(message.chat.id)
    msg_chat_type = int(message.chat.type)
    for src_id, src_type in config.sources:
        if msg_chat_id == src_id and msg_chat_type == int(src_type):
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
    for attempt in range(1, config.max_retries + 1):
        try:
            return await transfer_message(client, message, config)
        except Exception as exc:
            if attempt >= config.max_retries:
                raise
            wait_seconds = min(
                config.retry_base_seconds * (2 ** (attempt - 1)), 20.0
            )
            logger.warning(
                "Transfer attempt %s/%s failed for message %s: %s; retrying in %.1fs",
                attempt,
                config.max_retries,
                message.message_id,
                exc,
                wait_seconds,
            )
            await asyncio.sleep(wait_seconds)
    raise RuntimeError("Unreachable retry state")


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
    logger = setup_logging(log_path)

    if args.reset_session and session_path.exists():
        session_path.unlink()
        logger.info("Deleted session file: %s", session_path)

    if args.setup:
        config = run_setup(config_path)
    elif args.inspect:
        config = None
    else:
        try:
            config = load_config(config_path)
        except ConfigurationError as exc:
            logger.error("Configuration error: %s", exc)
            return 2

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
        if not message_matches(message, config):
            return

        if not store.claim(message):
            logger.info(
                "Skipped duplicate message chat=%s message=%s",
                message.chat.id,
                message.message_id,
            )
            return

        async with transfer_lock:
            try:
                action = await transfer_with_retries(client, message, config, logger)
                successful_count += 1
                logger.info(
                    "%s | source=%s/%s | sender=%s | message=%s | target=%s/%s",
                    action,
                    message.chat.id,
                    getattr(message.chat.type, "name", message.chat.type),
                    message.sender_id,
                    message.message_id,
                    config.target_chat_id,
                    config.target_chat_type.name,
                )

                if config.mark_as_read:
                    try:
                        await message.seen()
                    except Exception as exc:
                        logger.warning("Could not mark message as read: %s", exc)

                if successful_count % 100 == 0:
                    store.prune(config.dedupe_max_rows)

                if config.delay_seconds:
                    await asyncio.sleep(config.delay_seconds)
            except Exception:
                store.release(message)
                logger.exception(
                    "Transfer failed permanently; message was released for a later retry. "
                    "chat=%s message=%s",
                    message.chat.id,
                    message.message_id,
                )

    if args.inspect:
        logger.info(
            "Inspect mode is active. Send a message in the desired source/target chats, "
            "copy the printed IDs, then press Ctrl+C and run with --setup."
        )
    else:
        assert config is not None
        sources_desc = ", ".join(f"{sid}/{st.name}" for sid, st in config.sources)
        logger.info(
            "Relay active | sources=[%s] -> %s/%s | mode=%s | sender_filter=%s",
            sources_desc,
            config.target_chat_id,
            config.target_chat_type.name,
            config.mode,
            config.allowed_sender_id or "ALL",
        )

    logger.info(
        "If no saved session exists, enter the Bale phone number in international format "
        "when prompted (example: 98912..., without '+')."
    )

    try:
        client.run()
    except KeyboardInterrupt:
        logger.info("Stopped by user.")
    except Exception:
        logger.exception("Client stopped because of an unexpected error.")
        return 1
    finally:
        store.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
