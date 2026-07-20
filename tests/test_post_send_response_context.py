import asyncio
from types import SimpleNamespace

import main
from baleclient.client.session.aiohttp import AiohttpSession
from baleclient.enums import ChatType, PeerType
from baleclient.methods.messaging.send_message import SendMessage
from baleclient.types import Chat, MessageContent, Peer
from baleclient.types.message_content import TextMessage


class FakeResponse:
    headers = {}

    def __init__(self):
        self.released = False

    async def read(self):
        return b"response"

    def release(self):
        self.released = True


class FakeHTTPSession:
    async def post(self, **kwargs):
        return FakeResponse()


class DummyClient:
    id = 42


def test_http_post_passes_method_and_client_context(monkeypatch):
    session = AiohttpSession()
    session._bind_client(DummyClient())
    session.session = FakeHTTPSession()
    session.encoder = lambda value: b"request"
    session.decoder = lambda value: {"1": 1, "2": 1234567890}

    monkeypatch.setattr(main, "_add_header", lambda value: value)
    monkeypatch.setattr(main, "_clean_grpc", lambda value: value)

    method = SendMessage(
        peer=Peer(type=PeerType.GROUP, id=999),
        message_id=123,
        content=MessageContent(text=TextMessage(value="hello")),
        chat=Chat(type=ChatType.CHANNEL, id=999),
    )

    result = asyncio.run(session.post(method))
    assert result.message is not None
    assert result.message.message_id == 123
    assert result.message.sender_id == 42
    assert result.message.text == "hello"


def test_post_send_parse_failure_is_not_marked_for_retry():
    from main import is_ambiguous_post_send_error

    error = main.PostSendResponseError("response decode failed")
    assert is_ambiguous_post_send_error(error) is True
    assert is_ambiguous_post_send_error(
        AttributeError("'NoneType' object has no attribute 'get'")
    ) is False
    assert is_ambiguous_post_send_error(RuntimeError("WebSocket is not connected")) is False


def test_ambiguous_post_send_failure_keeps_dedupe_claim(tmp_path):
    from main import StateStore, handle_transfer_failure

    store = StateStore(tmp_path / "state.sqlite3")
    message = SimpleNamespace(
        chat=SimpleNamespace(id=111111111, type=ChatType.PRIVATE),
        message_id=987654321,
        date=1234567890,
    )
    assert store.claim(message) is True

    released = handle_transfer_failure(
        store,
        message,
        main.PostSendResponseError("response decode failed"),
    )
    assert released is False
    assert store.claim(message) is False
    store.close()


def test_ambiguous_send_response_is_never_retried():
    calls = 0

    class FakeClient:
        async def send_message(self, **kwargs):
            nonlocal calls
            calls += 1
            raise main.PostSendResponseError("response parse failed after send")

    config = SimpleNamespace(
        all_targets=((999999999, ChatType.CHANNEL),),
        mode="copy",
        message_prefix=None,
        message_suffix=None,
        silent=False,
        max_retries=4,
        retry_base_seconds=0.2,
    )
    message = SimpleNamespace(
        text="hello",
        document=None,
        replied_to=None,
        message_id=123,
    )
    logger = __import__("logging").getLogger("test")

    result = asyncio.run(
        main.transfer_with_retries(FakeClient(), message, config, logger)
    )
    assert result == "sent-unconfirmed"
    assert calls == 1


def test_decode_failure_after_post_is_ambiguous_and_releases_response(monkeypatch):
    response = FakeResponse()

    class Session(FakeHTTPSession):
        closed = False

        async def post(self, **kwargs):
            return response

    session = AiohttpSession()
    session._bind_client(DummyClient())
    session.session = Session()
    session.encoder = lambda value: b"request"
    session.decoder = lambda value: (_ for _ in ()).throw(ValueError("bad response"))
    monkeypatch.setattr(main, "_add_header", lambda value: value)
    monkeypatch.setattr(main, "_clean_grpc", lambda value: value)

    method = SendMessage(
        peer=Peer(type=PeerType.GROUP, id=999),
        message_id=123,
        content=MessageContent(text=TextMessage(value="hello")),
        chat=Chat(type=ChatType.CHANNEL, id=999),
    )

    try:
        asyncio.run(session.post(method))
        raise AssertionError("expected PostSendResponseError")
    except main.PostSendResponseError:
        pass
    assert response.released is True


def test_partial_multi_target_retry_skips_already_delivered_target(tmp_path):
    from main import StateStore

    calls = []

    class FakeClient:
        async def send_message(self, *, chat_id, **kwargs):
            calls.append(chat_id)
            if chat_id == 222 and calls.count(222) == 1:
                raise RuntimeError("temporary pre-send failure")

    config = SimpleNamespace(
        all_targets=((111, ChatType.CHANNEL), (222, ChatType.CHANNEL)),
        mode="copy",
        message_prefix=None,
        message_suffix=None,
        silent=False,
        max_retries=1,
        retry_base_seconds=0.2,
    )
    message = SimpleNamespace(
        chat=SimpleNamespace(id=333, type=ChatType.PRIVATE),
        text="hello",
        document=None,
        replied_to=None,
        message_id=444,
        date=1234567890,
    )
    store = StateStore(tmp_path / "state.sqlite3")
    logger = __import__("logging").getLogger("test")

    try:
        asyncio.run(
            main.transfer_with_retries(FakeClient(), message, config, logger, store)
        )
    except RuntimeError:
        pass
    result = asyncio.run(
        main.transfer_with_retries(FakeClient(), message, config, logger, store)
    )

    assert result == "already-delivered+copied-text"
    assert calls == [111, 222, 222]
    store.close()
