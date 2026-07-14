# 📨 Bale Messenger Forwarder

> Relay messages from one or more **Bale (بله)** chats to a single target — powered by a personal Bale account (userbot).

[🇮🇷 مستندات فارسی](README_FA.md)

---

## What It Does

This tool logs into your **personal Bale account** and continuously monitors source chats for new messages. When a new message arrives, it copies (or forwards) it to your target chat in real-time.

**Use cases:**
- Aggregate messages from multiple bot PVs into one channel
- Monitor a channel and relay its posts to your own chat
- Bridge between different Bale chats automatically

## Features

| Feature | Description |
|---|---|
| 🔄 **Multi-source** | Relay from multiple chats simultaneously |
| 📡 **Push + Polling** | Real-time push updates + polling fallback for bot messages |
| 📋 **Copy mode** | Send message text as a new message (no "Forwarded" label) |
| ↗️ **Forward mode** | Real forward with original sender info |
| 🔁 **Auto-retry** | Exponential backoff on failures (up to 4 retries) |
| 🗄️ **Deduplication** | SQLite-backed — no duplicate forwards even after restart |
| 💾 **Session persistence** | Login once, stays logged in |
| 🔍 **Inspect mode** | Discover chat IDs and types interactively |
| 🛡️ **BaleClient bug fixes** | Built-in patches for known BaleClient 1.0.9 bugs |
| ⚙️ **systemd service** | Auto-start on boot, auto-restart on crash |

## Quick Start

### Prerequisites

- Python 3.11+
- A Bale account with access to source and target chats

### 1. Clone & Install

```bash
git clone https://github.com/BobbyJoonz/Bale-Messenger-Forwarder.git
cd Bale-Messenger-Forwarder
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Find Chat IDs

```bash
python main.py --inspect
```

Enter your Bale phone number (e.g., `989121234567` without `+`). Then send a message in each source/target chat. The tool prints:

```
chat_id=123456 | chat_type=PRIVATE | sender_id=789
```

Press `Ctrl+C` when done.

### 3. Configure

```bash
python main.py --setup
```

Or edit `config.json` directly (copy from `config.example.json`):

```json
{
  "sources": [
    { "id": 111111111, "type": "PRIVATE" },
    { "id": 222222222, "type": "CHANNEL" }
  ],
  "target": { "id": 999999999, "type": "CHANNEL" },
  "mode": "copy"
}
```

### 4. Run

```bash
python main.py
```

### 5. Deploy as a Service (Linux)

```bash
sudo cp bale-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bale-relay

# View logs
journalctl -u bale-relay -f
```

## Configuration Reference

| Key | Type | Default | Description |
|---|---|---|---|
| `sources` | `array` | — | List of source chats `[{id, type}]` |
| `source` | `object` | — | Single source (legacy, still works) |
| `target` | `object` | — | Target chat `{id, type}` |
| `mode` | `string` | `"forward"` | `"forward"` or `"copy"` |
| `allowed_sender_id` | `int\|null` | `null` | Filter by sender; `null` = all senders |
| `mark_as_read` | `bool` | `false` | Mark source messages as read |
| `copy_fallback_to_forward` | `bool` | `true` | In copy mode, forward if content can't be copied |
| `delay_seconds` | `float` | `0.35` | Delay between transfers (rate limit protection) |
| `max_retries` | `int` | `4` | Retry count on failure |
| `retry_base_seconds` | `float` | `1.5` | Base delay for exponential backoff |
| `dedupe_max_rows` | `int` | `20000` | Max dedup history entries |

### Chat Types

| Value | Description |
|---|---|
| `PRIVATE` | 1-on-1 conversation |
| `BOT` | Bot private chat (PV) |
| `GROUP` | Basic group |
| `SUPER_GROUP` | Supergroup |
| `CHANNEL` | Channel |

> **Note:** The relay matches messages by `chat_id` only. The `type` field is used for API calls but doesn't affect message matching.

## How It Works

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Source Chat  │────▶│  Bale Relay Bot  │────▶│ Target Chat  │
│  (bot PV)     │     │                  │     │  (channel)   │
├──────────────┤     │  ┌────────────┐  │     ├──────────────┤
│  Source Chat  │────▶│  │  Polling   │  │────▶│              │
│  (channel)    │     │  │  (10s)     │  │     │              │
└──────────────┘     │  └────────────┘  │     └──────────────┘
                     │  ┌────────────┐  │
                     │  │  Push      │  │
                     │  │  (realtime)│  │
                     │  └────────────┘  │
                     └──────────────────┘
```

1. **Push mode:** Listens for real-time updates from Bale (works for regular user messages)
2. **Polling mode:** Every 10 seconds, fetches recent messages via `load_history` (needed for bot messages that Bale doesn't push)
3. **Dedup:** Each message ID is stored in SQLite — never forwarded twice
4. **Copy vs Forward:** Copy sends the text as a new message; Forward uses Bale's native forward API

## CLI Options

```
python main.py [OPTIONS]

  --config PATH     Path to config JSON (default: config.json)
  --setup           Run interactive setup wizard
  --inspect         Print chat_id/type/sender for incoming messages
  --reset-session   Delete saved login session
```

## Project Structure

```
Bale-Messenger-Forwarder/
├── main.py                  # Application code (741 lines)
│   ├── RelayConfig          # Configuration dataclass
│   ├── StateStore           # SQLite deduplication
│   ├── Monkey-patches       # BaleClient 1.0.9 bug fixes
│   ├── Push handler         # Real-time message handler
│   └── Polling engine       # load_history polling loop
├── config.example.json      # Example configuration
├── requirements.txt         # Python dependencies
├── bale-relay.service       # systemd service file
├── run_linux.sh             # Linux launcher script
├── run_windows.bat          # Windows launcher script
├── README.md                # This file
└── README_FA.md             # Persian documentation
```

## BaleClient Bug Fixes

This project includes monkey-patches for three bugs in `BaleClient==1.0.9`:

### 1. String annotation crash
`CallableObject.call()` crashes with `AttributeError: 'str' object has no attribute '__name__'` when the handler uses `from __future__ import annotations`.

**Fix:** Uses `getattr()` to safely access annotation names.

### 2. Text content stripped
`MessageContent._check_empty()` forcibly sets `text=None` for all messages, discarding actual content.

**Fix:** Replaces the validator to only set the `empty` flag without destroying text.

### 3. Hex decode crash on reactions
`int64.decode_list()` crashes with `ValueError: non-hexadecimal number` when Bale sends reaction data as a dict instead of a hex string.

**Fix:** Wraps `fromhex()` in try-except and handles dict input gracefully.

## Important Notes

- Uses Bale's **unofficial internal API** — may break with Bale updates
- The logged-in account must have access to all source and target chats
- Bot messages (keyboards, inline buttons) may not have extractable text content
- Only use on accounts and chats you own or have explicit permission for
- The `account_session.bale` file contains your login token — **never share it**

## Troubleshooting

| Problem | Solution |
|---|---|
| No messages relayed | Check if source chat IDs are correct with `--inspect` |
| `PermissionDenied` | Account doesn't have send permission in target chat |
| `InvalidArgument` | Wrong chat type in config (e.g., PRIVATE vs CHANNEL) |
| Empty text in relayed messages | BaleClient bug — patched automatically |
| Session expired | Run `--reset-session` and re-login |
| Service won't start | Check `journalctl -u bale-relay -n 50` |

## License

This project uses the unofficial `BaleClient` library. Use at your own risk.
