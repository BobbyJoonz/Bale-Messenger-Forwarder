# 📨 Bale Messenger Forwarder

> Relay messages from one or more **Bale (بله)** chats to one or more targets — powered by a personal Bale account (userbot).

[🇮🇷 مستندات فارسی](README_FA.md)

---

## What It Does

This tool logs into your **personal Bale account** and continuously monitors source chats for new messages. When a new message arrives, it copies (or forwards) it to your target chat(s) in real-time.

**Use cases:**
- Aggregate messages from multiple bot PVs into one channel
- Monitor a channel and relay its posts to your own chat
- Bridge between different Bale chats automatically
- Filter and forward only messages matching keywords
- Multi-target broadcast

## Features

| Feature | Description |
|---|---|
| 🔄 **Multi-source** | Relay from multiple chats simultaneously |
| 🎯 **Multi-target** | Send to multiple target chats at once |
| 📡 **Push + Polling** | Real-time push updates + polling fallback for bot messages |
| 📋 **Copy mode** | Send message text as a new message (no "Forwarded" label) |
| ↗️ **Forward mode** | Real forward with original sender info |
| 🔍 **Keyword filter** | Include/exclude messages by keywords |
| 🏷️ **Prefix/Suffix** | Add custom text before/after relayed messages |
| 👤 **Admin chat** | Error notifications + stats + commands via Bale |
| ⏰ **Active hours** | Only relay during specified hours |
| 📊 **Statistics** | Track relay counts by source, hour, errors |
| 🏥 **Health check** | HTTP endpoint for monitoring |
| 🔇 **Silent mode** | Send without notification sound |
| 🔗 **Webhook** | POST message data to external URL |
| 🔁 **Auto-retry** | Exponential backoff on failures (up to 4 retries) |
| 🗄️ **Deduplication** | SQLite-backed — no duplicate forwards even after restart |
| 💾 **Session persistence** | Login once, stays logged in |
| 🔍 **Inspect mode** | Discover chat IDs and types interactively |
| 🛡️ **Bug fixes** | Built-in patches for known BaleClient 1.0.9 bugs |
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

Enter your phone number, send a message in each chat, copy the printed IDs.

### 3. Configure

Copy `config.example.json` to `config.json` and edit:

```json
{
  "sources": [
    { "id": 111111111, "type": "PRIVATE" },
    { "id": 222222222, "type": "CHANNEL" }
  ],
  "targets": [
    { "id": 999999999, "type": "CHANNEL" }
  ],
  "mode": "copy"
}
```

### 4. Run

```bash
python main.py
```

### 5. Deploy as Service (Linux)

```bash
sudo cp bale-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bale-relay
journalctl -u bale-relay -f
```

## Configuration Reference

### Core

| Key | Type | Default | Description |
|---|---|---|---|
| `sources` | `array` | — | Source chats `[{id, type}]` |
| `source` | `object` | — | Single source (legacy) |
| `targets` | `array` | — | Target chats `[{id, type}]` |
| `target` | `object` | — | Single target (legacy) |
| `mode` | `string` | `"forward"` | `"forward"` or `"copy"` |
| `allowed_sender_id` | `int\|null` | `null` | Filter by sender |
| `mark_as_read` | `bool` | `false` | Mark source as read |
| `copy_fallback_to_forward` | `bool` | `true` | Fallback to forward in copy mode |
| `delay_seconds` | `float` | `0.35` | Delay between transfers |
| `max_retries` | `int` | `4` | Retry count |
| `retry_base_seconds` | `float` | `1.5` | Exponential backoff base |
| `dedupe_max_rows` | `int` | `20000` | Max dedup history |

### Filtering

| Key | Type | Default | Description |
|---|---|---|---|
| `keyword_filter` | `array\|null` | `null` | Only relay if text contains one of these keywords |
| `keyword_exclude` | `array\|null` | `null` | Skip if text contains any of these keywords |

### Formatting

| Key | Type | Default | Description |
|---|---|---|---|
| `message_prefix` | `string\|null` | `null` | Prepend to relayed text |
| `message_suffix` | `string\|null` | `null` | Append to relayed text |

### Admin & Monitoring

| Key | Type | Default | Description |
|---|---|---|---|
| `admin_chat_id` | `int\|null` | `null` | Chat ID for error notifications and commands |
| `admin_chat_type` | `string` | `"PRIVATE"` | Type of admin chat |
| `health_port` | `int\|null` | `null` | HTTP health check port |
| `log_level` | `string` | `"INFO"` | Log level (DEBUG, INFO, WARNING, ERROR) |

### Advanced

| Key | Type | Default | Description |
|---|---|---|---|
| `active_hours` | `object\|null` | `null` | `{"start": 8, "end": 23}` — only relay during these hours (UTC) |
| `silent` | `bool` | `false` | Send messages without notification sound |
| `webhook_url` | `string\|null` | `null` | POST message data to this URL |

### Chat Types

| Value | Description |
|---|---|
| `PRIVATE` | 1-on-1 conversation |
| `BOT` | Bot private chat (PV) |
| `GROUP` | Basic group |
| `SUPER_GROUP` | Supergroup |
| `CHANNEL` | Channel |

## Admin Commands

Set `admin_chat_id` in config, then send these commands to your Bale account:

| Command | Description |
|---|---|
| `/stats` | Show relay statistics |
| `/pause` | Pause relay |
| `/resume` | Resume relay |
| `/sources` | List configured sources |

## Health Check

Set `health_port` (e.g., `8080`) to enable:

```bash
curl http://localhost:8080/
```

Returns:
```json
{
  "status": "running",
  "uptime_seconds": 3600,
  "total_relayed": 150,
  "sources": ["270066638/PRIVATE", "5379211084/CHANNEL"],
  "last_message_at": "2026-07-14T16:21:54Z"
}
```

## Webhook

Set `webhook_url` to receive POST requests for each relayed message:

```json
{
  "source_chat_id": 270066638,
  "source_chat_type": "PRIVATE",
  "sender_id": 270066638,
  "message_id": 123456,
  "text": "message content...",
  "timestamp": "2026-07-14T16:21:54Z",
  "action": "copied-text"
}
```

## Architecture

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────┐
│  Source Chat  │────▶│                  │────▶│ Target Chat  │
│  (bot PV)     │     │   Bale Relay     │     │  (channel)   │
├──────────────┤     │                  │     ├──────────────┤
│  Source Chat  │────▶│  ┌────────────┐  │────▶│ Target Chat  │
│  (channel)    │     │  │  Polling   │  │     │  (channel2)  │
└──────────────┘     │  │  (10s)     │  │     └──────────────┘
                     │  └────────────┘  │
                     │  ┌────────────┐  │     ┌──────────────┐
                     │  │  Filters   │  │────▶│ Admin Chat   │
                     │  │  Keywords  │  │     │  (commands)  │
                     │  └────────────┘  │     └──────────────┘
                     │  ┌────────────┐  │
                     │  │  Webhook   │──│────▶ External URL
                     │  └────────────┘  │
                     │  ┌────────────┐  │
                     │  │  Health    │──│────▶ HTTP :8080
                     │  └────────────┘  │
                     └──────────────────┘
```

## CLI Options

```
python main.py [OPTIONS]
  --config PATH     Path to config JSON
  --setup           Interactive setup wizard
  --inspect         Print chat info for incoming messages
  --reset-session   Delete login session
```

## BaleClient Bug Fixes

Three monkey-patches for BaleClient 1.0.9:
1. **String annotation crash** — `CallableObject.call()` AttributeError fix
2. **Text content stripped** — `MessageContent._check_empty()` fix
3. **Hex decode crash** — `int64.decode_list()` ValueError fix

## Important Notes

- Uses Bale's **unofficial internal API** — may break with updates
- Bot messages may not have extractable content (keyboards, buttons)
- Only use on accounts you own or have permission for
- `account_session.bale` is your login token — **never share it**

## License

Uses the unofficial `BaleClient` library. Use at your own risk.
