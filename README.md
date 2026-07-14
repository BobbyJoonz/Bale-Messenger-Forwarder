# 📨 Bale Messenger Forwarder

> Relay messages between **Bale (بله)** chats — multi-source, multi-target, full control from Bale

[🇬🇧 English](README.md) | [🇮🇷 فارسی](README_FA.md)

---

## What It Does

Logs into your **personal Bale account** and automatically relays messages from source chats to target chat(s). Control everything from inside Bale with admin commands — no SSH needed!

## ✨ Features

| Feature | Description |
|---|---|
| 🔄 Multi-source | Relay from multiple chats simultaneously |
| 🎯 Multi-target | Send to multiple targets at once |
| 📡 Push + Polling | Real-time updates + 10s polling for bot messages |
| 📋 Copy / ↗️ Forward | Two relay modes |
| 🔍 Keyword filter | Include/exclude messages by keywords |
| 🏷 Prefix / Suffix | Add custom text before/after messages |
| 👤 Admin commands | Full control from Bale (20+ commands) |
| ⏰ Active hours | Only relay during specified hours |
| 📊 Statistics | Track relay counts, errors, performance |
| 🏥 Health check | HTTP monitoring endpoint |
| 🔇 Silent mode | Send without notification sound |
| 🔗 Webhook | POST message data to external URL |
| 🛡 Crash-proof | Graceful error handling + auto-restart |
| 🗄️ Deduplication | SQLite-backed — no duplicate forwards |
| 💾 Session persistence | Login once, stays logged in |

## 🚀 Quick Start

```bash
git clone https://github.com/BobbyJoonz/Bale-Messenger-Forwarder.git
cd Bale-Messenger-Forwarder
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Find chat IDs
python main.py --inspect

# Configure interactively
python main.py --setup

# Run
python main.py
```

### Deploy as Service (Linux)

```bash
sudo cp bale-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bale-relay

# View logs
journalctl -u bale-relay -f
```

## 📱 Admin Commands

Set `admin_chat_id` in config.json, then send commands from Bale:

### Status & Info
| Command | Description |
|---|---|
| `/help` | Show all commands |
| `/stats` | Relay statistics |
| `/config` | Current configuration |
| `/sources` | List configured sources |
| `/targets` | List configured targets |

### Source & Target Management
| Command | Description |
|---|---|
| `/add <id> <TYPE>` | Add a source chat |
| `/remove <id>` | Remove a source chat |
| `/target <id> <TYPE>` | Set target (replaces all) |
| `/addtarget <id> <TYPE>` | Add additional target |

### Keyword Filtering
| Command | Description |
|---|---|
| `/filter <word>` | Add keyword filter |
| `/unfilter <word>` | Remove keyword filter |
| `/exclude <word>` | Add exclude keyword |
| `/unexclude <word>` | Remove exclude keyword |
| `/filters` | Show current filters |

### Message Formatting
| Command | Description |
|---|---|
| `/prefix <text>` | Set message prefix |
| `/suffix <text>` | Set message suffix |

### Scheduling & Control
| Command | Description |
|---|---|
| `/hours <start> <end>` | Set active hours (UTC) |
| `/hours off` | Disable active hours |
| `/silent on/off` | Toggle silent mode |
| `/pause` | Pause relay |
| `/resume` | Resume relay |

### Service Management
| Command | Description |
|---|---|
| `/logs [N]` | Show last N log lines |
| `/restart` | Restart the service |
| `/webhook <url>` | Set webhook URL |
| `/webhook off` | Disable webhook |

> **All changes take effect immediately — no restart needed!**

## ⚙️ Configuration

Copy `config.example.json` to `config.json`:

```json
{
  "sources": [
    { "id": 111111111, "type": "PRIVATE" },
    { "id": 222222222, "type": "CHANNEL" }
  ],
  "targets": [
    { "id": 999999999, "type": "CHANNEL" }
  ],
  "mode": "copy",
  "keyword_filter": null,
  "keyword_exclude": null,
  "message_prefix": null,
  "message_suffix": null,
  "admin_chat_id": null,
  "active_hours": null,
  "health_port": null,
  "silent": false,
  "webhook_url": null,
  "log_level": "INFO"
}
```

### All Options

| Key | Default | Description |
|---|---|---|
| `sources` | — | Source chats `[{id, type}]` |
| `targets` | — | Target chats `[{id, type}]` |
| `source` / `target` | — | Legacy single source/target |
| `mode` | `"forward"` | `"forward"` or `"copy"` |
| `allowed_sender_id` | `null` | Filter by sender |
| `mark_as_read` | `false` | Mark source as read |
| `delay_seconds` | `0.35` | Delay between transfers |
| `max_retries` | `4` | Retry count |
| `keyword_filter` | `null` | Include keywords `["word1", "word2"]` |
| `keyword_exclude` | `null` | Exclude keywords |
| `message_prefix` | `null` | Prepend to text |
| `message_suffix` | `null` | Append to text |
| `admin_chat_id` | `null` | Admin chat for commands |
| `admin_chat_type` | `"PRIVATE"` | Admin chat type |
| `active_hours` | `null` | `{"start": 8, "end": 23}` (UTC) |
| `health_port` | `null` | HTTP health check port |
| `silent` | `false` | Suppress notifications |
| `webhook_url` | `null` | Webhook POST URL |
| `log_level` | `"INFO"` | DEBUG/INFO/WARNING/ERROR |

### Chat Types

| Value | Short | Description |
|---|---|---|
| `PRIVATE` | `PV` | Private chat |
| `BOT` | — | Bot chat |
| `GROUP` | `GR` | Group |
| `CHANNEL` | `CH` | Channel |
| `SUPER_GROUP` | — | Supergroup |

## 🏗 Architecture

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

## BaleClient Bug Fixes

Three monkey-patches for `BaleClient==1.0.9`:
1. **String annotation crash** — `CallableObject.call()` fix
2. **Text content stripped** — `MessageContent._check_empty()` fix
3. **Hex decode crash** — `int64.decode_list()` fix

## 📁 Project Structure

```
Bale-Messenger-Forwarder/
├── main.py                  # Application (~2100 lines)
│   ├── Monkey-patches       # BaleClient bug fixes
│   ├── RelayConfig          # Configuration management
│   ├── StateStore           # SQLite + statistics
│   ├── Admin commands       # 20+ Bale commands
│   ├── Push handler         # Real-time message handler
│   ├── Polling engine       # 10s polling loop
│   ├── Keyword filter       # Include/exclude filtering
│   ├── Webhook sender       # HTTP POST notifications
│   └── Health server        # HTTP monitoring
├── config.example.json      # Example configuration
├── requirements.txt         # Python dependencies
├── bale-relay.service       # systemd service
├── run_linux.sh             # Linux launcher
├── run_windows.bat          # Windows launcher
├── README.md                # This file
└── README_FA.md             # Persian documentation
```

## Troubleshooting

| Problem | Solution |
|---|---|
| No messages relayed | Check `/sources` — correct IDs? |
| `PermissionDenied` | Account lacks send permission in target |
| Empty text | BaleClient bug — auto-patched |
| Session expired | `--reset-session` + re-login |
| Service crash | `journalctl -u bale-relay -n 50` |

## ⚠️ Important

- Uses Bale's **unofficial internal API** — may break with updates
- Only use on accounts you own
- `account_session.bale` is your login token — **never share it**

## 📄 License

Uses the unofficial `BaleClient` library. Use at your own risk.
