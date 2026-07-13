# Bale Messenger Forwarder

A userbot that logs into a personal **Bale (بله)** account and forwards new messages from **one or more source chats** to a single target chat in real-time.

> 🇵🇸 [مستندات فارسی](README_FA.md)

## Features

- **Multi-source relay** — forward from multiple chats simultaneously
- Login with phone number, OTP, and optional 2FA
- Session persistence — no re-login on restart
- Supports private chats, groups, supergroups, and channels
- Two transfer modes:
  - `forward` — real forwarded message (with "Forwarded from..." label)
  - `copy` — copies text/media without the forward label
- Optional sender filter (`allowed_sender_id`)
- SQLite-based deduplication — prevents duplicate forwards
- Auto-retry with exponential backoff
- `--inspect` mode to discover `chat_id`, `chat_type`, and `sender_id`
- Built-in fix for BaleClient string annotation bug

## Quick Start

### Prerequisites

- Python 3.11 or newer
- A Bale account with access to both source and target chats

### Windows

```bat
run_windows.bat --inspect
# Find chat IDs, then Ctrl+C
run_windows.bat --setup
# Configure source/target, then:
run_windows.bat
```

### Linux / VPS

```bash
chmod +x run_linux.sh
./run_linux.sh --inspect     # discover chat IDs
./run_linux.sh --setup       # configure relay
./run_linux.sh               # start relay
```

### As a systemd Service (Linux)

```bash
sudo cp bale-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable bale-relay
sudo systemctl start bale-relay

# View logs
journalctl -u bale-relay -f
```

## Configuration

After running `--setup`, a `config.json` is created. See `config.example.json` for reference.

### Multi-source (recommended):

```json
{
  "sources": [
    { "id": 111111111, "type": "PRIVATE" },
    { "id": 222222222, "type": "CHANNEL" }
  ],
  "target": { "id": 999999999, "type": "PRIVATE" },
  "mode": "forward",
  "allowed_sender_id": null,
  "mark_as_read": false,
  "copy_fallback_to_forward": true,
  "delay_seconds": 0.35,
  "max_retries": 4
}
```

### Single-source (backward compatible):

```json
{
  "source": { "id": 111111111, "type": "PRIVATE" },
  "target": { "id": 999999999, "type": "PRIVATE" },
  "mode": "forward"
}
```

### Options

| Key | Description | Default |
|---|---|---|
| `sources` | Array of source chats `[{id, type}]` | — |
| `source` | Single source chat (legacy) | — |
| `target` | Target chat `{id, type}` | — |
| `mode` | `forward` or `copy` | `forward` |
| `allowed_sender_id` | Filter by sender ID, `null` for all | `null` |
| `mark_as_read` | Mark source as read after transfer | `false` |
| `copy_fallback_to_forward` | Forward unsupported types in copy mode | `true` |
| `delay_seconds` | Delay between transfers | `0.35` |
| `max_retries` | Retry count on failure | `4` |
| `retry_base_seconds` | Base for exponential backoff | `1.5` |
| `dedupe_max_rows` | Max dedup history rows | `20000` |

## Chat Types

| Value | Description |
|---|---|
| `PRIVATE` | Direct message |
| `GROUP` | Basic group |
| `SUPER_GROUP` | Supergroup |
| `CHANNEL` | Channel |
| `BOT` | Bot chat |

## Important Notes

- The logged-in account must have access to **all source and target chats**
- The account must have send permission in the target chat
- Source and target cannot be the same chat
- Uses Bale's unofficial internal API — may break with Bale updates
- Only use on accounts and chats you own or have explicit permission for

## File Structure

```
bale_userbot_relay/
├── main.py                 # Main application
├── config.example.json     # Example configuration
├── requirements.txt        # Python dependencies
├── run_linux.sh            # Linux launcher
├── run_windows.bat         # Windows launcher
├── bale-relay.service      # systemd service file
├── README.md               # This file
├── README_FA.md            # Persian documentation
└── .gitignore              # Git ignore rules
```

## License

This project uses the unofficial `BaleClient` library. Use at your own risk.
