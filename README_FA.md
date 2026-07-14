# 📨 Bale Messenger Forwarder

> انتقال پیام بین چتهای **بله** — چند مبدا، چند مقصد، کنترل کامل از داخل بله

[🇬🇧 English](README.md) | [🇮🇷 فارسی](README_FA.md)

---

## 🇮🇷 فارسی

### چیکار میکنه؟

با **اکانت شخصی بله** وارد میشه و پیامهای چتهای مبدا رو **خودکار** به مقصد فوروارد میکنه. همه چیز از داخل خود بله قابل کنترله — بدون نیاز به SSH!

### ✨ قابلیتها

| قابلیت | توضیح |
|---|---|
| 🔄 چند مبدا | انتقال همزمان از چند چت |
| 🎯 چند مقصد | ارسال به چند کانال همزمان |
| 📡 Push + Polling | آپدیت لحظه‌ای + پولینگ ۱۰ ثانیه‌ای |
| 📋 کپی / ↗️ فوروارد | دو حالت انتقال |
| 🔍 فیلتر کلمات | شامل/مستثنی بر اساس کلمه |
| 🏷 پیشوند/پسوند | اضافه کردن متن به پیام |
| 👤 ادمین چت | کنترل کامل از داخل بله |
| ⏰ ساعات فعال | فقط در ساعات خاص |
| 📊 آمار | ردیابی کامل عملکرد |
| 🏥 Health check | HTTP endpoint مانیتورینگ |
| 🔇 سایلنت | ارسال بدون صدا |
| 🔗 وبهوک | POST به URL خارجی |
| 🛡 ضدضربه | مدیریت خطا و ریستارت خودکار |

### 🚀 نصب سریع

```bash
git clone https://github.com/BobbyJoonz/Bale-Messenger-Forwarder.git
cd Bale-Messenger-Forwarder
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# پیدا کردن chat_id ها
python main.py --inspect

# تنظیم (interactive)
python main.py --setup

# اجرا
python main.py

# نصب سرویس (لینوکس)
sudo cp bale-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bale-relay
```

### 📱 دستورات ادمین (از داخل بله)

تنظیم `admin_chat_id` در config.json، سپس:

```
📊 آمار و وضعیت
/help          راهنمای دستورات
/stats         آمار رله
/config        تنظیمات فعلی
/sources       لیست مبداها
/targets       لیست مقاصد

📡 مبدا و مقصد
/add <id> PV   اضافه کردن مبدا
/remove <id>   حذف مبدا
/target <id> CH  تنظیم مقصد
/addtarget <id>  اضافه کردن مقصد

🔍 فیلتر
/filter <کلمه>   فیلتر کلمه
/unfilter <کلمه> حذف فیلتر
/exclude <کلمه>  حذف کلمه
/filters         نمایش فیلترها

🏷 فرمت
/prefix <متن>    پیشوند
/suffix <متن>    پسوند

⏰ زمان‌بندی
/hours 8 23      ساعات فعال
/hours off       ۲۴ ساعته

🔇 کنترل
/silent on/off   سایلنت
/pause           توقف موقت
/resume          ادامه
/logs            آخرین لاگها
/restart         ریستارت
/webhook <url>   تنظیم وبهوک
```

> **هر تغییری فوری ذخیره و اعمال میشه — بدون نیاز به ریستارت!**

### 📁 ساختار پروژه

```
Bale-Messenger-Forwarder/
├── main.py                  # کد اصلی (~۲۱۰۰ خط)
│   ├── Monkey-patches       # رفع باگهای BaleClient
│   ├── RelayConfig          # مدیریت تنظیمات
│   ├── StateStore           # SQLite + آمار
│   ├── Admin commands       # ۲۰+ دستور ادمین
│   ├── Push handler         # پردازش لحظه‌ای
│   ├── Polling engine       # پولینگ ۱۰ ثانیه‌ای
│   ├── Keyword filter       # فیلتر کلمات
│   ├── Webhook sender       # ارسال به URL
│   └── Health server        # HTTP مانیتورینگ
├── config.example.json      # نمونه تنظیمات
├── requirements.txt         # وابستگیها
├── bale-relay.service       # سرویس systemd
├── run_linux.sh             # اسکریپت لینوکس
├── run_windows.bat          # اسکریپت ویندوز
└── README_FA.md             # این فایل
```

### ⚙️ تنظیمات

`config.example.json` رو کپی کنید به `config.json`:

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

| کلید | پیشفرض | توضیح |
|---|---|---|
| `sources` | — | لیست مبداها `[{id, type}]` |
| `targets` | — | لیست مقاصد `[{id, type}]` |
| `source` / `target` | — | فرمت قدیمی (همچنان کار میکنه) |
| `mode` | `"forward"` | `forward` یا `copy` |
| `keyword_filter` | `null` | فقط پیامهای شامل این کلمات |
| `keyword_exclude` | `null` | رد کردن پیامهای شامل این کلمات |
| `message_prefix` | `null` | متن قبل پیام |
| `message_suffix` | `null` | متن بعد پیام |
| `admin_chat_id` | `null` | شناسه چت ادمین |
| `active_hours` | `null` | `{"start": 8, "end": 23}` |
| `health_port` | `null` | پورت HTTP |
| `silent` | `false` | بدون صدا |
| `webhook_url` | `null` | URL وبهوک |
| `log_level` | `"INFO"` | سطح لاگ |

### انواع چت

| مقدار | توضیح |
|---|---|
| `PRIVATE` / `PV` | خصوصی |
| `BOT` | ربات |
| `GROUP` / `GR` | گروه |
| `CHANNEL` / `CH` | کانال |
| `SUPER_GROUP` | سوپرگروه |

### 🔧 عیب‌یابی

| مشکل | راه حل |
|---|---|
| پیامی فوروارد نمیشود | `/sources` چک کنید، chat_id درسته؟ |
| `PermissionDenied` | اکانت اجازه ارسال به مقصد ندارد |
| متن خالی | باگ BaleClient — خودکار پچ شده |
| سشن منقضی | `--reset-session` و ورود مجدد |
| سرویس کرش | `journalctl -u bale-relay -n 50` |

### ⚠️ نکات مهم

- از **API غیررسمی بله** استفاده میکنه
- فقط روی اکانت خودتان استفاده کنید
- `account_session.bale` محرمانه است

---

## 🇬🇧 English

### What It Does

Logs into your **personal Bale account** and automatically relays messages from source chats to target chat(s). Everything is controllable from inside Bale via admin commands — no SSH needed!

### ✨ Features

| Feature | Description |
|---|---|
| 🔄 Multi-source | Relay from multiple chats simultaneously |
| 🎯 Multi-target | Send to multiple targets at once |
| 📡 Push + Polling | Real-time + 10s polling for bot messages |
| 📋 Copy / ↗️ Forward | Two relay modes |
| 🔍 Keyword filter | Include/exclude by keywords |
| 🏷 Prefix/Suffix | Add text before/after messages |
| 👤 Admin chat | Full control from Bale |
| ⏰ Active hours | Only relay during specified hours |
| 📊 Statistics | Track relay performance |
| 🏥 Health check | HTTP monitoring endpoint |
| 🔇 Silent mode | Send without notification sound |
| 🔗 Webhook | POST to external URL |
| 🛡 Crash-proof | Error handling + auto-restart |

### 🚀 Quick Start

```bash
git clone https://github.com/BobbyJoonz/Bale-Messenger-Forwarder.git
cd Bale-Messenger-Forwarder
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Find chat IDs
python main.py --inspect

# Configure
python main.py --setup

# Run
python main.py

# Install as service (Linux)
sudo cp bale-relay.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now bale-relay
```

### 📱 Admin Commands (from Bale)

Set `admin_chat_id` in config.json, then:

| Command | Description |
|---|---|
| `/help` | Show all commands |
| `/stats` | Relay statistics |
| `/config` | Current configuration |
| `/sources` | List sources |
| `/targets` | List targets |
| `/add <id> <TYPE>` | Add source |
| `/remove <id>` | Remove source |
| `/target <id> <TYPE>` | Set target |
| `/addtarget <id> <TYPE>` | Add target |
| `/filter <word>` | Add keyword filter |
| `/unfilter <word>` | Remove filter |
| `/exclude <word>` | Add exclude |
| `/filters` | Show filters |
| `/prefix <text>` | Set prefix |
| `/suffix <text>` | Set suffix |
| `/hours <start> <end>` | Active hours (UTC) |
| `/hours off` | Disable hours |
| `/silent on/off` | Silent mode |
| `/pause` / `/resume` | Pause/resume relay |
| `/logs` | Last log lines |
| `/restart` | Restart service |
| `/webhook <url>` | Set webhook |

> **Changes take effect immediately — no restart needed!**

### ⚙️ Configuration

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

### Chat Types

| Value | Description |
|---|---|
| `PRIVATE` / `PV` | Private chat |
| `BOT` | Bot chat |
| `GROUP` / `GR` | Group |
| `CHANNEL` / `CH` | Channel |
| `SUPER_GROUP` | Supergroup |

### ⚠️ Important Notes

- Uses Bale's **unofficial internal API**
- Only use on accounts you own
- `account_session.bale` is confidential

### 📄 License

Uses the unofficial `BaleClient` library. Use at your own risk.
