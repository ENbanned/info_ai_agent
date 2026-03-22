# Crypto Intelligence Agent

Telegram bot that monitors your crypto channels, filters noise, remembers everything, and sends you a market digest every 6 hours.

You add channels — the bot reads them, extracts key facts, builds a knowledge graph, and writes analytical reports with theses, risks, and actionable insights. You can also ask it questions about anything it has seen.

## Requirements

- Linux server with **KVM virtualization** (not OpenVZ/LXC), minimum 8GB RAM, 25GB disk
- [Claude Pro/Max/Teams](https://claude.ai) subscription
- Telegram account
- Bot token from [@BotFather](https://t.me/BotFather)
- [Voyage AI](https://www.voyageai.com/) API key (free tier works)

## Step-by-step Setup

### 1. Install Claude Code

```bash
curl -fsSL https://claude.ai/install.sh | bash
```

If `claude` is not found after install:
```bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc
```

### 2. Install Docker

Follow the official guide for your OS: [docs.docker.com/engine/install](https://docs.docker.com/engine/install/)

Verify it works:
```bash
docker --version
docker compose version
```

### 3. Log in to Claude

```bash
claude login
```

Opens a browser link — log in with your Claude account.

### 4. Clone and configure

```bash
git clone https://github.com/ENbanned/info_ai_agent.git
cd info_ai_agent

cp config.json.example config.json
nano config.json
```

Fill in:
- `bot.token` — bot token from @BotFather
- `bot.owner_chat_id` — your Telegram user ID (get it from [@userinfobot](https://t.me/userinfobot))
- `voyage.api_key` — API key from [voyageai.com](https://www.voyageai.com/)

> `api_id` and `api_hash` are pre-set to Telegram Desktop native client values (reverse-engineered). Don't change them — they tell Telegram servers this is a real desktop client, which reduces the risk of account restrictions.

### 5. Start

```bash
docker compose up -d qdrant neo4j
docker compose run --rm bot
```

First run will ask for your phone number and a verification code from Telegram. Enter them, wait until you see `System running`, then press `Ctrl+C`.

```bash
docker compose up -d
```

Done. The bot is running.

## Usage

Talk to the bot in Telegram:

| Command | What it does |
|---------|-------------|
| `/add <link>` | Start monitoring a channel — copy link to any message in the channel or topic |
| `/remove <name>` | Stop monitoring |
| `/channels` | See what's being monitored |
| `/pause <name>` | Temporarily mute a channel |
| `/resume <name>` | Unmute |
| `/ask <question>` | Ask about anything the bot has seen |

Reports are delivered automatically every 6 hours.

## Updating

```bash
git pull
docker compose up -d --build
```

## Logs

```bash
docker compose logs -f bot
```

## License

MIT
