# Crypto Intelligence Agent

Telegram bot that monitors your crypto channels, filters noise, remembers everything, and sends you a market digest every 6 hours.

You add channels — the bot reads them, extracts key facts, builds a knowledge graph, and writes analytical reports with theses, risks, and actionable insights. You can also ask it questions about anything it has seen.

## What You Need

- Linux server (Ubuntu recommended)
- [Claude Max](https://claude.ai) subscription
- [Claude Code](https://docs.anthropic.com/en/docs/claude-code) CLI installed on the server
- Telegram account
- Bot token from [@BotFather](https://t.me/BotFather)
- [Voyage AI](https://www.voyageai.com/) API key (free tier works)

## Setup

```bash
git clone https://github.com/ENbanned/info_ai_agent.git
cd info_ai_agent

# Configure
cp config.json.example config.json
nano config.json  # fill in bot token, owner_chat_id, voyage API key

# Authenticate Claude
claude login

# Start
docker compose up -d qdrant neo4j
docker compose run --rm bot   # first time only — enter phone + code
docker compose up -d
```

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

> **Note:** `api_id` and `api_hash` in config are pre-set to Telegram Desktop native client values (reverse-engineered). Don't change them — they tell Telegram servers this is a real desktop client, which reduces the risk of account restrictions.

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
