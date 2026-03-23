# Crypto Intelligence Agent

Telegram bot that monitors your crypto channels, filters noise, remembers everything, and sends you a market digest every 6 hours.

You add channels — the bot reads them, extracts key facts, builds a knowledge graph, and writes analytical reports with theses, risks, and actionable insights. You can also ask it questions about anything it has seen.

## Requirements

- Linux server (Ubuntu/Debian), minimum 8GB RAM
- [Claude Pro/Max/Teams](https://claude.ai) subscription
- Python 3.12+, Node.js 20+, Docker
- Bot token from [@BotFather](https://t.me/BotFather)
- [Voyage AI](https://www.voyageai.com/) API key (free tier works)

## Setup

### 1. Install Claude Code

```bash
curl -fsSL https://claude.ai/install.sh | bash
echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc && source ~/.bashrc

# Check
claude --version
claude login
```

### 2. Install Docker

Follow [docs.docker.com/engine/install](https://docs.docker.com/engine/install/) for your OS.

```bash
# Check
docker --version && docker compose version
```

### 3. Install uv, Node.js 22, screen

```bash
# uv (manages Python + packages)
curl -LsSf https://astral.sh/uv/install.sh | sh
source ~/.bashrc
uv python install 3.12

# Node.js 22 LTS
curl -fsSL https://deb.nodesource.com/setup_22.x | bash -
apt install -y nodejs screen

# Check
uv --version && python3 --version && node --version
```

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

### 5. Install and patch

```bash
uv sync
bash mem0bot/patches/apply_patches.sh
```

### 6. Start infrastructure

```bash
docker compose up -d
```

### 7. First run — Telegram login

```bash
uv run main.py
```

Enter your phone number and verification code when prompted. Wait for `System running`, then `Ctrl+C`.

### 8. Run

```bash
bash run.sh
```

This creates a service user, copies Claude credentials, and launches the bot in a screen session.

## Logs

```bash
# Attach to the live session
sudo -u agent screen -r agent

# Detach without stopping: Ctrl+A D
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

## Updating

```bash
sudo -u agent screen -S agent -X quit
git pull
uv sync
bash mem0bot/patches/apply_patches.sh
bash run.sh
```

## License

MIT
