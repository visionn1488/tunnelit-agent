# tunnelit-agent

CLI-клиент для туннельного сервиса **tunnelit** (аналог playit.gg). Позволяет пробросить любой локальный TCP/UDP порт (Minecraft, веб-сервер, SSH и др.) в интернет через ваш relay-сервер без проброса портов на роутере.

## Возможности

- 🚀 Проброс TCP и UDP трафика
- 🔄 Автоматическое переподключение при разрыве соединения
- 🏷️ Возможность запросить выделенный порт на relay
- 🌐 Поддержка подачи заявки на субдомен (для веб-сервисов)
- ⚙️ Простой и понятный CLI на базе Clap

---

## Установка и сборка (Ubuntu / Debian / Linux)

### 1. Установка зависимостей и Rust

```bash
sudo apt update && sudo apt install -y build-essential curl git

# Установка Rust (если еще не установлен)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
```

### 2. Клонирование и компиляция

```bash
git clone https://github.com/visionn1488/tunnelit-agent.git
cd tunnelit-agent
cargo build --release

# Установка бинарника в систему
sudo cp target/release/tunnelit-agent /usr/local/bin/
```

---

## Использование

### Основной синтаксис

```bash
tunnelit-agent --relay <URL> --local <PORT> --proto <tcp|udp> [ОПЦИИ]
```

### Параметры CLI

| Флаг / Опция | Обязательный | Описание |
|---|---|---|
| `-r, --relay` | Да | WebSocket URL relay-сервера (например, `wss://ws.ezbchat.fun/ws` или `ws://IP:9090/ws`) |
| `-l, --local` | Да | Локальный порт, на котором слушает ваш сервер |
| `-p, --proto` | Да | Протокол: `tcp` или `udp` |
| `--remote-port`| Нет | Желаемый публичный порт на relay (если свободен) |
| `--subdomain`  | Нет | Желаемое имя субдомена (отправит заявку в админку relay) |

---

## Примеры запуска

### 1. Проброс сервера Minecraft (TCP 25565)
```bash
tunnelit-agent --relay wss://ws.ezbchat.fun/ws --local 25565 --proto tcp
```

### 2. Minecraft с запросом конкретного публичного порта (25565)
```bash
tunnelit-agent --relay wss://ws.ezbchat.fun/ws --local 25565 --proto tcp --remote-port 25565
```

### 3. Голосовой сервер / игра по UDP (например, 19132)
```bash
tunnelit-agent --relay wss://ws.ezbchat.fun/ws --local 19132 --proto udp
```

### 4. Веб-сервер (8080) с заявкой на субдомен `b543kjb`
```bash
tunnelit-agent --relay wss://ws.ezbchat.fun/ws --local 8080 --proto tcp --subdomain b543kjb
```

---

## Автозапуск через Systemd (Фоновый режим на Ubuntu)

Чтобы агент работал в фоне и перезапускался при перезагрузке:

```bash
sudo tee /etc/systemd/system/tunnelit-agent.service << 'EOF'
[Unit]
Description=Tunnelit Client Agent
After=network.target

[Service]
Type=simple
User=ubuntu
ExecStart=/usr/local/bin/tunnelit-agent --relay wss://ws.ezbchat.fun/ws --local 25565 --proto tcp
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable tunnelit-agent
sudo systemctl start tunnelit-agent

# Проверить логи:
journalctl -u tunnelit-agent -f
```

---

## Лицензия

MIT
