#!/bin/bash

# setup.sh - Настройка автозапуска main.py проекта bot_template

# Прерывать выполнение при ошибке
set -e

# Переменные
PROJECT_DIR="/bot_template"
VENV_DIR="$PROJECT_DIR/venv"
SERVICE_NAME="bot_template.service"
SERVICE_FILE="/etc/systemd/system/$SERVICE_NAME"
TIMER_NAME="bot_template.timer"
TIMER_FILE="/etc/systemd/system/$TIMER_NAME"
USER="$(whoami)"
PYTHON_EXEC="$VENV_DIR/bin/python"
MAIN_SCRIPT="$PROJECT_DIR/main.py"

echo "=== Начало настройки проекта bot_templatebot_template ==="

# Проверка наличия директории проекта
if [ ! -d "$PROJECT_DIR" ]; then
    echo "❌ Директория проекта $PROJECT_DIR не найдена. Завершение."
    exit 1
fi

cd "$PROJECT_DIR"

# Шаг 1: Остановка и удаление существующих systemd сервисов и таймеров
echo "📦 Остановка и удаление существующих systemd сервисов и таймеров..."

if systemctl list-unit-files | grep -q "^$SERVICE_NAME"; then
    echo "🔧 Остановка сервиса $SERVICE_NAME..."
    systemctl stop "$SERVICE_NAME" || true
    echo "🗑️ Удаление сервиса $SERVICE_NAME..."
    rm -f "$SERVICE_FILE"
fi

if systemctl list-unit-files | grep -q "^$TIMER_NAME"; then
    echo "🔧 Остановка таймера $TIMER_NAME..."
    systemctl stop "$TIMER_NAME" || true
    echo "🗑️ Удаление таймера $TIMER_NAME..."
    rm -f "$TIMER_FILE"
fi

systemctl daemon-reload

# Шаг 2: Пересоздание виртуального окружения
echo "🔧 Пересоздание виртуального окружения..."

if [ -d "$VENV_DIR" ]; then
    echo "🗑️ Удаление старого виртуального окружения..."
    rm -rf "$VENV_DIR"
fi

echo "🚀 Создание нового виртуального окружения..."
python3 -m venv "$VENV_DIR"

echo "📦 Установка зависимостей..."
source "$VENV_DIR/bin/activate"
pip install --upgrade pip
pip install python-telegram-bot aiohttp python-dotenv
deactivate

# Шаг 3: Создание systemd сервиса
echo "🔧 Создание systemd сервиса в $SERVICE_FILE..."

cat <<EOL > "$SERVICE_FILE"
[Unit]
Description=bot_template Project Service
After=network.target

[Service]
Type=oneshot
User=$USER
WorkingDirectory=$PROJECT_DIR
Environment="PATH=$VENV_DIR/bin"
ExecStart=$PYTHON_EXEC $MAIN_SCRIPT
Restart=no

[Install]
WantedBy=multi-user.target
EOL

# Шаг 4: Создание systemd таймера
echo "🔧 Создание systemd таймера в $TIMER_FILE..."

cat <<EOL > "$TIMER_FILE"
[Unit]
Description=Run bot_template Project

[Timer]
OnCalendar=*:0/15
Persistent=true

[Install]
WantedBy=timers.target
EOL

systemctl daemon-reload

# Шаг 5: Запуск и включение сервиса
echo "🚀 Первый запуск сервиса $SERVICE_NAME..."
systemctl start "$SERVICE_NAME"

# Ждем завершения
while systemctl is-active --quiet "$SERVICE_NAME"; do
    sleep 1
done
echo "✅ Первый запуск завершён."

echo "🚀 Запуск и включение таймера $TIMER_NAME..."
systemctl start "$TIMER_NAME"
systemctl enable "$TIMER_NAME"

echo "🔍 Проверка статуса таймера..."
systemctl status "$TIMER_NAME" --no-pager

echo "🎉 Настройка завершена. Таймер настроен на выполнение каждые 5 минут!"

# Шаг 6: Явный запуск main.py для первого выполнения
echo "🚀 Явный запуск main.py для первого выполнения..."
source "$VENV_DIR/bin/activate"
MANUAL_RUN=true $PYTHON_EXEC $MAIN_SCRIPT || echo "❌ Ошибка первого запуска main.py. Проверьте логи."
deactivate
