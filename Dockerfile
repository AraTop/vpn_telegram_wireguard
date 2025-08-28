# Dockerfile
FROM python:3.12-slim

WORKDIR /app

# Системные пакеты для asyncpg и т.п.
RUN apt-get update && apt-get install -y build-essential libpq-dev && rm -rf /var/lib/apt/lists/*

# Скопируем зависимости отдельно (чтобы кешировалось)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Кладём исходники
COPY . /app

# Укажем переменную окружения для питона (буферизация логов off)
ENV PYTHONUNBUFFERED=1

CMD ["python", "bot.py"]
