FROM python:3.11-slim

# System deps que o Chromium precisa
RUN apt-get update && apt-get install -y \
    libnss3 libnspr4 libdbus-1-3 \
    libatk1.0-0 libatk-bridge2.0-0 \
    libcups2 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 \
    libpango-1.0-0 libcairo2 libatspi2.0-0 \
    libx11-xcb1 libxcb1 libxext6 libxi6 \
    libxrender1 libx11-6 fonts-liberation wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

ENV PLAYWRIGHT_BROWSERS_PATH=/app/.playwright-browsers

RUN pip install --no-cache-dir -r requirements.txt && \
    playwright install chromium

COPY . .

EXPOSE 8000

CMD uvicorn main:app --host 0.0.0.0 --port $PORT
