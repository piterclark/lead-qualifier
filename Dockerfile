FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# apt-get update MUST run before playwright install-deps to refresh the package cache
RUN apt-get update \
    && playwright install-deps chromium \
    && playwright install chromium \
    && rm -rf /var/lib/apt/lists/*

COPY . .

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
