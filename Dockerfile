FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# playwright install --with-deps: faz apt-get update + instala deps do sistema + baixa Chromium
RUN playwright install --with-deps chromium

COPY . .

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
