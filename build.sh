#!/bin/bash
set -e
pip install -r requirements.txt
export PLAYWRIGHT_BROWSERS_PATH=/app/playwright-browsers
mkdir -p /app/playwright-browsers
playwright install chromium
echo "==> Playwright browsers instalados em: /app/playwright-browsers"
ls /app/playwright-browsers/
