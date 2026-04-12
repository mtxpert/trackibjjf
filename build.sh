#!/bin/bash
set -e

echo "=== Step 1: pip install ==="
pip install -r requirements.txt
echo "=== pip install done ==="

echo "=== Step 2: playwright install ==="
echo "PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}"
export PLAYWRIGHT_BROWSERS_PATH=/opt/render/project/src/.playwright-browsers
echo "PLAYWRIGHT_BROWSERS_PATH set to: ${PLAYWRIGHT_BROWSERS_PATH}"
echo "Build dir: $(pwd)"
echo "Checking /opt/render/project/src exists: $(ls /opt/render/project/src 2>&1 | head -3)"

python -m playwright install chromium
echo "=== playwright install done ==="
