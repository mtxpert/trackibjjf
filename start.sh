#!/bin/bash
# Playwright browsers live on the persistent disk — survives redeploys
export PLAYWRIGHT_BROWSERS_PATH=/opt/render/project/src/instance/.playwright-browsers

# Install chromium on first boot (or after disk wipe). Fast no-op if already present.
python -m playwright install chromium 2>&1 | tail -3 || true

exec gunicorn app:app --worker-class gevent --workers 2 --worker-connections 1000 --bind 0.0.0.0:$PORT --timeout 120
