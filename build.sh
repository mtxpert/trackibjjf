#!/bin/bash
set -e

pip install -r requirements.txt

# Install Playwright browser into the project source dir so it's
# available at runtime (Render's build and runtime share /opt/render/project/src)
export PLAYWRIGHT_BROWSERS_PATH=/opt/render/project/src/.playwright-browsers
python -m playwright install chromium
