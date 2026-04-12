#!/bin/bash
set -e

pip install -r requirements.txt

# Install Playwright system deps then browser
python -m playwright install-deps chromium
python -m playwright install chromium
