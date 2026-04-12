#!/bin/bash
set -e

pip install -r requirements.txt

# Install Playwright browser — Render's Python env has the required system libs
python -m playwright install chromium
