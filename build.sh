#!/bin/bash
set -e

pip install -r requirements.txt

# Install Chromium with all system dependencies in one command
python -m playwright install --with-deps chromium
