#!/bin/bash
exec gunicorn app:app --worker-class gevent --workers 1 --worker-connections 1000 --bind 0.0.0.0:$PORT --timeout 120
