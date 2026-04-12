#!/bin/bash
exec gunicorn app:app --worker-class gthread --workers 1 --threads 20 --bind 0.0.0.0:$PORT --timeout 120
