#!/bin/bash
# Startup script for bluetooth sensors data collection

# Activate virtual environment
source "$(dirname "$0")/../.venv/bin/activate"

# Run the main script
cd "$(dirname "$0")"
python main.py