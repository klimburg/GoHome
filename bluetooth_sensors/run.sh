#!/bin/bash
# Startup script for bluetooth sensors data collection

# Activate virtual environment
source "$(dirname "$0")/../.venv/bin/activate"
pip install -e "$(dirname "$0")/../"

# Run the main script
cd "$(dirname "$0")"
python -m bluetooth_sensors.main