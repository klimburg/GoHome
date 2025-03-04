#!/usr/bin/env python3
"""
Main module for reading data from Bluetooth sensors and uploading to Sift.
"""

import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import yaml
from dotenv import load_dotenv

# Import sensor classes
from bluetooth_sensors.sensors import GVH5100, WavePlus
from sift_py.grpc.transport import use_sift_channel
from sift_py.ingestion.channel import ChannelConfig, ChannelDataType, double_value
from sift_py.ingestion.config.telemetry import TelemetryConfig
from sift_py.ingestion.flow import Flow, FlowConfig
from sift_py.ingestion.service import IngestionService

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Map of sensor types to their classes
SENSOR_TYPES = {
    "WavePlus": WavePlus,
    "GVH5100": GVH5100,
}

# ... rest of the file remains the same, but update imports to use the new module structure 