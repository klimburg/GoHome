#!/usr/bin/env python3
"""
Main module for reading data from Bluetooth sensors and uploading to Sift.
"""

import argparse
import asyncio
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import yaml
from dotenv import load_dotenv
from sift_py.grpc.transport import SiftChannelConfig, use_sift_channel
from sift_py.ingestion.channel import (
    ChannelConfig,
    ChannelValue,
)
from sift_py.ingestion.config.telemetry import TelemetryConfig
from sift_py.ingestion.flow import Flow, FlowConfig
from sift_py.ingestion.service import IngestionService

# Import sensor classes
from bluetooth_sensors.sensors import gvh5100, wave_plus
from bluetooth_sensors.sensors._ble_sensor import BluetoothSensor  # Add this import

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv("../.env", override=True)

# Get Sift credentials from environment
SIFT_API_KEY = os.getenv("SIFT_API_KEY")
BASE_URI = os.getenv("BASE_URI")
USE_SSL = os.getenv("USE_SSL", "true").lower() == "true"

# Asset name for Sift
ASSET_NAME = "HomeAirQuality"
CONFIG_KEY = "home-air-quality-config-v1"

# Map of sensor types to their classes
SENSOR_TYPES = {
    "WavePlus": wave_plus.WavePlus,
    "GVH5100": gvh5100.GVH5100,
}

# Map of sensor types to their channel definitions
SENSOR_CHANNELS = {
    "WavePlus": wave_plus.CHANNELS,
    "GVH5100": gvh5100.CHANNELS,
}


async def read_sensor_data(sensor_instance: BluetoothSensor) -> Dict[str, Any]:
    """Read data from a sensor.

    Args:
        sensor_instance: Instance of a BluetoothSensor

    Returns:
        Dictionary containing sensor data
    """
    try:
        # Connect to sensor
        connected = await sensor_instance.connect(retries=3)
        if not connected:
            logger.error(f"Failed to connect to sensor {sensor_instance.serial_number}")
            return {}

        # Read data
        success = await sensor_instance.read()

        # Get sensor data
        if success:
            # Use the common get_channel_data method and extract just the values
            channel_data = sensor_instance.get_channel_data()
            data = {name: info["value"] for name, info in channel_data.items()}
            return data
        else:
            logger.warning(
                f"Failed to read from sensor {sensor_instance.serial_number}"
            )
            return {}

    except Exception as e:
        logger.exception(f"Error reading sensor data: {e}")
        return {}
    finally:
        # Disconnect from sensor
        await sensor_instance.disconnect()


def build_telemetry_config(
    sensors_config: List[Dict[str, Any]],
) -> Tuple[TelemetryConfig, Dict[str, FlowConfig]]:
    """Build a telemetry config based on available sensors.

    Args:
        sensors_config: List of sensor configurations

    Returns:
        Tuple of (TelemetryConfig for Sift, mapping of sensor names to flow names)
    """
    flows = []
    sensor_to_flow_map = {}  # Maps sensor names to their flow names

    # Create a flow for each sensor
    for sensor_config in sensors_config:
        sensor_type = sensor_config["type"]
        sensor_name = sensor_config["name"]

        # Skip unknown sensor types
        if sensor_type not in SENSOR_TYPES:
            logger.warning(f"Unknown sensor type: {sensor_type}")
            continue

        # Skip sensor types without channel definitions
        if sensor_type not in SENSOR_CHANNELS:
            logger.warning(f"No channel definitions for sensor type: {sensor_type}")
            continue

        # Get available channels from the mapping
        channels = []
        for channel in SENSOR_CHANNELS[sensor_type]:
            name = channel["name"]
            unit = channel["unit"]
            channels.append(
                ChannelConfig(
                    name=f"{sensor_name}.{name}",
                    data_type=channel["sift_type"],
                    unit=unit,
                )
            )

        # Create a unique flow name and store it in the mapping
        flow_name = f"{sensor_name}-readings-{int(time.time())}"
        flow_config = FlowConfig(name=flow_name, channels=channels)

        # Add flow for this sensor
        flows.append(flow_config)
        sensor_to_flow_map[sensor_name] = flow_config

    # Create the telemetry config
    telemetry_config = TelemetryConfig(
        asset_name=ASSET_NAME, ingestion_client_key=CONFIG_KEY, flows=flows
    )

    return telemetry_config, sensor_to_flow_map


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Bluetooth sensor data collection and ingestion to Sift"
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )
    return parser.parse_args()


async def main() -> None:
    """Main function to read sensor data and upload to Sift."""
    # Parse command line arguments
    args = parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        # Also set debug level for sensor loggers
        logging.getLogger("bluetooth_sensors").setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

    # Load sensor configuration
    try:
        config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        return

    # Extract sensor configs
    sensors_config = config.get("sensors", [])
    logger.info(f"Sensors config: {sensors_config}")
    if not sensors_config:
        logger.error("No sensors configured")
        return

    # Build telemetry config and get sensor-to-flow mapping
    telemetry_config, sensor_to_flow_map = build_telemetry_config(sensors_config)

    # Create sensor instances
    sensor_instances = []
    for sensor_config in sensors_config:
        sensor_type = sensor_config["type"]
        if sensor_type in SENSOR_TYPES:
            sensor_class = SENSOR_TYPES[sensor_type]
            sensor_instance = sensor_class(
                sensor_config["serial_number"],
                name=sensor_config["name"],
                address=sensor_config.get("address", None),
                logger=logger,
            )
            sensor_instances.append((sensor_config["name"], sensor_instance))

    if not sensor_instances:
        logger.error("No valid sensor instances created")
        return

    # Connect to Sift

    if SIFT_API_KEY is None or BASE_URI is None:
        raise ValueError("SIFT_API_KEY and BASE_URI must be set")

    credentials = SiftChannelConfig(
        apikey=SIFT_API_KEY,
        uri=BASE_URI,
        use_ssl=USE_SSL,
    )
    logger.info(f"Credentials: {credentials}")
    logger.info(f"Connecting to Sift at {BASE_URI}")

    # Create the channel without using async with
    grpc_channel = use_sift_channel(credentials)

    try:
        # Create ingestion service
        ingestion_service = IngestionService(grpc_channel, telemetry_config)

        # Create a run
        run_name = f"{ASSET_NAME}.{datetime.now().timestamp():.0f}"
        logger.info(f"Creating run: {run_name}")
        ingestion_service.attach_run(grpc_channel, run_name)

        # Main loop to read and send data
        try:
            while True:
                logger.info("Reading from sensors...")
                for sensor_name, sensor_instance in sensor_instances:
                    # Read sensor data
                    data = await read_sensor_data(sensor_instance)

                    if data:
                        logger.info(f"Got data from {sensor_name}: {data}")

                        # Get the correct flow name from the mapping
                        flow_config = sensor_to_flow_map.get(sensor_name)
                        if flow_config is None:
                            logger.error(f"No flow mapping for sensor {sensor_name}")
                            continue

                        channel_values = []

                        for channel_config in flow_config.channels:
                            channel_name = channel_config.name
                            value = data.get(channel_name)
                            if value is None:
                                logger.warning(f"No value for channel {channel_name}")
                                continue

                            channel_values.append(
                                ChannelValue(
                                    channel_name=channel_name,
                                    value=channel_config.value_from(value),
                                )
                            )

                        # Ingest data
                        if channel_values:
                            ingestion_service.try_ingest_flows(
                                Flow(
                                    flow_name=flow_config.name,
                                    timestamp=datetime.now(timezone.utc),
                                    channel_values=channel_values,
                                )
                            )
                            logger.info(
                                f"Data sent to Sift for {sensor_name} with flow {flow_config.name}"
                            )

                # Wait before next reading
                logger.info("Waiting 20 seconds for next reading")
                await asyncio.sleep(20)

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        except Exception as e:
            logger.exception(f"Error in main loop: {e}")
    finally:
        # Clean up the channel
        logger.info("Closing Sift connection")
        grpc_channel.close()
        logger.info("Shutting down")


if __name__ == "__main__":
    asyncio.run(main())
