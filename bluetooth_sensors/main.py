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
from typing import Any, Dict, List, Optional, Tuple

import kasa
import yaml
from dotenv import load_dotenv
from sift_py.grpc.transport import SiftChannel, SiftChannelConfig, use_sift_channel
from sift_py.ingestion.channel import (
    ChannelConfig,
    ChannelValue,
)
from sift_py.ingestion.config.telemetry import TelemetryConfig
from sift_py.ingestion.flow import Flow, FlowConfig
from sift_py.ingestion.service import IngestionService

# Import sensor classes
from bluetooth_sensors.sensors import gvh5100, wave_plus
from bluetooth_sensors.sensors._ble_sensor import BluetoothSensor

# Import Kasa sensor class
from bluetooth_sensors.sensors.kasa import KasaSensor, setup_kasa_telemetry

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

# Get Kasa credentials from environment
KASA_USERNAME = os.getenv("KASA_USERNAME")
KASA_PASSWORD = os.getenv("KASA_PASSWORD")

# Asset name for Sift
ASSET_NAME = "LimburgHome"
CONFIG_KEY = "limburg-home-config-v1"

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

# Global mutex for Bluetooth operations
BT_MUTEX = asyncio.Lock()


class BaseTask:
    """Base class for all sensor tasks."""

    task: Optional[asyncio.Task] = None

    async def run(self) -> None:
        """Run the task."""
        pass

    def start(self) -> None:
        """Start the task."""
        pass

    def stop(self) -> None:
        """Stop the task."""
        pass


class SensorTask(BaseTask):
    """Task for reading from a sensor at a specified interval."""

    def __init__(
        self,
        sensor_name: str,
        sensor_instance: BluetoothSensor,
        ingestion_service: IngestionService,
        flow_config: FlowConfig,
        sample_period: int = 60,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the sensor task.

        Args:
            sensor_name: Name of the sensor
            sensor_instance: Instance of a BluetoothSensor
            ingestion_service: The Sift ingestion service
            flow_config: Flow configuration for this sensor
            sample_period: Time in seconds between readings
            logger: Optional logger instance
        """
        self.sensor_name = sensor_name
        self.sensor_instance = sensor_instance
        self.ingestion_service = ingestion_service
        self.flow_config = flow_config
        self.sample_period = sample_period
        self.logger = logger or logging.getLogger(__name__)
        self.running = False

    async def read_sensor_data(self) -> Dict[str, Any]:
        """Read data from the sensor with mutex protection."""
        # Use global mutex to ensure only one Bluetooth operation at a time
        async with BT_MUTEX:
            try:
                # Connect to sensor
                connected = await self.sensor_instance.connect(retries=3)
                if not connected:
                    self.logger.error(f"Failed to connect to sensor {self.sensor_name}")
                    return {}

                # Read data
                success = await self.sensor_instance.read()

                # Get sensor data
                if success:
                    # Use the common get_channel_data method and extract just the values
                    channel_data = self.sensor_instance.get_channel_data()
                    data = {name: info["value"] for name, info in channel_data.items()}
                    return data
                else:
                    self.logger.warning(
                        f"Failed to read from sensor {self.sensor_name}"
                    )
                    return {}

            except Exception as e:
                self.logger.exception(f"Error reading sensor data: {e}")
                return {}
            finally:
                # Disconnect from sensor
                await self.sensor_instance.disconnect()

    async def send_data_to_sift(self, data: Dict[str, Any]) -> None:
        """Send sensor data to Sift.

        Args:
            data: Dictionary with channel names as keys and values as values
        """
        if not data:
            return

        self.logger.info(f"Got data from {self.sensor_name}: {data}")

        channel_values = []

        for channel_config in self.flow_config.channels:
            channel_name = channel_config.name
            value = data.get(channel_name)
            self.logger.debug(
                f"Channel {channel_name}: value={value}, type={type(value)}"
            )

            if value is None:
                self.logger.warning(f"No value for channel {channel_name}")
                continue

            channel_values.append(
                ChannelValue(
                    channel_name=channel_name,
                    value=channel_config.try_value_from(value),
                )
            )

        # Ingest data
        if channel_values:
            self.logger.debug(f"Ingesting channel values: {channel_values}")
            self.ingestion_service.try_ingest_flows(
                Flow(
                    flow_name=self.flow_config.name,
                    timestamp=datetime.now(timezone.utc),
                    channel_values=channel_values,
                )
            )
            self.logger.info(
                f"Data sent to Sift for {self.sensor_name} with flow {self.flow_config.name}"
            )

    async def run(self) -> None:
        """Run the sensor reading task."""
        self.running = True
        self.logger.info(
            f"Starting sensor task for {self.sensor_name} with period {self.sample_period}s"
        )

        while self.running:
            try:
                # Read sensor data
                data = await self.read_sensor_data()

                # Send data to Sift
                await self.send_data_to_sift(data)

            except asyncio.CancelledError:
                self.logger.info(f"Sensor task for {self.sensor_name} cancelled")
                self.running = False
                break
            except Exception as e:
                self.logger.exception(
                    f"Error in sensor task for {self.sensor_name}: {e}"
                )

            # Wait for next reading
            self.logger.debug(f"Waiting {self.sample_period}s for next reading")
            await asyncio.sleep(self.sample_period)

    def start(self) -> None:
        """Start the sensor task."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run())

    def stop(self) -> None:
        """Stop the sensor task."""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()


class KasaSensorTask(BaseTask):
    """Task for reading from a Kasa device at a specified interval."""

    def __init__(
        self,
        kasa_sensor: KasaSensor,
        ingestion_service: IngestionService,
        flow_config: FlowConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Kasa sensor task.

        Args:
            kasa_sensor: KasaSensor instance
            ingestion_service: The Sift ingestion service
            flow_config: Flow configuration for this sensor
            logger: Optional logger instance
        """
        self.kasa_sensor = kasa_sensor
        self.name = kasa_sensor.name
        self.sample_period = kasa_sensor.sample_period
        self.ingestion_service = ingestion_service
        self.flow_config = flow_config
        self.logger = logger or logging.getLogger(__name__)
        self.running = False

    async def read_sensor_data(self) -> Dict[str, Any]:
        """Read data from the Kasa device.

        Returns:
            Dictionary with channel names as keys and values as values
        """
        try:
            # Get channel data from Kasa device
            channel_data = await self.kasa_sensor.get_channel_data()

            # Extract just the values
            data = {name: info["value"] for name, info in channel_data.items()}
            return data

        except Exception as e:
            self.logger.exception(f"Error reading Kasa device data: {e}")
            return {}

    async def send_data_to_sift(self, data: Dict[str, Any]) -> None:
        """Send Kasa device data to Sift.

        Args:
            data: Dictionary with channel names as keys and values as values
        """
        if not data:
            return

        self.logger.info(f"Got data from {self.name}: {data}")

        channel_values = []

        for channel_config in self.flow_config.channels:
            channel_name = channel_config.name
            value = data.get(channel_name)
            self.logger.debug(
                f"Channel {channel_name}: value={value}, type={type(value)}"
            )

            if value is None:
                self.logger.warning(f"No value for channel {channel_name}")
                continue

            channel_values.append(
                ChannelValue(
                    channel_name=channel_name,
                    value=channel_config.try_value_from(value),
                )
            )

        # Ingest data
        if channel_values:
            self.logger.debug(f"Ingesting channel values: {channel_values}")
            self.ingestion_service.try_ingest_flows(
                Flow(
                    flow_name=self.flow_config.name,
                    timestamp=datetime.now(timezone.utc),
                    channel_values=channel_values,
                )
            )
            self.logger.info(
                f"Data sent to Sift for {self.name} with flow {self.flow_config.name}"
            )

    async def run(self) -> None:
        """Run the Kasa sensor reading task."""
        self.running = True
        self.logger.info(
            f"Starting Kasa sensor task for {self.name} with period {self.sample_period}s"
        )

        # Ensure connection to the device
        connected = await self.kasa_sensor.connect()
        if not connected:
            self.logger.error(f"Failed to connect to Kasa device {self.name}")
            self.running = False
            return

        while self.running:
            try:
                # Read sensor data
                data = await self.read_sensor_data()

                # Send data to Sift
                await self.send_data_to_sift(data)

            except asyncio.CancelledError:
                self.logger.info(f"Kasa sensor task for {self.name} cancelled")
                self.running = False
                break
            except Exception as e:
                self.logger.exception(f"Error in Kasa sensor task for {self.name}: {e}")

            # Wait for next reading
            self.logger.debug(f"Waiting {self.sample_period}s for next reading")
            await asyncio.sleep(self.sample_period)

    def start(self) -> None:
        """Start the Kasa sensor task."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run())

    def stop(self) -> None:
        """Stop the Kasa sensor task."""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()


async def build_telemetry_config(
    sensors_config: List[Dict[str, Any]],
    kasa_flow_configs: Optional[List[FlowConfig]] = None,
) -> Tuple[TelemetryConfig, Dict[str, FlowConfig]]:
    """Build a telemetry config based on available sensors.

    Args:
        sensors_config: List of sensor configurations
        kasa_flow_configs: Optional list of FlowConfig objects for Kasa devices

    Returns:
        Tuple of (TelemetryConfig for Sift, mapping of sensor names to flow names)
    """
    flows = []
    sensor_to_flow_map = {}  # Maps sensor names to their flow names

    # Create a flow for each sensor
    for sensor_config in sensors_config:
        sensor_type = sensor_config["type"]
        sensor_name = sensor_config["name"]

        # Skip Kasa devices (they are handled separately)
        if sensor_type == "Kasa":
            continue

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
        for channel_config in channels:
            logger.debug(
                f"Channel Config: {channel_config.name}, {channel_config.data_type}, {channel_config.unit}"
            )

        # Create a unique flow name and store it in the mapping
        flow_name = f"{sensor_name}-readings-{int(time.time())}"
        flow_config = FlowConfig(name=flow_name, channels=channels)

        # Add flow for this sensor
        flows.append(flow_config)
        sensor_to_flow_map[sensor_name] = flow_config

    # Add Kasa flows if provided
    if kasa_flow_configs:
        # Add each Kasa flow to our flow list
        for flow_config in kasa_flow_configs:
            flows.append(flow_config)
            # Extract the device name from the flow name
            device_name = flow_config.name.split("-readings-")[0]
            sensor_to_flow_map[device_name] = flow_config

    # Create the telemetry config
    telemetry_config = TelemetryConfig(
        asset_name=ASSET_NAME, ingestion_client_key=CONFIG_KEY, flows=flows
    )

    return telemetry_config, sensor_to_flow_map


async def setup_sift_connection() -> Tuple[
    IngestionService,
    SiftChannel,
    List[Dict[str, Any]],
    Dict[str, FlowConfig],
    List[KasaSensor],
]:
    """Set up connection to Sift.

    Returns:
        Tuple of (IngestionService, grpc_channel, sensors_config, sensor_to_flow_map, kasa_sensors)
    """
    if SIFT_API_KEY is None or BASE_URI is None:
        raise ValueError("SIFT_API_KEY and BASE_URI must be set")

    credentials = SiftChannelConfig(
        apikey=SIFT_API_KEY,
        uri=BASE_URI,
        use_ssl=USE_SSL,
    )
    logger.info(f"Connecting to Sift at {BASE_URI}")

    # Create the channel without using async with
    grpc_channel = use_sift_channel(credentials)

    # Load config file
    config_path = os.path.join(os.path.dirname(__file__), "config.yaml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    # Extract sensor configs
    sensors_config = config.get("sensors", [])
    logger.info(f"Found {len(sensors_config)} sensor configs in configuration file")

    # Setup Kasa devices telemetry if credentials are available
    kasa_sensors: List[KasaSensor] = []
    kasa_flow_configs: List[FlowConfig] = []
    if KASA_USERNAME and KASA_PASSWORD:
        kasa_credentials = kasa.Credentials(
            username=KASA_USERNAME, password=KASA_PASSWORD
        )
        logger.info("Setting up Kasa device telemetry")

        kasa_sensors, kasa_flow_configs = await setup_kasa_telemetry(
            config_path=config_path,
            credentials=kasa_credentials,
            device_name=None,
            logger=logger,
        )
        logger.info(f"Found {len(kasa_sensors)} Kasa devices")
    else:
        logger.warning(
            "Kasa credentials not found in environment variables, skipping Kasa device telemetry"
        )

    # Build telemetry config including both BLE and Kasa devices
    telemetry_config, sensor_to_flow_map = await build_telemetry_config(
        sensors_config, kasa_flow_configs
    )
    logger.info(f"Telemetry config: {telemetry_config}")

    # Create ingestion service
    ingestion_service = IngestionService(grpc_channel, telemetry_config)

    # Create a run
    run_name = f"{ASSET_NAME}.{datetime.now().timestamp():.0f}"
    logger.info(f"Creating run: {run_name}")
    ingestion_service.attach_run(grpc_channel, run_name)

    return (
        ingestion_service,
        grpc_channel,
        sensors_config,
        sensor_to_flow_map,
        kasa_sensors,
    )


def create_sensor_instances(
    sensors_config: List[Dict[str, Any]],
) -> List[Tuple[str, BluetoothSensor, int]]:
    """Create sensor instances from configuration.

    Args:
        sensors_config: List of sensor configurations

    Returns:
        List of tuples (sensor_name, sensor_instance, sample_period)
    """
    sensor_instances = []

    for sensor_config in sensors_config:
        sensor_type = sensor_config["type"]
        # Skip Kasa devices (handled separately)
        if sensor_type == "Kasa":
            continue

        if sensor_type in SENSOR_TYPES:
            sensor_class = SENSOR_TYPES[sensor_type]
            sensor_instance = sensor_class(
                sensor_config["serial_number"],
                name=sensor_config["name"],
                address=sensor_config.get("address", None),
                logger=logger,
            )
            sample_period = sensor_config.get("sample_period", 60)
            sensor_instances.append(
                (sensor_config["name"], sensor_instance, sample_period)
            )

    return sensor_instances


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
    parser.add_argument(
        "--skip-ble", action="store_true", help="Skip BLE sensor data collection"
    )
    parser.add_argument(
        "--skip-kasa", action="store_true", help="Skip Kasa device data collection"
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

    try:
        # Set up Sift connection
        (
            ingestion_service,
            grpc_channel,
            sensors_config,
            sensor_to_flow_map,
            kasa_sensors,
        ) = await setup_sift_connection()

        # Track all tasks
        all_tasks: List[BaseTask] = []

        # Create and start BLE sensor tasks if not skipped
        if not args.skip_ble:
            # Create sensor instances
            sensor_tuples = create_sensor_instances(sensors_config)

            if not sensor_tuples:
                logger.warning("No valid BLE sensor instances created")
            else:
                # Create and start sensor tasks
                for sensor_name, sensor_instance, sample_period in sensor_tuples:
                    flow_config = sensor_to_flow_map.get(sensor_name)
                    if flow_config is None:
                        logger.error(f"No flow mapping for sensor {sensor_name}")
                        continue

                    sensor_task: SensorTask = SensorTask(
                        sensor_name=sensor_name,
                        sensor_instance=sensor_instance,
                        ingestion_service=ingestion_service,
                        flow_config=flow_config,
                        sample_period=sample_period,
                        logger=logger,
                    )
                    sensor_task.start()
                    all_tasks.append(sensor_task)  # type: ignore
                    logger.info(f"Started task for BLE sensor {sensor_name}")
        else:
            logger.info("Skipping BLE sensor data collection")

        # Create and start Kasa sensor tasks if not skipped
        if not args.skip_kasa and kasa_sensors:
            for kasa_sensor in kasa_sensors:
                flow_config = sensor_to_flow_map.get(kasa_sensor.name)
                if flow_config is None:
                    logger.error(f"No flow mapping for Kasa sensor {kasa_sensor.name}")
                    continue

                kasa_task: KasaSensorTask = KasaSensorTask(
                    kasa_sensor=kasa_sensor,
                    ingestion_service=ingestion_service,
                    flow_config=flow_config,
                    logger=logger,
                )
                kasa_task.start()
                all_tasks.append(kasa_task)  # type: ignore
                logger.info(f"Started task for Kasa device {kasa_sensor.name}")
        else:
            if args.skip_kasa:
                logger.info("Skipping Kasa device data collection")
            elif not kasa_sensors:
                logger.warning("No Kasa devices found")

        if not all_tasks:
            logger.error("No sensor tasks created, nothing to do")
            return

        # Main loop - just wait for keyboard interrupt
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            # Stop all sensor tasks
            for task in all_tasks:  # type: ignore
                task.stop()

            # Wait for all tasks to finish with correct type annotation
            await asyncio.gather(
                *[task.task for task in all_tasks if task.task],
                return_exceptions=True,
            )

    except Exception as e:
        logger.exception(f"Error in main: {e}")
    finally:
        # Clean up the channel
        if "grpc_channel" in locals():
            logger.info("Closing Sift connection")
            grpc_channel.close()

        logger.info("Shutting down")


if __name__ == "__main__":
    asyncio.run(main())
