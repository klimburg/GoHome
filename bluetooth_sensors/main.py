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
from typing import Any, Dict, List, Optional, Tuple, Union

import kasa
import yaml
from dotenv import load_dotenv
from sift_py.grpc.transport import SiftChannel, SiftChannelConfig, use_sift_channel
from sift_py.ingestion.channel import (
    ChannelConfig,
    ChannelDataType,
    ChannelValue,
    double_value,
    int32_value,
    string_value,
)
from sift_py.ingestion.config.telemetry import TelemetryConfig
from sift_py.ingestion.flow import Flow, FlowConfig
from sift_py.ingestion.service import IngestionService

# Import sensor classes
from bluetooth_sensors.sensors import gvh5100, tesla_wall_connector, wave_plus
from bluetooth_sensors.sensors._ble_sensor import BluetoothSensor

# Import Kasa sensor class
from bluetooth_sensors.sensors.kasa import KasaSensor, setup_kasa_telemetry

# Import task classes
from bluetooth_sensors.tasks import (
    BaseTask,
    KasaSensorTask,
    SensorTask,
    TeslaWallConnectorTask,
)

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
    "TeslaWallConnector": tesla_wall_connector.TeslaWallConnector,
}

# Map of sensor types to their channel definitions
SENSOR_CHANNELS = {
    "WavePlus": wave_plus.CHANNELS,
    "GVH5100": gvh5100.CHANNELS,
}

# Global mutex for Bluetooth operations
BT_MUTEX = asyncio.Lock()

MAIN_PROC_UPDATE_INTERVAL = 10


class SiftLogHandler(logging.Handler):
    """A logging handler that sends log messages to Sift."""

    def __init__(
        self, ingestion_service: IngestionService, flow_name: str, channel_name: str
    ) -> None:
        """Initialize the SiftLogHandler.

        Args:
            ingestion_service: The Sift ingestion service
            flow_name: The flow name to use for log messages
            channel_name: The channel name to use for log messages
        """
        super().__init__()
        self.ingestion_service = ingestion_service
        self.flow_name = flow_name
        self.channel_name = channel_name
        self.setLevel(logging.INFO)
        self.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))

    def emit(self, record: logging.LogRecord) -> None:
        """Send the log record to Sift.

        Args:
            record: The log record to send
        """
        try:
            msg = self.format(record)
            self.ingestion_service.try_ingest_flows(
                Flow(
                    flow_name=self.flow_name,
                    timestamp=datetime.now(timezone.utc),
                    channel_values=[
                        ChannelValue(
                            channel_name=self.channel_name,
                            value=string_value(msg),
                        )
                    ],
                )
            )
        except Exception:
            self.handleError(record)


async def create_main_process_flow(asset_name: str) -> FlowConfig:
    """Create a flow config for the main process.

    Args:
        asset_name: The asset name

    Returns:
        FlowConfig for the main process
    """
    # Create channels for main process
    log_channel = ChannelConfig(
        name="log_message",
        data_type=ChannelDataType.STRING,
        unit="",
    )

    task_count_channel = ChannelConfig(
        name="active_tasks",
        data_type=ChannelDataType.INT_32,
        unit="count",
    )

    uptime_channel = ChannelConfig(
        name="uptime",
        data_type=ChannelDataType.DOUBLE,
        unit="seconds",
    )

    # Create flow for main process
    flow_name = f"{asset_name}-main-process-{int(time.time())}"

    return FlowConfig(
        name=flow_name,
        channels=[log_channel, task_count_channel, uptime_channel],
    )


async def update_main_proc_telem(
    ingestion_service: IngestionService,
    flow_name: str,
    tasks: List[BaseTask],
    start_time: float,
) -> None:
    """Update the task count in Sift.

    Args:
        ingestion_service: The Sift ingestion service
        flow_name: The flow name
        tasks: The list of tasks
    """
    active_count = sum(1 for task in tasks if task.task and not task.task.done())

    ingestion_service.try_ingest_flows(
        Flow(
            flow_name=flow_name,
            timestamp=datetime.now(timezone.utc),
            channel_values=[
                ChannelValue(
                    channel_name="active_tasks",
                    value=int32_value(active_count),
                ),
                ChannelValue(
                    channel_name="uptime",
                    value=double_value(time.time() - start_time),
                ),
            ],
        )
    )


async def build_telemetry_config(
    sensors_config: List[Dict[str, Any]],
    kasa_flow_configs: Optional[List[FlowConfig]] = None,
    main_flow_config: Optional[FlowConfig] = None,
) -> Tuple[TelemetryConfig, Dict[str, FlowConfig]]:
    """Build a telemetry config based on available sensors.

    Args:
        sensors_config: List of sensor configurations
        kasa_flow_configs: Optional list of FlowConfig objects for Kasa devices
        main_flow_config: Optional FlowConfig for the main process

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

        # Handle Tesla Wall Connector differently - it dynamically discovers channels
        if sensor_type == "TeslaWallConnector":
            # Create an instance so we can query capabilities
            address = sensor_config.get("address")
            if not address:
                logger.error(f"No address for Tesla Wall Connector {sensor_name}")
                continue

            # Create the sensor instance
            sensor_instance = tesla_wall_connector.TeslaWallConnector(
                serial_number="",  # Not important for this device
                name=sensor_name,
                address=address,
                logger=logger,
            )

            # Connect and read data to discover channels
            try:
                await sensor_instance.connect()
                success = await sensor_instance.read()
                if not success:
                    logger.error(
                        f"Failed to read data from Tesla Wall Connector {sensor_name}"
                    )
                    await sensor_instance.disconnect()
                    continue

                # Get discovered channels
                discovered_channels = sensor_instance.get_discovered_channels()
                await sensor_instance.disconnect()

                # Create a flow for each endpoint (vitals, lifetime, wifi)
                for endpoint, channel_list in discovered_channels.items():
                    if not channel_list:
                        logger.warning(
                            f"No channels discovered for {sensor_name} {endpoint}"
                        )
                        continue

                    # Create channels for this endpoint
                    channels = []
                    for channel in channel_list:
                        name = channel["name"]
                        unit = channel["unit"]
                        channels.append(
                            ChannelConfig(
                                name=f"{sensor_name}.{endpoint}.{name}",
                                data_type=channel["sift_type"],
                                unit=unit,
                            )
                        )

                    # Create a unique flow name for this endpoint
                    flow_name = f"{sensor_name}-{endpoint}-{int(time.time())}"
                    flow_config = FlowConfig(name=flow_name, channels=channels)

                    # Add flow for this endpoint
                    flows.append(flow_config)

                    # Add to mapping - use endpoint as part of key
                    sensor_to_flow_map[f"{sensor_name}.{endpoint}"] = flow_config

                    logger.info(
                        f"Created flow {flow_name} with {len(channels)} channels for {sensor_name} {endpoint}"
                    )
            except Exception as e:
                logger.exception(
                    f"Error discovering Tesla Wall Connector channels: {e}"
                )
                continue

        # Handle regular sensors
        elif sensor_type in SENSOR_CHANNELS:
            # Regular handling for standard sensors (non-dictionary channel structure)
            channels = []

            # We need to check if the CHANNELS is a list or a dict
            channel_data = SENSOR_CHANNELS[sensor_type]

            if isinstance(channel_data, list):
                # Standard format where CHANNELS is a list of channel dicts
                for channel in channel_data:
                    name = channel["name"]  # type: ignore
                    unit = channel["unit"]  # type: ignore
                    channels.append(
                        ChannelConfig(
                            name=f"{sensor_name}.{name}",
                            data_type=channel["sift_type"],  # type: ignore
                            unit=unit,
                        )
                    )

                # Create a unique flow name and store it in the mapping
                flow_name = f"{sensor_name}-readings-{int(time.time())}"
                flow_config = FlowConfig(name=flow_name, channels=channels)

                # Add flow for this sensor
                flows.append(flow_config)
                sensor_to_flow_map[sensor_name] = flow_config
            else:
                logger.warning(f"Unsupported channel format for {sensor_type}")
                continue
        else:
            logger.warning(f"No channel definitions for sensor type: {sensor_type}")
            continue

    # Add Kasa flows if provided
    if kasa_flow_configs:
        # Add each Kasa flow to our flow list
        for flow_config in kasa_flow_configs:
            flows.append(flow_config)
            # Extract the device name from the flow name
            device_name = flow_config.name.split("-readings-")[0]
            sensor_to_flow_map[device_name] = flow_config

    # Add main process flow if provided
    if main_flow_config:
        flows.append(main_flow_config)
        logger.debug(f"Added main process flow: {main_flow_config.name}")

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
    str,  # Add main flow name as a return value
]:
    """Set up connection to Sift.

    Returns:
        Tuple of (IngestionService, grpc_channel, sensors_config, sensor_to_flow_map, kasa_sensors, main_flow_name)
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

    # Create main process flow config
    main_flow_config = await create_main_process_flow(ASSET_NAME)
    main_flow_name = main_flow_config.name

    # Build telemetry config including both BLE, Kasa devices, and main process
    telemetry_config, sensor_to_flow_map = await build_telemetry_config(
        sensors_config, kasa_flow_configs, main_flow_config
    )
    logger.info(f"Built telemetry config with {len(telemetry_config.flows)} flows")

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
        main_flow_name,  # Return the main flow name
    )


def create_sensor_instances(
    sensors_config: List[Dict[str, Any]],
) -> List[
    Tuple[str, Union[BluetoothSensor, tesla_wall_connector.TeslaWallConnector], int]
]:
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

            # Handle Tesla Wall Connector differently - it doesn't use serial_number in the same way
            if sensor_type == "TeslaWallConnector":
                serial_number = sensor_config.get("serial_number", "")
                sensor_instance = sensor_class(
                    serial_number=serial_number,
                    name=sensor_config["name"],
                    address=sensor_config.get("address", None),
                    logger=logger,
                )
            else:
                # Regular handling for other sensor types
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
    start_time = time.time()

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
            main_flow_name,  # Get the main flow name
        ) = await setup_sift_connection()

        # Add Sift log handler
        sift_log_handler = SiftLogHandler(
            ingestion_service=ingestion_service,
            flow_name=main_flow_name,
            channel_name="log_message",
        )
        logger.addHandler(sift_log_handler)
        logging.getLogger("bluetooth_sensors").addHandler(sift_log_handler)

        logger.info("Main process flow created and log handler added")

        # Track all tasks
        all_tasks: List[BaseTask] = []

        # Create and start BLE sensor tasks if not skipped
        if not args.skip_ble:
            # Create sensor instances
            sensor_tuples = create_sensor_instances(sensors_config)

            if not sensor_tuples:
                logger.warning("No valid sensor instances created")
            else:
                # Create and start sensor tasks
                for sensor_name, sensor_instance, sample_period in sensor_tuples:
                    # Handle Tesla Wall Connector differently
                    if isinstance(
                        sensor_instance, tesla_wall_connector.TeslaWallConnector
                    ):
                        # Get the flow configs for each endpoint (vitals, lifetime, wifi)
                        tesla_flow_configs = {
                            f"{sensor_name}.vitals": sensor_to_flow_map.get(
                                f"{sensor_name}.vitals"
                            ),
                            f"{sensor_name}.lifetime": sensor_to_flow_map.get(
                                f"{sensor_name}.lifetime"
                            ),
                            f"{sensor_name}.wifi": sensor_to_flow_map.get(
                                f"{sensor_name}.wifi"
                            ),
                        }

                        # Check if we have all needed flow configs
                        missing_configs = [
                            k for k, v in tesla_flow_configs.items() if v is None
                        ]
                        if missing_configs:
                            logger.error(
                                f"Missing flow configs for Tesla Wall Connector {sensor_name}: {missing_configs}"
                            )
                            continue

                        # Create Tesla Wall Connector task
                        tesla_task: TeslaWallConnectorTask = TeslaWallConnectorTask(
                            sensor_name=sensor_name,
                            sensor_instance=sensor_instance,
                            ingestion_service=ingestion_service,
                            flow_configs=tesla_flow_configs,  # type: ignore
                            sample_period=sample_period,
                            logger=logger,
                        )
                        tesla_task.start()
                        all_tasks.append(tesla_task)  # type: ignore
                        logger.info(
                            f"Started task for Tesla Wall Connector {sensor_name}"
                        )
                    else:
                        # Regular handling for BLE sensors
                        flow_config = sensor_to_flow_map.get(sensor_name)
                        if flow_config is None:
                            logger.error(f"No flow mapping for sensor {sensor_name}")
                            continue

                        sensor_task: SensorTask = SensorTask(
                            sensor_name=sensor_name,
                            sensor_instance=sensor_instance,  # type: ignore
                            ingestion_service=ingestion_service,
                            flow_config=flow_config,
                            sample_period=sample_period,
                            logger=logger,
                            bt_mutex=BT_MUTEX,  # Pass the mutex
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

        # Send initial task count
        await update_main_proc_telem(
            ingestion_service, main_flow_name, all_tasks, start_time
        )
        logger.info(f"Initial task count: {len(all_tasks)}")

        # Main loop - just wait for keyboard interrupt
        try:
            last_task_count_update = time.time()

            while True:
                await asyncio.sleep(1)

                # Update task count periodically
                current_time = time.time()
                if current_time - last_task_count_update >= MAIN_PROC_UPDATE_INTERVAL:
                    await update_main_proc_telem(
                        ingestion_service, main_flow_name, all_tasks, start_time
                    )
                    last_task_count_update = current_time
                    logger.debug(f"Updated task count: {len(all_tasks)}")

        except KeyboardInterrupt:
            logger.info("Interrupted by user")
        finally:
            # Send final task count
            await update_main_proc_telem(
                ingestion_service, main_flow_name, all_tasks, start_time
            )
            logger.info("Shutting down tasks")

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
        # Remove the Sift log handler to avoid errors during shutdown
        if "sift_log_handler" in locals():
            logger.removeHandler(sift_log_handler)
            logging.getLogger("bluetooth_sensors").removeHandler(sift_log_handler)

        # Clean up the channel
        if "grpc_channel" in locals():
            logger.info("Closing Sift connection")
            grpc_channel.close()

        logger.info("Shutting down")


if __name__ == "__main__":
    asyncio.run(main())
