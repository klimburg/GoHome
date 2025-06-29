#!/usr/bin/env python3
"""
Task classes for reading data from various sensors and uploading to Sift.
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from sift_py.ingestion.channel import ChannelValue
from sift_py.ingestion.config.telemetry import FlowConfig
from sift_py.ingestion.flow import Flow
from sift_py.ingestion.service import IngestionService

from bluetooth_sensors.sensors import tesla_wall_connector
from bluetooth_sensors.sensors._ble_sensor import BluetoothSensor
from bluetooth_sensors.sensors.kasa_sensors import KasaSensor


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
        flow_queue: asyncio.Queue[Flow],
        flow_config: FlowConfig,
        sample_period: int = 60,
        logger: Optional[logging.Logger] = None,
        bt_mutex: Optional[asyncio.Lock] = None,
    ) -> None:
        """Initialize the sensor task.

        Args:
            sensor_name: Name of the sensor
            sensor_instance: Instance of a BluetoothSensor
            flow_queue: Queue to put formatted flow data into
            flow_config: Flow configuration for this sensor
            sample_period: Time in seconds between readings
            logger: Optional logger instance
            bt_mutex: Optional mutex for BLE operations
        """
        self.sensor_name = sensor_name
        self.sensor_instance = sensor_instance
        self.flow_queue = flow_queue
        self.flow_config = flow_config
        self.sample_period = sample_period
        self.logger = logger or logging.getLogger(__name__)
        self.bt_mutex = bt_mutex or asyncio.Lock()
        self.running = False

    async def read_sensor_data(self) -> Dict[str, Any]:
        """Read data from the sensor with mutex protection."""
        # Use mutex to ensure only one Bluetooth operation at a time
        async with self.bt_mutex:
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

    async def add_data_to_flow_queue(self, data: Dict[str, Any]) -> None:
        """Format sensor data as a Flow and add to the flow queue.

        Args:
            data: Dictionary with channel names as keys and values as values
        """
        if not data:
            return

        self.logger.debug(f"Got data from {self.sensor_name}: {data}")

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

        # Create flow and add to queue
        if channel_values:
            flow = Flow(
                flow_name=self.flow_config.name,
                timestamp=datetime.now(timezone.utc),
                channel_values=channel_values,
            )

            await self.flow_queue.put(flow)
            self.logger.debug(
                f"Flow data queued for {self.sensor_name} with flow {self.flow_config.name}"
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

                # Format and queue data
                await self.add_data_to_flow_queue(data)

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
        flow_queue: asyncio.Queue[Flow],
        flow_config: FlowConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Kasa sensor task.

        Args:
            kasa_sensor: KasaSensor instance
            flow_queue: Queue to put formatted flow data into
            flow_config: Flow configuration for this sensor
            logger: Optional logger instance
        """
        self.kasa_sensor = kasa_sensor
        self.name = kasa_sensor.name
        self.sample_period = kasa_sensor.sample_period
        self.flow_queue = flow_queue
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

    async def add_data_to_flow_queue(self, data: Dict[str, Any]) -> None:
        """Format Kasa device data as a Flow and add to the flow queue.

        Args:
            data: Dictionary with channel names as keys and values as values
        """
        if not data:
            return

        self.logger.debug(f"Got data from {self.name}: {data}")

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

        # Create flow and add to queue
        if channel_values:
            flow = Flow(
                flow_name=self.flow_config.name,
                timestamp=datetime.now(timezone.utc),
                channel_values=channel_values,
            )

            await self.flow_queue.put(flow)
            self.logger.debug(
                f"Flow data queued for {self.name} with flow {self.flow_config.name}"
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

                # Format and queue data
                await self.add_data_to_flow_queue(data)

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


class TeslaWallConnectorTask(BaseTask):
    """Task for reading from a Tesla Wall Connector at a specified interval."""

    def __init__(
        self,
        sensor_name: str,
        sensor_instance: tesla_wall_connector.TeslaWallConnector,
        flow_queue: asyncio.Queue[Flow],
        flow_configs: Dict[
            str, FlowConfig
        ],  # One for each endpoint (vitals, lifetime, wifi)
        sample_period: int = 60,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Tesla Wall Connector task.

        Args:
            sensor_name: Name of the sensor
            sensor_instance: Instance of a TeslaWallConnector
            flow_queue: Queue to put formatted flow data into
            flow_configs: Dictionary mapping endpoint names to their FlowConfig
            sample_period: Time in seconds between readings
            logger: Optional logger instance
        """
        self.sensor_name = sensor_name
        self.sensor_instance = sensor_instance
        self.flow_queue = flow_queue
        self.flow_configs = flow_configs
        self.sample_period = sample_period
        self.logger = logger or logging.getLogger(__name__)
        self.running = False

    async def read_sensor_data(self) -> Dict[str, Dict[str, Any]]:
        """Read data from the Tesla Wall Connector.

        Returns:
            Dictionary with data from the Tesla Wall Connector
        """
        try:
            # Connect to sensor
            connected = await self.sensor_instance.connect(retries=3)
            if not connected:
                self.logger.error(
                    f"Failed to connect to Tesla Wall Connector {self.sensor_name}"
                )
                return {}

            # Read data
            success = await self.sensor_instance.read()

            # Get sensor data
            if success:
                # Get the common get_channel_data method and extract the values
                return self.sensor_instance.get_channel_data()
            else:
                self.logger.warning(
                    f"Failed to read from Tesla Wall Connector {self.sensor_name}"
                )
                return {}

        except Exception as e:
            self.logger.exception(f"Error reading Tesla Wall Connector data: {e}")
            return {}
        finally:
            # Disconnect from sensor
            await self.sensor_instance.disconnect()

    async def add_data_to_flow_queue(self, data: Dict[str, Dict[str, Any]]) -> None:
        """Format sensor data as Flows and add to the flow queue.

        Args:
            data: Dictionary with channel names as keys and values as values
        """
        if not data:
            return

        self.logger.debug(f"Got data from {self.sensor_name}: {len(data)} channels")
        self.logger.debug(f"Data: {data}")

        # Group data by endpoint (vitals, lifetime, wifi)
        endpoint_data: Dict[str, List[ChannelValue]] = {
            "vitals": [],
            "lifetime": [],
            "wifi": [],
        }

        # Organize channel values by endpoint
        for channel_name, channel_info in data.items():
            # Extract endpoint from channel name (format: sensor_name.endpoint.field_name)
            parts = channel_name.split(".")
            if len(parts) < 3:
                self.logger.warning(f"Unexpected channel name format: {channel_name}")
                continue

            endpoint = parts[1]  # vitals, lifetime, or wifi
            if endpoint not in endpoint_data:
                self.logger.warning(f"Unknown endpoint: {endpoint}")
                continue

            value = channel_info["value"]

            # Find the corresponding flow config
            flow_config_key = f"{self.sensor_name}.{endpoint}"
            flow_config = self.flow_configs.get(flow_config_key)
            if not flow_config:
                self.logger.warning(f"No flow config for {flow_config_key}")
                continue

            # Find the matching channel config
            channel_config = None
            for config in flow_config.channels:
                if config.name == channel_name:
                    channel_config = config
                    break

            if not channel_config:
                self.logger.warning(f"No channel config for {channel_name}")
                continue

            endpoint_data[endpoint].append(
                ChannelValue(
                    channel_name=channel_name,
                    value=channel_config.try_value_from(value),
                )
            )

        # Create flow for each endpoint and add to queue
        for endpoint, channel_values in endpoint_data.items():
            if not channel_values:
                continue

            flow_config_key = f"{self.sensor_name}.{endpoint}"
            flow_config = self.flow_configs.get(flow_config_key)
            if not flow_config:
                continue

            self.logger.debug(
                f"Creating flow with {len(channel_values)} values for {flow_config_key}"
            )

            flow = Flow(
                flow_name=flow_config.name,
                timestamp=datetime.now(timezone.utc),
                channel_values=channel_values,
            )

            await self.flow_queue.put(flow)
            self.logger.debug(
                f"Flow data queued for {self.sensor_name} {endpoint} flow"
            )

    async def run(self) -> None:
        """Run the Tesla Wall Connector reading task."""
        self.running = True
        self.logger.info(
            f"Starting Tesla Wall Connector task for {self.sensor_name} with period {self.sample_period}s"
        )

        while self.running:
            try:
                # Read sensor data
                data = await self.read_sensor_data()

                # Format and queue data
                await self.add_data_to_flow_queue(data)

            except asyncio.CancelledError:
                self.logger.info(
                    f"Tesla Wall Connector task for {self.sensor_name} cancelled"
                )
                self.running = False
                break
            except Exception as e:
                self.logger.exception(
                    f"Error in Tesla Wall Connector task for {self.sensor_name}: {e}"
                )

            # Wait for next reading
            self.logger.debug(f"Waiting {self.sample_period}s for next reading")
            await asyncio.sleep(self.sample_period)

    def start(self) -> None:
        """Start the Tesla Wall Connector task."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run())

    def stop(self) -> None:
        """Stop the Tesla Wall Connector task."""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()


class FlowIngestionTask(BaseTask):
    """Task for consuming flows from a queue and ingesting them to one or more services."""

    def __init__(
        self,
        flow_queue: asyncio.Queue[Flow],
        ingestion_services: Dict[str, IngestionService],
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the flow ingestion task.

        Args:
            flow_queue: Queue to consume Flow objects from
            ingestion_services: Dictionary mapping environment names to ingestion services
            logger: Optional logger instance
        """
        self.flow_queue = flow_queue
        self.ingestion_services = ingestion_services
        self.logger = logger or logging.getLogger(__name__)
        self.running = False

        # Initialize counters for success and errors per service/environment
        self.success_counts: Dict[str, int] = {
            env_name: 0 for env_name in ingestion_services.keys()
        }
        self.error_counts: Dict[str, int] = {
            env_name: 0 for env_name in ingestion_services.keys()
        }

    def get_stats(self) -> Dict[str, Dict[str, int]]:
        """Get statistics about flow ingestion per service.

        Returns:
            Dictionary mapping environment names to dictionaries with success_count and error_count
        """
        stats = {}

        # Since we now have environment names as keys, we can directly use them
        for env_name in self.ingestion_services.keys():
            stats[env_name] = {
                "success_count": self.success_counts[env_name],
                "error_count": self.error_counts[env_name],
            }

        return stats

    async def run(self) -> None:
        """Run the flow ingestion task."""
        self.running = True
        self.logger.info(
            f"Starting flow ingestion task with {len(self.ingestion_services)} services"
        )

        errored_envs: Set[str] = set()
        while self.running:
            try:
                # Get a flow from the queue
                flow = await self.flow_queue.get()
                # Process the flow by sending to all ingestion services
                for env_name, service in self.ingestion_services.items():
                    try:
                        service.try_ingest_flows(flow)
                        # Increment success counter for this environment
                        self.success_counts[env_name] += 1
                        if env_name in errored_envs:
                            errored_envs.remove(env_name)
                    except Exception as e:
                        # Increment error counter for this environment
                        self.error_counts[env_name] += 1
                        if env_name not in errored_envs:
                            self.logger.error(
                                f"Error ingesting flow to {env_name} service",
                                exc_info=e,
                            )
                            errored_envs.add(env_name)

                self.flow_queue.task_done()

            except asyncio.CancelledError:
                self.logger.info("Flow ingestion task cancelled")
                self.running = False
                break
            except Exception as e:
                self.logger.exception(f"Error in flow ingestion task: {e}")

    def start(self) -> None:
        """Start the flow ingestion task."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(self.run())

    def stop(self) -> None:
        """Stop the flow ingestion task."""
        self.running = False
        if self.task and not self.task.done():
            self.task.cancel()
