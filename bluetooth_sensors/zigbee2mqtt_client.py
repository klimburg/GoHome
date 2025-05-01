"""Module for handling MQTT communication with zigbee2mqtt bridge."""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import paho.mqtt.client as mqtt
import yaml
from sift_py.ingestion.channel import ChannelConfig, ChannelDataType
from sift_py.ingestion.config.telemetry import FlowConfig

logger = logging.getLogger(__name__)

# Type mapping from zigbee2mqtt to Sift ChannelDataType
ZIGBEE_TO_SIFT_TYPE = {
    "binary": ChannelDataType.BOOL,
    "numeric": ChannelDataType.DOUBLE,
    "enum": ChannelDataType.STRING,
    "text": ChannelDataType.STRING,
    "composite": ChannelDataType.STRING,  # We'll JSON serialize composite objects
    "list": ChannelDataType.STRING,  # We'll JSON serialize lists
    "switch": ChannelDataType.BOOL,  # Special case for switch type
}


class Zigbee2MQTTClient:
    """Client for communicating with zigbee2mqtt bridge over MQTT.

    This client handles:
    - Connecting to the MQTT broker
    - Discovering zigbee devices
    - Subscribing to device telemetry
    - Publishing commands to devices
    """

    def __init__(self, config_path: str) -> None:
        """Initialize the client with configuration.

        Args:
            config_path: Path to the config.yaml file containing MQTT settings
        """
        self.config_path = config_path
        self.client: Optional[mqtt.Client] = None
        self.devices: Dict[str, Dict[str, Any]] = {}
        self.channel_configs: List[ChannelConfig] = []
        self.channel_updates: Dict[str, List[Tuple[datetime, Any]]] = {}
        self._load_config()

    def _load_config(self) -> None:
        """Load MQTT configuration from config.yaml."""
        with open(self.config_path, "r") as f:
            config = yaml.safe_load(f)

        # Find zigbee2mqtt sensor config
        zigbee_config = next(
            (sensor for sensor in config["sensors"] if sensor["type"] == "zigbee2mqtt"),
            None,
        )

        if not zigbee_config:
            raise ValueError("No zigbee2mqtt configuration found in config.yaml")

        # Parse host and port from address
        host, port = zigbee_config["address"].split(":")
        self.host = host
        self.port = int(port)
        self.sample_period = zigbee_config.get("sample_period", 60)

    def connect(self) -> None:
        """Connect to the MQTT broker and setup callbacks."""
        self.client = mqtt.Client()
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

        try:
            self.client.connect(self.host, self.port)
            self.client.loop_start()
        except Exception as e:
            logger.error(f"Failed to connect to MQTT broker: {e}")
            raise

    def _on_connect(
        self, client: mqtt.Client, userdata: Any, flags: Dict[str, int], rc: int
    ) -> None:
        """Callback for when the client connects to the broker."""
        if rc == 0:
            logger.info("Connected to MQTT broker")
            # Subscribe to bridge/devices topic for device discovery
            client.subscribe("zigbee2mqtt/bridge/devices")
            # Subscribe to all device topics
            client.subscribe("zigbee2mqtt/#")
        else:
            logger.error(f"Failed to connect to MQTT broker with code: {rc}")

    def _on_message(
        self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage
    ) -> None:
        """Callback for when a message is received."""
        try:
            payload = json.loads(msg.payload)
            if msg.topic == "zigbee2mqtt/bridge/devices":
                self._process_devices(payload)
            else:
                # Handle device state updates
                device_name = msg.topic.split("/")[1]
                if device_name in self.devices:
                    self._process_device_update(device_name, payload)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse MQTT message: {e}")

    def _process_devices(self, devices: List[Dict[str, Any]]) -> None:
        """Process the device discovery payload and store device info."""
        for device in devices:
            if not device.get("definition") or not device.get("friendly_name"):
                continue

            friendly_name = device["friendly_name"]
            self.devices[friendly_name] = device

            # Process telemetry channels
            if "exposes" in device["definition"]:
                for expose in device["definition"]["exposes"]:
                    if "features" in expose:
                        # Handle features within an expose
                        for feature in expose["features"]:
                            self._process_feature(friendly_name, feature)
                    else:
                        # Handle direct expose properties
                        self._process_feature(friendly_name, expose)

    def _process_feature(self, friendly_name: str, feature: Dict[str, Any]) -> None:
        """Process a single feature/expose and add it to channel configs.

        Args:
            friendly_name: The friendly name of the device
            feature: The feature/expose definition from zigbee2mqtt
        """
        # Skip diagnostic features
        if feature.get("category") == "diagnostic":
            return

        channel_name = f"zigbee2mqtt.{friendly_name}.{feature['name']}"

        # Map zigbee type to sift type
        zigbee_type = feature.get("type", "")
        sift_type = ZIGBEE_TO_SIFT_TYPE.get(zigbee_type)
        if not sift_type:
            logger.warning(f"Unknown type {zigbee_type} for channel {channel_name}")
            return

        # Create channel config
        channel_config = ChannelConfig(
            name=channel_name,
            data_type=sift_type,
            unit=feature.get("unit", ""),
            description=feature.get("description", ""),
        )

        self.channel_configs.append(channel_config)
        logger.info(f"Added channel config for {channel_name}")

    def _process_device_update(self, device_name: str, payload: Dict[str, Any]) -> None:
        """Process a device state update message.

        Args:
            device_name: Name of the device that was updated
            payload: The update payload
        """
        # Store the latest values for each channel
        for key, value in payload.items():
            channel_name = f"zigbee2mqtt.{device_name}.{key}"
            timestamp = datetime.now(timezone.utc)

            # Store the raw value in devices for backward compatibility
            self.devices[device_name][key] = value

            # Store the value with timestamp in channel_updates
            if channel_name not in self.channel_updates:
                self.channel_updates[channel_name] = []
            self.channel_updates[channel_name].append((timestamp, value))

    def generate_flow_config(self) -> FlowConfig:
        """Generate a FlowConfig for all discovered channels.

        Returns:
            FlowConfig containing all discovered channels
        """
        flow_name = f"zigbee2mqtt-{int(time.time())}"
        return FlowConfig(name=flow_name, channels=self.channel_configs)

    def get_channel_data(self) -> Dict[str, Dict[str, Any]]:
        """Get current data for all channels.

        Returns:
            Dictionary mapping channel names to their current values and update history
        """
        data = {}
        for device_name, device_data in self.devices.items():
            for key, value in device_data.items():
                if key not in ["definition", "friendly_name"]:  # Skip metadata
                    channel_name = f"zigbee2mqtt.{device_name}.{key}"
                    updates = self.channel_updates.get(channel_name, [])
                    data[channel_name] = {"value": value, "updates": updates}
        return data

    def disconnect(self) -> None:
        """Disconnect from the MQTT broker."""
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()

    def clear_channel_updates(self) -> None:
        """Clear the channel update history."""
        self.channel_updates.clear()
