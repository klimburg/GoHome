import asyncio
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

import dotenv
import kasa
import yaml
from sift_py.ingestion.channel import ChannelConfig, ChannelDataType
from sift_py.ingestion.config.telemetry import FlowConfig

# Setup logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(CURRENT_DIR)

print(PARENT_DIR)

# Map of Python types to Sift data types using proper enum values
TYPE_MAPPING = {
    bool: ChannelDataType.BOOL,
    int: ChannelDataType.INT_32,
    float: ChannelDataType.DOUBLE,
}


class KasaSensor:
    """Class to interact with a Kasa device and extract telemetry data."""

    def __init__(
        self,
        config: Dict[str, Any],
        credentials: kasa.Credentials,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the KasaSensor.

        Args:
            config: Configuration for the sensor from config.yaml
            credentials: Kasa credentials
            logger: Optional logger instance
        """
        self.config = config
        self.name = config.get("name", "UnknownKasaDevice")
        self.address = config.get("address")
        self.model = config.get("model", "Unknown")
        self.sample_period = config.get("sample_period", 60)
        self.credentials = credentials
        self.device_instance: Optional[kasa.SmartDevice] = None
        self.features: Dict[str, Dict[str, Any]] = {}
        self.logger = logger or logging.getLogger(__name__)
        self.channel_configs: List[ChannelConfig] = []

    async def connect(self) -> bool:
        """Connect to the Kasa device.

        Returns:
            True if successful, False otherwise
        """
        if not self.address:
            self.logger.error(f"No address specified for device {self.name}")
            return False

        try:
            self.device_instance = await kasa.Discover.discover_single(
                self.address,
                credentials=self.credentials,
            )

            if self.device_instance is None:
                self.logger.error(f"Device not found: {self.name} at {self.address}")
                return False

            await self.device_instance.update()
            self.logger.info(
                f"Connected to {self.name} ({self.device_instance.alias}) at {self.address}"
            )
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to {self.name} at {self.address}: {e}")
            return False

    async def discover_features(self) -> Dict[str, Dict[str, Any]]:
        """Discover available features of the device.

        Returns:
            Dictionary of feature names to feature info
        """
        if not self.device_instance:
            self.logger.error(f"Device {self.name} not connected")
            return {}

        try:
            # Make sure we have the latest data
            await self.device_instance.update()

            device_features = {}
            # Explore the features
            for feature_name in self.device_instance.features:
                feature = self.device_instance.features.get(feature_name)
                if feature is None:
                    continue

                # Get feature type
                value_type = type(feature.value)
                self.logger.debug(f"Feature: {feature_name} = {feature.value} (type: {value_type})")

                # Only include supported types
                if value_type in TYPE_MAPPING:
                    device_features[feature_name] = {
                        "value": feature.value,
                        "unit": feature.unit,
                        "value_type": value_type,
                        "sift_type": TYPE_MAPPING.get(value_type),
                    }
                    self.logger.debug(
                        f"{self.name} feature: {feature_name} = {feature.value} {feature.unit} (type: {value_type})"
                    )

            self.features = device_features
            return device_features

        except Exception as e:
            self.logger.error(f"Error discovering features for {self.name}: {e}")
            return {}

    def generate_channel_configs(self) -> List[ChannelConfig]:
        """Generate Sift channel configs for this device.

        Returns:
            List of ChannelConfig objects
        """
        channel_configs = []

        for feature_name, feature_info in self.features.items():
            sift_type = feature_info.get("sift_type")
            if not sift_type:
                continue

            channel_name = f"{self.name}.{feature_name}"
            unit = feature_info.get("unit", "")

            channel_config = ChannelConfig(
                name=channel_name,
                data_type=sift_type,
                unit=unit,
            )
            channel_configs.append(channel_config)

        self.channel_configs = channel_configs
        return channel_configs

    async def get_channel_data(self) -> Dict[str, Dict[str, Any]]:
        """Get current data for all channels.

        Returns:
            Dictionary mapping channel names to channel data
        """
        if not self.device_instance:
            self.logger.error(f"Device {self.name} not connected")
            return {}

        try:
            # Update device state
            await self.device_instance.update()

            data = {}
            # Get values for each feature
            for feature_name, feature_info in self.features.items():
                feature = self.device_instance.features.get(feature_name)
                if feature is None:
                    continue

                channel_name = f"{self.name}.{feature_name}"
                data[channel_name] = {
                    "value": feature.value,
                    "unit": feature.unit,
                }

            return data

        except Exception as e:
            self.logger.error(f"Error getting channel data for {self.name}: {e}")
            return {}


async def load_kasa_devices(
    config_path: str,
    credentials: kasa.Credentials,
    logger: Optional[logging.Logger] = None,
) -> List[KasaSensor]:
    """Load Kasa devices from config file.

    Args:
        config_path: Path to the config file
        credentials: Kasa credentials
        logger: Optional logger instance

    Returns:
        List of KasaSensor objects
    """
    logger = logger or logging.getLogger(__name__)

    try:
        with open(config_path, "r") as f:
            config = yaml.safe_load(f)

        kasa_devices = []

        # Filter only Kasa type devices
        if "sensors" in config and isinstance(config["sensors"], list):
            kasa_configs = [
                device for device in config["sensors"] if device.get("type") == "Kasa"
            ]

            # Create KasaSensor objects
            for device_config in kasa_configs:
                kasa_device = KasaSensor(device_config, credentials, logger)
                kasa_devices.append(kasa_device)

        return kasa_devices

    except FileNotFoundError:
        logger.error(f"Config file not found: {config_path}")
        return []
    except yaml.YAMLError as e:
        logger.error(f"Error parsing config file: {e}")
        return []


async def setup_kasa_telemetry(
    config_path: str,
    credentials: kasa.Credentials,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[KasaSensor], List[FlowConfig]]:
    """Set up telemetry for Kasa devices.

    Args:
        config_path: Path to the config file
        credentials: Kasa credentials
        logger: Optional logger instance

    Returns:
        Tuple of (list of KasaSensor objects, list of FlowConfig objects)
    """
    logger = logger or logging.getLogger(__name__)

    # Load devices from config
    kasa_devices = await load_kasa_devices(config_path, credentials, logger)

    if not kasa_devices:
        logger.warning("No Kasa devices found in config")
        return [], []

    flow_configs = []

    # Connect to each device and discover features
    for device in kasa_devices:
        connected = await device.connect()
        if not connected:
            continue

        # Discover features
        features = await device.discover_features()
        if not features:
            logger.warning(f"No supported features found for {device.name}")
            continue

        # Generate channel configs
        channel_configs = device.generate_channel_configs()
        if not channel_configs:
            logger.warning(f"No channel configs generated for {device.name}")
            continue

        # Create flow config
        flow_name = f"{device.name}-readings-{int(asyncio.get_event_loop().time())}"
        flow_config = FlowConfig(name=flow_name, channels=channel_configs)
        flow_configs.append(flow_config)

        logger.info(
            f"Created flow {flow_name} with {len(channel_configs)} channels for {device.name}"
        )

    return kasa_devices, flow_configs


async def main() -> None:
    """Main function for testing."""
    # Load environment variables
    dotenv.load_dotenv(dotenv_path=os.path.join(os.path.dirname(PARENT_DIR), ".env"))

    # Load Kasa credentials from .env
    kasa_username = os.getenv("KASA_USERNAME")
    kasa_password = os.getenv("KASA_PASSWORD")
    if not kasa_username or not kasa_password:
        logger.error("Kasa credentials not found in environment variables")
        return

    credentials = kasa.Credentials(username=kasa_username, password=kasa_password)
    config_path = os.path.join(PARENT_DIR, "config.yaml")

    # Setup telemetry
    kasa_devices, flow_configs = await setup_kasa_telemetry(
        config_path, credentials, logger
    )

    # Print discovered devices and their features
    for device in kasa_devices:
        logger.info(f"Device: {device.name} ({device.model}) at {device.address}")
        for feature_name, feature_info in device.features.items():
            logger.info(
                f"  Feature: {feature_name} = {feature_info['value']} {feature_info['unit']} (type: {feature_info['value_type']})"
            )

        # Get current channel data
        channel_data = await device.get_channel_data()
        logger.info(f"Channel data: {channel_data}")


if __name__ == "__main__":
    asyncio.run(main())
