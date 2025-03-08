import argparse
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
        self.child_features: Dict[
            str, Dict[str, Any]
        ] = {}  # Store child device features separately
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

            # Check if the device has children
            if (
                hasattr(self.device_instance, "children")
                and self.device_instance.children
            ):
                self.logger.info(
                    f"Device has {len(self.device_instance.children)} children"
                )
                for child in self.device_instance.children:
                    await child.update()
                    self.logger.debug(f"Child device: {child.alias}")

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
            self.child_features = {}  # Reset child features

            # First handle the main device features
            for feature_name in self.device_instance.features:
                feature = self.device_instance.features.get(feature_name)
                if feature is None:
                    continue

                # Get feature type
                value_type = type(feature.value)
                self.logger.debug(
                    f"Main feature: {feature_name} = {feature.value} (type: {value_type})"
                )

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

            # Now handle child devices if any
            if (
                hasattr(self.device_instance, "children")
                and self.device_instance.children
            ):
                for child in self.device_instance.children:
                    try:
                        await child.update()
                        # Safely handle the case where alias might be None
                        child_alias = "Child"
                        if hasattr(child, "alias") and child.alias is not None:
                            child_alias = child.alias.replace(
                                " ", "_"
                            )  # Replace spaces with underscores
                        else:
                            child_alias = (
                                f"Child_{id(child)}"  # Use object id as fallback
                            )

                        self.logger.debug(f"Processing child device: {child_alias}")

                        # Get features from the child device
                        for feature_name in child.features:
                            feature = child.features.get(feature_name)
                            if feature is None:
                                continue

                            value_type = type(feature.value)
                            self.logger.debug(
                                f"Child feature: {child_alias}.{feature_name} = {feature.value} (type: {value_type})"
                            )

                            # Only include supported types
                            if value_type in TYPE_MAPPING:
                                # Use child_alias.feature_name as the key
                                feature_key = f"{child_alias}.{feature_name}"
                                self.child_features[feature_key] = {
                                    "child": child,
                                    "feature_name": feature_name,
                                    "value": feature.value,
                                    "unit": feature.unit,
                                    "value_type": value_type,
                                    "sift_type": TYPE_MAPPING.get(value_type),
                                }
                                self.logger.debug(
                                    f"Added child feature: {feature_key} = {feature.value} {feature.unit}"
                                )
                    except Exception as e:
                        self.logger.error(
                            f"Error processing child device {getattr(child, 'alias', 'Unknown')}: {e}"
                        )

            # Log the summary of discovered features
            total_features = len(self.features) + len(self.child_features)
            self.logger.info(
                f"Discovered {total_features} features ({len(self.features)} main, {len(self.child_features)} child)"
            )

            # Combine main device features and child features for the return value
            combined_features = {
                **self.features,
                **{k: v for k, v in self.child_features.items() if "child" not in k},
            }
            return combined_features

        except Exception as e:
            self.logger.error(f"Error discovering features for {self.name}: {e}")
            return {}

    def generate_channel_configs(self) -> List[ChannelConfig]:
        """Generate Sift channel configs for this device.

        Returns:
            List of ChannelConfig objects
        """
        channel_configs = []

        # Add main device features
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

        # Add child device features
        for feature_key, feature_info in self.child_features.items():
            sift_type = feature_info.get("sift_type")
            if not sift_type:
                continue

            # Use the parent device name as prefix for consistency
            channel_name = f"{self.name}.{feature_key}"
            unit = feature_info.get("unit", "")

            channel_config = ChannelConfig(
                name=channel_name,
                data_type=sift_type,
                unit=unit,
            )
            channel_configs.append(channel_config)

        self.channel_configs = channel_configs
        self.logger.info(
            f"Generated {len(channel_configs)} channel configs for {self.name}"
        )
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
            # Get values for each main device feature
            for feature_name, feature_info in self.features.items():
                feature = self.device_instance.features.get(feature_name)
                if feature is None:
                    continue

                channel_name = f"{self.name}.{feature_name}"
                data[channel_name] = {
                    "value": feature.value,
                    "unit": feature.unit,
                }

            # Update child devices and get their feature values
            if (
                hasattr(self.device_instance, "children")
                and self.device_instance.children
            ):
                for child in self.device_instance.children:
                    await child.update()

                    # Safely handle the case where alias might be None
                    child_alias = "Child"
                    if hasattr(child, "alias") and child.alias is not None:
                        child_alias = child.alias.replace(" ", "_")
                    else:
                        child_alias = f"Child_{id(child)}"  # Use object id as fallback

                    # Find all features for this child
                    child_feature_keys = [
                        key
                        for key in self.child_features.keys()
                        if key.startswith(f"{child_alias}.")
                    ]

                    for feature_key in child_feature_keys:
                        feature_info = self.child_features[feature_key]
                        # Get feature name and handle potential None value
                        feature_name_value = feature_info.get("feature_name")
                        if not feature_name_value:  # Skip if no feature name
                            continue

                        # Now we know feature_name_value is not None
                        feature_name: str = feature_name_value

                        if feature_name in child.features:
                            feature = child.features.get(feature_name)
                            if feature is None:
                                continue

                            channel_name = f"{self.name}.{feature_key}"
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
    device_name: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> List[KasaSensor]:
    """Load Kasa devices from config file.

    Args:
        config_path: Path to the config file
        credentials: Kasa credentials
        device_name: Optional device name to filter by
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

            # If a specific device name is provided, filter for that device
            if device_name:
                kasa_configs = [
                    device
                    for device in kasa_configs
                    if device.get("name") == device_name
                ]
                if not kasa_configs:
                    logger.warning(f"No Kasa device found with name: {device_name}")

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
    device_name: Optional[str] = None,
    logger: Optional[logging.Logger] = None,
) -> Tuple[List[KasaSensor], List[FlowConfig]]:
    """Set up telemetry for Kasa devices.

    Args:
        config_path: Path to the config file
        credentials: Kasa credentials
        device_name: Optional device name to filter by
        logger: Optional logger instance

    Returns:
        Tuple of (list of KasaSensor objects, list of FlowConfig objects)
    """
    logger = logger or logging.getLogger(__name__)

    # Load devices from config
    kasa_devices = await load_kasa_devices(
        config_path, credentials, device_name, logger
    )

    if not kasa_devices:
        # Fix the string concatenation issue by ensuring device_name is a string
        name_info = f" with name: {device_name}" if device_name else ""
        logger.warning(f"No Kasa devices found in config{name_info}")
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


def parse_args() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Discover and interact with Kasa devices"
    )
    parser.add_argument(
        "--device",
        help="Name of a specific device to discover (must match name in config.yaml)",
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Enable verbose (debug) logging"
    )
    return parser.parse_args()


async def main() -> None:
    """Main function for testing."""
    # Parse command line arguments
    args = parse_args()

    # Set logging level based on verbose flag
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Debug logging enabled")

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

    if args.device:
        logger.info(f"Discovering specific device: {args.device}")
    else:
        logger.info("Discovering all Kasa devices")

    # Setup telemetry
    kasa_devices, flow_configs = await setup_kasa_telemetry(
        config_path, credentials, args.device, logger
    )

    # Print discovered devices and their features
    for device in kasa_devices:
        logger.info(f"Device: {device.name} ({device.model}) at {device.address}")

        # Print main device features
        if device.features:
            logger.info("Main device features:")
            for feature_name, feature_info in device.features.items():
                logger.info(
                    f"  {feature_name} = {feature_info['value']} {feature_info['unit']} (type: {feature_info['value_type']})"
                )

        # Print child device features
        if device.child_features:
            logger.info("Child device features:")
            for feature_key, feature_info in device.child_features.items():
                if "child" not in feature_key:  # Skip internal keys
                    logger.info(
                        f"  {feature_key} = {feature_info['value']} {feature_info['unit']} (type: {feature_info['value_type']})"
                    )

        # Get current channel data
        channel_data = await device.get_channel_data()
        logger.info(f"Channel data: {channel_data}")


if __name__ == "__main__":
    asyncio.run(main())
