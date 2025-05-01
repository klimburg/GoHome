"""Test script for zigbee2mqtt client."""

import json
import logging
import time
from pathlib import Path

from zigbee2mqtt_client import Zigbee2MQTTClient

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main() -> None:
    """Test the zigbee2mqtt client."""
    config_path = Path(__file__).parent / "config.yaml"
    client = Zigbee2MQTTClient(str(config_path))

    try:
        logger.info("Connecting to MQTT broker...")
        client.connect()

        # Wait for device discovery
        logger.info("Waiting for device discovery...")
        time.sleep(5)  # Give some time for devices to be discovered

        # Print discovered telemetry config
        config = client.get_telemetry_config()
        logger.info(f"Generated telemetry configuration for {len(config)} channels:")
        for channel_name, channel_config in config.items():
            logger.info(f"\nChannel: {channel_name}")
            logger.info(json.dumps(channel_config, indent=2))

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        client.disconnect()


if __name__ == "__main__":
    main()
