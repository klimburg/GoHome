"""Module for reading data from a Tesla Wall Connector."""

import asyncio
import logging
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import ClientTimeout
from sift_py.ingestion.channel import ChannelDataType

# Define unit mappings based on field suffixes
UNIT_MAPPINGS = {
    "_v": "V",  # Volts
    "_a": "A",  # Amps
    "_temp_c": "°C",  # Celsius
    "_hz": "Hz",  # Hertz
    "_wh": "Wh",  # Watt hours
    "_s": "s",  # Seconds
    "_uv": "μV",  # Microvolts
}

# Special cases for specific fields
SPECIAL_UNITS = {
    "wifi_rssi": "dB",
    "wifi_signal_strength": "%",
    "power_w": "W",
    "wifi_snr": "dB",
}

# Define the endpoints we'll be querying
ENDPOINTS = ["vitals", "lifetime", "wifi"]

# Mapping of Python types to Sift data types
TYPE_MAPPING = {
    bool: ChannelDataType.BOOL,
    int: ChannelDataType.INT_32,
    float: ChannelDataType.DOUBLE,
}


class TeslaWallConnector:
    """Class for reading data from a Tesla Wall Connector."""

    def __init__(
        self,
        serial_number: str,
        name: str = "TeslaWallConnector",
        address: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        """Initialize the Tesla Wall Connector sensor.

        Args:
            serial_number: The serial number of the sensor (not used but kept for interface consistency)
            name: The name of the sensor
            address: The IP address of the Tesla Wall Connector
            logger: Optional logger instance
        """
        self.serial_number = serial_number
        self.name = name
        self.address = address
        self.logger = logger or logging.getLogger(__name__)
        self.connected = False
        self.session: Optional[aiohttp.ClientSession] = None

        # Store the latest data from each endpoint
        self.vitals_data: Dict[str, Any] = {}
        self.lifetime_data: Dict[str, Any] = {}
        self.wifi_data: Dict[str, Any] = {}

        # Store discovered channels for Sift configuration
        self.discovered_channels: Dict[str, List[Dict[str, Any]]] = {
            endpoint: [] for endpoint in ENDPOINTS
        }

    async def connect(self, retries: int = 3) -> bool:
        """Connect to the Tesla Wall Connector.

        Args:
            retries: Number of connection retries

        Returns:
            True if successful, False otherwise
        """
        if self.session is None:
            self.session = aiohttp.ClientSession()

        if not self.address:
            self.logger.error("No address provided for Tesla Wall Connector")
            return False

        self.logger.debug(f"Connecting to Tesla Wall Connector at {self.address}")
        self.connected = True
        return True

    async def disconnect(self) -> None:
        """Disconnect from the Tesla Wall Connector."""
        if self.session:
            self.logger.info("Closing aiohttp session for Tesla Wall Connector")
            await self.session.close()
            self.session = None
        self.connected = False
        self.logger.info("Disconnected from Tesla Wall Connector")

    async def read(self) -> bool:
        """Read data from all Tesla Wall Connector endpoints.

        Returns:
            True if successful, False otherwise
        """
        if not self.connected or not self.session:
            self.logger.error("Not connected to Tesla Wall Connector")
            return False

        try:
            # Read data from all three endpoints
            vitals_success = await self._read_vitals()
            lifetime_success = await self._read_lifetime()
            wifi_success = await self._read_wifi_status()

            # Return True only if all readings were successful
            return vitals_success and lifetime_success and wifi_success
        except Exception as e:
            self.logger.exception(f"Error reading from Tesla Wall Connector: {e}")
            return False

    async def _read_vitals(self) -> bool:
        """Read data from the vitals endpoint.

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            return False

        try:
            url = f"http://{self.address}/api/1/vitals"
            timeout = ClientTimeout(total=10)
            async with self.session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    self.logger.error(f"Error reading vitals: {response.status}")
                    return False

                data = await response.json()

                # Extract and clean up the data
                if isinstance(data, dict):
                    self.vitals_data = self._extract_typed_values(data)

                    # Update discovered channels
                    self._update_discovered_channels("vitals", self.vitals_data)

                    return True
                else:
                    self.logger.error(f"Unexpected vitals data format: {data}")
                    return False
        except asyncio.TimeoutError:
            self.logger.error("Timeout reading vitals data")
            return False
        except Exception as e:
            self.logger.exception(f"Error reading vitals data: {e}")
            return False

    async def _read_lifetime(self) -> bool:
        """Read data from the lifetime endpoint.

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            return False

        try:
            url = f"http://{self.address}/api/1/lifetime"
            timeout = ClientTimeout(total=10)
            async with self.session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    self.logger.error(f"Error reading lifetime: {response.status}")
                    return False

                data = await response.json()

                # Extract and clean up the data
                if isinstance(data, dict):
                    self.lifetime_data = self._extract_typed_values(data)

                    # Update discovered channels
                    self._update_discovered_channels("lifetime", self.lifetime_data)

                    return True
                else:
                    self.logger.error(f"Unexpected lifetime data format: {data}")
                    return False
        except asyncio.TimeoutError:
            self.logger.error("Timeout reading lifetime data")
            return False
        except Exception as e:
            self.logger.exception(f"Error reading lifetime data: {e}")
            return False

    async def _read_wifi_status(self) -> bool:
        """Read data from the wifi_status endpoint.

        Returns:
            True if successful, False otherwise
        """
        if not self.session:
            return False

        try:
            url = f"http://{self.address}/api/1/wifi_status"
            timeout = ClientTimeout(total=10)
            async with self.session.get(url, timeout=timeout) as response:
                if response.status != 200:
                    self.logger.error(f"Error reading wifi_status: {response.status}")
                    return False

                data = await response.json()

                # Extract and clean up the data
                if isinstance(data, dict):
                    self.wifi_data = self._extract_typed_values(data)

                    # Update discovered channels
                    self._update_discovered_channels("wifi", self.wifi_data)

                    return True
                else:
                    self.logger.error(f"Unexpected wifi_status data format: {data}")
                    return False
        except asyncio.TimeoutError:
            self.logger.error("Timeout reading wifi_status data")
            return False
        except Exception as e:
            self.logger.exception(f"Error reading wifi_status data: {e}")
            return False

    def _extract_typed_values(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Extract values of numeric or boolean types from the data.

        Args:
            data: The raw data from the Tesla Wall Connector

        Returns:
            Dictionary with extracted numeric or boolean values
        """
        result: Dict[str, Any] = {}

        # Recursively process nested dictionaries
        def process_dict(d: Dict[str, Any], prefix: str = "") -> None:
            for key, value in d.items():
                full_key = f"{prefix}{key}" if prefix else key

                # Handle nested dictionaries
                if isinstance(value, dict):
                    process_dict(value, f"{full_key}_")
                # Only include numeric or boolean values
                elif isinstance(value, (int, float, bool)):
                    result[full_key] = value

        process_dict(data)
        return result

    def _update_discovered_channels(self, endpoint: str, data: Dict[str, Any]) -> None:
        """Update the discovered channels list based on discovered data.

        Args:
            endpoint: The endpoint name (vitals, lifetime, wifi)
            data: The data from the endpoint
        """
        # Reset channels for this endpoint
        self.discovered_channels[endpoint] = []

        for key, value in data.items():
            # Determine data type
            value_type = type(value)
            if value_type not in TYPE_MAPPING:
                continue

            # Determine unit
            unit = self._determine_unit(key)

            # Create channel config
            channel = {"name": key, "sift_type": TYPE_MAPPING[value_type], "unit": unit}

            self.discovered_channels[endpoint].append(channel)

    def get_discovered_channels(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get the discovered channels for Sift configuration.

        Returns:
            Dictionary of channel lists by endpoint
        """
        return self.discovered_channels

    def get_channel_data(self) -> Dict[str, Dict[str, Any]]:
        """Get data for all channels.

        Returns:
            Dictionary mapping channel names to channel data
        """
        data = {}

        # Process vitals data
        for key, value in self.vitals_data.items():
            channel_name = f"{self.name}.vitals.{key}"
            unit = self._determine_unit(key)
            data[channel_name] = {
                "value": value,
                "unit": unit,
            }

        # Process lifetime data
        for key, value in self.lifetime_data.items():
            channel_name = f"{self.name}.lifetime.{key}"
            unit = self._determine_unit(key)
            data[channel_name] = {
                "value": value,
                "unit": unit,
            }

        # Process wifi data
        for key, value in self.wifi_data.items():
            channel_name = f"{self.name}.wifi.{key}"
            unit = self._determine_unit(key)
            data[channel_name] = {
                "value": value,
                "unit": unit,
            }

        return data

    def _determine_unit(self, key: str) -> str:
        """Determine the unit based on field name suffix.

        Args:
            key: The field name

        Returns:
            The corresponding unit or empty string
        """
        # Check for special cases first
        if key in SPECIAL_UNITS:
            return SPECIAL_UNITS[key]

        # Check for suffix-based units
        for suffix, unit in UNIT_MAPPINGS.items():
            if key.endswith(suffix):
                return unit

        # No unit identified
        return ""


async def main() -> None:
    """Example usage of the TeslaWallConnector class."""
    # Set up logging
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger(__name__)

    # Create a Tesla Wall Connector instance
    connector = TeslaWallConnector(
        serial_number="",  # Not used
        name="TeslaWallConnector",
        address="192.168.50.108",  # Example address
        logger=logger,
    )

    try:
        # Connect to the Tesla Wall Connector
        connected = await connector.connect()
        if not connected:
            logger.error("Failed to connect to Tesla Wall Connector")
            return

        # Read data from the Tesla Wall Connector
        success = await connector.read()
        if not success:
            logger.error("Failed to read data from Tesla Wall Connector")
            return

        # Get discovered channels
        discovered_channels = connector.get_discovered_channels()
        for endpoint, channels in discovered_channels.items():
            logger.info(f"\nDiscovered {len(channels)} channels for {endpoint}:")
            for channel in channels:
                logger.info(
                    f"  - {channel['name']} ({channel['sift_type']}) [{channel['unit']}]"
                )

        # Get and print channel data
        channel_data = connector.get_channel_data()
        logger.info("\nChannel data:")
        for channel_name, data in channel_data.items():
            logger.info(f"  - {channel_name}: {data['value']} {data['unit']}")

    finally:
        # Disconnect from the Tesla Wall Connector
        await connector.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
