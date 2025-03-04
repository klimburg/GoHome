import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, Optional, Set, TypedDict, Union

from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from sift_py.ingestion.channel import ChannelDataType

# Define the type for channel conversion functions
ChannelValueType = Union[float, int, str, None]
ConversionFunc = Callable[[Union[int, float]], ChannelValueType]


# Define TypedDict for channel configuration
class ChannelDict(TypedDict):
    name: str
    unpacked_index: int
    conversion: ConversionFunc
    unit: str
    sift_type: ChannelDataType


class BluetoothSensor(ABC):
    """Abstract base class for Bluetooth sensors."""

    # Class-wide mapping of channel names to units
    channel_units: Dict[str, str] = {}

    def __init__(
        self,
        serial_number: Any,
        address: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        name: Optional[str] = None,
    ) -> None:
        """Initialize a Bluetooth sensor.

        Args:
            serial_number: The serial number or identifier of the device
            address: Optional Bluetooth address if already known
            logger: Optional logger instance
            name: Optional name of the sensor. If not provided, the class name and serial number will be used.
        """
        self.serial_number = serial_number
        self.address: Optional[str] = address
        self.client: Optional[BleakClient] = None
        self.logger = logger or logging.getLogger(self.__class__.__name__)
        self.channel_data: Dict[str, Any] = {}
        self.name = name or f"{self.__class__.__name__}-{self.serial_number}"

    @abstractmethod
    def _detection_callback(
        self,
        device: BLEDevice,
        advertisement_data: AdvertisementData,
        found_devices: Set[str],
    ) -> None:
        """Callback for BLE device detection.

        Args:
            device: The discovered BLE device
            advertisement_data: Advertisement data from the device
            found_devices: Set to add device addresses to when found
        """
        pass

    async def discover(self, scan_time: float = 10.0) -> Optional[str]:
        """Discover the device by scanning for its serial number.

        Args:
            scan_time: Time in seconds to scan for devices

        Returns:
            The device address if found, None otherwise
        """
        self.logger.debug(
            f"Scanning for device with serial number {self.serial_number}..."
        )

        found_devices: Set[str] = set()

        def callback_wrapper(
            device: BLEDevice, advertisement_data: AdvertisementData
        ) -> None:
            self._detection_callback(device, advertisement_data, found_devices)

        scanner = BleakScanner(detection_callback=callback_wrapper)

        try:
            await scanner.start()
            await asyncio.sleep(scan_time)
            await scanner.stop()

            if found_devices:
                device_address = next(iter(found_devices))
                self.logger.debug(f"Found device: {device_address}")
                return device_address
            else:
                self.logger.debug("Device not found")
                return None

        except Exception as e:
            self.logger.error(f"Error during device discovery: {e}")
            return None

    async def connect(self, retries: int = 1) -> bool:
        """Connect to the device.

        Args:
            retries: Number of connection attempts

        Returns:
            True if connected successfully, False otherwise
        """
        if not self.address:
            self.address = await self.discover()
            if not self.address:
                self.logger.error(
                    f"Could not find device with serial number {self.serial_number}. Please check the serial number."
                )
                return False

        for attempt in range(retries):
            try:
                self.logger.debug(
                    f"Connecting to {self.address} (attempt {attempt + 1}/{retries})..."
                )
                self.client = BleakClient(self.address)
                self.logger.debug("Client created")
                await self.client.connect()
                self.logger.debug("Connected successfully")
                return True
            except Exception as e:
                self.logger.error(f"Connection failed: {e}")
                if attempt < retries - 1:
                    self.logger.debug("Retrying...")
                else:
                    self.logger.error("Maximum retries reached")
                    return False

        return False

    @abstractmethod
    async def read(self) -> bool:
        """Read values from the device.

        Returns:
            True if reading was successful, False otherwise
        """
        pass

    async def disconnect(self) -> None:
        """Disconnect from the device."""
        if self.client and self.client.is_connected:
            self.logger.debug("Disconnecting...")
            await self.client.disconnect()
            self.client = None

    def get_value(self, channel_name: str) -> Any:
        """Get value for a specific channel.

        Args:
            channel_name: Name of the channel

        Returns:
            Channel value
        """
        return self.channel_data.get(channel_name)

    def get_unit(self, channel_name: str) -> str:
        """Get unit for a specific channel.

        Args:
            channel_name: Name of the channel

        Returns:
            Channel unit
        """
        return self.__class__.channel_units.get(channel_name, "")

    def get_channel_data(self) -> Dict[str, Dict[str, Any]]:
        """Get all sensor data as a dictionary.

        Returns:
            Dictionary with channel names as keys and values with units as values
        """
        data: Dict[str, Dict[str, Any]] = {}
        for channel_name, value in self.channel_data.items():
            if value is not None:
                data[f"{self.name}.{channel_name}"] = {
                    "value": value,
                    "unit": self.get_unit(channel_name),
                }
        return data

    @classmethod
    def get_channel_units(cls) -> Dict[str, str]:
        """Get the mapping of channel names to units.

        Returns:
            Dictionary mapping channel names to their units
        """
        return cls.channel_units
