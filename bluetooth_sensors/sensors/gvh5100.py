import asyncio
import logging
import struct
import time
from typing import Dict, List, Optional, cast

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from sift_py.ingestion.channel import ChannelDataType

from ._ble_sensor import BluetoothSensor, ChannelDict, ConversionFunc

# GVH5100 characteristic UUID for data
DATA_UUID = "0000ec88-0000-1000-8000-00805f9b34fb"
SERVICE_UUIDS = [
    "00001800-0000-1000-8000-00805f9b34fb",
    "00001801-0000-1000-8000-00805f9b34fb",
    "0000ec88-0000-1000-8000-00805f9b34fb",
    "02f00000-0000-0000-0000-00000000fe00",
    "494e5445-4c4c-495f-524f-434b535f4857",
]


# Conversion functions for GVH5100 sensor data
def temp_convert(raw_value: int) -> float:
    """Convert raw temperature value.

    Args:
        raw_value: Raw temperature data

    Returns:
        Temperature in degrees Celsius
    """
    return raw_value / 100.0


def humidity_convert(raw_value: int) -> float:
    """Convert raw humidity value.

    Args:
        raw_value: Raw humidity data

    Returns:
        Relative humidity percentage
    """
    return raw_value / 100.0


def battery_convert(raw_value: int) -> float:
    """Convert raw battery value.

    Args:
        raw_value: Raw battery level

    Returns:
        Battery percentage
    """
    return raw_value


# Channel definitions for GVH5100
CHANNELS: List[ChannelDict] = [
    {
        "name": "temperature",
        "unpacked_index": 0,
        "conversion": cast(ConversionFunc, temp_convert),
        "unit": "degC",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "humidity",
        "unpacked_index": 1,
        "conversion": cast(ConversionFunc, humidity_convert),
        "unit": "%rH",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "battery",
        "unpacked_index": 2,
        "conversion": cast(ConversionFunc, battery_convert),
        "unit": "%",
        "sift_type": ChannelDataType.DOUBLE,
    },
]


class GVH5100(BluetoothSensor):
    """Class representing a Govee GVH5100 bluetooth temperature/humidity sensor."""

    # Class property for channel units mapping
    channel_units: Dict[str, str] = {
        channel["name"]: channel["unit"] for channel in CHANNELS
    }

    def __init__(
        self,
        serial_number: str,
        address: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        name: Optional[str] = None,
    ) -> None:
        """Initialize the GVH5100 instance.

        Args:
            serial_number: The serial number or MAC address of the GVH5100 device
            address: Optional Bluetooth address if already known (may be same as serial)
            logger: Optional logger instance
        """
        super().__init__(serial_number, address, logger, name)
        self.read_time: Optional[float] = None

    async def discover(self) -> Optional[str]:
        """Discover the GVH5100 device by scanning for its serial number or MAC address.

        Returns:
            The device address if found, None otherwise
        """
        self.logger.debug(
            f"Scanning for Govee GVH5100 with identifier {self.serial_number}..."
        )

        found_device_address = None

        def detection_callback(
            device: BLEDevice, advertisement_data: AdvertisementData
        ) -> None:
            nonlocal found_device_address

            # Check by name
            if device.name == self.serial_number:
                # Check if this is the device we're looking for
                # This would require additional identifier matching logic
                # specific to how Govee devices advertise their identifiers
                self.logger.debug(
                    f"Found device: {device.name}, Address: {device.address}"
                )
                found_device_address = device.address
                return

        scanner = BleakScanner(detection_callback=detection_callback)

        await scanner.start()
        await asyncio.sleep(5.0)  # Scan for 5 seconds
        await scanner.stop()

        if found_device_address:
            self.logger.debug(f"Found device: {found_device_address}")
        else:
            self.logger.debug("Device not found")

        return found_device_address

    async def read(self) -> bool:
        """Read sensor values from the GVH5100 device.

        Returns:
            True if reading was successful, False otherwise
        """
        if not self.client or not self.client.is_connected:
            self.logger.error("Device not connected")
            return False

        try:
            # Read the characteristic that contains sensor data
            rawdata = await self.client.read_gatt_char(DATA_UUID)
            self._parse_sensor_data(rawdata)
            return True
        except Exception as e:
            self.logger.error(f"Error reading values: {e}")
            return False

    def _parse_sensor_data(self, rawdata: bytes) -> None:
        """Parse raw sensor data.

        Args:
            rawdata: Raw bytes read from the device
        """
        try:
            # Unpack the binary data - adjust format based on actual device data format
            # This is an example - you'll need to replace with the actual data format
            unpacked_data = struct.unpack("<HHB", rawdata[:5])

            self.read_time = time.time()

            for channel in CHANNELS:
                index = channel["unpacked_index"]
                if index < len(unpacked_data):
                    raw_value = unpacked_data[index]
                    self.channel_data[channel["name"]] = channel["conversion"](
                        raw_value
                    )
        except Exception as e:
            self.logger.error(f"Error parsing sensor data: {e}")
