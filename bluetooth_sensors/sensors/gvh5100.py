import asyncio
import logging
from typing import Dict, List, Optional, Set, cast

from bleak import BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from sift_py.ingestion.channel import ChannelDataType

from ._ble_sensor import BluetoothSensor, ChannelDict, ConversionFunc

DATA_ID = 1

# Sensor name pattern
GVH5100_NAME_PATTERN = "GVH5100"


# Comprehensive sensor definitions
CHANNELS: List[ChannelDict] = [
    {
        "name": "temperature",
        "unpacked_index": 0,
        "conversion": cast(ConversionFunc, lambda x: x),
        "unit": "degC",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "humidity",
        "unpacked_index": 1,
        "conversion": cast(ConversionFunc, lambda x: x),
        "unit": "%rH",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "battery",
        "unpacked_index": 2,
        "conversion": cast(ConversionFunc, lambda x: x),
        "unit": "%",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "error",
        "unpacked_index": 3,
        "conversion": cast(ConversionFunc, lambda x: x),
        "unit": "",
        "sift_type": ChannelDataType.BOOL,
    },
]


def decode_temp_humid(temp_humid_bytes: bytes) -> tuple[float, float]:
    """Decode potential negative temperatures."""
    base_num = (
        (temp_humid_bytes[0] << 16) + (temp_humid_bytes[1] << 8) + temp_humid_bytes[2]
    )
    is_negative = base_num & 0x800000
    temp_as_int = base_num & 0x7FFFFF
    temp_as_float = int(temp_as_int / 1000) / 10.0
    if is_negative:
        temp_as_float = -temp_as_float
    humid = (temp_as_int % 1000) / 10.0
    return temp_as_float, humid


def decode_temp_humid_battery_error(data: bytes) -> tuple[float, float, int, bool]:
    temp, humi = decode_temp_humid(data[0:3])
    batt = int(data[-1] & 0x7F)
    err = bool(data[-1] & 0x80)
    return temp, humi, batt, err


class GVH5100(BluetoothSensor):
    """Class representing a Govee GVH5100 bluetooth temperature/humidity sensor.

    This sensor serves up the data in the manufacturer data field, so we dont need to read any
    characteristics or connect to the device.
    """

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
            name: Optional name for the sensor
        """
        super().__init__(serial_number, address, logger, name)
        self.read_time: Optional[float] = None

    def _detection_callback(
        self,
        device: BLEDevice,
        advertisement_data: AdvertisementData,
        found_devices: Set[str],
    ) -> None:
        """Detect GVH5100 devices by name and address.

        Args:
            device: The discovered BLE device
            advertisement_data: Advertisement data from the device
            found_devices: Set to add device addresses to when found
        """
        # Check by name
        if device.name == self.serial_number:
            # Check if this is the device we're looking for
            # This would require additional identifier matching logic
            # specific to how Govee devices advertise their identifiers
            self.logger.debug(
                f"Found device: {device.name}, Address: {device.address}, advertisement_data: {advertisement_data}"
            )
            found_devices.add(device.address)
            return

    async def connect(self, retries: int = 1) -> bool:
        """Connect to the GVH5100 device.

        Returns:
            True if connected successfully,
        """
        del retries
        if not self.address:
            self.address = await self.discover()
            if not self.address:
                self.logger.error(
                    f"Could not find device with serial number {self.serial_number}. Please check the serial number."
                )
                return False
        return True

    async def read(self) -> bool:
        """Read sensor values from the GVH5100 device.

        Returns:
            True if reading was successful, False otherwise
        """

        def _detection_callback(
            device: BLEDevice, advertisement_data: AdvertisementData
        ) -> None:
            if device.name and device.name == self.serial_number:
                if 1 in advertisement_data.manufacturer_data.keys():
                    data = advertisement_data.manufacturer_data[1]
                    temp, humi, batt, err = decode_temp_humid_battery_error(data[2:6])
                    self.channel_data["temperature"] = temp
                    self.channel_data["humidity"] = humi
                    self.channel_data["battery"] = batt
                    self.channel_data["error"] = err
                    self.logger.info(
                        f"{device.name} temp: {temp} C, humi: {humi} %rH, batt: {batt} %, err: {err}"
                    )

        scanner = BleakScanner(detection_callback=_detection_callback)
        try:
            await scanner.start()
            await asyncio.sleep(5)
            await scanner.stop()
            return True
        except Exception as e:
            self.logger.error(f"Error reading values: {e}")
            return False
