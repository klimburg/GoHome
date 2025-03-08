import argparse
import asyncio
import logging
import signal
import struct
import time
from typing import Any, Callable, Dict, List, Optional, Set, Union, cast
from uuid import UUID

from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from sift_py.ingestion.channel import ChannelDataType

from bluetooth_sensors.sensors._ble_sensor import (
    BluetoothSensor,
    ChannelDict,
    ConversionFunc,
)

# Airthings Wave Plus characteristic UUID
CURR_VAL_UUID = "b42e2a68-ade7-11e4-89d3-123b93f75cba"
COMMAND_UUID_WAVE_PLUS = UUID("b42e2d06-ade7-11e4-89d3-123b93f75cba")
COMMAND_VALUE = b"\x6d"
COMMAND_FORMAT = "<L2BH2B9H"
# Airthings manufacturer ID
AIRTHINGS_MFG_ID = 0x0334

"""
0 - 49 Bq/m3  (0 - 1.3 pCi/L):
No action necessary.
50 - 99 Bq/m3 (1.4 - 2.6 pCi/L):
Experiment with ventilation and sealing cracks to reduce levels.
100 Bq/m3 - 299 Bq/m3 (2.7 - 8 pCi/L):
Keep measuring. If levels are maintained for more than 3 months,
contact a professional radon mitigator.
300 Bq/m3 (8.1 pCi/L) and up:
Keep measuring. If levels are maintained for more than 1 month,
contact a professional radon mitigator.
"""
VERY_LOW = (0, 49, "very low")
LOW = (50, 99, "low")
MODERATE = (100, 299, "moderate")
HIGH = (300, None, "high")

BQ_TO_PCI_MULTIPLIER = 0.027

CO2_MAX = 65534
VOC_MAX = 65534
PERCENTAGE_MAX = 100
PRESSURE_MAX = 1310
RADON_MAX = 16383
TEMPERATURE_MAX = 100


# Define conversion functions
def convert_radon(raw_value: int) -> Union[int, str]:
    """Convert raw radon value."""
    if 0 <= raw_value <= 16383:
        return raw_value
    return "N/A"


def divide_by(divisor: float) -> Callable[[float], float]:
    """Create a function that divides by the specified value."""
    return lambda x: x / divisor


def multiply_by(multiplier: float) -> Callable[[float], float]:
    """Create a function that multiplies by the specified value."""
    return lambda x: x * multiplier


def illuminance_convert(raw_value: int | float) -> float:
    """Convert raw illuminance value."""
    return raw_value / 255.0 * 100.0


def _interpolate(
    voltage: float,
    voltage_range: tuple[float, float],
    percentage_range: tuple[int, int],
) -> float:
    return (voltage - voltage_range[0]) / (voltage_range[1] - voltage_range[0]) * (
        percentage_range[1] - percentage_range[0]
    ) + percentage_range[0]


def batt_voltage_to_pct(voltage: float) -> float:
    """Convert battery voltage to percentage."""
    if voltage >= 3.00:
        return 100
    if 2.80 <= voltage < 3.00:
        return _interpolate(
            voltage=voltage, voltage_range=(2.80, 3.00), percentage_range=(81, 100)
        )
    if 2.60 <= voltage < 2.80:
        return _interpolate(
            voltage=voltage, voltage_range=(2.60, 2.80), percentage_range=(53, 81)
        )
    if 2.50 <= voltage < 2.60:
        return _interpolate(
            voltage=voltage, voltage_range=(2.50, 2.60), percentage_range=(28, 53)
        )
    if 2.20 <= voltage < 2.50:
        return _interpolate(
            voltage=voltage, voltage_range=(2.20, 2.50), percentage_range=(5, 28)
        )
    if 2.10 <= voltage < 2.20:
        return _interpolate(
            voltage=voltage, voltage_range=(2.10, 2.20), percentage_range=(0, 5)
        )
    return 0


# Comprehensive sensor definitions
CHANNELS: List[ChannelDict] = [
    {
        "name": "humidity",
        "unpacked_index": 1,
        "conversion": cast(ConversionFunc, divide_by(2.0)),
        "unit": "%rH",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "radon_short_term_avg",
        "unpacked_index": 4,
        "conversion": cast(ConversionFunc, convert_radon),
        "unit": "Bq/m3",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "radon_long_term_avg",
        "unpacked_index": 5,
        "conversion": cast(ConversionFunc, convert_radon),
        "unit": "Bq/m3",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "temperature",
        "unpacked_index": 6,
        "conversion": cast(ConversionFunc, divide_by(100.0)),
        "unit": "degC",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "pressure",
        "unpacked_index": 7,
        "conversion": cast(ConversionFunc, divide_by(50.0)),
        "unit": "hPa",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "CO2_level",
        "unpacked_index": 8,
        "conversion": cast(ConversionFunc, multiply_by(1.0)),
        "unit": "ppm",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "VOC_level",
        "unpacked_index": 9,
        "conversion": cast(ConversionFunc, multiply_by(1.0)),
        "unit": "ppb",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "illuminance",
        "unpacked_index": 2,
        "conversion": cast(ConversionFunc, illuminance_convert),
        "unit": "lux",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": True,
    },
    {
        "name": "battery_voltage",
        "unpacked_index": -1,
        "conversion": cast(ConversionFunc, multiply_by(1.0)),
        "unit": "V",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": False,
    },
    {
        "name": "battery_pct",
        "unpacked_index": -1,
        "conversion": cast(ConversionFunc, multiply_by(1.0)),
        "unit": "%",
        "sift_type": ChannelDataType.DOUBLE,
        "is_sensor": False,
    },
]


class _NotificationReceiver:
    """Receiver for a single notification message.

    A notification message that is larger than the MTU can get sent over multiple
    packets. This receiver knows how to reconstruct it.
    """

    message: bytearray | None

    def __init__(self, message_size: int):
        self.message = None
        self._message_size = message_size
        self._loop = asyncio.get_running_loop()
        self._future: asyncio.Future[None] = self._loop.create_future()

    def _full_message_received(self) -> bool:
        return self.message is not None and len(self.message) >= self._message_size

    def __call__(self, _: Any, data: bytearray) -> None:
        if self.message is None:
            self.message = data
        elif not self._full_message_received():
            self.message += data
        if self._full_message_received():
            self._future.set_result(None)

    def _on_timeout(self) -> None:
        if not self._future.done():
            self._future.set_exception(
                asyncio.TimeoutError("Timeout waiting for message")
            )

    async def wait_for_message(self, timeout: float) -> None:
        """Waits until the full message is received.

        If the full message has already been received, this method returns immediately.
        """
        if not self._full_message_received():
            timer_handle = self._loop.call_later(timeout, self._on_timeout)
            try:
                await self._future
            finally:
                timer_handle.cancel()


class CommandDecode:
    """Decoder for the command response"""

    cmd: bytes = b"\x6d"

    def __init__(self) -> None:
        """Initialize command decoder"""
        self.format_type = "<L2BH2B9H"  # <4B8H

    def decode_data(
        self, logger: logging.Logger, raw_data: bytearray | None
    ) -> dict[str, float | str | None] | None:
        """Decoder returns dict with battery"""
        logger.info(f"Raw data: {raw_data!r}")

        if values := self.validate_data(logger, raw_data):
            assert raw_data is not None
            data = {}
            data["battery"] = values[13] / 1000.0
            logger.info(f"Data: {data!r}")

            return data

        return None

    def validate_data(
        self, logger: logging.Logger, raw_data: bytearray | None
    ) -> Optional[Any]:
        """Validate data. Make sure the data is for the command."""
        if raw_data is None:
            logger.debug("Validate data: No data received")
            return None
        logger.info(f"Raw data: {raw_data!r}")

        cmd = raw_data[0:1]
        if cmd != self.cmd:
            logger.warning(
                "Result for wrong command received, expected %s got %s",
                self.cmd.hex(),
                cmd.hex(),
            )
            return None

        if len(raw_data[2:]) != struct.calcsize(self.format_type):
            logger.warning(
                "Wrong length data received (%s) versus expected (%s)",
                len(raw_data[2:]),
                struct.calcsize(self.format_type),
            )
            return None

        return struct.unpack(self.format_type, raw_data[2:])

    def make_data_receiver(self) -> _NotificationReceiver:
        """Creates a notification receiver for the command."""
        return _NotificationReceiver(struct.calcsize(self.format_type))


class WavePlus(BluetoothSensor):
    """Class representing an Airthings Wave Plus device."""

    # Class property for channel units mapping
    channel_units: Dict[str, str] = {
        channel["name"]: channel["unit"] for channel in CHANNELS
    }

    def __init__(
        self,
        serial_number: int,
        address: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        name: Optional[str] = None,
    ) -> None:
        """Initialize the WavePlus instance.

        Args:
            serial_number: The serial number of the Airthings Wave Plus device
            address: Optional Bluetooth address if already known
            logger: Optional logger instance
        """
        super().__init__(serial_number, address, logger, name)
        # Channel data specific to WavePlus
        self._sensor_version: Optional[int] = None
        self.read_time: Optional[float] = None
        self.command_decoder = CommandDecode()
        self._notification_receiver = self.command_decoder.make_data_receiver()

    def _detection_callback(
        self,
        device: BLEDevice,
        advertisement_data: AdvertisementData,
        found_devices: Set[str],
    ) -> None:
        """Detect Wave Plus devices by their serial number in the manufacturer data.

        Args:
            device: The discovered BLE device
            advertisement_data: Advertisement data from the device
            found_devices: Set to add device addresses to when found
        """
        if AIRTHINGS_MFG_ID in advertisement_data.manufacturer_data:
            mfg_data = advertisement_data.manufacturer_data[AIRTHINGS_MFG_ID]
            serial_number, _ = struct.unpack("<LH", mfg_data)
            if serial_number == self.serial_number:
                found_devices.add(device.address)

    async def _read_command_data(self) -> None:
        """Send command to get next reading."""
        if not self.client or not self.client.is_connected:
            self.logger.error("Device not connected")
            return

        self.logger.info("Sending command")

        await self.client.start_notify(
            COMMAND_UUID_WAVE_PLUS, self._notification_receiver
        )
        await self.client.write_gatt_char(
            COMMAND_UUID_WAVE_PLUS, bytearray(self.command_decoder.cmd)
        )
        try:
            await self._notification_receiver.wait_for_message(timeout=5)
        except asyncio.TimeoutError:
            self.logger.warning("Command timed out")

        cmd_data = self.command_decoder.decode_data(
            logger=self.logger, raw_data=self._notification_receiver.message
        )
        self.logger.info(f"Sensor data: {cmd_data}")
        if cmd_data is None:
            self.logger.warning("Failed to decode command data")
        else:
            self.logger.info(f"Sensor data: {cmd_data}")
            if (bat_data := cmd_data.get("battery")) is not None:
                self.channel_data["battery_voltage"] = bat_data
                self.channel_data["battery_pct"] = batt_voltage_to_pct(float(bat_data))
        await self.client.stop_notify(COMMAND_UUID_WAVE_PLUS)

    async def read(self) -> bool:
        """Read sensor and command values from the Wave Plus device.

        Returns:
            True if reading was successful, False otherwise
        """
        if not self.client or not self.client.is_connected:
            self.logger.error("Device not connected")
            return False

        try:
            rawdata = await self.client.read_gatt_char(CURR_VAL_UUID)
            self._parse_sensor_data(rawdata)
            #await asyncio.sleep(1)
            await self._read_command_data()
            self.logger.info(f"Channel data: {self.channel_data}")
            return True
        except Exception as e:
            self.logger.error(f"Error reading values: {e}")
            return False

    def _parse_sensor_data(self, rawdata: bytes) -> None:
        """Parse raw sensor data.

        Args:
            rawdata: Raw bytes read from the device
        """
        # Unpack the binary data
        self.logger.info(f"Raw data: {rawdata!r}")
        unpacked_data = struct.unpack("<BBBBHHHHHHHH", rawdata)
        self.logger.info(f"Len: {len(unpacked_data)}")
        self.logger.info(f"Unpacked data: {unpacked_data}")

        self.read_time = time.time()
        self._sensor_version = unpacked_data[0]

        for channel in CHANNELS:
            if channel["is_sensor"]:
                raw_value = unpacked_data[channel["unpacked_index"]]
                self.channel_data[channel["name"]] = channel["conversion"](raw_value)


def _argparser() -> argparse.Namespace:
    """Parse command line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        prog="read_wave_plus",
        description="Script for reading current values from an Airthings Wave Plus product",
    )
    parser.add_argument(
        "-s",
        "--serial_number",
        type=int,
        help="Airthings device 10-digit serial number found under the magnetic backplate",
    )
    parser.add_argument(
        "-a",
        "--address",
        type=str,
        help="Bluetooth address of the device",
    )
    parser.add_argument(
        "-p",
        "--sample_period",
        default=60,
        type=int,
        help="Time in seconds between reading the current values",
    )
    return parser.parse_args()


async def run_waveplus(args: argparse.Namespace) -> None:
    """Main function to run Wave Plus data collection.

    Args:
        args: Command line arguments
    """
    serial_number = args.serial_number
    address = args.address
    sample_period = args.sample_period

    # Set up logger
    logger = logging.getLogger("WavePlus")

    # Create and initialize the Wave Plus device
    waveplus = WavePlus(serial_number, address=address, logger=logger)

    logger.info(f"Device serial number: {serial_number}")
    logger.info("Press Ctrl+C to exit program")

    try:
        while True:
            # Connect to device
            connected = await waveplus.connect(retries=3)
            if not connected:
                logger.error("Failed to connect. Exiting.")
                return

            # Read values
            success = await waveplus.read()

            if success:
                logger.info(f"Reading at {time.strftime('%Y-%m-%d %H:%M:%S')}")

                # Print sensor data
                for channel in CHANNELS:
                    name = channel["name"]
                    value = waveplus.get_value(name)
                    unit = waveplus.get_unit(name)
                    logger.info(f"{name}: {value} {unit}")

            # Disconnect from device
            await waveplus.disconnect()

            # Sleep until next reading
            await asyncio.sleep(sample_period)

    except KeyboardInterrupt:
        logger.info("User interrupted. Exiting.")
    except Exception as e:
        logger.exception(f"Error: {e}")
    finally:
        # Ensure device is disconnected
        await waveplus.disconnect()


def main() -> None:
    """Main entry point for the script."""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    args = _argparser()

    # Set up signal handler for graceful exit
    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        loop.run_until_complete(run_waveplus(args))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
