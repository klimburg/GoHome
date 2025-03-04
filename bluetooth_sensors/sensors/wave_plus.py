import argparse
import asyncio
import logging
import signal
import struct
import time
from typing import Callable, Dict, List, Optional, Set, Union, cast

from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from sift_py.ingestion.channel import ChannelDataType

from ._ble_sensor import BluetoothSensor, ChannelDict, ConversionFunc

# Airthings Wave Plus characteristic UUID
CURR_VAL_UUID = "b42e2a68-ade7-11e4-89d3-123b93f75cba"

# Airthings manufacturer ID
AIRTHINGS_MFG_ID = 0x0334


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


def illuminance_convert(raw_value: int) -> float:
    """Convert raw illuminance value."""
    return raw_value / 255.0 * 100.0


# Comprehensive sensor definitions
CHANNELS: List[ChannelDict] = [
    {
        "name": "humidity",
        "unpacked_index": 1,
        "conversion": cast(ConversionFunc, divide_by(2.0)),
        "unit": "%rH",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "radon_short_term_avg",
        "unpacked_index": 4,
        "conversion": cast(ConversionFunc, convert_radon),
        "unit": "Bq/m3",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "radon_long_term_avg",
        "unpacked_index": 5,
        "conversion": cast(ConversionFunc, convert_radon),
        "unit": "Bq/m3",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "temperature",
        "unpacked_index": 6,
        "conversion": cast(ConversionFunc, divide_by(100.0)),
        "unit": "degC",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "pressure",
        "unpacked_index": 7,
        "conversion": cast(ConversionFunc, divide_by(50.0)),
        "unit": "hPa",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "CO2_level",
        "unpacked_index": 8,
        "conversion": cast(ConversionFunc, multiply_by(1.0)),
        "unit": "ppm",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "VOC_level",
        "unpacked_index": 9,
        "conversion": cast(ConversionFunc, multiply_by(1.0)),
        "unit": "ppb",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "illuminance",
        "unpacked_index": 2,
        "conversion": cast(ConversionFunc, illuminance_convert),
        "unit": "lux",
        "sift_type": ChannelDataType.DOUBLE,
    },
    {
        "name": "battery",
        "unpacked_index": 11,
        "conversion": cast(ConversionFunc, divide_by(10.0)),
        "unit": "%",
        "sift_type": ChannelDataType.DOUBLE,
    },
]


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

    async def read(self) -> bool:
        """Read sensor values from the Wave Plus device.

        Returns:
            True if reading was successful, False otherwise
        """
        if not self.client or not self.client.is_connected:
            self.logger.error("Device not connected")
            return False

        try:
            rawdata = await self.client.read_gatt_char(CURR_VAL_UUID)
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
        # Unpack the binary data
        unpacked_data = struct.unpack("<BBBBHHHHHHHH", rawdata)

        self.read_time = time.time()
        self._sensor_version = unpacked_data[0]

        for channel in CHANNELS:
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
        "serial_number",
        type=int,
        help="Airthings device 10-digit serial number found under the magnetic backplate",
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
    sample_period = args.sample_period

    # Set up logger
    logger = logging.getLogger("WavePlus")

    # Validate serial number format
    if len(str(serial_number)) != 10:
        logger.error("Error: Serial number must be 10 digits.")
        return

    # Create and initialize the Wave Plus device
    waveplus = WavePlus(serial_number, logger=logger)

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
