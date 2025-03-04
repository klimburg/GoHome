import asyncio
import logging
import signal
from typing import Set

from bleak import BleakScanner, BleakClient
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData


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


async def run_discovery(logger: logging.Logger) -> None:
    found_devices: Set[str] = set()

    def _detection_callback(
        device: BLEDevice, advertisement_data: AdvertisementData
    ) -> None:
        if device.name and device.name.startswith("GVH"):
            logger.debug(
                f"Found device: {device.name}, Address: {device.address}, advertisement_data: {advertisement_data}"
            )
            if 1 in advertisement_data.manufacturer_data.keys():
                data = advertisement_data.manufacturer_data[1]
                temp, humi, batt, err = decode_temp_humid_battery_error(data[2:6])
                logger.info(
                    f"{device.name} temp: {temp} C, humi: {humi} %rH, batt: {batt} %, err: {err}, data: {data!r}, msg_len: {len(data)}"
                )

            found_devices.add(device.address)

    scanner = BleakScanner(detection_callback=_detection_callback)
    try:
        await scanner.start()
        await asyncio.sleep(30)
        await scanner.stop()

        if found_devices:
            logger.info(f"Found devices: {found_devices}")
            for device in found_devices:
                client = BleakClient(device)
                await client.connect()
                await client.

                await device.get_services()
                await device.get_characteristics()
                await device.disconnect()
        else:
            logger.info("Device not found")
            return None
    except Exception as e:
        logger.error(f"Error discovering devices: {e}")
        return None


def main() -> None:
    """Main entry point for the script."""
    # Configure logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Create a new event loop explicitly
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Set up signal handler for graceful exit
    def signal_handler() -> None:
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        loop.run_until_complete(run_discovery(logger))
    finally:
        loop.close()


if __name__ == "__main__":
    main()
