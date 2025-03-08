import asyncio
import logging
import os
import signal
from typing import Any, Dict, Set, Tuple

# Import python-kasa for TP-Link Kasa devices
import kasa
from bleak import BleakClient, BleakScanner
from bleak.backends.device import BLEDevice
from bleak.backends.scanner import AdvertisementData
from dotenv import load_dotenv


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
    """Decode temperature, humidity, battery and error from bytes.

    Args:
        data: Raw bytes from the GVH sensor

    Returns:
        Tuple of (temperature, humidity, battery_percentage, error_flag)
    """
    temp, humi = decode_temp_humid(data[0:3])
    batt = int(data[-1] & 0x7F)
    err = bool(data[-1] & 0x80)
    return temp, humi, batt, err


async def discover_ble_devices(logger: logging.Logger, duration: int = 30) -> Set[str]:
    """Discover BLE devices.

    Args:
        logger: Logger instance
        duration: Scan duration in seconds

    Returns:
        Set of discovered device addresses
    """
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
        await asyncio.sleep(duration)
        await scanner.stop()

        if found_devices:
            logger.info(f"Found BLE devices: {found_devices}")
            for device_addr in found_devices:
                client = BleakClient(device_addr)
                try:
                    await client.connect()
                    logger.info(f"Connected to {device_addr}")
                    # Get services - we keep this for debugging purposes
                    services = await client.get_services()
                    logger.debug(f"Services: {services}")
                except Exception as e:
                    logger.error(f"Error connecting to device {device_addr}: {e}")
                finally:
                    await client.disconnect()
        else:
            logger.info("No BLE devices found")
    except Exception as e:
        logger.error(f"Error discovering BLE devices: {e}")

    return found_devices


async def discover_kasa_devices(logger: logging.Logger) -> Dict[str, Any]:
    """Discover Kasa devices on the network.

    Args:
        logger: Logger instance

    Returns:
        Dictionary mapping device addresses to device objects
    """
    # Load Kasa credentials from .env
    kasa_username = os.getenv("KASA_USERNAME")
    kasa_password = os.getenv("KASA_PASSWORD")
    if not kasa_username or not kasa_password:
        logger.error("Kasa credentials not found in environment variables")
        return {}

    credentials = kasa.Credentials(username=kasa_username, password=kasa_password)

    logger.info("Discovering Kasa devices...")
    found_devices = {}

    try:
        # Create client and discover devices
        devices = await kasa.Discover.discover(
            credentials=credentials, interface="wlo1", target="192.168.50.255"
        )

        if devices:
            logger.info(f"Found {len(devices)} Kasa devices")
            for addr, device in devices.items():
                try:
                    # Connect to device to get more info
                    await device.update()
                    logger.info(
                        f"Kasa device: {device.alias} ({device.model}) at {addr}"
                    )
                    found_devices[addr] = device
                except Exception as e:
                    logger.error(f"Error getting info for Kasa device at {addr}: {e}")
        else:
            logger.info("No Kasa devices found")
    except Exception as e:
        logger.error(f"Error discovering Kasa devices: {e}")

    return found_devices


async def run_discovery(
    logger: logging.Logger,
    discover_ble: bool = True,
    discover_kasa: bool = False,
    duration: int = 30,
) -> Tuple[Set[str], Dict[str, Any]]:
    """Run device discovery.

    Args:
        logger: Logger instance
        discover_ble: Whether to discover BLE devices
        discover_kasa: Whether to discover Kasa devices
        duration: Duration for BLE scanning in seconds

    Returns:
        Tuple of (set of discovered BLE addresses, dict of Kasa devices)
    """
    ble_devices: Set[str] = set()
    kasa_devices: Dict[str, Any] = {}

    # Create tasks for selected discovery methods
    tasks = []
    if discover_ble:
        tasks.append(discover_ble_devices(logger, duration))
    if discover_kasa:
        tasks.append(discover_kasa_devices(logger))

    # Run discovery tasks concurrently
    if tasks:
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        result_idx = 0
        if discover_ble:
            if isinstance(results[result_idx], Exception):
                logger.error(f"BLE discovery failed: {results[result_idx]}")
            else:
                ble_devices = results[result_idx]
            result_idx += 1

        if discover_kasa:
            if isinstance(results[result_idx], Exception):
                logger.error(f"Kasa discovery failed: {results[result_idx]}")
            else:
                kasa_devices = results[result_idx]

    return ble_devices, kasa_devices


def main() -> None:
    """Main entry point for the script."""
    # Load environment variables
    load_dotenv("../.env", override=True)

    # Configure logging
    logger = logging.getLogger(__name__)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # Parse command line arguments
    import argparse

    parser = argparse.ArgumentParser(description="Discover devices on the network")
    parser.add_argument("--ble", action="store_true", help="Discover BLE devices")
    parser.add_argument("--kasa", action="store_true", help="Discover Kasa devices")
    parser.add_argument("--all", action="store_true", help="Discover all device types")
    parser.add_argument(
        "--duration", type=int, default=30, help="BLE scan duration in seconds"
    )
    args = parser.parse_args()

    # If no specific discovery type is selected, default to all
    discover_ble = args.ble or args.all or not (args.ble or args.kasa or args.all)
    discover_kasa = args.kasa or args.all

    # Create a new event loop explicitly
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Set up signal handler for graceful exit
    def signal_handler() -> None:
        loop.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    try:
        ble_devices, kasa_devices = loop.run_until_complete(
            run_discovery(logger, discover_ble, discover_kasa, args.duration)
        )

        # Summarize findings
        if discover_ble:
            logger.info(f"Found {len(ble_devices)} BLE devices")

        if discover_kasa:
            logger.info(f"Found {len(kasa_devices)} Kasa devices")
    finally:
        loop.close()


if __name__ == "__main__":
    main()
