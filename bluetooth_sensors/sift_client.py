import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import yaml
from sift_py.grpc.transport import use_sift_channel
from sift_py.ingestion.channel import (
    bool_value,
    double_value,
    float_value,
    int32_value,
    int64_value,
    string_value,
)
from sift_py.ingestion.config.telemetry import TelemetryConfig
from sift_py.ingestion.flow import Flow
from sift_py.ingestion.service import IngestionService


class SiftClient:
    """Client for sending telemetry data to Sift."""

    # Mapping of YAML data types to Sift casting functions
    TYPE_CASTERS = {
        "double": double_value,
        "float": float_value,
        "int32": int32_value,
        "int64": int64_value,
        "bool": bool_value,
        "string": string_value,
    }

    def __init__(
        self, config_path: str, api_key: str, base_uri: str, use_ssl: bool
    ) -> None:
        """Initialize the Sift client.

        Args:
            config_path: Path to the telemetry configuration YAML file
            api_key: Sift API key
            base_uri: Base URI for Sift API
            use_ssl: Whether to use SSL for API connection
        """
        # Load telemetry configuration
        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.asset_name = self.config["asset_name"]
        self._api_key = api_key
        self._base_uri = base_uri
        self._use_ssl = use_ssl

        self.run_name = f"{self.asset_name}.{time.time()}"

        # Setup Sift configuration
        sift_channel_config = {
            "apikey": self._api_key,
            "uri": self._base_uri,
            "use_ssl": self._use_ssl,
        }
        self.sift_channel = use_sift_channel(sift_channel_config)

        self.sift_telemetry_config = TelemetryConfig.try_from_yaml(config_path)

        # Create ingestion service
        self.ingestion_service = IngestionService(
            channel=self.sift_channel,
            config=self.sift_telemetry_config,
            end_stream_on_error=True,
        )
        self.ingestion_service.attach_run(self.sift_channel, self.run_name)
        self.run_id = self.ingestion_service.run_id

        # Create channel type mapping for faster lookups
        self.channel_types = {
            channel_config["name"]: channel_config["data_type"]
            for channel_config in self.config["channels"].values()
        }

    def format_flows(self, flow_data: List[Dict[str, Any]]) -> List[Flow]:
        """Format telemetry data into Sift Flow format.

        Args:
            flow_data: List of dictionaries containing telemetry data for each flow.
                Each dictionary has channel names as keys and their values as values.

        Returns:
            List of Flow dictionaries formatted for Sift ingestion.
        """
        formatted_flows = []
        current_time = datetime.now(timezone.utc)

        for flow_dict, flow_config in zip(flow_data, self.config["flows"]):
            # Format channel values
            channel_values = []
            for channel_name, value in flow_dict.items():
                # Get the data type and cast the value
                data_type = self.channel_types[channel_name]
                cast_func = self.TYPE_CASTERS[data_type]

                channel_values.append(
                    {"channel_name": channel_name, "value": cast_func(value)}
                )

            # Create Flow dictionary
            flow: Flow = {
                "flow_name": flow_config["name"],
                "timestamp": current_time,
                "channel_values": channel_values,
            }
            formatted_flows.append(flow)

        return formatted_flows

    def send_telemetry(self, flow_data: List[Dict[str, Any]]) -> None:
        """Send telemetry data to Sift.

        Args:
            flow_data: List of dictionaries containing telemetry data for each flow.
                Each dictionary has channel names as keys and their values as values.
        """
        # Format data into Sift Flow format
        formatted_flows = self.format_flows(flow_data)

        # Send flows to Sift
        self.ingestion_service.try_ingest_flows(*formatted_flows)

    def close(self) -> None:
        """Close the Sift client connection."""
        self.sift_channel.close()
