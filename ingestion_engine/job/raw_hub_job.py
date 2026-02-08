from typing import Any, Dict
from ingestion_engine.etl.schema.csv_validator import load_and_validate_metadata


class RawHubJob:
    def __init__(self, config: Dict[str, Any], metadata_path: str):
        """
        Check metadata path given and Raw Hub given.

        :param config:
        :param metadata_path:
        """
        if metadata_path is None:
            raise ValueError("metadata_path not given")

        self.config = config
        self.metadata_path = metadata_path
        self.metadata = load_and_validate_metadata(self.metadata_path)
