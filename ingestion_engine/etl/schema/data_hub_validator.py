from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import json


# --------- Leaf Classes ---------

@dataclass
class Source:
    type: str
    catalog: str
    schema: str
    table: str
    alias: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Source":
        required = ["type", "catalog", "schema", "table", "alias"]

        for field in required:
            if field not in data:
                raise ValueError(f"Source missing required field: {field}")

        return Source(
            type=data["type"],
            catalog=data["catalog"],
            schema=data["schema"],
            table=data["table"],
            alias=data["alias"],
        )


@dataclass
class InputEntity:
    sources: List[Source]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "InputEntity":
        if "sources" not in data or not isinstance(data["sources"], list):
            raise ValueError("input_entity must contain a 'sources' list")

        if len(data["sources"]) == 0:
            raise ValueError("input_entity.sources cannot be empty")

        sources = [Source.from_dict(s) for s in data["sources"]]
        return InputEntity(sources=sources)


@dataclass
class TargetEntity:
    catalog: str
    schema: str
    table: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "TargetEntity":
        required = ["catalog", "schema", "table"]

        for field in required:
            if field not in data:
                raise ValueError(f"target_entity missing required field: {field}")

        return TargetEntity(
            catalog=data["catalog"],
            schema=data["schema"],
            table=data["table"],
        )


@dataclass
class IngestionInformation:
    mode: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "IngestionInformation":
        if "mode" not in data:
            raise ValueError("ingestion_information must contain 'mode'")

        return IngestionInformation(mode=data["mode"])


@dataclass
class Transformation:
    loyalty_min_orders: int
    loyalty_min_total_spend: float

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Transformation":
        return Transformation(
            loyalty_min_orders=int(data.get("loyalty_min_orders", 3)),
            loyalty_min_total_spend=float(data.get("loyalty_min_total_spend", 200.0)),
        )


# --------- Root Reader ---------

@dataclass
class DataHubReader:
    version: float
    enabled: bool
    input_entity: InputEntity
    target_entity: TargetEntity
    ingestion_information: IngestionInformation
    transformation: Transformation

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "DataHubReader":
        required = [
            "version",
            "enabled",
            "input_entity",
            "target_entity",
            "ingestion_information",
        ]

        for field in required:
            if field not in data:
                raise ValueError(f"Missing required field at root level: {field}")

        return DataHubReader(
            version=data["version"],
            enabled=data["enabled"],
            input_entity=InputEntity.from_dict(data["input_entity"]),
            target_entity=TargetEntity.from_dict(data["target_entity"]),
            ingestion_information=IngestionInformation.from_dict(
                data["ingestion_information"]
            ),
            transformation=Transformation.from_dict(
                data.get("transformation", {})
            ),
        )


# --------- Public API ---------

def load_and_validate_data_hub_metadata(json_string: str) -> DataHubReader:
    try:
        data = json.loads(json_string)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

    return DataHubReader.from_dict(data)


# --------- Example Usage ---------

if __name__ == "__main__":
    with open("ingestion_engine/tests/metadata/data_hub_loyal_customers.json", "r") as f:
        metadata = load_and_validate_data_hub_metadata(f.read())

    print("Data Hub metadata is valid.")
