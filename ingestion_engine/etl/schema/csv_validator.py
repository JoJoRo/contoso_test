from dataclasses import dataclass
from typing import List, Optional, Any, Dict
import json


# --------- Leaf Classes ---------

@dataclass
class Column:
    name: str
    type: str
    nullable: bool
    transform_sql: Optional[str] = None
    date_format: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Column":
        required = ["name", "type", "nullable"]
        for field in required:
            if field not in data:
                raise ValueError(f"Column missing required field: {field}")

        return Column(
            name=data["name"],
            type=data["type"],
            nullable=data["nullable"],
            transform_sql=data.get("transform_sql"),
            date_format=data.get("date_format"),
        )


@dataclass
class Schema:
    columns: List[Column]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Schema":
        if "columns" not in data or not isinstance(data["columns"], list):
            raise ValueError("Schema must contain a 'columns' list")

        columns = [Column.from_dict(col) for col in data["columns"]]
        return Schema(columns=columns)


@dataclass
class Source:
    type: str
    folder: str
    file_name: str
    read_options: Dict[str, Any]
    schema: Schema

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Source":
        required = ["type", "folder", "file_name", "read_options", "schema"]
        for field in required:
            if field not in data:
                raise ValueError(f"Source missing required field: {field}")

        return Source(
            type=data["type"],
            folder=data["folder"],
            file_name=data["file_name"],
            read_options=data["read_options"],
            schema=Schema.from_dict(data["schema"]),
        )


@dataclass
class InputEntity:
    source: Source

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "InputEntity":
        if "source" not in data:
            raise ValueError("input_entity must contain 'source'")
        return InputEntity(source=Source.from_dict(data["source"]))


@dataclass
class TargetEntity:
    catalog: str
    schema: str
    table: str
    columns_mapping: List[Any]

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "TargetEntity":
        required = ["catalog", "schema", "table", "columns_mapping"]
        for field in required:
            if field not in data:
                raise ValueError(f"target_entity missing required field: {field}")

        return TargetEntity(
            catalog=data["catalog"],
            schema=data["schema"],
            table=data["table"],
            columns_mapping=data["columns_mapping"],
        )


@dataclass
class IngestionInformation:
    mode: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "IngestionInformation":
        if "mode" not in data:
            raise ValueError("ingestion_information must contain 'mode'")
        return IngestionInformation(mode=data["mode"])


# --------- Root CSV Readers ---------

@dataclass
class CSVReader:
    version: float
    enabled: bool
    input_entity: InputEntity
    target_entity: TargetEntity
    ingestion_information: IngestionInformation

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "CSVReader":
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

        return CSVReader(
            version=data["version"],
            enabled=data["enabled"],
            input_entity=InputEntity.from_dict(data["input_entity"]),
            target_entity=TargetEntity.from_dict(data["target_entity"]),
            ingestion_information=IngestionInformation.from_dict(
                data["ingestion_information"]
            ),
        )


# --------- Public API ---------

def load_and_validate_metadata(json_string: str) -> CSVReader:
    try:
        data = json.loads(json_string)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

    return CSVReader.from_dict(data)


# --------- Example Usage ---------
if __name__ == "__main__":
    with open("ingestion_engine/tests/metadata/orders.json", "r") as f:
        metadata = load_and_validate_metadata(f.read())
    print("CSV metadata is valid.")
