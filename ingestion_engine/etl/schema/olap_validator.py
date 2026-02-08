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
class Authentication:
    mode: str
    tenant_id: str
    client_id: str
    client_secret: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Authentication":
        required = ["mode", "tenant_id", "client_id", "client_secret"]
        for field in required:
            if field not in data:
                raise ValueError(f"authentication missing required field: {field}")

        return Authentication(
            mode=data["mode"],
            tenant_id=data["tenant_id"],
            client_id=data["client_id"],
            client_secret=data["client_secret"],
        )


@dataclass
class Connection:
    host: str
    database: str
    port: int
    authentication: Authentication
    encrypt: bool
    trust_server_certificate: bool

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Connection":
        required = ["host", "database", "port", "authentication", "encrypt", "trust_server_certificate"]
        for field in required:
            if field not in data:
                raise ValueError(f"connection missing required field: {field}")

        return Connection(
            host=data["host"],
            database=data["database"],
            port=data["port"],
            authentication=Authentication.from_dict(data["authentication"]),
            encrypt=data["encrypt"],
            trust_server_certificate=data["trust_server_certificate"],
        )


@dataclass
class ObjectRef:
    schema: str
    table: str
    predicate: Optional[str] = None

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ObjectRef":
        required = ["schema", "table"]
        for field in required:
            if field not in data:
                raise ValueError(f"object missing required field: {field}")

        return ObjectRef(
            schema=data["schema"],
            table=data["table"],
            predicate=data.get("predicate"),
        )


@dataclass
class Source:
    type: str
    subtype: str
    connection: Connection
    object: ObjectRef
    read_options: Dict[str, Any]
    schema: Schema

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "Source":
        required = ["type", "subtype", "connection", "object", "read_options", "schema"]
        for field in required:
            if field not in data:
                raise ValueError(f"Source missing required field: {field}")

        return Source(
            type=data["type"],
            subtype=data["subtype"],
            connection=Connection.from_dict(data["connection"]),
            object=ObjectRef.from_dict(data["object"]),
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


# --------- Root OLAP Reader ---------

@dataclass
class OLAPReader:
    version: float
    enabled: bool
    input_entity: InputEntity
    target_entity: TargetEntity
    ingestion_information: IngestionInformation

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "OLAPReader":
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

        return OLAPReader(
            version=data["version"],
            enabled=data["enabled"],
            input_entity=InputEntity.from_dict(data["input_entity"]),
            target_entity=TargetEntity.from_dict(data["target_entity"]),
            ingestion_information=IngestionInformation.from_dict(
                data["ingestion_information"]
            ),
        )


# --------- Public API ---------

def olap_load_and_validate_metadata(json_string: str) -> OLAPReader:
    try:
        data = json.loads(json_string)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON: {e}")

    return OLAPReader.from_dict(data)


# --------- Example Usage ---------
if __name__ == "__main__":
    with open("ingestion_engine/tests/metadata/customers.json", "r") as f:
        metadata = olap_load_and_validate_metadata(f.read())
    print("OLAP metadata is valid.")
