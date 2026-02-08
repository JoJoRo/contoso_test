from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StringType,
    LongType,
    IntegerType,
    ShortType,
    ByteType,
    BooleanType,
    FloatType,
    DoubleType,
    DecimalType,
    DateType,
    TimestampType,
)

from ingestion_engine.etl.schema.csv_validator import load_and_validate_metadata
from ingestion_engine.etl.schema.olap_validator import olap_load_and_validate_metadata


logger = logging.getLogger(__name__)


class RawHubJob:
    CDC_INGESTION_DATE_COL = "ingestion_date"
    CDC_RUN_ID_COL = "run_id"

    def __init__(self, config: Dict[str, Any], metadata_path: str):
        if metadata_path is None:
            logger.error("metadata_path not given")
            raise ValueError("metadata_path not given")

        self.config = config or {}
        self.metadata_path = metadata_path

        logger.info("Loading metadata from: %s", self.metadata_path)
        try:
            raw = self._read_text_file(self.metadata_path)
            src_type = ((json.loads(raw).get("input_entity") or {}).get("source") or {}).get("type", "")
            src_type = (src_type or "").strip().lower()

            if src_type == "olap":
                self.metadata = olap_load_and_validate_metadata(raw)
                self.source_kind = "olap"
            else:
                self.metadata = load_and_validate_metadata(raw)
                self.source_kind = "csv"
        except Exception:
            logger.exception("Failed to load/validate metadata from: %s", self.metadata_path)
            raise

        logger.info(
            "Metadata loaded. kind=%s enabled=%s mode=%s target=%s.%s.%s",
            getattr(self, "source_kind", None),
            getattr(self.metadata, "enabled", None),
            getattr(getattr(self.metadata, "ingestion_information", None), "mode", None),
            getattr(getattr(self.metadata, "target_entity", None), "catalog", None),
            getattr(getattr(self.metadata, "target_entity", None), "schema", None),
            getattr(getattr(self.metadata, "target_entity", None), "table", None),
        )

    def run(self, spark: Optional[SparkSession] = None) -> None:
        if not self.metadata.enabled:
            logger.info("Job disabled by metadata. Exiting.")
            return

        logger.info("Starting RawHubJob run")
        try:
            spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
        except Exception:
            logger.exception("Failed to acquire/create SparkSession")
            raise

        src = self.metadata.input_entity.source
        tgt = self.metadata.target_entity
        mode = (self.metadata.ingestion_information.mode or "append").strip().lower()
        full_table_name = f"{tgt.catalog}.{tgt.schema}.{tgt.table}"

        logger.info("Resolved target_table=%s mode=%s", full_table_name, mode)

        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tgt.catalog}.{tgt.schema}")
        except Exception:
            logger.exception("Failed to ensure schema exists: %s.%s", tgt.catalog, tgt.schema)
            raise

        # Read source
        try:
            if (src.type or "").strip().lower() == "olap":
                logger.info("Reading OLAP source")
                df = self._read_olap(spark, src)
            else:
                input_path = self._build_input_path(self.config, src.folder, src.file_name)
                logger.info("Reading CSV from: %s", input_path)
                df = self._read_csv(spark, input_path, src.read_options)
        except Exception:
            logger.exception("Failed to read source")
            raise

        try:
            logger.info("Applying schema and transforms")
            df = self._apply_schema_and_transforms(df, src.schema)
        except Exception:
            logger.exception("Failed while applying schema/transforms")
            raise

        try:
            logger.info("Adding CDC columns: %s, %s", self.CDC_INGESTION_DATE_COL, self.CDC_RUN_ID_COL)
            df = self._add_cdc(df, spark)
        except Exception:
            logger.exception("Failed while adding CDC columns")
            raise

        if mode == "overwrite":
            logger.warning("Overwrite mode requested. Dropping table if exists: %s", full_table_name)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
            except Exception:
                logger.exception("Failed to drop table in overwrite mode: %s", full_table_name)
                raise

        try:
            logger.info("Ensuring table exists: %s", full_table_name)
            self._ensure_table_exists(spark, full_table_name, df)
        except Exception:
            logger.exception("Failed to ensure table exists: %s", full_table_name)
            raise

        logger.info("Writing to Delta table (append): %s", full_table_name)
        try:
            writer = df.write.format("delta").mode("append")
            writer.saveAsTable(full_table_name)
        except Exception:
            logger.exception("Failed while writing to table: %s", full_table_name)
            raise

        logger.info("RawHubJob completed successfully")

    def _read_text_file(self, path: str) -> str:
        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def _build_input_path(self, config: Dict[str, Any], folder: str, file_name: str) -> str:
        base = (
            config.get("input_base_path")
            or config.get("base_path")
            or config.get("landing_path")
            or ""
        ).rstrip("/")

        if base:
            return f"{base}/{folder}/{file_name}"
        return f"{folder}/{file_name}"

    def _read_csv(self, spark: SparkSession, path: str, read_options: Dict[str, Any]) -> DataFrame:
        opts = (read_options or {}).copy()

        for k, v in list(opts.items()):
            if isinstance(v, bool):
                opts[k] = "true" if v else "false"
            elif v is None:
                opts.pop(k)

        logger.debug("CSV read options: %s", opts)
        return spark.read.options(**opts).csv(path)

    def _read_olap(self, spark: SparkSession, src) -> DataFrame:
        conn = src.connection
        auth = conn.authentication
        obj = src.object

        # Minimal: Azure SQL via JDBC. In real runs you'd pull secret from a secret scope.
        url = f"jdbc:sqlserver://{conn.host}:{conn.port};database={conn.database};encrypt=true;trustServerCertificate=false;"

        # Keep it simple: for service principal we still need an access token flow;
        # this assumes you use a username/password compatible auth OR you inject token externally.
        # If you already have a working method, adapt only these 2 options.
        jdbc_opts: Dict[str, Any] = {
            "url": url,
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }

        # If your environment uses username/password for demos:
        if auth.mode.strip().lower() in {"sql", "username_password", "basic"}:
            jdbc_opts["user"] = auth.client_id
            jdbc_opts["password"] = auth.client_secret

        table = f"{obj.schema}.{obj.table}"
        predicate = getattr(obj, "predicate", None)
        if predicate:
            dbtable = f"(SELECT * FROM {table} WHERE {predicate}) AS t"
        else:
            dbtable = table
        jdbc_opts["dbtable"] = dbtable

        # Optional read options (partitioning etc.) are passed through as-is
        ro = (src.read_options or {}).copy()
        for k, v in list(ro.items()):
            if v is None:
                ro.pop(k)
        jdbc_opts.update(ro)

        logger.debug("OLAP JDBC options keys: %s", list(jdbc_opts.keys()))
        return spark.read.format("jdbc").options(**jdbc_opts).load()

    def _apply_schema_and_transforms(self, df: DataFrame, schema_meta) -> DataFrame:
        logger.info("Input columns count=%d", len(df.columns))
        logger.debug("Input columns: %s", df.columns)

        for col_meta in schema_meta.columns:
            name = col_meta.name
            if name not in df.columns:
                if col_meta.nullable is False:
                    logger.error("Missing required column in input: %s", name)
                    raise ValueError(f"Missing required column in input: {name}")
                logger.warning("Optional column missing in input: %s", name)
                continue

            if col_meta.transform_sql:
                logger.info("Applying transform on column=%s expr=%s", name, col_meta.transform_sql)
                df = df.withColumn(name, F.expr(col_meta.transform_sql))
            else:
                spark_type = self._spark_type(col_meta.type)
                logger.info("Casting column=%s to type=%s", name, col_meta.type)
                df = df.withColumn(name, F.col(name).cast(spark_type))

        non_nullable = [c.name for c in schema_meta.columns if c.nullable is False]
        if non_nullable:
            logger.info("Enforcing NOT NULL on columns: %s", non_nullable)
        for c in non_nullable:
            df = df.filter(F.col(c).isNotNull())

        return df

    def _add_cdc(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        df = df.withColumn(self.CDC_INGESTION_DATE_COL, F.to_date(F.current_timestamp()))

        run_id = self._get_databricks_run_id(spark) or spark.sparkContext.applicationId
        if not run_id:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            logger.warning("No run_id found from Databricks or Spark app id. Using timestamp fallback=%s", run_id)
        else:
            logger.info("Using run_id=%s", str(run_id))

        df = df.withColumn(self.CDC_RUN_ID_COL, F.lit(str(run_id)))
        return df

    def _get_databricks_run_id(self, spark: SparkSession) -> Optional[str]:
        try:
            v = spark.conf.get("spark.databricks.job.runId", None)
            if v:
                logger.info("Found Databricks runId from spark.conf: %s", v)
                return str(v)
            logger.debug("Databricks runId not present in spark.conf")
        except Exception:
            logger.exception("Error while reading spark.databricks.job.runId from spark.conf")
        return None

    def _ensure_table_exists(self, spark: SparkSession, full_table_name: str, df: DataFrame) -> None:
        try:
            exists = spark.catalog.tableExists(full_table_name)
        except Exception:
            logger.exception("Failed to check table existence: %s", full_table_name)
            raise

        if exists:
            logger.info("Table already exists: %s", full_table_name)
            return

        logger.warning("Table does not exist. Creating empty Delta table: %s", full_table_name)
        try:
            empty = df.limit(0)
            empty.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        except Exception:
            logger.exception("Failed to create table: %s", full_table_name)
            raise

    def _spark_type(self, type_str: str):
        t = type_str.strip().lower()

        if t == "string":
            return StringType()
        if t == "long" or t == "bigint":
            return LongType()
        if t == "int" or t == "integer":
            return IntegerType()
        if t == "short":
            return ShortType()
        if t == "byte":
            return ByteType()
        if t == "boolean":
            return BooleanType()
        if t == "float":
            return FloatType()
        if t == "double":
            return DoubleType()
        if t == "date":
            return DateType()
        if t == "timestamp":
            return TimestampType()

        if t.startswith("decimal"):
            inside = t[t.find("(") + 1 : t.find(")")] if "(" in t and ")" in t else ""
            if inside and "," in inside:
                p_str, s_str = [x.strip() for x in inside.split(",", 1)]
                return DecimalType(int(p_str), int(s_str))
            return DecimalType(38, 18)

        logger.warning("Unknown type in metadata: %s (passing through to Spark)", type_str)
        return t
