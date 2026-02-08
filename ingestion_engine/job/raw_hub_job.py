from __future__ import annotations

from typing import Any, Dict, Optional
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
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


class RawHubJob:
    CDC_INGESTION_DATE_COL = "ingestion_date"
    CDC_RUN_ID_COL = "run_id"

    def __init__(self, config: Dict[str, Any], metadata_path: str):
        """
        Check metadata path given and Raw Hub given.

        :param config:
        :param metadata_path:
        """
        if metadata_path is None:
            raise ValueError("metadata_path not given")

        self.config = config or {}
        self.metadata_path = metadata_path
        self.metadata = load_and_validate_metadata(self.metadata_path)

    def run(self, spark: Optional[SparkSession] = None) -> None:
        if not self.metadata.enabled:
            return

        spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

        src = self.metadata.input_entity.source
        tgt = self.metadata.target_entity
        mode = (self.metadata.ingestion_information.mode or "append").strip().lower()

        input_path = self._build_input_path(self.config, src.folder, src.file_name)
        full_table_name = f"{tgt.catalog}.{tgt.schema}.{tgt.table}"

        # Ensure catalog/schema exist (no catalog for pre-UC Databricks workspaces)
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tgt.catalog}.{tgt.schema}")

        # Read input CSV using options
        df = self._read_csv(spark, input_path, src.read_options)

        # Cast/apply transforms according to metadata schema
        df = self._apply_schema_and_transforms(df, src.schema)

        # Add CDC columns (ingestion_date + run_id)
        df = self._add_cdc(df, spark)

        # Handle overwrite: drop table first
        if mode == "overwrite":
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")

        # Create table if missing (after overwrite drop too)
        self._ensure_table_exists(spark, full_table_name, df)

        # Write
        writer = df.write.format("delta").mode("append")  # always append after ensuring table exists
        writer.saveAsTable(full_table_name)

    def _build_input_path(self, config: Dict[str, Any], folder: str, file_name: str) -> str:
        """
        config can optionally provide:
          - base_path: e.g. 'dbfs:/mnt/landing' or '/dbfs/mnt/landing'
          - input_base_path: alternative key
        """
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

        # Spark expects string values for options usually; also header can be bool -> normalize
        for k, v in list(opts.items()):
            if isinstance(v, bool):
                opts[k] = "true" if v else "false"
            elif v is None:
                opts.pop(k)

        return spark.read.options(**opts).csv(path)

    def _apply_schema_and_transforms(self, df: DataFrame, schema_meta) -> DataFrame:
        """
        For each column in metadata:
          - if transform_sql exists, apply via expr()
          - else cast to declared type
        """
        for col_meta in schema_meta.columns:
            name = col_meta.name
            if name not in df.columns:
                # If the CSV missed a required column, you'll find out later when writing/casting fails.
                # Keep it small: raise early for non-nullable expected columns.
                if col_meta.nullable is False:
                    raise ValueError(f"Missing required column in input: {name}")
                continue

            if col_meta.transform_sql:
                df = df.withColumn(name, F.expr(col_meta.transform_sql))
            else:
                df = df.withColumn(name, F.col(name).cast(self._spark_type(col_meta.type)))

        # Optionally enforce not-null for non-nullable columns (simple check)
        non_nullable = [c.name for c in schema_meta.columns if c.nullable is False]
        for c in non_nullable:
            df = df.filter(F.col(c).isNotNull())

        return df

    def _add_cdc(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        # ingestion_date as date (UTC)
        df = df.withColumn(self.CDC_INGESTION_DATE_COL, F.to_date(F.current_timestamp()))

        # run_id from Databricks job if available; fallback to Spark app id; then fallback to timestamp
        run_id = self._get_databricks_run_id(spark) or spark.sparkContext.applicationId
        if not run_id:
            run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        df = df.withColumn(self.CDC_RUN_ID_COL, F.lit(str(run_id)))
        return df

    def _get_databricks_run_id(self, spark: SparkSession) -> Optional[str]:
        """
        Best-effort:
          - Databricks sets spark.conf 'spark.databricks.job.runId' in Jobs runs (commonly).
        """
        try:
            v = spark.conf.get("spark.databricks.job.runId", None)
            if v:
                return str(v)
        except Exception:
            pass
        return None

    def _ensure_table_exists(self, spark: SparkSession, full_table_name: str, df: DataFrame) -> None:
        if spark.catalog.tableExists(full_table_name):
            return

        # Create empty Delta table with the same schema as df
        empty = df.limit(0)
        empty.write.format("delta").mode("overwrite").saveAsTable(full_table_name)

    def _spark_type(self, type_str: str):
        """
        Minimal parser for common Spark SQL type strings used in your metadata.
        Examples: 'long', 'string', 'decimal(12,2)', 'date', 'timestamp', 'int'
        """
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
            # decimal(p,s)
            inside = t[t.find("(") + 1: t.find(")")] if "(" in t and ")" in t else ""
            if inside and "," in inside:
                p_str, s_str = [x.strip() for x in inside.split(",", 1)]
                return DecimalType(int(p_str), int(s_str))
            # default decimal if not provided
            return DecimalType(38, 18)

        # fallback: let Spark try to interpret it
        return t
