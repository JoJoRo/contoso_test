from __future__ import annotations

import json
import logging
from typing import Any, Dict, Optional

from pyspark.sql import SparkSession, DataFrame, functions as F

from ingestion_engine.etl.transform.data_cleaner import DataCleaner


logger = logging.getLogger(__name__)


class DataHubJob:
    def __init__(self, config: Dict[str, Any], metadata_path: str):
        if not metadata_path:
            logger.error("metadata_path not given")
            raise ValueError("metadata_path not given")

        self.config = config or {}
        self.metadata_path = metadata_path

        logger.info("Loading Data Hub metadata from: %s", self.metadata_path)
        with open(self.metadata_path, "r", encoding="utf-8") as f:
            self.metadata = json.loads(f.read())

        if "input_entity" not in self.metadata or "target_entity" not in self.metadata:
            raise ValueError("Invalid metadata: missing input_entity or target_entity")

        self.cleaner = DataCleaner()

    def run(self, spark: Optional[SparkSession] = None) -> None:
        if not self.metadata.get("enabled", False):
            logger.info("DataHubJob disabled by metadata. Exiting.")
            return

        spark = spark or SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()

        tgt = self.metadata["target_entity"]
        mode = (self.metadata.get("ingestion_information", {}).get("mode") or "append").strip().lower()
        rules = self.metadata.get("transformation", {}) or {}

        full_target = f"{tgt['catalog']}.{tgt['schema']}.{tgt['table']}"
        logger.info("Target=%s mode=%s", full_target, mode)

        # Ensure schema exists
        try:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {tgt['catalog']}.{tgt['schema']}")
        except Exception:
            logger.exception("Failed to ensure schema exists: %s.%s", tgt["catalog"], tgt["schema"])
            raise

        # Read raw sources
        sources = self.metadata["input_entity"].get("sources", [])
        if not sources or not isinstance(sources, list):
            raise ValueError("Invalid metadata: input_entity.sources must be a non-empty list")

        dfs: Dict[str, DataFrame] = {}
        for src in sources:
            if (src.get("type") or "").strip().lower() != "delta_table":
                raise ValueError(f"Unsupported source type: {src.get('type')}")

            alias = src.get("alias")
            if not alias:
                raise ValueError("Each source must include an alias")

            full_name = f"{src['catalog']}.{src['schema']}.{src['table']}"
            logger.info("Reading source table: %s as alias=%s", full_name, alias)
            try:
                dfs[alias] = spark.table(full_name)
            except Exception:
                logger.exception("Failed reading table: %s", full_name)
                raise

        # Minimal cleaning: drop nulls on required keys
        customers = self.cleaner.drop_nulls(dfs["customers"], columns=["customer_id"])
        orders = self.cleaner.drop_nulls(dfs["orders"], columns=["customer_id", "order_id", "order_amount"])

        # Build loyal_customers
        min_orders = int(rules.get("loyalty_min_orders", 3))
        min_spend = float(rules.get("loyalty_min_total_spend", 200.0))

        logger.info("Building loyal_customers with min_orders=%s min_total_spend=%s", min_orders, min_spend)

        orders_agg = (
            orders.groupBy("customer_id")
            .agg(
                F.countDistinct("order_id").alias("order_count"),
                F.sum(F.col("order_amount")).alias("total_spend"),
                F.max(F.col("order_date")).alias("last_order_date"),
            )
        )

        loyal = (
            customers.alias("c")
            .join(orders_agg.alias("o"), on="customer_id", how="inner")
            .filter((F.col("order_count") >= F.lit(min_orders)) & (F.col("total_spend") >= F.lit(min_spend)))
            .select(
                F.col("customer_id"),
                F.col("c.customer_code").alias("customer_code"),
                F.col("c.first_name").alias("first_name"),
                F.col("c.last_name").alias("last_name"),
                F.col("c.country").alias("country"),
                F.col("order_count"),
                F.col("total_spend"),
                F.col("last_order_date"),
            )
        )

        if mode == "overwrite":
            logger.warning("Overwrite mode requested. Dropping table if exists: %s", full_target)
            try:
                spark.sql(f"DROP TABLE IF EXISTS {full_target}")
            except Exception:
                logger.exception("Failed dropping table: %s", full_target)
                raise

        # Ensure table exists
        if not spark.catalog.tableExists(full_target):
            logger.info("Creating target table (empty) for: %s", full_target)
            try:
                loyal.limit(0).write.format("delta").mode("overwrite").saveAsTable(full_target)
            except Exception:
                logger.exception("Failed creating table: %s", full_target)
                raise

        # Write
        logger.info("Writing Data Hub table: %s", full_target)
        try:
            loyal.write.format("delta").mode("append").saveAsTable(full_target)
        except Exception:
            logger.exception("Failed writing Data Hub table: %s", full_target)
            raise

        logger.info("DataHubJob completed successfully")
