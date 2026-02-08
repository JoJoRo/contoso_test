import logging
from typing import List, Optional

from pyspark.sql import DataFrame


logger = logging.getLogger(__name__)


class DataCleaner:
    def drop_nulls(self, df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        if columns:
            logger.info("Dropping rows with NULLs in columns=%s", columns)
            return df.dropna(subset=columns)

        logger.info("Dropping rows with any NULLs")
        return df.dropna()
