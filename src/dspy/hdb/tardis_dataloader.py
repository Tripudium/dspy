"""
Module for loading and processing Tardis data obtained via Terank.
This module provides functionality to access and manipulate Tardis datasets.
"""

from pathlib import Path
from datetime import datetime
import logging
import polars as pl

# Local imports
from dspy.hdb.base import DataLoader
from dspy.hdb.registry import register_dataset
from dspy.utils import str_to_timedelta, round_up_to_nearest

logger = logging.getLogger(__name__)    

TARDIS_DATA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "tardis"

@register_dataset("tardis")
class TardisData(DataLoader):
    """
    Dataloader for Tardis data (obtained via Terank)
    """
    def __init__(self, root: str | Path = TARDIS_DATA_PATH):
        logger.info("Initializing TardisDataLoader with path %s", root)
        super().__init__(root)

    def load_book(self, products: list[str], times: list[str], depth: int = 1, lazy=False) -> pl.DataFrame:
        """
        Load book data for a given product and times.
        """
        df = super().load_book(products, times, depth, lazy)
        df = df.rename({"prc__s0": "prc_s0", "prc__s1": "prc_s1", "vol__s0": "vol_s0", "vol__s1": "vol_s1"})
        return df


