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
        return super().load_book(products, times, depth, lazy)

    def load(self, products: list[str], times: list[str], col: str = "mid", freq: str = "1s", lazy=False) -> pl.DataFrame:
        """
        Load data for a given product and times.
        """
        df = self.load_book(products, times, lazy=lazy)
        df = df._dt.add_datetime()
        dtimes = [datetime.strptime(t, "%y%m%d.%H%M%S") for t in times]
        try:
            td = str_to_timedelta(freq)
        except ValueError:
            raise ValueError(f"Invalid frequency: {freq}")
        
        min_dt = round_up_to_nearest(df["dts"][0], td)
        max_dt = dtimes[1]

        # Make sure that every timestamp is present in the dataframe
        rdf = pl.DataFrame(
            { "dts": pl.datetime_range(min_dt, max_dt, freq, time_unit="ns", eager=True) }
        )
        rdf = rdf.join_asof(df, on="dts", strategy="backward")

        if col == "mid":
            rdf = rdf._feat.add_mid(cols=["prc__s0", "prc__s1"])
            if len(products) > 1:
                rdf = rdf.select([pl.col("dts").alias("ts")] + [pl.col(f"mid_{product}") for product in products])
            else:
                rdf = rdf.select([pl.col("dts").alias("ts"), pl.col("mid")])
        elif col == "vwap":
            rdf = rdf._feat.add_vwap(cols=["prc__s0", "prc__s1", "vol__s0", "vol__s1"])
            if len(products) > 1:
                rdf= rdf.select([pl.col("dts").alias("ts")] + [pl.col(f"vwap_{product}") for product in products]) 
            else:
                rdf = rdf.select([pl.col("dts").alias("ts"), pl.col("vwap")])
        return rdf


