"""
Module for loading and processing Tardis data obtained via Terank.
This module provides functionality to access and manipulate Tardis datasets.
"""

from pathlib import Path
from datetime import datetime
import logging
import polars as pl

from trpy_data.data.load_data import load_contract

# Local imports
from dspy.hdb.base import DataLoader
from dspy.hdb.registry import register_dataset
from dspy.utils import str_to_timedelta, round_up_to_nearest

logger = logging.getLogger(__name__)    

TERANK_DATA_PATH = DATA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "terank"

@register_dataset("terank")
class TerankData(DataLoader):
    """
    Dataloader for Terank data (obtained via Terank)
    """
    def __init__(self, root: str | Path = TERANK_DATA_PATH):
        logger.info("Initializing TerankDataLoader with path %s", root)
        super().__init__(root)

    def load_book(self, products: list[str], times: list[str], depth: int = 1, lazy=False) -> pl.DataFrame:
        """
        Load book data for a given product and times.
        """
        if isinstance(products, str):
            products = [products]
        timestr = f"{times[0][:-2]}:{times[1][:-2]}"
        
        dfs = []

        for product in products:
            prodname = product+"__BNCE_USDTM"
            df = load_contract(update_type="depth", contract_spec=prodname, times_range=timestr)
            df = df.select([pl.col("tse").alias("ts"), 
                            pl.col("prc_s0").alias("prc__s0"), 
                            pl.col("prc_s1").alias("prc__s1"),
                            pl.col("vols_s0").alias("vol__s0"),
                            pl.col("vols_s1").alias("vol__s1")])
            if lazy:
                columns = df.collect_schema().names()
            else:
                df = df.collect()
                columns = df.columns
            rename_map = {
                col: f"{col}_{product}" for col in columns if col != "ts"
            }
            df = df.rename(rename_map)
            dfs.append(df)
        merged_df = pl.concat([df.select('ts') for df in dfs], how='vertical').unique('ts').sort('ts')
        for i, df in enumerate(dfs):
            merged_df = merged_df.join_asof(df, on='ts')
        return merged_df.drop_nulls().sort('ts')
    
    def load_trades(self, products: list[str] | str, times: list[str], lazy=False) -> pl.DataFrame:
        """
        Load trades data for a given product and times.
        """
        if isinstance(products, str):
            products = [products]
        timestr = f"{times[0][:-2]}:{times[1][:-2]}"
        
        dfs = []
        for product in products:
            prodname = product+"__BNCE_USDTM"
            df = load_contract(update_type="trade", contract_spec=prodname, times_range=timestr)
            df = df.select([pl.col("tse").alias("ts"), pl.col("prc"), pl.col("qty"), pl.col("blended_id").alias("trade_id")])
            df = df.with_columns(pl.lit(product).alias('product'))
            dfs.append(df)
        tdf = pl.concat(dfs).sort('ts')
        if not lazy:
            tdf = tdf.collect()
        return tdf

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


