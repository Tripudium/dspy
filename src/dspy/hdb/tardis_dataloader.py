"""
Module for loading and processing Tardis data obtained via Terank.
This module provides functionality to access and manipulate Tardis datasets.
"""

from pathlib import Path
import os
import logging
import polars as pl
from datetime import datetime, timedelta
from tardis_dev import datasets
import gzip
import shutil
import nest_asyncio


# Local imports
from dspy.hdb.base import DataLoader
from dspy.hdb.registry import register_dataset
from dspy.utils import nanoseconds, str_to_timedelta, round_up_to_nearest

logger = logging.getLogger(__name__)    

TARDIS_DATA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "tardis"
TARDIS_API_KEY = ""

def get_days(start_date: datetime, end_date: datetime) -> list[str]:
    """
    Given two datetime objects, generate a list of months between them as strings in 'MM' format.
    """
    days = []   
    current_date = start_date
    
    while current_date <= end_date:
        days.append(current_date.strftime('%Y-%m-%d'))
        current_date += timedelta(days=1)
    return sorted(days)

def default_file_name(exchange, data_type, date, symbol, format):
    return f"{exchange}_{data_type}_{date.strftime('%Y-%m-%d')}_{symbol}.{format}.gz"

def generate_schema(type: str="book_snapshot_25") -> dict[str, pl.DataType]:
    """
    Generate a schema for a given type of data from tardis
    """
    schema = {}
    if type == "book_snapshot_25":
        first_columns = ["exchange", "symbol", "timestamp", "local_timestamp"]
        second_columns = [[f"asks[{i}].price", f"asks[{i}].amount" , f"bids[{i}].price", f"bids[{i}].amount"] for i in range(25)]
        columns = first_columns + [col for sublist in second_columns for col in sublist]
        schema = {
            "exchange": pl.String,
            "symbol": pl.String,
            "timestamp": pl.Int64,
            "local_timestamp": pl.Int64,
            **{col: pl.Float64 for col in columns[4:]},
        }
    else:
        raise ValueError(f"Unknown type: {type}")
    return schema

@register_dataset("tardis")
class TardisData(DataLoader):
    """
    Dataloader for Tardis data
    """
    def __init__(self, root: str | Path = TARDIS_DATA_PATH, market: str = "binance-futures"):
        logger.info("Initializing TardisDataLoader with path %s", root)
        super().__init__(root)
        self.market = market

    def _load_data(self, product: str, times: list[str], type: str="book_snapshot_25", lazy=False) -> pl.DataFrame:    
        """
        Load data for a given product and times.
        """
        if len(times) != 2:
            raise ValueError("Times must be a list of two strings in the format '%y%m%d.%H%M'")
        try:
            dtimes = [datetime.strptime(t, "%y%m%d.%H%M%S") for t in times]
        except ValueError:
            raise ValueError("Times must be in the format '%y%m%d.%H%M%S'")
        days = get_days(dtimes[0], dtimes[1])

        dfs = []
        for day in days:
            filename = f"{str(self.processed_path)}/{self.market}_{type}_{day}_{product}.parquet"
            if not Path(filename).exists():
                logger.info(f"File {filename} not found, trying to download...")
                self.download(product, day, type)
                logger.info("File downloaded, processing...")
                df = self.process(product, day, type)
                if df is None:
                    logger.info(f"Product {product} with type {type} and day {day} is not available")
                    return None
            else:
                # check if the dataframe is already in the cache
                if self.cache is not None and filename in self.cache and not lazy:
                    df = self.cache[filename]
                else:
                    if lazy:
                        df = pl.scan_parquet(filename)
                    else:
                        df = pl.read_parquet(filename)
                        if self.cache is not None:
                            self.cache[filename] = df
            dfs.append(df)

        df = pl.concat(dfs)
        df = df.filter(pl.col('ts').is_between(nanoseconds(times[0]), nanoseconds(times[1])))
        return df

    def load_book(self, product: str, times: list[str], depth: int = 10, lazy=False, type: str = "book_snapshot_25") -> pl.DataFrame:
        """
        Load book data for a given product and times.
        """
        df = self._load_data(product, times, type, lazy)
        price_columns = [[f"asks[{i}].price", f"asks[{i}].amount" , f"bids[{i}].price", f"bids[{i}].amount"] for i in range(depth)]
        price_columns = [col for sublist in price_columns for col in sublist]
        columns = ['ts', 'ts_local'] + price_columns
        df = df.select(columns)
        df = df.unique(subset=price_columns, maintain_order=True)
        return df

    def load_bar(self, products: list[str] | str, times: list[str], col: str = "mid", freq: str = "1s", lazy=False) -> pl.DataFrame:
        """
        Load data for a given set of products and times.
        """
        if isinstance(products, str):
            # load book data for a single product
            return self.load_book(products, times, depth=1, lazy=lazy, type="book_snapshot_25")

        # Load book data for each product and combine into one dataframe
        dfs = []

        for product in products:
            df = self.load_book(product, times, depth=1, lazy=lazy, type="book_snapshot_25").sort('ts')
            if lazy:
                columns = df.collect_schema().names()
            else:
                columns = df.columns
            rename_map = {
                col: f"{col}_{product}" for col in columns if col not in ["ts", "ts_local"]
            }
            df = df.rename(rename_map)
            dfs.append(df)
        merged_df = pl.concat([df.select('ts') for df in dfs], how='vertical').unique('ts').sort('ts')
        for i, df in enumerate(dfs):
            merged_df = merged_df.join_asof(df, on='ts')
        df = merged_df.drop_nulls().sort('ts')

        df = df.ds.add_datetime()
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
            rdf = rdf.feature.add_mid(cols=["asks[0].price", "bids[0].price"])
            if len(products) > 1:
                rdf = rdf.select([pl.col("dts").alias("ts")] + [pl.col(f"mid_{product}") for product in products])
            else:
                rdf = rdf.select([pl.col("dts").alias("ts"), pl.col("mid")])
        elif col == "vwap":
            rdf = rdf.feature.add_vwap(cols=["asks[0].price", "bids[0].price", "asks[0].amount", "bids[0].amount"])
            if len(products) > 1:
                rdf= rdf.select([pl.col("dts").alias("ts")] + [pl.col(f"vwap_{product}") for product in products]) 
            else:
                rdf = rdf.select([pl.col("dts").alias("ts"), pl.col("vwap")])
        return rdf

    def download(self, product: str, day: str, type: str):
        """
        Download data for a given product and day.
        """
        nest_asyncio.apply()
        datasets.download(
            exchange=self.market,
            data_types=[type],
            from_date=day,
            to_date=day,
            symbols=[product],
            api_key=TARDIS_API_KEY,
            download_dir=self.raw_path,
            get_filename=default_file_name
        )

    def process(self, product: str, day: str, type: str):
        """
        Process data for a given product and day.
        """
        schema = generate_schema(type)
        filename = f"{str(self.raw_path)}/{self.market}_{type}_{day}_{product}.csv.gz"
        with gzip.open(filename, 'rb') as f_in, open(filename.replace('.gz', ''), 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
        df = pl.read_csv(filename.replace('.gz', ''), schema=schema)
        df = df.rename({'timestamp': 'ts', 'local_timestamp': 'ts_local', 'symbol': 'product'})
        outfilename = f"{str(self.processed_path)}/{self.market}_{type}_{day}_{product}.parquet"
        df = df.with_columns(
            pl.col('ts').mul(1000).cast(pl.Int64).alias('ts'),
            pl.col('ts_local').mul(1000).cast(pl.Int64).alias('ts_local')
        )
        df = df.select([
            'ts', 'ts_local', 'product'  
        ]+list(schema.keys())[4:])
        df.write_parquet(outfilename)
        os.remove(filename.replace('.gz', ''))
        return df

        
        