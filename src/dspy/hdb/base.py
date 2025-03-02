"""
Base class for loading data
"""

from pathlib import Path
import logging
from datetime import datetime
import polars as pl

# Local imports
from dspy.utils import nanoseconds

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DATA_PATH = Path(__file__).parent.parent.parent.parent / "data"

def get_months(start_date: datetime, end_date: datetime) -> list[str]:
    """
    Given two datetime objects, generate a list of months between them as strings in 'MM' format.
    """
    months = []
    current_date = start_date.replace(day=1)
    
    while current_date <= end_date:
        months.append(current_date.strftime('%y%m'))
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)
    return sorted(months)

class DataLoader:
    """
    Base class for loading and processing financial data.
    
    This class provides common functionality for downloading, processing,
    and accessing financial data from various sources.
    """
    def __init__(self, root: str | Path = DATA_PATH, cache: bool = True):
        """
        Initialize the DataLoader with a path to the data.
        """
        logger.info("Initializing DataLoader with path %s"%root)
        self.root = root
        self.raw_path = root / "raw"
        self.processed_path = root / "processed"
        # maintain a cache of dataframes
        if cache:
            self.cache = {}
        else:
            self.cache = None

    @property
    def raw_path(self):
        """
        Get the path to raw data files.
        
        Returns:
            Path: The directory containing raw data files.
        """
        return self._raw_path
    
    @property
    def processed_path(self):
        """
        Get the path to processed data files.
        
        Returns:
            Path: The directory containing processed data files.
        """
        return self._processed_path

    @raw_path.setter
    def raw_path(self, path: str | Path):
        """
        Set the path to raw data files.
        
        Args:
            path (str | Path): The directory to store raw data files.
        """
        self._raw_path = path

    @processed_path.setter
    def processed_path(self, path: str | Path):
        """
        Set the path to processed data files.
        
        Args:
            path (str | Path): The directory to store processed data files.
        """
        self._processed_path = path

    def _load_data(self, product: str, times: list[str], type: str, lazy=False) -> pl.DataFrame:    
        """
        Load data for a given product and times.
        """
        if len(times) != 2:
            raise ValueError("Times must be a list of two strings in the format '%y%m%d.%H%M'")
        try:
            dtimes = [datetime.strptime(t, "%y%m%d.%H%M%S") for t in times]
        except ValueError:
            raise ValueError("Times must be in the format '%y%m%d.%H%M%S'")
        months = get_months(dtimes[0], dtimes[1])

        dfs = []
        for month in months:
            filename = "{}/{}_{}_{}.parquet".format(str(self.processed_path), product, month, type)
            if not Path(filename).exists():
                logger.info(f"File {filename} not found, downloading...")
                self.download(product, month, type)
                logger.info("File downloaded, processing...")
                df = self.process(product, month, type)
                if df is None:
                    logger.info(f"Product {product} with type {type} and month {month} is not available")
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
    
    def load_trades(self, products: list[str] | str, times: list[str], lazy=False) -> pl.DataFrame:
        """
        Load trades data for a given product and times.
        """
        if isinstance(products, str):
            products = [products]
        dfs = []
        for product in products:
            df = self._load_data(product, times, "trade", lazy)
            df = df.with_columns(pl.lit(product).alias('product'))
            dfs.append(df)
        df = pl.concat(dfs).sort('ts')
        return df

    def load_book(self, products: list[str] | str, times: list[str], _depth: int = 10, lazy=False) -> pl.DataFrame:
        """
        Load book data for a given product and times.
        """
        if isinstance(products, str):
            return self._load_data(products, times, "book", lazy)
        
        dfs = []
        for product in products:
            df = self._load_data(product, times, "book", lazy).sort('ts')
            if lazy:
                columns = df.collect_schema().names()
            else:
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
    
    def load(self, _products: list[str], _times: list[str], _col: str="mid",_freq: str="1s", _lazy=False) -> pl.DataFrame:
        """
        Load data for a given product and times.
        """
        raise NotImplementedError("Subclasses must implement this method")
        
    def download(self, product: str, month: str, type: str):
        pass

    def process(self, product: str, month: str, type: str):
        return None