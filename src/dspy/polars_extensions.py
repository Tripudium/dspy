import polars as pl
from typing import List
from datetime import timedelta
from cooc.classify import classify_trades
from cooc.features import add_coi

# Register a custom namespace for our additional DataFrame functionality.
@pl.api.register_dataframe_namespace("_dt")
@pl.api.register_lazyframe_namespace("_dt")
class DatetimeMethods:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def add_datetime(self, ts_col: str='ts') -> pl.DataFrame:
        """
        Add a datetime column to the DataFrame.
        """
        return self._df.with_columns([pl.from_epoch(ts_col, time_unit='ns').alias('dts')])

@pl.api.register_dataframe_namespace("_feat")
@pl.api.register_lazyframe_namespace("_feat")
class FeatureMethods:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def add_mid(self, products: List[str] | None = None, cols: List[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a mid column to the DataFrame.
        """
        if products is None:
            return self._df.with_columns(
                ((pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}"))/2).alias('mid'))
        for product in products:
            self._df = self._df.with_columns(
                ((pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}"))/2).alias(f'mid_{product}'))
        return self._df

    def add_spread(self, products: List[str] | None = None, cols: List[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a spread column to the DataFrame.
        """
        if products is None:
            return self._df.with_columns(
                (pl.col(f"{cols[0]}") - pl.col(f"{cols[1]}")).alias('spread'))
        for product in products:
            self._df = self._df.with_columns(
                (pl.col(f"{cols[0]}_{product}") - pl.col(f"{cols[1]}_{product}")).alias(f'spread_{product}'))
        return self._df
    
    def add_volume(self, products: List[str] | None = None, cols: List[str]=['vol__s0', 'vol__s1']) -> pl.DataFrame:
        """
        Add a volume column to the DataFrame.
        """
        if products is None:
            return self._df.with_columns(
                (pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}")).alias('volume'))
        for product in products:
            self._df = self._df.with_columns(
                (pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}")).alias(f'volume_{product}'))
        return self._df

    def add_vwap(self, products: List[str] | None = None, cols: List[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a VWAP column to the DataFrame.
        """
        if products is None:
            return self._df.with_columns(
                (pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}")).alias('vwap'))
        for product in products:
            self._df = self._df.with_columns(
                (pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}")).alias(f'vwap_{product}'))
        return self._df
    
@pl.api.register_dataframe_namespace("_trade")
@pl.api.register_lazyframe_namespace("_trade")
class TradeMethods:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def agg_trades(self, cols: List[str]=['ts', 'prc', 'product']) -> pl.DataFrame:
        """
        Aggregate trades by timestamp and price.
        """
        return self._df.group_by(cols, maintain_order=True).agg(pl.col('trade_id').first(), pl.col('qty').sum())
    
    def add_side(self, col: str='qty') -> pl.DataFrame:
        """
        Add a side column to the DataFrame.
        """
        df = self._df.with_columns(
            pl.when(pl.col(col) > 0).then(1).otherwise(-1).alias('side'))
        return df
    
    def add_size(self, col: str='qty') -> pl.DataFrame:
        """
        Add a size column to the DataFrame.
        """
        df = self._df.with_columns(
            pl.col(col).abs().alias('size'))
        return df
    
    def classify_trades(self, products: List[str], ts_col: str, delta: str | timedelta) -> pl.DataFrame:
        """
        Classify trades based on co-trading relationships.
        """
        mapping: dict = {0: "iso", 1: "nis-c", 2: "nis-s", 3: "nis-b"}
        return classify_trades(self._df, products, ts_col, delta, mapping)
    
    def coi(self, products: List[str], ts_col: str, delta: str | timedelta, type: str) -> pl.DataFrame:
        """
        Add a COI column to the DataFrame.
        """
        return add_coi(self._df, products, ts_col, delta, type)
