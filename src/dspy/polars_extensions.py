"""
This module provides additional functionality for Polars DataFrames.
"""
import polars as pl


def _get_products(df: pl.DataFrame, cols: list[str]) -> list[str]:
    all_columns = df.columns
    product_parts = []
    for col in cols:
        for column_name in all_columns:
            if column_name.startswith(f"{col}_"):
                product_part = column_name[len(col)+1:]
                product_parts.append(product_part)
    if product_parts != []: 
        products = list(set(product_parts))
    else:
        products = []
    return products

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

    def add_mid(self, products: list[str] | None = None, cols: list[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a mid column to the DataFrame.
        """
        if products is None:
            products = _get_products(self._df, cols)
            
        if products == []:
            return self._df.with_columns(
                ((pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}"))/2).alias('mid'))
        for product in products:
            self._df = self._df.with_columns(
                ((pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}"))/2).alias(f'mid_{product}'))
        return self._df

    def add_spread(self, products: list[str] | None = None, cols: list[str]=['prc__s0', 'prc__s1']) -> pl.DataFrame:
        """
        Add a spread column to the DataFrame.
        """
        if products is None:
            products = _get_products(self._df, cols)

        if products == []:
            return self._df.with_columns(
                (pl.col(f"{cols[0]}") - pl.col(f"{cols[1]}")).alias('spread'))
        for product in products:
            self._df = self._df.with_columns(
                (pl.col(f"{cols[0]}_{product}") - pl.col(f"{cols[1]}_{product}")).alias(f'spread_{product}'))
        return self._df
    
    def add_volume(self, products: list[str] | None = None, cols: list[str]=['vol__s0', 'vol__s1']) -> pl.DataFrame:
        """
        Add a volume column to the DataFrame.
        """
        if products is None:
            products = _get_products(self._df, cols)

        if products == []:
            return self._df.with_columns(
                (pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}")).alias('volume'))
        for product in products:
            self._df = self._df.with_columns(
                (pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}")).alias(f'volume_{product}'))
        return self._df

    def add_vwap(self, products: list[str] | None = None, cols: list[str]=['prc__s0', 'prc__s1', 'vol__s0', 'vol__s1']) -> pl.DataFrame:
        """
        Add a VWAP column to the DataFrame.
        """
        if products is None:
            products = _get_products(self._df, cols)

        if products == []:
            self._df = self._df.with_columns(
                pl.when(pl.col(f"{cols[2]}") + pl.col(f"{cols[3]}") > 0)
                .then(
                    (
                        (pl.col(f"{cols[0]}") * pl.col(f"{cols[2]}") + 
                         pl.col(f"{cols[1]}") * pl.col(f"{cols[3]}")) /
                        (pl.col(f"{cols[2]}") + pl.col(f"{cols[3]}"))
                    )
                )
                .otherwise(pl.lit(0))
                .alias('vwap')
            )
        else:
            for product in products:
                self._df = self._df.with_columns(
                    pl.when(pl.col(f"{cols[2]}_{product}") + pl.col(f"{cols[3]}_{product}") > 0)
                    .then(
                        (
                            (pl.col(f"{cols[0]}_{product}") * pl.col(f"{cols[2]}_{product}") + 
                             pl.col(f"{cols[1]}_{product}") * pl.col(f"{cols[3]}_{product}")) /
                            (pl.col(f"{cols[2]}_{product}") + pl.col(f"{cols[3]}_{product}"))
                        )
                    )
                    .otherwise(pl.lit(0))
                    .alias(f'vwap_{product}')
                )
        return self._df
    
@pl.api.register_dataframe_namespace("_trade")
@pl.api.register_lazyframe_namespace("_trade")
class TradeMethods:
    def __init__(self, df: pl.DataFrame):
        self._df = df

    def agg_trades(self, cols: list[str]=['ts', 'prc', 'product']) -> pl.DataFrame:
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
