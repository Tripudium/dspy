"""
Module for generating features from book data.

This module provides functions to add spread, volume, and other features to order book data.
"""

import polars as pl

from dspy.features.utils import get_products
# Features for prices   

def add_mid(df: pl.DataFrame, products: list[str] | None = None, cols: list[str]=['prc_s0', 'prc_s1']) -> pl.DataFrame:
    """
    Add a mid column to the DataFrame.
    """
    if products is None:
        products = get_products(df, cols)
        
    if products == []:
        return df.with_columns(
            ((pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}"))/2).alias('mid'))
    for product in products:
        df = df.with_columns(
            ((pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}"))/2).alias(f'mid_{product}'))
    return df

def add_spread(df: pl.DataFrame, products: list[str] | None = None, cols: list[str]=['prc_s0', 'prc_s1']) -> pl.DataFrame:
    """
    Add a spread column to the DataFrame.
    """
    if products is None:
        products = get_products(df, cols)

    if products == []:
        return df.with_columns(
            (pl.col(f"{cols[0]}") - pl.col(f"{cols[1]}")).alias('spread'))
    for product in products:
        df = df.with_columns(
            (pl.col(f"{cols[0]}_{product}") - pl.col(f"{cols[1]}_{product}")).alias(f'spread_{product}'))
    return df

def add_volume(df: pl.DataFrame, products: list[str] | None = None, cols: list[str]=['vol_s0', 'vol_s1']) -> pl.DataFrame:
    """
    Add a volume column to the DataFrame.
    """
    if products is None:
        products = get_products(df, cols)

    if products == []:
        return df.with_columns(
            (pl.col(f"{cols[0]}") + pl.col(f"{cols[1]}")).alias('volume'))
    for product in products:
        df = df.with_columns(
            (pl.col(f"{cols[0]}_{product}") + pl.col(f"{cols[1]}_{product}")).alias(f'volume_{product}'))
    return df

def add_vwap(df: pl.DataFrame, products: list[str] | None = None, cols: list[str]=['prc_s0', 'prc_s1', 'vol_s0', 'vol_s1']) -> pl.DataFrame:
    """
    Add a VWAP column to the DataFrame.
    """
    if products is None:
        products = get_products(df, cols)

    if products == []:
        df = df.with_columns(
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
            df = df.with_columns(
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
    return df

def add_rel_returns(df: pl.DataFrame, products: list[str] | None = None, cols: list[str]=['prc_s0', 'prc_s1']) -> pl.DataFrame:
    """
    Add a relative return column to the DataFrame.
    """
    if products is None:
        products = get_products(df, cols)

    return
    