"""
Module for generating features from book data.

This module provides functions to add spread, volume, and other features to order book data.
"""

import polars as pl
from typing import List

# Features for prices   

def add_spread(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """
    Add a spread column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(cols).abs().alias('spread'))
    return df

def add_volume(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """
    Add a volume column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(cols).abs().alias('volume'))
    return df

def add_vwap(df: pl.DataFrame, cols: List[str]) -> pl.DataFrame:
    """
    Add a VWAP column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(cols).alias('vwap'))
    return df
