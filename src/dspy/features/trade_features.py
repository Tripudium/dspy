"""
Module for generating features from trade data.

This module provides functions to add size, side, and other features to trade data.
"""

import polars as pl
from typing import List

# Local imports
from dspy.utils import str_to_timedelta, timedelta_to_str

# Freatures for trades

def add_size(df: pl.DataFrame, col: str='qty') -> pl.DataFrame:
    """
    Add a size column to the DataFrame.
    """
    df = df.with_columns(
        pl.col(col).abs().alias('size'))
    return df

def add_side(df: pl.DataFrame, col: str='qty') -> pl.DataFrame:
    """
    Add a side column to the DataFrame.
    """
    df = df.with_columns(
        pl.when(pl.col(col) > 0).then(1).otherwise(-1).alias('side'))
    return df
