"""
Utility functions for features.
"""

import polars as pl

def get_products(df: pl.DataFrame, cols: list[str]) -> list[str]:
    """
    Get the products from the columns.
    """
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

def sync_with_book(
        df: pl.DataFrame, 
        bdf: pl.DataFrame, 
        on: str = "ts", 
        cols: list[str] = ['prc_s0', 'prc_s1', 'vol_s0', 'vol_s1', 'sig_pnl']
        ) -> pl.DataFrame:
    """
    Sync a dataframe with the book dataframe on a given timestamp column.
    """
    bdf = bdf.select([on, *cols])
    
    df = df.join_asof(bdf, on=on, strategy="backward")
    return df
