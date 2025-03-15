
import polars as pl

from dspy.utils import str_to_timedelta, timedelta_to_nanoseconds

def add_sig_pnl(
        df: pl.DataFrame,
        ts_col: str = 'ts',
        col: str = 'prc',
        signal: str | None = None,
        horizon: str = '1s',
        in_bp: bool = True,
        fee_in_bp: float = 0.0
        ) -> pl.DataFrame:
    """
    Add a signal PnL column to the DataFrame.
    """

    tdelta = str_to_timedelta(horizon)
    if df[ts_col].dtype == pl.UInt64 or df[ts_col].dtype == pl.Int64:
        tdelta = timedelta_to_nanoseconds(tdelta)

    expr_diff = (pl.col(f"fut_{col}") - pl.col(col))
    if in_bp:
        expr_diff = (expr_diff * 10_000 / pl.col(col)) - fee_in_bp
    if signal is not None:
        expr_diff *= pl.col(signal) if not in_bp else pl.col(signal).sign()
    
    fut_df = df.select(pl.col(ts_col), pl.col(col).alias(f"fut_{col}"))
    df = df.filter(pl.col(ts_col) <= pl.col(ts_col).max() - tdelta)

    df_shift = df.select(
        [ (pl.col(ts_col)+tdelta).alias('ts').set_sorted(), pl.col(col), pl.col(signal) ]
    ).join_asof(
        fut_df, 
        on=ts_col, 
        strategy='backward'
    ).select(
        (expr_diff).alias(f'pnl_sig_{horizon}')
    )

    df_sig = pl.concat((df.select(pl.col(ts_col)), df_shift), how='horizontal')
    return df.join(df_sig, on=ts_col, how='left')