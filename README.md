# dspy

A Python data handling system for high-frequency data. Can handle both freely available data and act as a wrapper for proprietary packages.

## Installation

```zsh
git clone git@github.com:Tripudium/dspy.git
```

Install using the [uv](https://docs.astral.sh/uv/) package manager:

```zsh
uv python list
uv .venv --python 3.13.2
source .venv/bin/activate
uv sync
```

To make with work with the proprietary Terank ```trpy-data``` framework, this needs to be installed:

```zsh
uv pip install -e /path/to/trpy-data
```

Some further hacking may be necessary.

## Usage

Data is available in two forms: limit order book (LOB) and trade data. The available depth depends on the ultimate data source being used. The timestamps are given in nanosecond resolution as Unix timestamps. A simple dataloader and some helper function to convert Python datetime objects or strings of the form '240802.145010' into timestamps are provided.

```python
from dspy.hdb import get_dataset

dl = get_dataset("terank") # uses trpy-data, replace with "tardis" or "bybit" for other sources
```

To get book data:

```python
df = dl.load_book(products=['BTCUSDT', 'ETHUSDT'], times=['250120.000100', '250120.215000'], depth=1, lazy=True)
# Add human readable timestamp and mid prices
df = df.ds.add_datetime('ts').feature.add_mid(products=['BTCUSDT'])
```

To get trade data:

```python
tdf = dl.load_trades(products=['BTCUSDT', 'ETHUSDT'], times=['250120.000100', '250120.215000'], lazy=True)
# By default, the timestamp column is named 'ts'
tdf = tdf.trade.agg_trades().trade.add_side().ds.add_datetime()
```

Fixed-frequency data:
```python
fdf = dl.load(products=['BTCUSDT', 'ETHUSDT'], times=['250120.000100', '250120.215000'], freq='1s')
```

There are additional features to add signal pnl, positions and pnl, various features, etc. Things are deliberately kept simple. See the [example notebook](examples/dataloading.ipynb) for more.

## Additional packages

This package is used by downstream packages such as [cooc](https://github.com/Tripudium/cooc) and [statarb](https://github.com/Tripudium/statarb). Finer control over the data (including dealing with delays) and better performance is provided by ```trpy-data```.





