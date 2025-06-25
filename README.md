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

## API Keys Configuration

For accessing live data and historical data sources, you'll need to configure API keys:

```bash
# Copy the environment template
cp .env.example .env

# Edit .env with your actual API keys
```

Required API keys in `.env`:
- `TARDIS_API_KEY`: For historical market data from [Tardis](https://tardis.dev/)
- `BYBIT_API_KEY` & `BYBIT_API_SECRET`: For live trading via [Bybit API](https://www.bybit.com/app/user/api-management)

The `.env` file is automatically loaded when importing `dspy`, so your API keys will be available throughout the application.

## Usage

Data is available in two forms: limit order book (LOB) and fixed frequency data (trade data will be included too). The available depth depends on the ultimate data source being used. The timestamps are given in nanosecond resolution as Unix timestamps. A simple dataloader and some helper function to convert Python datetime objects or strings of the form '240802.145010' into timestamps are provided.

```python
from dspy.hdb import get_dataset

dl = get_dataset("tardis") # uses data provided by tardis
```

To get book data:

```python
df = dl.load_book(product='BTCUSDT', times=['250120.000100', '250120.215000'], depth=10)
# Add human readable timestamp and mid prices
df = df.ds.add_datetime('ts').feature.add_mid(products=['BTCUSDT'])
```

**Data Sources:**
- **Local data**: Expected as parquet files in `data/tardis/processed/` directory
- **Tardis API**: Automatically fetches and preprocesses data (requires `TARDIS_API_KEY` in `.env`)
- **HuggingFace**: Preprocessed BTCUSDT data from April-June 2025 available at [tripudium/tardisdata](https://huggingface.co/datasets/tripudium/tardisdata/tree/main)

See the [example notebook](examples/dataloading.ipynb) for more.

## Additional packages

This package is used by downstream packages such as [cooc](https://github.com/Tripudium/cooc).





