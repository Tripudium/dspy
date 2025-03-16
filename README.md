# dspy

A Python data handling system for high-frequency data

## Installation

```zsh
git clone git@github.com:Tripudium/dspy.git
```

Install using the [uv](https://docs.astral.sh/uv/) package manager:

```zsh
uv python list
uv .venv --python 3.13.2
source .venv/bin/activate
uv sync```

To make with work with the proprietary Terank ```trpy-data``` framework, this needs to be installed:

```zsh
uv pip install -e /path/to/trpy-data```

Some further hacking may be necessary.



