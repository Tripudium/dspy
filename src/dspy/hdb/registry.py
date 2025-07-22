"""
Dataset registry system for dynamic data loader discovery and instantiation.

This module implements a registry pattern that allows data loaders to be
registered by name and instantiated dynamically. This provides a clean,
extensible interface for accessing different data sources without tight coupling.

Key Features:
    - Decorator-based registration system
    - Factory pattern for data loader instantiation
    - Type-safe registry with runtime validation
    - Support for parameterized data loader initialization
    - Extensible architecture for adding new data sources

Example:
    Registering a data loader:

    >>> @register_dataset("custom")
    ... class CustomDataLoader(DataLoader):
    ...     def load_book(self, symbol, times, depth=5):
    ...         return polars_dataframe

    Using the registry:

    >>> loader = get_dataset("tardis")
    >>> df = loader.load_book("BTCUSDT", times=['250120.000000', '250120.235959'])

    Available datasets:

    >>> print(list(DATASET_REGISTRY.keys()))
    ['tardis', 'custom']

Notes:
    Data loaders must inherit from DataLoader base class and implement all
    abstract methods. Registration typically happens at module import time
    via decorators on class definitions.

See Also:
    dspy.hdb.base.DataLoader: Base class that all registered loaders inherit from
    dspy.hdb.tardis_dataloader: Example of registered data loader implementation
"""

from typing import Type

DATASET_REGISTRY: dict[str, Type] = {}


def register_dataset(name):
    """
    Decorator to register a dataset class under a specified name.

    Parameters:
        name (str): The key under which the dataset class will be registered.

    Returns:
        A decorator that registers the class.
    """

    def decorator(cls):
        DATASET_REGISTRY[name] = cls
        return cls

    return decorator


def get_dataset(name, **kwargs):
    """
    Factory function to instantiate a dataset based on its registered name.

    Parameters:
        name (str): The registered name of the dataset (e.g., "as-733").
        **kwargs: Additional keyword arguments that will be passed to the dataset's constructor.

    Returns:
        An instance of the dataset class corresponding to the given name.

    Raises:
        ValueError: If the dataset name is not found in the registry.
    """
    if name not in DATASET_REGISTRY:
        available = list(DATASET_REGISTRY.keys())
        raise ValueError(
            f"Dataset '{name}' is not registered. Available datasets: {available}"
        )

    return DATASET_REGISTRY[name](**kwargs)
