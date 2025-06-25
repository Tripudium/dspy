"""
Module for loading and processing Tardis data.
This module provides functionality to access and manipulate Tardis datasets.
"""
import os
from pathlib import Path

# API credentials - loaded from environment variables
TARDIS_API_KEY = os.getenv('TARDIS_API_KEY', 'your-tardis-api-key-here')
TARDIS_DATA_PATH = Path(__file__).parent.parent.parent.parent / "data" / "tardis"

