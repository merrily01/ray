"""
Delta Lake datasource package for Ray Data.

This package provides comprehensive Delta Lake functionality including:
- Read/write with ACID transactions
- Change Data Feed (CDF) for incremental processing
- Time travel and partition filtering
- Table optimization and maintenance
- Multi-cloud storage support (S3, GCS, Azure, HDFS)
- Unity Catalog compatibility
"""

# Configuration classes
from .config import (
    DeltaJSONEncoder,
    DeltaWriteConfig,
    WriteMode,
)

# Core datasink
from .delta_datasink import DeltaDatasink

# CDF reading
from .delta_cdf import read_delta_cdf_distributed

# Utilities and table operations
from .utilities import (
    AWSUtilities,
    AzureUtilities,
    DeltaUtilities,
    GCPUtilities,
    optimize_delta_table,
    try_get_deltatable,
    vacuum_delta_table,
)

__all__ = [
    # Core classes
    "DeltaDatasink",
    # Configuration classes
    "DeltaJSONEncoder",
    "DeltaWriteConfig",
    # Enums
    "WriteMode",
    # Cloud utilities
    "AWSUtilities",
    "AzureUtilities",
    "DeltaUtilities",
    "GCPUtilities",
    # Helper utilities
    "try_get_deltatable",
    # CDF reading
    "read_delta_cdf_distributed",
    # Table operations
    "optimize_delta_table",
    "vacuum_delta_table",
]
