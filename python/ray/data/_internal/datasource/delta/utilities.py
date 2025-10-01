"""
Utility classes and functions for Delta Lake datasource.

This module contains cloud-specific utilities, helper functions, and
standalone operations for Delta Lake functionality.
"""

import logging
from typing import Any, Dict, List, Optional

import pyarrow.fs as pa_fs
from deltalake import DeltaTable


logger = logging.getLogger(__name__)


class AWSUtilities:
    """Utility class for Amazon Web Services credential management and configuration."""

    @staticmethod
    def _get_aws_credentials():
        """
        Get AWS credentials using boto3.

        Returns:
            Dict with AWS credentials
        """
        try:
            import boto3

            session = boto3.Session()
            credentials = session.get_credentials()

            if credentials:
                return {
                    "AWS_ACCESS_KEY_ID": credentials.access_key,
                    "AWS_SECRET_ACCESS_KEY": credentials.secret_key,
                    "AWS_SESSION_TOKEN": credentials.token,
                    "AWS_REGION": session.region_name or "us-east-1",
                }
        except Exception:
            pass

        return {}

    @staticmethod
    def get_s3_storage_options(path: str) -> Dict[str, str]:
        """
        Get S3 storage options with automatic credential detection.

        Args:
            path: S3 path

        Returns:
            Dict with S3 storage options
        """
        storage_options = {}

        # Try to get AWS credentials
        credentials = AWSUtilities._get_aws_credentials()
        storage_options.update(credentials)

        return storage_options


class GCPUtilities:
    """Utility class for Google Cloud Platform credential management and configuration."""

    @staticmethod
    def _get_gcp_project():
        """
        Get the GCP project ID from various sources.

        Returns:
            Project ID if found
        """
        try:
            # Try to get from google-cloud-core
            from google.cloud import storage

            client = storage.Client()
            return client.project
        except Exception:
            pass

        # Try to get from Ray cluster resources (Anyscale)
        try:
            import ray

            cluster_resources = ray.cluster_resources()
            project_resources = [
                k.split(":")[1]
                for k in cluster_resources.keys()
                if k.startswith("anyscale/gcp-project:")
            ]
            if project_resources:
                return project_resources[0]
        except Exception:
            pass

        return None


class AzureUtilities:
    """Utility class for Microsoft Azure credential management and configuration."""

    @staticmethod
    def _get_azure_credentials():
        """
        Get Azure credentials using azure-identity library.

        Returns:
            Credentials object
        """
        try:
            from azure.core.exceptions import ClientAuthenticationError
            from azure.identity import DefaultAzureCredential

            try:
                credential = DefaultAzureCredential()
                # Test the credential by attempting to get a token
                credential.get_token("https://storage.azure.com/.default")
                return credential
            except ClientAuthenticationError:
                return None
        except ImportError:
            logger.warning(
                "azure-identity not available. Install azure-identity for automatic credential detection."
            )
            return None
        except Exception:
            return None

    @staticmethod
    def get_azure_storage_options(path: str) -> Dict[str, str]:
        """
        Get Azure storage options with automatic credential detection.

        Args:
            path: Azure storage path

        Returns:
            Dict with Azure storage options
        """
        storage_options = {}

        # Try to get Azure credentials
        credentials = AzureUtilities._get_azure_credentials()
        if credentials:
            try:
                token = credentials.get_token("https://storage.azure.com/.default")
                storage_options["AZURE_STORAGE_TOKEN"] = token.token
            except Exception:
                pass

        return storage_options


def try_get_deltatable(
    table_uri: str, storage_options: Optional[Dict[str, str]] = None
) -> Optional[DeltaTable]:
    """
    Try to get a DeltaTable object, returning None if it doesn't exist or can't be read.

    Args:
        table_uri: Path to the Delta table
        storage_options: Storage options for the filesystem

    Returns:
        DeltaTable object if successful, None otherwise
    """
    try:
        return DeltaTable(table_uri, storage_options=storage_options)
    except Exception:
        return None


class DeltaUtilities:
    """Utility class for Delta Lake operations and helper functions."""

    def __init__(self, path: str, storage_options: Optional[Dict[str, str]] = None):
        """
        Initialize Delta utilities.

        Args:
            path: Path to the Delta table
            storage_options: Storage options for the filesystem
        """
        self.path = path
        self.provided_storage_options = storage_options or {}

        # Set up filesystem
        if storage_options:
            self.filesystem = pa_fs.S3FileSystem(**storage_options)
        else:
            self.filesystem = None

        # Detect cloud provider from path scheme
        path_lower = self.path.lower()
        self.is_aws = path_lower.startswith(("s3://", "s3a://"))
        self.is_gcp = path_lower.startswith(("gs://", "gcs://"))
        self.is_azure = path_lower.startswith(("abfss://", "abfs://", "adl://"))

        # Initialize utility classes
        self.aws_utils = AWSUtilities()
        self.gcp_utils = GCPUtilities()
        self.azure_utils = AzureUtilities()

        # Get storage options (merges provided with auto-detected)
        self.storage_options = self._get_storage_options()

    def _get_storage_options(self) -> Dict[str, str]:
        """
        Get storage options based on the path and detected cloud provider.

        Merges user-provided options with auto-detected options,
        with user-provided options taking precedence.

        Returns:
            Dict with storage options
        """
        # Start with auto-detected options
        auto_options = {}
        if self.is_aws:
            auto_options = self.aws_utils.get_s3_storage_options(self.path)
        elif self.is_azure:
            auto_options = self.azure_utils.get_azure_storage_options(self.path)

        # Merge with provided options (provided takes precedence)
        merged_options = auto_options.copy()
        merged_options.update(self.provided_storage_options)

        return merged_options

    def get_table(self) -> Optional[DeltaTable]:
        """
        Get the DeltaTable object.

        Returns:
            DeltaTable object if successful, None otherwise
        """
        return try_get_deltatable(self.path, self.storage_options)

    def table_exists(self) -> bool:
        """
        Check if the Delta table exists.

        Returns:
            True if table exists, False otherwise
        """
        return self.get_table() is not None

    def validate_path(self, path: str) -> None:
        """
        Validate a Delta table path.

        Args:
            path: Path to validate

        Raises:
            ValueError: If path is invalid
        """
        if not path or not isinstance(path, str):
            raise ValueError("Path must be a non-empty string")

        path_lower = path.lower()
        supported_schemes = [
            "file://",
            "local://",
            "s3://",
            "s3a://",
            "gs://",
            "gcs://",
            "abfss://",
            "abfs://",
            "adl://",
            "hdfs://",
        ]
        cloud_schemes = (
            "s3://",
            "s3a://",
            "gs://",
            "gcs://",
            "abfss://",
            "abfs://",
            "adl://",
        )

        if any(path_lower.startswith(scheme) for scheme in supported_schemes):
            if path_lower.startswith(cloud_schemes) and len(path.split("/")) < 4:
                raise ValueError(f"Cloud storage path appears incomplete: {path}")
        elif path_lower.startswith("azure://"):
            raise ValueError(
                "Use 'abfss://' or 'abfs://' instead of 'azure://' for Azure paths"
            )
        elif not path.startswith("/") and "://" not in path:
            # Assume local path
            pass
        else:
            logger.warning(f"Unrecognized path scheme for: {path}")


def optimize_delta_table(
    path: str,
    *,
    mode: str = "compact",
    z_order_columns: Optional[List[str]] = None,
    partition_filters: Optional[List[tuple]] = None,
    target_size: Optional[int] = None,
    max_concurrent_tasks: Optional[int] = None,
    storage_options: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """
    Optimize a Delta Lake table using compact or z-order.

    Args:
        path: Path to the Delta table
        mode: Optimization mode - "compact" or "z_order"
        z_order_columns: Columns for z-order optimization (required if mode="z_order")
        partition_filters: Optional partition filters to limit optimization scope
        target_size: Target file size in bytes
        max_concurrent_tasks: Maximum concurrent optimization tasks
        storage_options: Cloud storage authentication options

    Returns:
        Dict with optimization metrics including files_added, files_removed,
        partitions_optimized, etc.

    Raises:
        ValueError: If mode is invalid or z_order_columns missing for z_order mode
        ImportError: If deltalake package is not installed

    Examples:
        Compact small files:

        >>> import ray
        >>> metrics = ray.data.optimize_delta_table("s3://bucket/table") # doctest: +SKIP

        Z-order by columns:

        >>> metrics = ray.data.optimize_delta_table( # doctest: +SKIP
        ...     "s3://bucket/table",
        ...     mode="z_order",
        ...     z_order_columns=["customer_id", "date"]
        ... )
    """
    try:
        dt_kwargs = {}
        if storage_options:
            dt_kwargs["storage_options"] = storage_options

        dt = DeltaTable(path, **dt_kwargs)

        opt_kwargs = {}
        if partition_filters:
            opt_kwargs["partition_filters"] = partition_filters
        if target_size:
            opt_kwargs["target_size"] = target_size
        if max_concurrent_tasks:
            opt_kwargs["max_concurrent_tasks"] = max_concurrent_tasks

        if mode == "compact":
            metrics = dt.optimize.compact(**opt_kwargs)
        elif mode == "z_order":
            if not z_order_columns:
                raise ValueError("z_order_columns required for z_order mode")
            metrics = dt.optimize.z_order(columns=z_order_columns, **opt_kwargs)
        else:
            raise ValueError(f"Invalid mode: {mode}. Use 'compact' or 'z_order'")

        return dict(metrics)
    except Exception as e:
        logger.error(f"Optimize failed for {path}: {e}")
        raise


def vacuum_delta_table(
    path: str,
    *,
    retention_hours: Optional[int] = None,
    dry_run: bool = True,
    enforce_retention_duration: bool = True,
    storage_options: Optional[Dict[str, str]] = None,
) -> List[str]:
    """
    Vacuum a Delta Lake table to remove old files.

    Removes files that are no longer referenced by the Delta table and are older
    than the retention threshold. Use with caution as this permanently deletes files.

    Args:
        path: Path to the Delta table
        retention_hours: Retention period in hours (default: 168 hours = 7 days)
        dry_run: If True, only list files without deleting them. Defaults to True
            for safety.
        enforce_retention_duration: Whether to enforce minimum retention period
        storage_options: Cloud storage authentication options

    Returns:
        List of file paths that were deleted (or would be deleted if dry_run=True)

    Raises:
        ImportError: If deltalake package is not installed

    Examples:
        Preview files to delete (dry run):

        >>> import ray
        >>> files = ray.data.vacuum_delta_table("s3://bucket/table", dry_run=True) # doctest: +SKIP

        Actually delete old files:

        >>> files = ray.data.vacuum_delta_table( # doctest: +SKIP
        ...     "s3://bucket/table",
        ...     retention_hours=168,
        ...     dry_run=False
        ... )

    .. warning::
        This operation permanently deletes files. Ensure you have proper backups
        and understand the retention implications before running with dry_run=False.
    """
    try:
        dt_kwargs = {}
        if storage_options:
            dt_kwargs["storage_options"] = storage_options

        dt = DeltaTable(path, **dt_kwargs)

        files = dt.vacuum(
            retention_hours=retention_hours,
            dry_run=dry_run,
            enforce_retention_duration=enforce_retention_duration,
        )

        return list(files)
    except Exception as e:
        logger.error(f"Vacuum failed for {path}: {e}")
        raise
