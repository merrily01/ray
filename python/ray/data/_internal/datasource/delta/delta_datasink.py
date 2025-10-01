"""
Main Delta Lake datasink implementation.

This module contains the DeltaDatasink class for Delta Lake write operations
using a two-phase commit pattern for ACID compliance at the dataset level.
"""

import json
import logging
import os
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.config import (
    DeltaWriteConfig,
    WriteMode,
)
from ray.data._internal.datasource.delta.utilities import (
    AWSUtilities,
    AzureUtilities,
    DeltaUtilities,
    GCPUtilities,
    try_get_deltatable,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

logger = logging.getLogger(__name__)


class DeltaDatasink(Datasink[List["AddAction"]]):
    """
    A Ray Data datasink for Delta Lake tables using two-phase commit.

    This implementation ensures ACID compliance at the dataset level by:
    1. write() per task: Writes Parquet files, returns AddAction metadata (no commit)
    2. on_write_complete(): Collects all AddActions, creates single transaction log entry

    Supports append and overwrite write modes with multi-cloud storage:
    - Local filesystem: /path/to/table or file:///path/to/table
    - AWS S3: s3://bucket/path or s3a://bucket/path
    - Google Cloud Storage: gs://bucket/path or gcs://bucket/path
    - Azure Blob Storage/ADLS: abfss://container@account.dfs.core.windows.net/path
    - HDFS: hdfs://namenode:port/path
    """

    def __init__(
        self,
        path: str,
        *,
        mode: str = WriteMode.APPEND.value,
        partition_cols: Optional[List[str]] = None,
        filesystem: Optional[pa_fs.FileSystem] = None,
        schema: Optional[pa.Schema] = None,
        **write_kwargs,
    ):
        """
        Initialize the Delta Lake datasink.

        Args:
            path: Path to the Delta table
            mode: Write mode (append, overwrite, error, ignore)
            partition_cols: Columns to partition by
            filesystem: PyArrow filesystem to use
            schema: Table schema
            **write_kwargs: Additional write configuration options
        """
        self.path = path
        
        # Validate mode
        if mode not in ["append", "overwrite", "error", "ignore"]:
            if mode == "merge":
                raise ValueError(
                    "Merge mode is not supported in v1. Use 'append' or 'overwrite' modes. "
                    "Merge functionality will be available in a future release."
                )
            else:
                raise ValueError(
                    f"Invalid mode '{mode}'. Supported modes: 'append', 'overwrite', 'error', 'ignore'"
                )
        
        self.mode = WriteMode(mode)
        self.partition_cols = partition_cols or []
        self.schema = schema
        self.write_kwargs = write_kwargs

        # Initialize Delta utilities
        self.delta_utils = DeltaUtilities(
            path, storage_options=write_kwargs.get("storage_options")
        )

        # Cloud provider utilities
        self.aws_utils = AWSUtilities()
        self.gcp_utils = GCPUtilities()
        self.azure_utils = AzureUtilities()

        # Validate path
        self.delta_utils.validate_path(path)

        # Delta-specific configurations
        self.delta_write_config = DeltaWriteConfig(
            mode=self.mode,
            partition_cols=partition_cols,
            schema=schema,
            **write_kwargs,
        )

        # Detect cloud provider and configure storage
        path_lower = self.path.lower()
        self.is_aws = path_lower.startswith(("s3://", "s3a://"))
        self.is_gcp = path_lower.startswith(("gs://", "gcs://"))
        self.is_azure = path_lower.startswith(("abfss://", "abfs://", "adl://"))
        self.storage_options = self._get_storage_options()

        # Set up filesystem
        if filesystem is not None:
            self.filesystem = filesystem
        else:
            # Create filesystem from path
            self.filesystem, _ = pa_fs.FileSystem.from_uri(path)

    def on_write_start(self) -> None:
        """
        Callback before write tasks start.
        
        Checks ERROR and IGNORE modes early to avoid unnecessary file writes.
        """
        _check_import(self, module="deltalake", package="deltalake")
        
        # Check if table exists
        existing_table = try_get_deltatable(self.path, self.storage_options)
        
        if self.mode == WriteMode.ERROR and existing_table is not None:
            raise ValueError(
                f"Delta table already exists at {self.path}. "
                f"Use mode='append' to add data or mode='overwrite' to replace it."
            )
        
        if self.mode == WriteMode.IGNORE and existing_table is not None:
            logger.info(
                f"Delta table already exists at {self.path}, "
                f"skipping write (mode='{self.mode.value}')"
            )
            # Note: We can't actually skip the write here, but on_write_complete will handle it

    def _get_storage_options(self) -> Dict[str, str]:
        """Get storage options based on the path and detected cloud provider."""
        storage_options = self.write_kwargs.get("storage_options", {}).copy()

        if self.is_aws and not any(k.startswith("AWS_") for k in storage_options):
            aws_options = self.aws_utils.get_s3_storage_options(self.path)
            storage_options.update(aws_options)
        elif self.is_azure and "AZURE_STORAGE_TOKEN" not in storage_options:
            azure_options = self.azure_utils.get_azure_storage_options(self.path)
            storage_options.update(azure_options)

        return storage_options

    def _get_partition_path(self, table: pa.Table, partition_cols: List[str]) -> str:
        """
        Generate Hive-style partition path from table data.
        
        Args:
            table: PyArrow table with partition columns
            partition_cols: List of partition column names
            
        Returns:
            Partition path like "year=2024/month=01/"
        """
        if not partition_cols or len(table) == 0:
            return ""
        
        # Get first row's partition values (all rows in this table have same partition)
        partition_parts = []
        for col in partition_cols:
            if col in table.column_names:
                value = table[col][0].as_py()
                # Handle None values
                value_str = "" if value is None else str(value)
                partition_parts.append(f"{col}={value_str}")
        
        if partition_parts:
            return "/".join(partition_parts) + "/"
        return ""

    def _get_partition_values(self, table: pa.Table, partition_cols: List[str]) -> Dict[str, Optional[str]]:
        """Extract partition values from table."""
        partition_values = {}
        
        if partition_cols and len(table) > 0:
            for col in partition_cols:
                if col in table.column_names:
                    value = table[col][0].as_py()
                    partition_values[col] = None if value is None else str(value)
        
        return partition_values

    def _compute_statistics(self, table: pa.Table) -> str:
        """
        Compute statistics JSON for the table.
        
        Args:
            table: PyArrow table
            
        Returns:
            JSON string with statistics
        """
        stats = {
            "numRecords": len(table),
        }
        
        # Add min/max/null_count for each column
        min_values = {}
        max_values = {}
        null_counts = {}
        
        for col_name in table.column_names:
            column = table[col_name]
            
            # Null count
            null_count = column.null_count
            null_counts[col_name] = null_count
            
            # Min/max for supported types
            try:
                if null_count < len(column):  # Has non-null values
                    col_type = column.type
                    # Only compute min/max for numeric and string types
                    if pa.types.is_integer(col_type) or pa.types.is_floating(col_type):
                        import pyarrow.compute as pc
                        min_val = pc.min(column).as_py()
                        max_val = pc.max(column).as_py()
                        if min_val is not None:
                            min_values[col_name] = min_val
                        if max_val is not None:
                            max_values[col_name] = max_val
                    elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
                        import pyarrow.compute as pc
                        min_val = pc.min(column).as_py()
                        max_val = pc.max(column).as_py()
                        if min_val is not None:
                            min_values[col_name] = str(min_val)
                        if max_val is not None:
                            max_values[col_name] = str(max_val)
            except Exception:
                # Skip stats for columns that don't support min/max
                pass
        
        if min_values:
            stats["minValues"] = min_values
        if max_values:
            stats["maxValues"] = max_values
        if null_counts:
            stats["nullCount"] = null_counts
        
        return json.dumps(stats)

    def _write_parquet_file(
        self,
        table: pa.Table,
        file_path: str,
        partition_cols: Optional[List[str]] = None,
    ) -> int:
        """
        Write PyArrow table to Parquet file.
        
        Args:
            table: PyArrow table to write
            file_path: Full path where to write the file
            partition_cols: Partition columns (will be excluded from file)
            
        Returns:
            File size in bytes
            
        Raises:
            Exception: If Parquet write fails
        """
        try:
            # Remove partition columns from the table (they're in the path)
            if partition_cols:
                write_table = table.drop(partition_cols)
            else:
                write_table = table
            
            # Create parent directory if needed
            parent_dir = os.path.dirname(file_path)
            if parent_dir:
                try:
                    self.filesystem.create_dir(parent_dir, recursive=True)
                except Exception:
                    # Directory may already exist
                    pass
            
            # Write Parquet file
            pq.write_table(
                write_table,
                file_path,
                filesystem=self.filesystem,
                compression=self.write_kwargs.get("compression", "snappy"),
                write_statistics=True,
            )
            
            # Get file size
            file_info = self.filesystem.get_file_info(file_path)
            return file_info.size
            
        except Exception as e:
            logger.error(f"Failed to write Parquet file {file_path}: {e}")
            raise

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["AddAction"]:
        """
        Write blocks to Parquet files and return AddAction metadata.
        
        This is Phase 1 of the two-phase commit:
        - Writes Parquet file(s) to the table directory
        - Collects metadata for each file
        - Returns AddAction objects WITHOUT committing to transaction log
        
        The actual transaction log commit happens in on_write_complete().

        Args:
            blocks: Iterable of blocks to write
            ctx: Task context for execution

        Returns:
            List of AddAction objects for files written by this task
            
        Raises:
            Exception: If write operation fails
        """
        _check_import(self, module="deltalake", package="deltalake")
        from deltalake.transaction import AddAction

        try:
            # Convert blocks to PyArrow tables and combine
            tables = []
            for block in blocks:
                if BlockAccessor.for_block(block).num_rows() > 0:
                    table = BlockAccessor.for_block(block).to_arrow()
                    tables.append(table)

            if not tables:
                # No data to write
                    return []

            # Combine all tables
            combined_table = pa.concat_tables(tables)

                logger.info(
                    f"Task {ctx.task_idx}: Writing {len(combined_table)} rows "
                    f"({combined_table.nbytes} bytes) to Delta table at {self.path}"
                )

                add_actions = []
            
                # Validate partition columns exist in table if specified
                if self.partition_cols:
                    missing_cols = [col for col in self.partition_cols if col not in combined_table.column_names]
                    if missing_cols:
                        raise ValueError(
                            f"Partition columns {missing_cols} not found in table. "
                            f"Available columns: {combined_table.column_names}"
                        )
            
                # Partition the table if needed
                if self.partition_cols:
                    # Group by partition values
                    partitioned_tables = self._partition_table(combined_table, self.partition_cols)
                
                    logger.info(
                        f"Task {ctx.task_idx}: Writing {len(partitioned_tables)} partition(s)"
                    )
                
                    for partition_values, partition_table in partitioned_tables.items():
                        # Write this partition
                        action = self._write_partition(
                            partition_table,
                            partition_values,
                            ctx.task_idx
                        )
                        add_actions.append(action)
                else:
                    # Write entire table as single file
                    action = self._write_partition(
                        combined_table,
                        {},
                        ctx.task_idx
                    )
                    add_actions.append(action)

                        logger.info(
                    f"Task {ctx.task_idx}: Created {len(add_actions)} AddAction(s)"
                )
            
                return add_actions

        except Exception as e:
            logger.error(
                f"Task {ctx.task_idx}: Failed to write Delta table at {self.path}: {e}"
            )
            # Re-raise to trigger on_write_failed()
            raise

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """
        Partition a PyArrow table by partition columns.
        
        Args:
            table: PyArrow table to partition
            partition_cols: Columns to partition by
            
        Returns:
            Dict mapping partition values tuple to partitioned table
            
        Raises:
            ValueError: If partition column not found in table
        """
        import pyarrow.compute as pc
        
        # Validate partition columns exist
        for col in partition_cols:
            if col not in table.column_names:
                raise ValueError(
                    f"Partition column '{col}' not found in table. "
                    f"Available columns: {table.column_names}"
                )
        
        # Group table by partition columns
        partitions = {}
        
        # Get unique partition combinations
        if len(partition_cols) == 1:
            col = partition_cols[0]
            unique_values = pc.unique(table[col])
            for val in unique_values:
                val_py = val.as_py()
                mask = pc.equal(table[col], val)
                filtered_table = table.filter(mask)
                partitions[(val_py,)] = filtered_table
            else:
            # For multiple partition columns, we need to iterate
            # This is less efficient but handles the general case
            seen_partitions = set()
            for i in range(len(table)):
                partition_tuple = tuple(
                    table[col][i].as_py() for col in partition_cols
                )
                if partition_tuple not in seen_partitions:
                    seen_partitions.add(partition_tuple)
                    # Filter for this partition
                    mask = None
                    for j, col in enumerate(partition_cols):
                        col_mask = pc.equal(table[col], partition_tuple[j])
                        mask = col_mask if mask is None else pc.and_(mask, col_mask)
                    filtered_table = table.filter(mask)
                    partitions[partition_tuple] = filtered_table
        
        return partitions

    def _write_partition(
        self,
        table: pa.Table,
        partition_values_tuple: tuple,
        task_idx: int,
    ) -> "AddAction":
        """
        Write a partitioned table and create AddAction.
        
        Args:
            table: PyArrow table for this partition
            partition_values_tuple: Tuple of partition values
            task_idx: Task index for unique filename
            
        Returns:
            AddAction object with file metadata
        """
        from deltalake.transaction import AddAction
        
        # Generate unique filename
        file_uuid = uuid.uuid4().hex
        filename = f"part-{task_idx:05d}-{file_uuid}.parquet"
        
        # Build partition path if partitioned
        if self.partition_cols and partition_values_tuple:
            partition_parts = []
            for col, val in zip(self.partition_cols, partition_values_tuple):
                val_str = "" if val is None else str(val)
                partition_parts.append(f"{col}={val_str}")
            partition_path = "/".join(partition_parts) + "/"
        else:
            partition_path = ""
        
        # Full file path relative to table root
        relative_path = partition_path + filename
        full_path = os.path.join(self.path, relative_path)
        
        # Write Parquet file
        file_size = self._write_parquet_file(table, full_path, self.partition_cols)
        
        # Get partition values dict
        partition_values_dict = {}
        if self.partition_cols and partition_values_tuple:
            for col, val in zip(self.partition_cols, partition_values_tuple):
                partition_values_dict[col] = None if val is None else str(val)
        
        # Compute statistics
        stats_json = self._compute_statistics(table)
        
        # Get modification time (milliseconds since epoch)
        modification_time = int(time.time() * 1000)
        
        # Create AddAction
        add_action = AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_values_dict,
            modification_time=modification_time,
            data_change=True,
            stats=stats_json,
        )
        
        logger.debug(
            f"Created AddAction: path={relative_path}, size={file_size}, "
            f"partition_values={partition_values_dict}"
        )
        
        return add_action

    def _infer_partition_column_type(self, value: Optional[str]) -> pa.DataType:
        """
        Infer PyArrow type from partition value string.
        
        Args:
            value: String partition value
            
        Returns:
            PyArrow data type
        """
        if value is None or value == "":
            # Default to string for null values
            return pa.string()
        
        # Try to infer type from value
        try:
            # Try int
            int(value)
            return pa.int64()
        except ValueError:
            pass
        
        try:
            # Try float
            float(value)
            return pa.float64()
        except ValueError:
            pass
        
        # Try date (YYYY-MM-DD format)
        if len(value) == 10 and value.count('-') == 2:
            try:
                from datetime import datetime
                datetime.strptime(value, '%Y-%m-%d')
                return pa.date32()
            except ValueError:
                pass
        
        # Default to string
        return pa.string()

    def on_write_complete(self, write_result: WriteResult[List["AddAction"]]):
        """
        Commit all written files in a single transaction.
        
        This is Phase 2 of the two-phase commit:
        - Collects all AddActions from all write tasks
        - Creates a SINGLE transaction log entry with all files
        - Ensures ACID compliance at the dataset level
        
        Args:
            write_result: Aggregated results from all write tasks
            
        Raises:
            Exception: If transaction commit fails
        """
        from deltalake.transaction import create_table_with_add_actions
        
        try:
            # Collect all AddActions from all tasks
            all_add_actions = []
            for task_add_actions in write_result.write_returns:
                all_add_actions.extend(task_add_actions)
            
            if not all_add_actions:
                logger.warning("No AddActions to commit")
                return
            
            logger.info(
                f"on_write_complete: Committing {len(all_add_actions)} file(s) "
                f"in single transaction to {self.path}"
            )
            
            # Check if table exists
            existing_table = try_get_deltatable(self.path, self.storage_options)
            
            if existing_table is None:
                # Create new table with all AddActions
                logger.info(f"Creating new Delta table at {self.path}")
                
                # Need to infer schema from the first file
                # For simplicity, use the schema we were given or read from first file
                if self.schema:
                    table_schema = self.schema
                else:
                    # Read schema from first written file
                    first_file_path = os.path.join(self.path, all_add_actions[0].path)
                    temp_table = pq.read_table(first_file_path, filesystem=self.filesystem)
                    # Add partition columns back to schema
                    table_schema = temp_table.schema
                    if self.partition_cols:
                        # Infer partition column types from first AddAction's partition values
                        first_partition_values = all_add_actions[0].partition_values
                        for col in self.partition_cols:
                            if col in first_partition_values:
                                val = first_partition_values[col]
                                # Infer PyArrow type from string partition value
                                # Delta Lake partitions are always stored as strings
                                # but we infer the logical type for schema
                                inferred_type = self._infer_partition_column_type(val)
                                table_schema = table_schema.append(pa.field(col, inferred_type))
                
                # Convert PyArrow schema to Delta schema
                from deltalake import Schema as DeltaSchema
                delta_schema = DeltaSchema.from_arrow(table_schema)
                
                # Create table with add actions
                create_table_with_add_actions(
                    table_uri=self.path,
                    schema=delta_schema,
                    add_actions=all_add_actions,
                    mode=self.mode.value,
                    partition_by=self.partition_cols if self.partition_cols else None,
                    name=self.delta_write_config.name,
                    description=self.delta_write_config.description,
                    configuration=self.delta_write_config.configuration,
                    storage_options=self.storage_options,
                    commit_properties=self.delta_write_config.commit_properties,
                    post_commithook_properties=self.delta_write_config.post_commithook_properties,
                )
                
                logger.info(
                    f"Created Delta table with {len(all_add_actions)} file(s) "
                    f"in single transaction (version 0)"
                )
            else:
                # Table exists - use create_write_transaction
                logger.info(f"Appending to existing Delta table at {self.path}")
                
                # Get current schema
                delta_schema = existing_table.schema()
                
                # Handle different write modes
                if self.mode == WriteMode.OVERWRITE:
                    # delta-rs create_write_transaction with mode='overwrite'
                    # automatically handles removing old files by creating RemoveActions
                    # internally before adding the new files
                    mode_value = "overwrite"
                elif self.mode == WriteMode.APPEND:
                    mode_value = "append"
                elif self.mode == WriteMode.ERROR:
                    raise ValueError(f"Table already exists at {self.path}")
                elif self.mode == WriteMode.IGNORE:
                    logger.info(f"Table exists at {self.path}, ignoring write (IGNORE mode)")
                    return
                else:
                    raise ValueError(f"Unsupported mode: {self.mode}")
                
                # Create write transaction
                existing_table.create_write_transaction(
                    actions=all_add_actions,
                    mode=mode_value,
                    schema=delta_schema,
                    partition_by=self.partition_cols if self.partition_cols else None,
                    commit_properties=self.delta_write_config.commit_properties,
                    post_commithook_properties=self.delta_write_config.post_commithook_properties,
                )
                
                logger.info(
                    f"Committed {len(all_add_actions)} file(s) to existing Delta table "
                    f"in single transaction (version {existing_table.version()})"
                )

        except Exception as e:
            logger.error(
                f"Failed to commit Delta transaction at {self.path}: {e}"
            )
            raise

    def on_write_failed(self, error: Exception) -> None:
        """
        Callback for when a write job fails.
        
        Attempts to clean up uncommitted Parquet files that were written
        but not yet committed to the Delta transaction log.
        
        Args:
            error: The error that caused the write to fail
        """
        logger.error(
            f"Delta write failed for {self.path}: {error}. "
            f"Note: Parquet files may have been written but not committed to transaction log. "
            f"These files will not be visible to Delta readers."
        )
        # Note: We don't delete the files because:
        # 1. They're not in the transaction log, so they're invisible to readers
        # 2. Delta's vacuum operation will clean them up later
        # 3. Attempting deletion could fail and mask the original error

    @property
    def supports_distributed_writes(self) -> bool:
        """Delta Lake supports distributed writes."""
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        """Return the target number of rows per write operation."""
        return None

    def get_name(self) -> str:
        """Get the name of this datasink."""
        return "Delta"
