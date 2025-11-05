"""
Delta Lake datasink implementation with two-phase commit for ACID compliance.
"""

import json
import logging
import os
import time
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional

import pyarrow as pa
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq

from ray.data._internal.datasource.delta.config import DeltaWriteConfig, WriteMode
from ray.data._internal.datasource.delta.utilities import (
    DeltaUtilities,
    try_get_deltatable,
)
from ray.data._internal.execution.interfaces import TaskContext
from ray.data._internal.util import _check_import
from ray.data.block import Block, BlockAccessor
from ray.data.datasource.datasink import Datasink, WriteResult

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.transaction import AddAction

logger = logging.getLogger(__name__)


class DeltaDatasink(Datasink[List["AddAction"]]):
    """Ray Data datasink for Delta Lake tables using two-phase commit."""

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
        _check_import(self, module="deltalake", package="deltalake")

        self.path = path
        self.mode = self._validate_mode(mode)
        self.partition_cols = partition_cols or []
        self.schema = schema
        self.write_kwargs = write_kwargs
        self._skip_write = False

        # Initialize Delta utilities
        self.delta_utils = DeltaUtilities(
            path, storage_options=write_kwargs.get("storage_options")
        )
        self.storage_options = self.delta_utils.storage_options

        # Delta write configuration
        self.delta_write_config = DeltaWriteConfig(
            mode=self.mode,
            partition_cols=partition_cols,
            schema=schema,
            **write_kwargs,
        )

        # Set up filesystem
        self.filesystem = filesystem or pa_fs.FileSystem.from_uri(path)[0]

    def _validate_mode(self, mode: str) -> WriteMode:
        """Validate and return WriteMode."""
        if mode not in ["append", "overwrite", "error", "ignore"]:
            if mode == "merge":
                raise ValueError("Merge mode not supported in v1. Use 'append' or 'overwrite' modes.")
            raise ValueError(f"Invalid mode '{mode}'. Supported: 'append', 'overwrite', 'error', 'ignore'")
        return WriteMode(mode)

    def on_write_start(self) -> None:
        """Check ERROR and IGNORE modes before writing files."""
        _check_import(self, module="deltalake", package="deltalake")

        existing_table = try_get_deltatable(self.path, self.storage_options)

        if self.mode == WriteMode.ERROR and existing_table:
            raise ValueError(f"Delta table already exists at {self.path}. Use mode='append' or 'overwrite'.")

        if self.mode == WriteMode.IGNORE and existing_table:
            logger.info(f"Delta table already exists at {self.path}. Skipping write due to mode='ignore'.")
            self._skip_write = True
        else:
            self._skip_write = False

    def write(
        self,
        blocks: Iterable[Block],
        ctx: TaskContext,
    ) -> List["AddAction"]:
        """Phase 1: Write Parquet files, return AddAction metadata (no commit)."""
        if self._skip_write:
            return []

        _check_import(self, module="deltalake", package="deltalake")

        combined_table = self._convert_blocks_to_table(blocks)
        if combined_table is None:
            return []

        self._validate_partition_columns(combined_table)
        return self._write_table_data(combined_table, ctx.task_idx)

    def _convert_blocks_to_table(self, blocks: Iterable[Block]) -> Optional[pa.Table]:
        """Convert Ray Data blocks to a single PyArrow table."""
        tables = []
        for block in blocks:
            block_accessor = BlockAccessor.for_block(block)
            if block_accessor.num_rows() > 0:
                tables.append(block_accessor.to_arrow())
        return pa.concat_tables(tables) if tables else None

    def _validate_partition_columns(self, table: pa.Table) -> None:
        """Validate that all partition columns exist in the table schema."""
        if not self.partition_cols:
            return
        missing_cols = [col for col in self.partition_cols if col not in table.column_names]
        if missing_cols:
            raise ValueError(f"Partition columns {missing_cols} not found in table schema. Available columns: {table.column_names}.")

    def _write_table_data(self, table: pa.Table, task_idx: int) -> List["AddAction"]:
        """Write table data as partitioned or non-partitioned Parquet files."""
        if self.partition_cols:
            partitioned_tables = self._partition_table(table, self.partition_cols)
            return [self._write_partition(partition_table, partition_values, task_idx) for partition_values, partition_table in partitioned_tables.items()]
        return [self._write_partition(table, (), task_idx)]

    def _partition_table(
        self, table: pa.Table, partition_cols: List[str]
    ) -> Dict[tuple, pa.Table]:
        """Partition table by columns efficiently using vectorized operations."""
        from collections import defaultdict

        import pyarrow.compute as pc

        partitions = {}
        if len(partition_cols) == 1:
            col_name = partition_cols[0]
            for partition_value in pc.unique(table[col_name]):
                partition_value_py = partition_value.as_py()
                row_mask = pc.equal(table[col_name], partition_value)
                partitions[(partition_value_py,)] = table.filter(row_mask)
        else:
            partition_values_lists = [table[col].to_pylist() for col in partition_cols]
            partition_indices = defaultdict(list)
            for row_idx, partition_tuple in enumerate(zip(*partition_values_lists)):
                partition_indices[partition_tuple].append(row_idx)
            for partition_tuple, row_indices in partition_indices.items():
                partitions[partition_tuple] = table.take(row_indices)
        return partitions

    def _write_partition(
        self,
        table: pa.Table,
        partition_values: tuple,
        task_idx: int,
    ) -> "AddAction":
        """Write a single partition to Parquet file and create AddAction metadata."""
        from deltalake.transaction import AddAction

        filename = self._generate_filename(task_idx)
        partition_path, partition_dict = self._build_partition_path(partition_values)
        relative_path = partition_path + filename
        full_path = os.path.join(self.path, relative_path)
        file_size = self._write_parquet_file(table, full_path)
        file_statistics = self._compute_statistics(table)

        return AddAction(
            path=relative_path,
            size=file_size,
            partition_values=partition_dict,
            modification_time=int(time.time() * 1000),
            data_change=True,
            stats=file_statistics,
        )

    def _generate_filename(self, task_idx: int) -> str:
        """Generate unique Parquet filename."""
        return f"part-{task_idx:05d}-{uuid.uuid4().hex}.parquet"

    def _build_partition_path(
        self, partition_values: tuple
    ) -> tuple[str, Dict[str, Optional[str]]]:
        """Build Hive-style partition path and dictionary for Delta metadata."""
        if not self.partition_cols or not partition_values:
            return "", {}

        partition_path_components = []
        partition_dict = {}
        for col_name, col_value in zip(self.partition_cols, partition_values):
            value_str = "" if col_value is None else str(col_value)
            partition_path_components.append(f"{col_name}={value_str}")
            partition_dict[col_name] = None if col_value is None else str(col_value)
        return "/".join(partition_path_components) + "/", partition_dict

    def _write_parquet_file(self, table: pa.Table, file_path: str) -> int:
        """Write PyArrow table to Parquet file and return file size."""
        table_to_write = self._prepare_table_for_write(table)
        self._ensure_parent_directory(file_path)

        pq.write_table(
            table_to_write,
            file_path,
            filesystem=self.filesystem,
            compression=self.write_kwargs.get("compression", "snappy"),
            write_statistics=True,
        )

        return self.filesystem.get_file_info(file_path).size

    def _prepare_table_for_write(self, table: pa.Table) -> pa.Table:
        """Prepare table for writing by removing partition columns."""
        return table if not self.partition_cols else table.drop(self.partition_cols)

    def _ensure_parent_directory(self, file_path: str) -> None:
        """Create parent directory for file if it doesn't exist."""
        parent_dir = os.path.dirname(file_path)
        if not parent_dir:
            return
        try:
            self.filesystem.create_dir(parent_dir, recursive=True)
        except Exception:
            pass

    def _compute_statistics(self, table: pa.Table) -> str:
        """Compute file-level statistics for Delta Lake transaction log."""
        statistics = {"numRecords": len(table)}
        min_values = {}
        max_values = {}
        null_counts = {}

        for col_name in table.column_names:
            column = table[col_name]
            null_counts[col_name] = column.null_count
            if column.null_count < len(column):
                min_val, max_val = self._compute_column_min_max(column)
                if min_val is not None:
                    min_values[col_name] = min_val
                if max_val is not None:
                    max_values[col_name] = max_val

        if min_values:
            statistics["minValues"] = min_values
        if max_values:
            statistics["maxValues"] = max_values
        if null_counts:
            statistics["nullCount"] = null_counts
        return json.dumps(statistics)

    def _compute_column_min_max(
        self, column: pa.Array
    ) -> tuple[Optional[Any], Optional[Any]]:
        """Compute min and max values for a single column."""
        import pyarrow.compute as pc

        try:
            col_type = column.type
            if pa.types.is_integer(col_type) or pa.types.is_floating(col_type):
                return pc.min(column).as_py(), pc.max(column).as_py()
            elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
                min_val = pc.min(column).as_py()
                max_val = pc.max(column).as_py()
                return (
                    str(min_val) if min_val is not None else None,
                    str(max_val) if max_val is not None else None,
                )
            return None, None
        except Exception:
            return None, None

    def on_write_complete(self, write_result: WriteResult[List["AddAction"]]) -> None:
        """Phase 2: Commit all files in single ACID transaction."""
        all_file_actions = self._collect_file_actions(write_result)
        existing_table = try_get_deltatable(self.path, self.storage_options)

        if not all_file_actions:
            if self.schema and not existing_table:
                logger.info(f"Creating empty Delta table at {self.path} with specified schema")
                self._create_empty_table()
            else:
                logger.info(f"No files to commit for Delta table at {self.path}. Skipping table creation.")
            return

        if existing_table:
            self._commit_to_existing_table(existing_table, all_file_actions)
        else:
            self._create_table_with_files(all_file_actions)

    def _collect_file_actions(
        self, write_result: WriteResult[List["AddAction"]]
    ) -> List["AddAction"]:
        """Collect all AddAction objects from distributed write tasks."""
        return [action for task_file_actions in write_result.write_returns for action in task_file_actions]

    def _create_empty_table(self) -> None:
        """Create empty Delta table with specified schema."""
        from deltalake.transaction import create_table_with_add_actions

        if not self.schema:
            raise ValueError("Cannot create empty Delta table without explicit schema. Provide schema parameter to write_delta().")

        delta_schema = self._convert_schema_to_delta(self.schema)
        create_table_with_add_actions(
            table_uri=self.path,
            schema=delta_schema,
            add_actions=[],
            mode=self.mode.value,
            partition_by=self.partition_cols or None,
            name=self.delta_write_config.name,
            description=self.delta_write_config.description,
            configuration=self.delta_write_config.configuration,
            storage_options=self.storage_options,
            commit_properties=self.delta_write_config.commit_properties,
            post_commithook_properties=self.delta_write_config.post_commithook_properties,
        )
        logger.info(f"Created empty Delta table at {self.path}")

    def _create_table_with_files(self, file_actions: List["AddAction"]) -> None:
        """Create new Delta table and commit files in single transaction."""
        from deltalake.transaction import create_table_with_add_actions

        table_schema = self._infer_schema(file_actions)
        delta_schema = self._convert_schema_to_delta(table_schema)

        create_table_with_add_actions(
            table_uri=self.path,
            schema=delta_schema,
            add_actions=file_actions,
            mode=self.mode.value,
            partition_by=self.partition_cols or None,
            name=self.delta_write_config.name,
            description=self.delta_write_config.description,
            configuration=self.delta_write_config.configuration,
            storage_options=self.storage_options,
            commit_properties=self.delta_write_config.commit_properties,
            post_commithook_properties=self.delta_write_config.post_commithook_properties,
        )
        logger.info(f"Created Delta table at {self.path} with {len(file_actions)} files")

    def _commit_to_existing_table(
        self, existing_table: "DeltaTable", file_actions: List["AddAction"]
    ) -> None:
        """Commit files to existing Delta table using write transaction."""
        if self.mode == WriteMode.ERROR:
            raise ValueError(
                f"Race condition detected: Delta table was created at {self.path} "
                f"after write started. Files have been written but not committed to "
                f"the transaction log. Use mode='append' or 'overwrite' if concurrent writes are expected."
            )

        if self.mode == WriteMode.IGNORE:
            logger.info(f"Table created at {self.path} during write. Skipping commit due to mode='ignore'.")
            return

        transaction_mode = "overwrite" if self.mode == WriteMode.OVERWRITE else "append"
        transaction = existing_table.create_write_transaction(
            actions=file_actions,
            mode=transaction_mode,
            schema=existing_table.schema(),
            partition_by=self.partition_cols or None,
            commit_properties=self.delta_write_config.commit_properties,
            post_commithook_properties=self.delta_write_config.post_commithook_properties,
        )
        transaction.commit()
        logger.info(f"Committed {len(file_actions)} files to Delta table at {self.path} (mode={transaction_mode})")

    def _infer_schema(self, add_actions: List["AddAction"]) -> pa.Schema:
        """Infer schema from first file and partition columns."""
        if self.schema:
            return self.schema

        first_file = os.path.join(self.path, add_actions[0].path)
        file_obj = self.filesystem.open_input_file(first_file)
        parquet_file = pq.ParquetFile(file_obj)
        schema = parquet_file.schema_arrow

        if self.partition_cols:
            for col in self.partition_cols:
                if col in add_actions[0].partition_values:
                    val = add_actions[0].partition_values[col]
                    col_type = self._infer_partition_type(val)
                    schema = schema.append(pa.field(col, col_type))

        return schema

    def _infer_partition_type(self, value: Optional[str]) -> pa.DataType:
        """Infer PyArrow type from partition value."""
        if not value:
            return pa.string()
        try:
            int(value)
            return pa.int64()
        except ValueError:
            pass
        try:
            float(value)
            return pa.float64()
        except ValueError:
            pass
        return pa.string()

    def _convert_schema_to_delta(self, pa_schema: pa.Schema) -> "Any":
        """Convert PyArrow schema to Delta schema with fallback for compatibility."""
        from deltalake import Schema as DeltaSchema

        try:
            return DeltaSchema.from_arrow(pa_schema)
        except (ValueError, TypeError) as e:
            logger.warning(f"Failed to convert PyArrow schema using Arrow C Data Interface: {e}. Falling back to JSON-based conversion.")

        try:
            schema_json = self._pyarrow_schema_to_delta_json(pa_schema)
            return DeltaSchema.from_json(schema_json)
        except Exception as e:
            raise ValueError(f"Failed to convert PyArrow schema to Delta schema. Both Arrow C Data Interface and JSON conversion failed. Error: {e}") from e

    def _pyarrow_schema_to_delta_json(self, pa_schema: pa.Schema) -> str:
        """Convert PyArrow schema to Delta schema JSON format."""
        fields = [
            {
                "name": field.name,
                "type": self._pyarrow_type_to_delta_type(field.type),
                "nullable": field.nullable,
                "metadata": {},
            }
            for field in pa_schema
        ]
        return json.dumps({"type": "struct", "fields": fields})

    def _pyarrow_type_to_delta_type(self, pa_type: pa.DataType) -> str:
        """Convert PyArrow data type to Delta Lake type string."""
        if pa.types.is_int8(pa_type):
            return "byte"
        elif pa.types.is_int16(pa_type):
            return "short"
        elif pa.types.is_int32(pa_type):
            return "integer"
        elif pa.types.is_int64(pa_type):
            return "long"
        elif pa.types.is_uint8(pa_type):
            return "short"
        elif pa.types.is_uint16(pa_type):
            return "integer"
        elif pa.types.is_uint32(pa_type):
            return "long"
        elif pa.types.is_uint64(pa_type):
            return "long"
        elif pa.types.is_float32(pa_type):
            return "float"
        elif pa.types.is_float64(pa_type):
            return "double"
        elif pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type):
            return "string"
        elif pa.types.is_binary(pa_type) or pa.types.is_large_binary(pa_type):
            return "binary"
        elif pa.types.is_boolean(pa_type):
            return "boolean"
        elif pa.types.is_date32(pa_type) or pa.types.is_date64(pa_type):
            return "date"
        elif pa.types.is_timestamp(pa_type):
            return "timestamp"
        elif pa.types.is_decimal(pa_type):
            return f"decimal({pa_type.precision},{pa_type.scale})"
        else:
            raise ValueError(f"Unsupported PyArrow type for Delta Lake: {pa_type}")

    def on_write_failed(self, error: Exception) -> None:
        """Handle write failure."""
        logger.error(f"Delta write failed for {self.path}: {error}. Uncommitted files will be cleaned by Delta vacuum.")

    @property
    def supports_distributed_writes(self) -> bool:
        return True

    @property
    def min_rows_per_write(self) -> Optional[int]:
        return None

    def get_name(self) -> str:
        return "Delta"
