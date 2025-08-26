"""Simple eimerdb database builder for pandas DataFrame data.

This is a simplified version of DatabaseBuilderAltinnEimerdb that works with
pandas DataFrames from parquet files. It creates a single table based on the
DataFrame structure rather than the complex multi-table setup needed for Altinn3 surveys.
"""

import logging

import eimerdb as db
import pandas as pd
from pandas._typing import Dtype


# Setup logging
eimerdb_logger = logging.getLogger(__name__)
eimerdb_logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
eimerdb_logger.addHandler(handler)


class DatabaseBuilderSimpleEimerdb:
    """A simplified class for creating an eimerdb datastorage from pandas DataFrames.

    This class provides a straightforward way to create an eimerdb storage from
    a pandas DataFrame containing tabular data.

    To use this class:
    1. Create an instance:
        db_builder = DatabaseBuilderSimpleEimerdb(
            database_name="my-simple-storage",
            bucket="my-bucket",
            dataframe=my_dataframe,
            table_name="my_table_name")
    2. Check the schema:
        print(db_builder.schema)
    3. Build the storage:
        db_builder.build_storage()
    4. Load data into database:
        db_builder.load_data()
    """

    def __init__(
        self,
        database_name: str,
        bucket: str,
        dataframe: pd.DataFrame,
        table_name: str = "observations",
    ) -> None:
        """Initialize the simple database builder.

        Args:
            database_name: Name of the database
            bucket: Name of the bucket to store the database, example: ssb-tip-tutorials-data-produkt-prod
            dataframe: pandas DataFrame containing the data
            table_name: Name of the table to create
        """
        self.database_name = database_name
        self.bucket = bucket
        self.dataframe = dataframe
        self.table_name = table_name
        self._validate_inputs()

        # Create schema from the full DataFrame
        self.schema = self._create_schema_from_dataframe()

    def _validate_inputs(self):
        """Validate input parameters."""
        if not isinstance(self.dataframe, pd.DataFrame):
            raise TypeError("dataframe parameter must be a pandas DataFrame")

        if self.dataframe.empty:
            raise ValueError("DataFrame cannot be empty")

        if not self.database_name or not isinstance(self.database_name, str):
            raise ValueError("Database name must be a non-empty string")

        if not self.bucket or not isinstance(self.bucket, str):
            raise ValueError("Storage location must be a non-empty string")

    def _create_schema_from_dataframe(self) -> list:
        """Create eimerdb schema from DataFrame structure."""
        schema = []

        for col_name in self.dataframe.columns:
            dtype = self.dataframe[col_name].dtype
            col_def = {
                "name": col_name,
                "type": self._pandas_to_eimerdb_type(dtype),
                "label": col_name.replace("_", " ").title(),
                "app_editable": True,
            }
            schema.append(col_def)

        return schema

    def _pandas_to_eimerdb_type(self, pandas_dtype: Dtype) -> str:
        """Convert pandas dtype to eimerdb type."""
        dtype_str = str(pandas_dtype).lower()

        if "int" in dtype_str:
            if "64" in dtype_str:
                return "int64"
            elif "32" in dtype_str:
                return "int32"
            else:
                return "int16"
        elif "float" in dtype_str:
            return "float64"
        elif "bool" in dtype_str:
            return "bool_"
        elif "datetime64" in dtype_str:
            return "pa.timestamp(s)"
        else:
            # Default to string for object, string[python], etc.
            return "string"

    def __str__(self) -> str:
        """String representation of the database builder."""
        return (
            f"DatabaseBuilderSimpleEimerdb\n"
            f"Database name: {self.database_name}\n"
            f"Bucket: {self.bucket}\n"
            f"DataFrame shape: {self.dataframe.shape}\n"
            f"Table name: {self.table_name}\n"
            f"Schema columns: {[col['name'] for col in self.schema]}"
        )

    def build_storage(self) -> None:
        """Create the eimerdb storage and table."""
        try:
            # Create the database
            db.create_eimerdb(bucket_name=self.bucket, db_name=self.database_name)
            eimerdb_logger.info(
                f"Created eimerdb at {self.bucket}/{self.database_name}"
            )

            # Connect to the database
            conn = db.EimerDBInstance(self.bucket, self.database_name)

            # Create the table
            conn.create_table(
                table_name=self.table_name,
                schema=self.schema,
                partition_columns=None,  # No partitioning for simple case
                editable=True,
            )

            eimerdb_logger.info(
                f"Created table '{self.table_name}' with {len(self.schema)} columns"
            )

        except Exception as e:
            eimerdb_logger.error(f"Error building storage: {e}")
            raise


def main() -> None:
    """Example usage of the DatabaseBuilderSimpleEimerdb."""
    # Configuration
    database_name = "frost-observations-db"
    bucket = "ssb-tip-tutorials-data-produkt-prod"
    parquet_file = "gs://ssb-tip-tutorials-data-produkt-prod/metstat/klargjorte-data/temp/pre-edit/frost/observations_p2025-04-30_p2025-08-20.parquet"
    # bucket = "arneso-test-bucket"
    # parquet_file = "gs://arneso-test-bucket/data/metstat/klargjorte-data/temp/pre-edit/frost/observations_p2025-04-30_p2025-08-20.parquet"

    table_name = "observations"

    try:
        # Load DataFrame from parquet file
        print("Loading DataFrame from parquet file...")
        df = pd.read_parquet(parquet_file)
        print(f"Loaded DataFrame with shape: {df.shape}")

        # Create database builder with DataFrame
        db_builder = DatabaseBuilderSimpleEimerdb(
            database_name=database_name,
            bucket=bucket,
            dataframe=df,
            table_name=table_name,
        )

        print("\nDatabase Builder Information:")
        print(db_builder)
        print("\nDetailed Schema:")
        for col in db_builder.schema:
            print(f"  {col['name']}: {col['type']} - {col['label']}")

        # Build the storage
        print("\nBuilding storage...")
        db_builder.build_storage()

        print("\nDatabase successfully created.")

    except Exception as e:
        eimerdb_logger.error(f"Failed to create database: {e}")
        raise


if __name__ == "__main__":
    main()
