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
            dataframes=my_dataframe,
            table_names="my_table_name")
    2. Check the schemas:
        print(db_builder.schemas)
    3. Build the storage:
        db_builder.build_storage()
    """

    def __init__(
        self,
        database_name: str,
        bucket: str,
        dataframes: pd.DataFrame | list[pd.DataFrame],
        table_names: str | list[str],
    ) -> None:
        """Initialize the simple database builder.

        Args:
            database_name: Name of the database
            bucket: Name of the bucket to store the database, example: ssb-tip-tutorials-data-produkt-prod
            dataframes: Either a single pandas DataFrame or a list of pandas DataFrames containing the data
            table_names: Name of the table to create
        """
        self.database_name = database_name
        self.bucket = bucket
        self.dataframes = dataframes
        self.table_names = table_names
        self._validate_inputs()
        self.schemas = self._create_schemas_from_dataframes()

    def _validate_inputs(self):
        """Validate input parameters."""
        if isinstance(self.dataframes, pd.DataFrame):
            self.dataframes = [self.dataframes]
        if isinstance(self.table_names, str):
            self.table_names = [self.table_names]

        if not (
            isinstance(self.dataframes, list)
            and all(isinstance(df, pd.DataFrame) for df in self.dataframes)
        ):
            raise TypeError("Dataframes parameter must be pandas DataFrames")

        if not (
            isinstance(self.table_names, list)
            and all(isinstance(name, str) for name in self.table_names)
        ):
            raise TypeError("Table names parameter must be strings")

        if len(self.dataframes) != len(self.table_names) or len(self.dataframes) == 0:
            raise ValueError(
                "The number of dataframes must be equal to the number of table names and > 0"
            )

        if any(df.empty for df in self.dataframes):
            raise ValueError("One or more DataFrames are empty")

        if not self.database_name or not isinstance(self.database_name, str):
            raise ValueError("Database name must be a non-empty string")

        if not self.bucket or not isinstance(self.bucket, str):
            raise ValueError("Bucket must be a non-empty string")

    def _create_schemas_from_dataframes(self) -> dict[str, list[dict[str, str | bool]]]:
        """Create eimerdb schemas from the DataFrame's structure."""
        schemas: dict[str, list[dict[str, str | bool]]] = {}
        for df, table_name in zip(self.dataframes, self.table_names, strict=False):
            schema: list[dict[str, str | bool]] = []
            for col_name in df.columns:
                dtype = df[col_name].dtype
                col_def = {
                    "name": col_name,
                    "type": self._pandas_to_eimerdb_type(dtype),
                    "label": col_name.replace("_", " ").title(),
                    "app_editable": True,
                }
                schema.append(col_def)
            schemas[table_name] = schema
        return schemas

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
        df_count = len(self.dataframes)
        # Build per-dataframe info with shape and schema columns
        df_lines: list[str] = []
        for idx, (df, tname) in enumerate(
            zip(self.dataframes, self.table_names, strict=False), start=1
        ):
            shape = df.shape
            schema_cols = [col["name"] for col in self.schemas.get(tname, [])]
            df_lines.append(f"  [{idx}] {tname}: shape={shape}, columns={schema_cols}")
        dataframes_info = f"DataFrames: {df_count}\n" + (
            "\n".join(df_lines) if df_lines else ""
        )
        table_names_info = f"Table names: {len(self.table_names)} {self.table_names}"
        return (
            f"DatabaseBuilderSimpleEimerdb\n"
            f"Database name: {self.database_name}\n"
            f"Bucket: {self.bucket}\n"
            f"{dataframes_info}\n"
            f"{table_names_info}"
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

            for table_name, schema in self.schemas.items():
                conn.create_table(
                    table_name=table_name,
                    schema=schema,
                    partition_columns=None,  # No partitioning for simple case
                    editable=True,
                )
                eimerdb_logger.info(
                    f"Created table '{table_name}' with {len(schema)} columns"
                )

        except Exception as e:
            eimerdb_logger.error(f"Error building storage: {e}")
            raise


def main() -> None:
    """Example usage of the DatabaseBuilderSimpleEimerdb."""
    # Configuration
    database_name = "frost-db"
    bucket = "ssb-tip-tutorials-data-produkt-prod"
    observations_file = "gs://ssb-tip-tutorials-data-produkt-prod/metstat/klargjorte-data/temp/pre-edit/frost/observations_p2025-01-19_p2025-03-27.parquet"
    ws_file = "gs://ssb-tip-tutorials-data-produkt-prod/metstat/inndata/frost/weather_stations_v1.parquet"

    # bucket = "arneso-test-bucket"
    # observations_file = "gs://arneso-test-bucket/data/metstat/klargjorte-data/temp/pre-edit/frost/observations_p2025-04-30_p2025-08-20.parquet"
    # ws_file = (
    #     "gs://arneso-test-bucket/data/metstat/inndata/frost/weather_stations_v2.parquet"
    # )

    table_names = ["observations", "weather_stations"]

    try:
        # Load DataFrame from parquet file
        print("Loading observations dataframe from parquet file...")
        df_obs = pd.read_parquet(observations_file)
        print(f"Loaded observations dataframe shape: {df_obs.shape}")

        print("Loading weather stations dataframe from parquet file...")
        df_ws = pd.read_parquet(ws_file)
        print(f"Loaded weather stations dataframe shape: {df_ws.shape}")
        dfs = [df_obs, df_ws]

        # Create database builder with DataFrame
        db_builder = DatabaseBuilderSimpleEimerdb(
            database_name=database_name,
            bucket=bucket,
            dataframes=dfs,
            table_names=table_names,
        )

        print("\nDatabase Builder Information:")
        print(db_builder)

        # Build the storage
        print("\nBuilding storage...")
        db_builder.build_storage()

        print("\nDatabase successfully created.")

    except Exception as e:
        eimerdb_logger.error(f"Failed to create database: {e}")
        raise


if __name__ == "__main__":
    main()
