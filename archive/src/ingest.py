import logging
import pandas as pd
from google.cloud import bigquery
from abc import ABC, abstractmethod


class IngestFactory:
    """
    Factory class to create ingestion objects based on the mode.
    """

    @staticmethod
    def get_ingester(mode: str) -> "Ingester":
        """
        Returns the appropriate ingester object based on the mode.

        Args:
            mode (str): The mode of ingestion ("stations" or "capacity").

        Returns:
            Ingester: An instance of the appropriate ingester class.
        """
        if mode == "stations":
            return StationsIngester()
        elif mode == "capacity":
            return CapacityIngester()
        else:
            raise ValueError(f"Unsupported mode: {mode}")


class Ingester(ABC):
    """
    Base class for data ingestion.
    """

    @abstractmethod
    def get_job_config(self) -> bigquery.LoadJobConfig:
        """
        Abstract method to get the BigQuery job configuration.

        Returns:
            bigquery.LoadJobConfig: The job configuration for BigQuery ingestion.
        """
        pass

    @abstractmethod
    def read_csv(self, bucket_name: str, input_path: str) -> pd.DataFrame:
        """
        Abstract method to read and prepare the CSV file.

        Args:
            input_path (str): The GCS path to the input CSV file.

        Returns:
            pd.DataFrame: The prepared DataFrame.
        """
        pass


class StationsIngester(Ingester):
    """
    Ingester for station data.
    """

    def get_job_config(self) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(
                    name="station_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    name="latitude",
                    field_type=bigquery.enums.SqlTypeNames.FLOAT64,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="longitude",
                    field_type=bigquery.enums.SqlTypeNames.FLOAT64,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="state_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="state_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="name",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="address",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="zip",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="city",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="network_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="network_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="is_virtual_station",
                    field_type=bigquery.enums.SqlTypeNames.BOOL,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="capacity",
                    field_type=bigquery.enums.SqlTypeNames.INT64,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="ingestion_time",
                    field_type=bigquery.enums.SqlTypeNames.TIMESTAMP,
                    mode="REQUIRED",
                ),
            ],
            write_disposition="WRITE_APPEND",
        )

    def read_csv(self, bucket_name: str, input_path: str) -> pd.DataFrame:
        """
        Reads and prepares the CSV file for station data.

        Args:
            input_path (str): The GCS path to the input CSV file.

        Returns:
            pd.DataFrame: The prepared DataFrame.
        """
        dtype = {
            "station_id": str,
            "latitude": float,
            "longitude": float,
            "state_id": str,
            "state_name": str,
            "name": str,
            "address": str,
            "zip": str,
            "city": str,
            "network_id": str,
            "network_name": str,
            "is_virtual_station": bool,
            "capacity": int,
            "ingestion_time": str,
        }

        df = pd.read_csv(
            filepath_or_buffer=f"gs://{bucket_name}/{input_path}", dtype=dtype
        )
        df["ingestion_time"] = pd.to_datetime(
            df["ingestion_time"], format="%Y-%m-%d %H:%M:%S"
        )
        return df


class CapacityIngester(Ingester):
    """
    Ingester for capacity data.
    """

    def get_job_config(self) -> bigquery.LoadJobConfig:
        return bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField(
                    name="station_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    name="vehicle_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="REQUIRED",
                ),
                bigquery.SchemaField(
                    name="vehicle_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="vehicle_ebike_battery_level",
                    field_type=bigquery.enums.SqlTypeNames.FLOAT64,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="vehicle_type_id",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="vehicle_type_name",
                    field_type=bigquery.enums.SqlTypeNames.STRING,
                    mode="NULLABLE",
                ),
                bigquery.SchemaField(
                    name="ingestion_time",
                    field_type=bigquery.enums.SqlTypeNames.TIMESTAMP,
                    mode="REQUIRED",
                ),
            ],
            write_disposition="WRITE_APPEND",
        )

    def read_csv(self, bucket_name: str, input_path: str) -> pd.DataFrame:
        """
        Reads and prepares the CSV file for capacity data.

        Args:
            input_path (str): The GCS path to the input CSV file.

        Returns:
            pd.DataFrame: The prepared DataFrame.
        """
        dtype = {
            "station_id": str,
            "vehicle_id": str,
            "vehicle_name": str,
            "vehicle_ebike_battery_level": float,
            "vehicle_type_id": str,
            "vehicle_type_name": str,
            "ingestion_time": str,
        }

        df = pd.read_csv(
            filepath_or_buffer=f"gs://{bucket_name}/{input_path}", dtype=dtype
        )
        df["ingestion_time"] = pd.to_datetime(
            df["ingestion_time"], format="%Y-%m-%d %H:%M:%S"
        )
        return df


class Ingest:
    """
    Main class to handle data ingestion using the factory pattern.
    """

    def __init__(self, project_id: str, dataset: str, table: str, bucket_name: str):
        """
        Initializes the Ingest class with project, dataset, and table information and the GCS bucket name.

        Args:
            project_id (str): The Google Cloud project ID.
            dataset (str): The BigQuery dataset name.
            table (str): The BigQuery table name.
            bucket_name (str): The name of the Google Cloud Storage bucket.
        """
        self.project_id = project_id
        self.dataset = dataset
        self.table = table
        self.bucket_name = bucket_name

    def ingest_data(self, input_path: str, mode: str) -> int:
        """
        Ingests data from a CSV file in GCS into BigQuery.

        Args:
            input_path (str): The GCS path to the input CSV file.
            mode (str): The mode of ingestion ("stations" or "capacity").

        Returns:
            int: HTTP status code indicating success.
        """
        # Get the appropriate ingester based on the mode
        ingester = IngestFactory.get_ingester(mode)

        # Read and prepare the data using the ingester
        df_transformed = ingester.read_csv(
            bucket_name=self.bucket_name, input_path=input_path
        )

        client = bigquery.Client(project=self.project_id)
        table_id = f"{self.project_id}.{self.dataset}.{self.table}"
        job_config = ingester.get_job_config()

        # Load data into BigQuery
        job = client.load_table_from_dataframe(
            df_transformed, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete

        table = client.get_table(table_id)  # Make an API request
        logging.info(
            f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}"
        )

        return 200  # HTTP status code for success
