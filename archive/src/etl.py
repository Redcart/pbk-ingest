from datetime import datetime
import pytz
import logging
from src.extract import Extract
from src.transform import Transform
from src.ingest import Ingest


class ETL:
    """
    Class to handle the ETL process for different modes.
    """

    def __init__(
        self, mode: str, url: str, bucket_name: str, project_id: str, dataset: str
    ):
        """
        Initializes the ETL class.

        Args:
            mode (str): The mode of the ETL process ("stations" or "capacity").
            url (str): The API URL for data extraction.
            bucket_name (str): The name of the GCS bucket.
            project_id (str): The Google Cloud project ID.
            dataset (str): The BigQuery dataset name.
        """
        self.mode = mode
        self.url = url
        self.bucket_name = bucket_name
        self.project_id = project_id
        self.dataset = dataset
        self.initialize_date_attributes()
        self.validate_environment_variables()

    def initialize_date_attributes(self):
        """
        Initializes the date-related attributes for the ETL process.
        """
        now = datetime.now(pytz.timezone("UTC"))
        self.current_full_date = now.strftime("%Y-%m-%d %H:%M:00")
        self.current_ymd = now.strftime("%Y-%m-%d")
        self.current_hour = now.strftime("%H:00:00")
        self.current_minute = now.strftime("%H:%M:00")

    def validate_environment_variables(self):
        """
        Validates the required environment variables for the ETL process.
        Raises an error if any required variable is missing.
        """

        if self.mode not in ["stations", "capacity"]:
            logging.error(
                "Invalid mode specified. Please use 'stations' or 'capacity'."
            )
            raise ValueError(
                "Invalid mode specified. Please use 'stations' or 'capacity'."
            )
        if not self.url:
            logging.error("API URL is not set. Please provide a valid API URL.")
            raise ValueError("API URL is not set. Please provide a valid API URL.")
        if not self.project_id:
            logging.error(
                "Project ID is not set. Please provide a valid Google Cloud project ID."
            )
            raise ValueError(
                "Project ID is not set. Please provide a valid Google Cloud project ID."
            )
        if not self.dataset:
            logging.error(
                "Dataset is not set. Please provide a valid BigQuery dataset name."
            )
            raise ValueError(
                "Dataset is not set. Please provide a valid BigQuery dataset name."
            )
        if not self.bucket_name:
            logging.error(
                "Bucket name is not set. Please provide a valid Google Cloud Storage bucket name."
            )
            raise ValueError(
                "Bucket name is not set. Please provide a valid Google Cloud Storage bucket name."
            )
        if (
            not self.current_full_date
            or not self.current_ymd
            or not self.current_hour
            or not self.current_minute
        ):
            logging.error(
                "Date attributes could not be initialized. Please check the system time and timezone settings."
            )
            raise ValueError(
                "Date attributes could not be initialized. Please check the system time and timezone settings."
            )

    def extract_data(self):
        """
        Extracts data from the API and saves it to GCS.
        """
        extract = Extract(
            url=self.url,
            bucket_name=self.bucket_name,
            mode=self.mode,
            output_path=f"{self.mode}/raw_data/{self.current_ymd}/{self.current_minute}/data.json",
        )
        extract.get_data()

    def transform_data(self):
        """
        Transforms the extracted data based on the mode and saves it to GCS.
        """
        transform = Transform(
            bucket_name=self.bucket_name,
            input_path=f"{self.mode}/raw_data/{self.current_ymd}/{self.current_minute}/data.json",
            output_path=f"{self.mode}/transformed_data/{self.current_ymd}/{self.current_minute}/data.csv",
            mode=self.mode,
            date_time=self.current_full_date,
        )
        transform.transform_data()

    def ingest_data(self):
        """
        Ingests the transformed data into BigQuery.
        """
        ingest = Ingest(
            project_id=self.project_id,
            dataset=self.dataset,
            table="stations" if self.mode == "stations" else "capacity",
            bucket_name=self.bucket_name,
        )
        ingest.ingest_data(
            input_path=f"{self.mode}/transformed_data/{self.current_ymd}/{self.current_minute}/data.csv",
            mode=self.mode,
        )

    def run(self):
        """
        Runs the ETL process.
        """
        logging.info(f"Starting ETL process for mode: {self.mode}")
        self.extract_data()
        logging.info(
            f"Data extracted from {self.url} and saved to GCS bucket: {self.bucket_name}"
        )
        if self.mode not in ["stations", "capacity"]:
            logging.error(
                "Invalid mode specified. Please use 'stations' or 'capacity'."
            )
            raise ValueError(
                "Invalid mode specified. Please use 'stations' or 'capacity'."
            )
        logging.info(f"Transforming data for mode: {self.mode}")
        self.transform_data()
        logging.info(f"Data transformed and saved to GCS bucket: {self.bucket_name}")
        logging.info(f"Ingesting data into BigQuery for mode: {self.mode}")
        self.ingest_data()
        logging.info(f"Data ingested into BigQuery table: {self.dataset}.{self.mode}")
        logging.info(f"ETL process for mode: {self.mode} completed successfully.")
