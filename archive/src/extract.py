import logging

import requests
from google.cloud import storage


class Extract:
    """
    A class to handle data extraction from an API and upload it to Google Cloud Storage (GCS).

    Attributes:
        url (str): The URL of the API to fetch data from.
        bucket_name (str): The name of the GCS bucket where data will be uploaded.
        output_path (str): The path within the GCS bucket to store the data.
    """

    def __init__(self, url: str, bucket_name: str, mode: str, output_path: str):
        """
        Initializes the Extract class with the API URL, GCS bucket name, and output path.

        Args:
            url (str): The URL of the API to fetch data from.
            bucket_name (str): The name of the GCS bucket where data will be uploaded.
            output_path (str): The path within the GCS bucket to store the data.
        """
        self.url: str = url
        self.bucket_name: str = bucket_name
        self.mode: str = mode
        self.output_path: str = output_path

    def get_data(self) -> str:
        """
        Fetches data from the API and uploads it to the specified GCS bucket.

        Returns:
            str: The HTTP status code of the API request as a string.
        """
        publibike_data = requests.get(url=self.url)
        logging.info(f"The data received from the API is: {publibike_data.text}")
        logging.info(
            f"The status code received from the API is: {publibike_data.status_code}"
        )

        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name=self.bucket_name)
        blob = bucket.blob(blob_name=self.output_path)

        # Upload the data to GCS
        blob.upload_from_string(data=publibike_data.text)

        # Log the output path
        logging.info(f"Data written at: {self.output_path}")

        return str(publibike_data.status_code)
