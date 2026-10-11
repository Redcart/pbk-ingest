from abc import ABC, abstractmethod
import json
import logging

import pandas as pd
from google.cloud import storage


class TransformFactory:
    """
    Factory class to create transformation objects based on the mode.
    """

    @staticmethod
    def get_transformer(mode: str, date_time: str) -> "Transformer":
        """
        Returns the appropriate transformer object based on the mode.

        Args:
            mode (str): The mode of transformation (e.g., "stations" or "capacity").
            date_time (str): The date and time of the transformation.

        Returns:
            Transformer: An instance of the appropriate transformer class.
        """
        if mode == "stations":
            return StationsTransformer(date_time)
        elif mode == "capacity":
            return CapacityTransformer(date_time)
        else:
            raise ValueError(
                f"Unsupported mode: {mode}. Please use stations or capacity."
            )


class Transformer(ABC):
    """
    Base class for data transformation.
    """

    def __init__(self, date_time: str):
        """
        Initializes the Transformer class.

        Args:
            date_time (str): The date and time of the transformation.
        """
        self.date_time = date_time

    @abstractmethod
    def transform(self, raw_data: dict, bucket_name: str, output_path: str) -> int:
        """
        Abstract method to transform data.

        Args:
            raw_data (dict): The raw data to transform.
            bucket_name (str): The name of the GCS bucket.
            output_path (str): The path to store the transformed data in GCS.

        Returns:
            int: HTTP status code indicating success.
        """
        raise NotImplementedError("Subclasses must implement this method")


class StationsTransformer(Transformer):
    """
    Transformer for station data.
    """

    def __init__(self, date_time: str):
        """
        Initializes the StationsTransformer class.

        Args:
            date_time (str): The date and time of the transformation.
        """
        super().__init__(date_time)

    def transform(self, raw_data: dict, bucket_name: str, output_path: str) -> int:
        list_of_stations = []

        for station in raw_data.get("stations", []):
            keys = [
                "station_id",
                "latitude",
                "longitude",
                "state_id",
                "state_name",
                "name",
                "address",
                "zip",
                "city",
                "network_id",
                "network_name",
                "is_virtual_station",
                "capacity",
                "ingestion_time",
            ]

            values = [
                station.get("id"),
                station.get("latitude"),
                station.get("longitude"),
                station.get("state").get("id"),
                station.get("state").get("name"),
                station.get("name"),
                station.get("address"),
                station.get("zip"),
                station.get("city"),
                station.get("network").get("id"),
                station.get("network").get("name"),
                station.get("is_virtual_station"),
                station.get("capacity"),
                self.date_time,
            ]

            data_one_station = {key: value for key, value in zip(keys, values)}
            list_of_stations.append(data_one_station)

        df_all_stations = pd.DataFrame.from_records(data=list_of_stations)
        df_all_stations.to_csv(
            path_or_buf=f"gs://{bucket_name}/{output_path}", index=False
        )
        logging.info(
            f"Transformed station data written at gs://{bucket_name}/{output_path}"
        )
        return 200


class CapacityTransformer(Transformer):
    """
    Transformer for capacity data.
    """

    def __init__(self, date_time: str):
        """
        Initializes the CapacityTransformer class.

        Args:
            date_time (str): The date and time of the transformation.
        """
        super().__init__(date_time)

    def transform(self, raw_data: dict, bucket_name: str, output_path: str) -> int:
        list_of_stations_with_capacity = []

        for station in raw_data.get("stations", []):
            for bike in station.get("vehicles", []):
                keys = [
                    "station_id",
                    "vehicle_id",
                    "vehicle_name",
                    "vehicle_ebike_battery_level",
                    "vehicle_type_id",
                    "vehicle_type_name",
                    "ingestion_time",
                ]

                values = [
                    station.get("id"),
                    bike.get("id"),
                    bike.get("name"),
                    bike.get("ebike_battery_level"),
                    bike.get("type").get("id"),
                    bike.get("type").get("name"),
                    self.date_time,
                ]

                data_one_bike = {key: value for key, value in zip(keys, values)}
                list_of_stations_with_capacity.append(data_one_bike)

        df_all_bikes = pd.DataFrame.from_records(data=list_of_stations_with_capacity)
        df_all_bikes.to_csv(
            path_or_buf=f"gs://{bucket_name}/{output_path}", index=False
        )
        logging.info(
            f"Transformed capacity data written at gs://{bucket_name}/{output_path}"
        )

        return 200


class Transform:
    """
    Main class to handle data transformation using the factory pattern.
    """

    def __init__(
        self,
        bucket_name: str,
        input_path: str,
        output_path: str,
        mode: str,
        date_time: str,
    ):
        """
        Initializes the Transform class with the input path, GCS bucket name, output path, mode, and date time.

        Args:
            input_path (str): The path to the input data in GCS.
            bucket_name (str): The name of the GCS bucket.
            output_path (str): The path to store the transformed data in GCS.
            mode (str): The mode of transformation (e.g., "stations" or "capacity").
            date_time (str): The date and time of the transformation.
        """
        self.bucket_name = bucket_name
        self.input_path = input_path
        self.output_path = output_path
        self.mode = mode
        self.date_time = date_time

    def transform_data(self) -> int:
        """
        Transforms the data based on the mode and writes the transformed data to GCS.

        Returns:
            int: HTTP status code indicating success.
        """
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name=self.bucket_name)
        blob = bucket.blob(blob_name=self.input_path)

        with blob.open(mode="r") as file:
            raw_data = json.load(file)

        transformer = TransformFactory.get_transformer(
            mode=self.mode, date_time=self.date_time
        )

        transformer.transform(
            raw_data=raw_data,
            bucket_name=self.bucket_name,
            output_path=self.output_path,
        )

        return 200
