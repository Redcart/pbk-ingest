from datetime import datetime

import pytz

from src.extract import Extract
from src.transform import Transform
from src.ingest import Ingest


def get_data(url, bucket_name, current_ymd, current_minute, mode):
    now = datetime.now(pytz.timezone("UTC"))
    current_ymd = now.strftime("%Y-%m-%d")
    current_minute = now.strftime("%H:%M:00")

    # Initialize the ETL process
    extractor = Extract(
        url=url,
        bucket_name=bucket_name,
        mode=mode,
        output_path=f"{mode}/raw_data/{current_ymd}/{current_minute}/data.json",
    )

    extractor.get_data()

    return 200


def transform_data(bucket_name, current_ymd, current_minute, mode, timestamp):
    # Initialize the ETL process
    transformer = Transform(
        bucket_name=bucket_name,
        input_path=f"{mode}/raw_data/{current_ymd}/{current_minute}/data.json",
        output_path=f"{mode}/transformed_data/{current_ymd}/{current_minute}/data.csv",
        mode=mode,
        date_time=timestamp,
    )

    transformer.transform_data()

    return 200


def ingest_data(project_id, dataset, bucket_name, current_ymd, current_minute, mode):
    # Initialize the ETL process
    ingestor = Ingest(
        project_id=project_id,
        dataset=dataset,
        table=mode,
        bucket_name=bucket_name,
    )

    # Run the Load process
    ingestor.ingest_data(
        input_path=f"{mode}/transformed_data/{current_ymd}/{current_minute}/data.csv",
        mode=mode,
    )

    return 200
