from pydantic_settings import BaseSettings


class ExtractSettings(BaseSettings):
    """Extract (Cloud Run Job) configuration."""

    gcp_project_id: str
    gcs_bucket: str
    pubsub_topic: str
    api_master_url: str
    gcs_output_path_template: str = "stations/raw_data/{date}/{time}/data.json"
    max_concurrent_requests: int = 10
    request_timeout_seconds: int = 30


class LoadSettings(BaseSettings):
    """Load (Cloud Function) configuration."""

    gcp_project_id: str
    bq_dataset: str = "bronze"
    bq_table: str = "publibike_snapshots"
