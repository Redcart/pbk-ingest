import base64
import logging
import sys

import functions_framework
from cloudevents.http import CloudEvent

from src.load import Load
from src.settings import LoadSettings

logger = logging.getLogger(__name__)


def load_snapshot(gcs_uri: str) -> None:
    loader = Load(LoadSettings())
    data = loader.load_from_gcs(gcs_uri)
    loader.validate_schema(data)
    logger.info(
        "Read %s: snapshot %s, %d stations",
        gcs_uri,
        data["snapshot_id"],
        len(data["stations"]),
    )
    loader.load_to_bigquery(data)
    logger.info("Loaded snapshot %s to BigQuery", data["snapshot_id"])


@functions_framework.cloud_event
def load(cloud_event: CloudEvent) -> None:
    """Triggered by a Pub/Sub message whose body is the `gs://` URI of data.json.

    Any exception propagates: the message is redelivered (retry / dead-letter).
    """
    load_snapshot(base64.b64decode(cloud_event.data["message"]["data"]).decode())


if __name__ == "__main__":
    # local run: python main_load.py gs://bucket/stations/raw_data/<date>/<time>/data.json
    logging.basicConfig(level=logging.INFO)
    load_snapshot(sys.argv[1])
