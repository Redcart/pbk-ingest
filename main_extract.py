import asyncio
import logging
from datetime import UTC, datetime

from google.cloud import pubsub_v1

from src.extract import Extract
from src.settings import ExtractSettings

logger = logging.getLogger(__name__)


async def main() -> None:
    settings = ExtractSettings()
    now = datetime.now(UTC).replace(second=0, microsecond=0)

    logger.info("Extract %s -> gs://%s", now.isoformat(), settings.gcs_bucket)
    extractor = Extract(settings)
    data, raw_pages = await extractor.collect_all_stations(now)
    bikes = sum(station["status"]["total_bikes"] for station in data["stations"])
    logger.info(
        "Snapshot %s: %d stations, %d bikes",
        data["snapshot_id"],
        len(data["stations"]),
        bikes,
    )
    gcs_uri = extractor.write_to_gcs(data, raw_pages, now)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(settings.gcp_project_id, settings.pubsub_topic)
    publisher.publish(topic_path, gcs_uri.encode()).result()
    logger.info("Published %s to topic %s", gcs_uri, settings.pubsub_topic)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logging.getLogger("httpx").setLevel(
        logging.WARNING
    )  # one INFO line per station otherwise
    asyncio.run(main())
