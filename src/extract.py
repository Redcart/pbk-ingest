import asyncio
import json
import logging
import time
from datetime import datetime

import httpx
from google.cloud import storage

from src.parsing import parse_station_response
from src.settings import ExtractSettings

logger = logging.getLogger(__name__)

SOURCE = "publibike"


class Extract:
    def __init__(
        self,
        config: ExtractSettings,
        gcs_client: storage.Client | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
    ):
        self.config = config
        self.transport = transport or httpx.AsyncHTTPTransport(retries=2)
        self.gcs_client = gcs_client or storage.Client(project=config.gcp_project_id)

    async def get_master_data(self, client: httpx.AsyncClient) -> list[dict]:
        """Fetch all stations from the master endpoint (`velospot.responseData`)."""
        response = await client.get(self.config.api_master_url)
        response.raise_for_status()
        stations = response.json()["velospot"]["responseData"]
        if not stations:
            raise ValueError("master endpoint returned no stations")
        return stations

    async def get_station_data(
        self, client: httpx.AsyncClient, semaphore: asyncio.Semaphore, master: dict
    ) -> tuple[dict, str]:
        """Fetch one station page. Return (merged station, raw response body)."""
        async with semaphore:
            response = await client.get(master["detailsRoute"])
        response.raise_for_status()
        body = response.text
        status = parse_station_response(body)

        expected = master.get("totalBike")
        if expected is not None and expected != status.total_bikes:
            logger.debug(
                "station %s: master says %s bikes, page has %s",
                master["station_id"],
                expected,
                status.total_bikes,
            )
        station = {
            "station_id": master["station_id"],
            "station_name": master["station_name"],
            "station_address": master["station_address"],
            "lat": float(master["lat"]),
            "lng": float(master["lng"]),
            "station_number": master["stationNumber"],
            "details_route": master["detailsRoute"],
            "status": status.model_dump(),
        }
        return station, body

    async def collect_all_stations(self, now: datetime) -> tuple[dict, dict[str, str]]:
        """Collect all stations concurrently. Return (snapshot, {station_id: raw body})."""
        semaphore = asyncio.Semaphore(self.config.max_concurrent_requests)
        timeout = httpx.Timeout(self.config.request_timeout_seconds)
        async with httpx.AsyncClient(
            timeout=timeout, transport=self.transport
        ) as client:
            started = time.perf_counter()
            masters = await self.get_master_data(client)
            logger.info("Master: %d stations, fetching station pages", len(masters))
            results = await asyncio.gather(
                *(self.get_station_data(client, semaphore, m) for m in masters)
            )

        logger.info(
            "Fetched and parsed %d stations in %.1fs",
            len(results),
            time.perf_counter() - started,
        )
        snapshot = {
            "snapshot_id": now.strftime("%Y%m%dT%H%M%SZ"),
            "observed_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": SOURCE,
            "stations": [station for station, _ in results],
        }
        raw_pages = {station["station_id"]: body for station, body in results}
        return snapshot, raw_pages

    def write_to_gcs(self, data: dict, raw_pages: dict[str, str], now: datetime) -> str:
        """Write raw.jsonl, then data.json. Return the `gs://` URI of data.json."""
        path = self.config.gcs_output_path_template.format(
            date=now.strftime("%Y-%m-%d"), time=now.strftime("%H:%M:%S")
        )
        raw_path = path.rsplit("/", 1)[0] + "/raw.jsonl"
        bucket = self.gcs_client.bucket(self.config.gcs_bucket)

        # one line per station, response body untouched; GCS bills per object, not per byte
        raw = "\n".join(
            json.dumps({"station_id": station_id, "body": body}, ensure_ascii=False)
            for station_id, body in raw_pages.items()
        )
        # raw first, data.json last: a data.json never exists without its raw data
        bucket.blob(raw_path).upload_from_string(
            raw, content_type="application/x-ndjson"
        )
        logger.info(
            "Wrote gs://%s/%s (%d KB)",
            self.config.gcs_bucket,
            raw_path,
            len(raw) // 1024,
        )
        payload = json.dumps(data, ensure_ascii=False)
        bucket.blob(path).upload_from_string(payload, content_type="application/json")
        logger.info(
            "Wrote gs://%s/%s (%d KB)",
            self.config.gcs_bucket,
            path,
            len(payload) // 1024,
        )
        return f"gs://{self.config.gcs_bucket}/{path}"
