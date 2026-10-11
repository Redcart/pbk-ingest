import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest

from src.extract import Extract
from src.settings import ExtractSettings

FIXTURES = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 9, 20, 12, 30, tzinfo=UTC)
MASTER_URL = "https://master.test/stations"


def handler(request: httpx.Request) -> httpx.Response:
    if str(request.url) == MASTER_URL:
        return httpx.Response(200, text=(FIXTURES / "master.json").read_text())
    master = json.loads((FIXTURES / "master.json").read_text())["velospot"][
        "responseData"
    ]
    mixed, empty = master
    if str(request.url) == mixed["detailsRoute"]:
        return httpx.Response(200, text=(FIXTURES / "station_mixed.json").read_text())
    if str(request.url) == empty["detailsRoute"]:
        return httpx.Response(200, text=(FIXTURES / "station_empty.json").read_text())
    return httpx.Response(404)


@pytest.fixture
def settings():
    return ExtractSettings(
        gcp_project_id="p", gcs_bucket="b", pubsub_topic="t", api_master_url=MASTER_URL
    )


def make_extract(settings, handler=handler, gcs=None):
    return Extract(
        settings, gcs_client=gcs or MagicMock(), transport=httpx.MockTransport(handler)
    )


async def test_collect_all_stations(settings):
    snapshot, raw = await make_extract(settings).collect_all_stations(NOW)

    assert snapshot["snapshot_id"] == "20260920T123000Z"
    assert snapshot["observed_at"] == "2026-09-20T12:30:00Z"
    assert snapshot["source"] == "publibike"
    assert len(snapshot["stations"]) == 2 and len(raw) == 2
    station = snapshot["stations"][0]
    assert isinstance(station["lat"], float)
    assert station["status"]["total_bikes"] == 3
    assert snapshot["stations"][1]["status"]["bikes"] == []


async def test_http_error_fails_the_run(settings):
    def failing(request):
        if str(request.url) == MASTER_URL:
            return handler(request)
        return httpx.Response(500)

    with pytest.raises(httpx.HTTPStatusError):
        await make_extract(settings, failing).collect_all_stations(NOW)


async def test_parser_failure_fails_the_run(settings):
    def broken(request):
        if str(request.url) == MASTER_URL:
            return handler(request)
        return httpx.Response(
            200, text=json.dumps({"renderHtml": "<div>new layout</div>"})
        )

    with pytest.raises(ValueError, match="bike table"):
        await make_extract(settings, broken).collect_all_stations(NOW)


def test_write_to_gcs_writes_raw_then_data_json(settings):
    gcs = MagicMock()
    uploaded = {}

    def blob(name):
        return MagicMock(
            upload_from_string=lambda body, **k: uploaded.__setitem__(name, body)
        )

    gcs.bucket.return_value.blob.side_effect = blob

    uri = make_extract(settings, gcs=gcs).write_to_gcs(
        {"stations": []}, {"a/b": '{"renderHtml": "x"}', "c": "{}"}, NOW
    )

    prefix = "stations/raw_data/2026-09-20/12:30:00"
    assert uri == f"gs://b/{prefix}/data.json"
    assert list(uploaded) == [f"{prefix}/raw.jsonl", f"{prefix}/data.json"]
    lines = [json.loads(line) for line in uploaded[f"{prefix}/raw.jsonl"].splitlines()]
    assert lines[0] == {"station_id": "a/b", "body": '{"renderHtml": "x"}'}
    assert len(lines) == 2
