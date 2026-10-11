from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import Conflict

from src.load import Load
from src.settings import LoadSettings
from src.validation import validate_snapshot

SNAPSHOT = {
    "snapshot_id": "20260920T123000Z",
    "observed_at": "2026-09-20T12:30:00Z",
    "source": "publibike",
    "stations": [{"station_id": "1"}],
}


def test_valid_snapshot_passes():
    validate_snapshot(SNAPSHOT)


@pytest.mark.parametrize("field", ["snapshot_id", "observed_at", "source", "stations"])
def test_missing_field_raises(field):
    with pytest.raises(ValueError, match=field):
        validate_snapshot({k: v for k, v in SNAPSHOT.items() if k != field})


def test_bad_observed_at_raises():
    with pytest.raises(ValueError, match="observed_at"):
        validate_snapshot({**SNAPSHOT, "observed_at": "yesterday"})


def make_load():
    bq = MagicMock()
    return Load(
        LoadSettings(gcp_project_id="p"), bq_client=bq, gcs_client=MagicMock()
    ), bq


def test_load_to_bigquery_appends_one_row():
    loader, bq = make_load()
    loader.load_to_bigquery(SNAPSHOT)

    rows, table = bq.load_table_from_json.call_args.args
    kwargs = bq.load_table_from_json.call_args.kwargs
    assert table == "p.bronze.publibike_snapshots"
    assert kwargs["job_id"] == "load_publibike_20260920T123000Z"
    assert rows[0]["payload"] == SNAPSHOT  # an object, not a JSON string
    assert rows[0]["ingested_at"]
    bq.load_table_from_json.return_value.result.assert_called_once()


def test_redelivered_message_is_a_noop():
    loader, bq = make_load()
    bq.load_table_from_json.side_effect = Conflict("Already Exists: Job")
    bq.get_job.return_value.error_result = None
    loader.load_to_bigquery(SNAPSHOT)  # must not raise
    assert bq.load_table_from_json.call_count == 1


def test_failed_previous_job_is_retried_with_a_new_id():
    loader, bq = make_load()
    bq.load_table_from_json.side_effect = [Conflict("exists"), MagicMock()]
    bq.get_job.return_value.error_result = {"reason": "invalid"}
    loader.load_to_bigquery(SNAPSHOT)
    ids = [c.kwargs["job_id"] for c in bq.load_table_from_json.call_args_list]
    assert ids == [
        "load_publibike_20260920T123000Z",
        "load_publibike_20260920T123000Z_r1",
    ]


def test_gives_up_after_max_attempts():
    loader, bq = make_load()
    bq.load_table_from_json.side_effect = Conflict("exists")
    bq.get_job.return_value.error_result = {"reason": "invalid"}
    with pytest.raises(RuntimeError):
        loader.load_to_bigquery(SNAPSHOT)
