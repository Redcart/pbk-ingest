import pytest
from pydantic import ValidationError

from src.settings import ExtractSettings, LoadSettings


def test_extract_settings_from_env(monkeypatch):
    for key, value in {
        "GCP_PROJECT_ID": "p",
        "GCS_BUCKET": "b",
        "PUBSUB_TOPIC": "t",
        "API_MASTER_URL": "u",
    }.items():
        monkeypatch.setenv(key, value)
    settings = ExtractSettings()
    assert settings.gcs_bucket == "b"
    assert settings.max_concurrent_requests == 10


def test_missing_variable_fails_fast(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT_ID", raising=False)
    with pytest.raises(ValidationError):
        LoadSettings()
