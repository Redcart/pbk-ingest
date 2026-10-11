from datetime import datetime

REQUIRED_FIELDS = ("snapshot_id", "observed_at", "source", "stations")


def validate_snapshot(data: dict) -> None:
    """Raise ValueError if the snapshot is not loadable."""
    missing = [field for field in REQUIRED_FIELDS if not data.get(field)]
    if missing:
        raise ValueError(f"snapshot missing fields: {missing}")
    if not isinstance(data["stations"], list):
        raise ValueError("snapshot 'stations' must be a list")  # noqa: TRY004
    try:
        datetime.fromisoformat(data["observed_at"])
    except ValueError as exc:
        raise ValueError(f"invalid observed_at: {data['observed_at']!r}") from exc
