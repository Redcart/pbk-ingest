from pathlib import Path

import pytest

from src.parsing import (
    parse_station_html,
    parse_station_response,
    unwrap_station_response,
)

FIXTURES = Path(__file__).parent / "fixtures"


def read(name: str) -> str:
    return (FIXTURES / name).read_text()


def test_mixed_station():
    status = parse_station_response(read("station_mixed.json"))
    assert status.total_bikes == 3
    assert status.total_electrical_bikes == 2
    assert status.total_non_electrical_bikes == 1
    assert [b.range for b in status.bikes] == ["26-35 km", "0-5 km", None]


def test_only_ebikes():
    status = parse_station_response(read("station_ebikes.json"))
    assert status.total_bikes == status.total_electrical_bikes == 3
    assert status.total_non_electrical_bikes == 0


def test_empty_station_is_zero_bikes_not_an_error():
    status = parse_station_response(read("station_empty.json"))
    assert status.bikes == []
    assert status.total_bikes == 0


def test_velostation_has_no_table_and_no_bikes():
    assert parse_station_response(read("station_velostation.json")).total_bikes == 0


def test_totals_are_in_dump():
    dumped = parse_station_response(read("station_mixed.json")).model_dump()
    assert {
        "bikes",
        "total_bikes",
        "total_electrical_bikes",
        "total_non_electrical_bikes",
    } <= set(dumped)


def test_nbsp_range_means_no_range():
    html = (
        '<div class="map-details-info-bottom"><table><tbody>'
        '<tr><td class="name">1h</td><td class="count">&nbsp;</td></tr>'
        "</tbody></table></div>"
    )
    assert parse_station_html(html).bikes[0].range is None


def test_layout_change_raises():
    with pytest.raises(ValueError, match="bike table"):
        parse_station_html("<div>something else</div>")


def test_row_without_id_raises():
    html = (
        '<div class="map-details-info-bottom"><table><tbody>'
        '<tr><td class="count">10 km</td></tr></tbody></table></div>'
    )
    with pytest.raises(ValueError, match="without id"):
        parse_station_html(html)


@pytest.mark.parametrize("body", ["not json", '{"other": 1}', '{"renderHtml": 3}'])
def test_unexpected_response_raises(body):
    with pytest.raises(ValueError):
        unwrap_station_response(body)
