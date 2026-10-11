import json

from bs4 import BeautifulSoup
from pydantic import BaseModel, computed_field


class Bike(BaseModel):
    bike_id: str
    range: str | None = None  # e-bikes only, e.g. "41-50 km"


class StationStatus(BaseModel):
    bikes: list[Bike]

    @computed_field
    @property
    def total_bikes(self) -> int:
        return len(self.bikes)

    @computed_field
    @property
    def total_electrical_bikes(self) -> int:
        return sum(bike.range is not None for bike in self.bikes)

    @computed_field
    @property
    def total_non_electrical_bikes(self) -> int:
        return self.total_bikes - self.total_electrical_bikes


def unwrap_station_response(body: str) -> str:
    """The station endpoint answers JSON `{"renderHtml": "<div>..."}`; return the HTML."""
    try:
        html = json.loads(body)["renderHtml"]
    except (ValueError, KeyError, TypeError) as exc:
        raise ValueError(f"unexpected station response: {body[:200]!r}") from exc
    if not isinstance(html, str):
        raise ValueError("station response 'renderHtml' is not a string")  # noqa: TRY004
    return html


def parse_station_html(html: str) -> StationStatus:
    """Extract the docked bikes from the velospot station page.

    Raises ValueError when the bike table is missing (layout change): an empty
    table body means "no bikes docked", a missing table means "parser broke".
    """
    soup = BeautifulSoup(html, "html.parser")
    table = soup.select_one("div.map-details-info-bottom table")
    if table is None or table.find("tbody") is None:
        # velostations (secured bike parkings) have no availability section at all
        if "stationIsVelostation" in html:
            return StationStatus(bikes=[])
        raise ValueError("bike table not found in station page")

    bikes = []
    for row in table.select("tbody > tr"):
        bike_id = row.select_one("td.name")
        if bike_id is None or not bike_id.get_text(strip=True):
            raise ValueError(f"bike row without id: {row!s}")
        range_cell = row.select_one("td.count")
        # non-breaking spaces (&nbsp;) mean "no range", i.e. a non-electric bike
        bike_range = (
            range_cell.get_text().replace("\xa0", " ").strip() if range_cell else ""
        )
        bikes.append(
            Bike(bike_id=bike_id.get_text(strip=True), range=bike_range or None)
        )
    return StationStatus(bikes=bikes)


def parse_station_response(body: str) -> StationStatus:
    return parse_station_html(unwrap_station_response(body))
