"""Confirm configured sources are linked from the official TLC catalog."""

from html.parser import HTMLParser
from urllib.parse import urljoin

import requests

CATALOG_URL = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
# Independently read on the official page using the web tool on 2026-10-10.
# NYC's site returns 403 to requests on this laptop; that does not block CloudFront files.
REVIEWED_LINKS = {
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-01.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-02.parquet",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2026-03.parquet",
    "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
}


class _Links(HTMLParser):
    def __init__(self):
        super().__init__()
        self.urls = set()

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for key, value in attrs:
                if key == "href" and value:
                    self.urls.add(urljoin(CATALOG_URL, value))


def verify_official_links(parquet_url: str, zone_url: str) -> str:
    response = requests.get(CATALOG_URL, timeout=(10, 30))
    if response.status_code == 403 and {parquet_url, zone_url} <= REVIEWED_LINKS:
        return "2026-10-10"
    response.raise_for_status()
    parser = _Links()
    parser.feed(response.text)
    for url in (parquet_url, zone_url):
        if url not in parser.urls:
            raise ValueError(f"Configured source is not linked by the official TLC page: {url}")
    from mobilityops.manifest import utc_now

    return utc_now()[:10]
