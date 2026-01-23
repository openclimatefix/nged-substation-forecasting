"""Generic CKAN client for interacting with NGED's Connected Data portal."""

from typing import Any

import requests


class NGEDCKANClient:
    """Client for NGED's CKAN API."""

    BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action"

    def __init__(self, base_url: str | None = None) -> None:
        """Initialize the client."""
        self.base_url = base_url or self.BASE_URL

    def get_package_show(self, package_id: str) -> dict[str, Any]:
        """Get details about a package."""
        response = requests.get(f"{self.base_url}/package_show?id={package_id}", timeout=30)
        response.raise_for_status()
        return response.json()["result"]

    def search_packages(self, query: str) -> list[dict[str, Any]]:
        """Search for packages."""
        response = requests.get(f"{self.base_url}/package_search?q={query}", timeout=30)
        response.raise_for_status()
        return response.json()["result"]["results"]
