"""Generic CKAN client for interacting with NGED's Connected Data portal."""

from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry


class NGEDCKANClient:
    """Client for NGED's CKAN API."""

    BASE_URL = "https://connecteddata.nationalgrid.co.uk/api/3/action"

    def __init__(self, base_url: str | None = None) -> None:
        """Initialize the client.

        Args:
            base_url: Optional base URL for the CKAN API.
        """
        self.base_url = base_url or self.BASE_URL
        self.session = requests.Session()

        # Configure retries with exponential backoff
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def get_package_show(self, package_id: str) -> dict[str, Any]:
        """Get details about a package.

        Args:
            package_id: The ID of the package.

        Returns:
            dict: The package details.
        """
        response = self.session.get(f"{self.base_url}/package_show?id={package_id}", timeout=30)
        response.raise_for_status()
        return response.json()["result"]

    def search_packages(self, query: str) -> list[dict[str, Any]]:
        """Search for packages.

        Args:
            query: The search query.

        Returns:
            list: The search results.
        """
        response = self.session.get(f"{self.base_url}/package_search?q={query}", timeout=30)
        response.raise_for_status()
        return response.json()["result"]["results"]

    def download_resource(self, url: str, dest_path: Path) -> None:
        """Download a resource from a URL to a file.

        Args:
            url: The URL to download from.
            dest_path: The path to save the file to.
        """
        response = self.session.get(url, timeout=30, stream=True)
        response.raise_for_status()

        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with open(dest_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
