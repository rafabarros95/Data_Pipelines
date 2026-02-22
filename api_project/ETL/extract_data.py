"""Data Engineering Project about extracting data about Covid-19 statistics through API, do the ETL stuff and load that within a database, probably postgresSQL"""

import json
import logging
import time
from pathlib import Path
from typing import Any

import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


class Covid_Api_Extract_Data:
    def __init__(
        self,
        base_url: str = "https://covid-api.com/api",
        raw_filename: str = "covid_api.json",
        tries: int = 5,
        timeout_s: int = 30,
    ):
        self.base_url = base_url.rstrip("/")
        self.tries = tries
        self.timeout_s = timeout_s

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

        self.project_root = Path(__file__).resolve().parent.parent  # api_project/
        self.raw_dir = self.project_root / "raw_data"
        self.raw_path = self.raw_dir / raw_filename

    def get_json(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        for attempt in range(self.tries):
            logger.info("Fetching %s...", url)
            r = self.session.get(url, params=params, timeout=self.timeout_s)

            if r.status_code == 429:
                retry_after = r.headers.get("Retry-After")
                sleep_s = float(retry_after) if retry_after else (0.5 * (2**attempt))
                logger.info("Rate limited (429). Sleeping %.2f seconds...", sleep_s)
                time.sleep(sleep_s)
                continue

            r.raise_for_status()
            logger.info("Fetched %s successfully.", url)
            return r.json()

        raise RuntimeError("Rate-limited too many times (429).")

    def fetch_all(self) -> dict[str, Any]:
        regions_payload = self.get_json("/regions", params={"per_page": 20, "order": "name", "sort": "asc"})

        iso = "CHN"
        provinces_payload = self.get_json(
            f"/provinces/{iso}",
            params={"per_page": 50, "order": "name", "sort": "asc"},
        )

        reports_payload = self.get_json(
            "/reports",
            params={"date": "2020-04-16", "iso": "USA", "per_page": 50},
        )

        return {
            "regions": regions_payload,
            "provinces_CHN": provinces_payload,
            "reports_USA_2020-04-16": reports_payload,
        }

    def save_raw(self, payloads: dict[str, Any]) -> Path:
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        with self.raw_path.open("w", encoding="utf-8") as f:
            json.dump(payloads, f, indent=2, ensure_ascii=False)

        logger.info("Saved API responses to: %s", self.raw_path)
        return self.raw_path

    def run(self) -> Path:
        payloads = self.fetch_all()
        return self.save_raw(payloads)


# local run for testing
if __name__ == "__main__":
   extractor = Covid_Api_Extract_Data()
   extractor.run()
   logger.info("Done!")