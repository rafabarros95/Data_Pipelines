from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

__all__ = ["TransformData"]


class TransformData:
    """
    Loads the raw API JSON saved by extract_data.py and converts selected payloads to DataFrames.
    """

    def __init__(self, raw_filename: str = "covid_api.json"):
        self.project_root = Path(__file__).resolve().parent.parent  # api_project/
        self.raw_path = self.project_root / "raw_data" / raw_filename
        self.processed_dir = self.project_root / "processed_data"

    def load_raw_api_json(self) -> dict:
        logger.info("Loading raw API JSON from %s", self.raw_path)
        with self.raw_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    @staticmethod
    def payload_to_df(payload: dict) -> pd.DataFrame:
        if not isinstance(payload, dict) or "data" not in payload:
            raise ValueError("Expected a payload dict with a top-level 'data' field.")
        return pd.json_normalize(payload["data"], sep=".")

    def load_reports_df(self, reports_key: str = "reports_USA_2020-04-16") -> pd.DataFrame:
        raw = self.load_raw_api_json()

        if reports_key not in raw:
            raise KeyError(f"Key '{reports_key}' not found. Available keys: {list(raw.keys())}")

        df = self.payload_to_df(raw[reports_key])

        # Friendly renames for nested region fields
        df = df.rename(
            columns={
                "region.iso": "iso",
                "region.name": "region_name",
                "region.province": "province",
                "region.lat": "region_lat",
                "region.long": "region_long",
                "region.cities": "region_cities",
            }
        )
        return df

    @staticmethod
    def reports_df_to_cities_df(
        reports_df: pd.DataFrame,
        cities_col: str = "region_cities",
        keep_parent_cols: tuple[str, ...] = ("iso", "region_name", "province", "date"),
    ) -> pd.DataFrame:
        """
        Takes a reports-level DataFrame that has a column like `region_cities`
        where each row is a list[dict], and returns a city-level DataFrame.

        Output: one row per city with parent keys attached.
        """
        missing = [c for c in keep_parent_cols if c not in reports_df.columns]
        if missing:
            raise KeyError(f"Missing parent columns in reports_df: {missing}")

        if cities_col not in reports_df.columns:
            raise KeyError(f"Missing cities column '{cities_col}' in reports_df.")

        tmp = reports_df[list(keep_parent_cols) + [cities_col]].copy()

        # Ensure every cell is a list (handles None/NaN)
        tmp[cities_col] = tmp[cities_col].apply(lambda x: x if isinstance(x, list) else [])

        # explode list-of-dicts into one dict per row
        tmp = tmp.explode(cities_col, ignore_index=True)

        # remove rows where the list was empty
        tmp = tmp.dropna(subset=[cities_col])

        # expand each city dict into columns
        city_details = pd.json_normalize(tmp[cities_col])
        cities_df = pd.concat([tmp.drop(columns=[cities_col]), city_details], axis=1)

        return cities_df

    def save_df(self, df: pd.DataFrame, filename: str) -> Path:
        self.processed_dir.mkdir(parents=True, exist_ok=True)
        out_path = self.processed_dir / filename
        logger.info("Saving DataFrame to %s", out_path)
        df.to_csv(out_path, index=False)
        return out_path