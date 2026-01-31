"""
This is the ETL pipeline script for data processing.

Method: I'll be adopting an OOP approach.

"""

import pandas as pd
import numpy as np
import json
import os
from pathlib import Path
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)

class DataPipeline:
    """Base ETL class for data processing operations."""

    def __init__(self, config_filepath: str):
        # Keep the config path as an absolute Path so relative paths work reliably
        self.config_filepath = Path(config_filepath).expanduser().resolve()
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info("Initializing DataPipeline with config: %s", config_filepath)
        self.config = self.load_config()

        # Resolve source/destination relative to where config.json lives
        base_dir = self.config_filepath.parent
        self.source = str((base_dir / self.config["source"]).resolve())
        self.destination = str((base_dir / self.config["destination"]).resolve())

    def load_config(self) -> dict:
        with self.config_filepath.open("r", encoding="utf-8") as config_file:
            return json.load(config_file)

    def extract_data(self):
        """Extract data from the source."""
        self.logger.info("Extracting data...")
        with open(self.source) as source_file:
            df = pd.read_csv(source_file)
            self.logger.info("Data extracted successfully!")
            return df

    def transform_data(self, df: pd.DataFrame):
        """Transform the extracted dataframe with pandas for learning purposes. The data is actually clean."""
        self.logger.info("Transforming data...")
        df["Customer ID"] = df["Customer ID"].str.replace("CUST", "", regex=False).astype(int)

        df["Age"] = df["Age"].astype(int)
        df["Date"] = pd.to_datetime(df["Date"])
        df[["Price per Unit", "Total Amount"]] = df[["Price per Unit", "Total Amount"]].astype(float)

        # converting the Male for M and Female for F
        df["Gender"] = df["Gender"].replace({"Male": "M", "Female": "F"})

        self.logger.info("Data transformed successfully!")

        return df
    def load_data(self, df: pd.DataFrame):
        """Load the transformed data into the destination."""
        with open(self.destination, "w") as destination_file:
            df.to_csv(destination_file, index=False)

    def run_pipeline(self):
        """Run the entire ETL pipeline."""
        self.logger.info("Running ETL pipeline...")

        extract_file = self.extract_data()
        transform_file = self.transform_data(extract_file)
        self.load_data(transform_file)
        self.logger.info("ETL pipeline completed successfully!")


extracted_obj = DataPipeline(config_filepath="config.json")
print(extracted_obj.source)
print(extracted_obj.destination)
print(extracted_obj.run_pipeline())
