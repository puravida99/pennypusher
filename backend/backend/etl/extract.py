""" """

from pathlib import Path
import pdfplumber
import polars as pl
from prefect import task


class PdfParser:
    def extract_table(path: Path, table_settings: dict) -> None:
        """ """
        pass


@task
class ParseCsv:
    """
    Wrapper class for parsing a CSV file using polars.scan_csv
    """
    def __init__(self, path: Path, **csv_options):
        self.path = path
        self.csv_options = csv_options

    def run(self) -> pl.LazyFrame:
        return pl.scan_csv(self.path, **self.csv_options)
