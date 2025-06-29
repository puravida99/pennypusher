# TODO: Sink the files to parquet using the schema derived from beancount

import polars as pl
from prefect import task

@task
class SinkParquet:
    
    def __init__(self, uri: str, lf: pl.LazyFrame):
        self.uri = uri
        self.lf = lf

    def run(self):
        self.lf.write_parquet(self.uri)
