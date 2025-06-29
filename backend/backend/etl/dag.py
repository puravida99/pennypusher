"""
Module for creating a DAG from a configuration file in an effort to obfuscate the raw data source (e.g., bank statements)
"""

from glob import glob
import polars as pl
from prefect import flow
from .extract import ParseCsv
from .load import SinkParquet
from .models import EtlDagConfig
from . import transforms
from ..dags.factory import IDagFactory
        

@flow
def sequential_transforms_flow(lf: pl.LazyFrame, step_defs: list):
    for step_def in step_defs:
        tform_name = step_def["step"]
        args = step_def.get("args", {})
        tform_cls = getattr(transforms, tform_name)
        tform_instance = tform_cls(**args)
        lf = tform_instance.apply(lf)
    return lf

class EtlDag(IDagFactory):

    def __init__(self, config_path):
        self.config = EtlDagConfig(IDagFactory.load_config(config_path))
        

    @classmethod
    def create_extractor_task(cls, source_type: str, fname: str, **args):
        if source_type == "csv":
            return ParseCsv(fname, **args["csv_options"])
        else:
            raise ValueError(f"Unknown source type: {source_type}")

    @classmethod
    def create_loader_task(cls, loader_type: str, uri: str, lf: pl.LazyFrame):
        if loader_type == "parquet":
            return SinkParquet(uri, lf)
        else:
            raise ValueError(f"Unknown loader type: {loader_type}")


    @flow
    def run(self):
        extract_config = self.config["extract"]
        transform_config = self.config["transforms"]
        load_config = self.config["load"]
        fnames = glob(extract_config["glob_pattern"])
        for fname in fnames:
            lf = EtlDag.create_extractor_task(extract_config["type"], fname, extract_config["args"]).submit()
            lf = sequential_transforms_flow(lf, transform_config).submit()
            EtlDag.create_loader_task(load_config["type"], load_config["uri"], lf).submit()


