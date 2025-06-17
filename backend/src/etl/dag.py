""" 
Module for creating a DAG from a configuration file in an effort to obfuscate the raw data source (e.g., bank statements)
"""

import yaml
from prefect import flow
from .models import DAGConfig
from . import transforms

class TransformDagFactory:
    def __init__(self):
        self.dags = {}

    def create(self, name, config_path):
        """
        TODO: Needs more description
        Creates transform DAG using a configuration file. 
        
        Example YAML config:
            ```yaml
                dag_name: "bank_chase_transform"
                source_file: data/bank_chase/txn.csv

                steps:
                    - step: parse_csv
                      args:
                        file_path: "{{source_file}}"

                    # Generate a currency column
                    - step: InferCurrency
                      args:
                        amount_col: "amount"
                        outcol: "currency"

                    # Remove non-alphanumeric characters from the narrations
                    - step: StringCleaner
                      args:
                        col: "narration"
                        outcol: "narration"
                        regex: "[^a-zA-Z0-9 ]"
                        replacement: ""

                    # Convert dates to UTC
                    - step: StandardizeDates
                      args:
                        col: "date"
                        outcol: "date"
                        fmt: "%m/%d/%Y"
            ```

        Args:
            name (str): The name of the DAG
            config_path (str): The path to the DAG configuration file
        Returns:
            The DAG
        
        """
        if name in self.dags:
            raise ValueError(f"A DAG named {name} already exists")

        def load_config(config_path):
            with open(config_path, "r") as f:
                raw_yaml = yaml.safe_load(f)
            return DAGConfig(**raw_yaml)

        config = load_config(config_path)
        step_defs = config["steps"]

        @flow(name=f"{config_path}_etl_pipeline")
        def etl_flow():
            context = {}
            for step_def in step_defs:
                tform_name = step_def["step"]
                args = step_def.get("args", {})

                # Replace templated vars like {{source_file}}
                resolved_args = {
                    k: config.get(v.strip("{{}}"), v) if isinstance(v, str) and "{{" in v else v
                    for k, v in args.items()
                }

                tform_cls = getattr(transforms, tform_name)
                tform_instance = tform_cls(**resolved_args)
                context["df"] = tform_instance.apply(context.get("df"))

            return context["df"]

        return etl_flow
    
    def get(self, name):
        if name not in self.dags:
            raise ValueError(f"A DAG named {name} does not exist")
        return self.dags[name]

