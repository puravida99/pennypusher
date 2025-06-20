""" 
Module for creating a DAG from a configuration file in an effort to obfuscate the raw data source (e.g., bank statements)
"""

from prefect import flow
from .models import TransformDAGConfig
from . import transforms
from ..dags.factory import IDagFactory

class ExtractDagFactory(IDagFactory):
    def create(self, config_path):
        pass

class TransformDagFactory:

    def create(self, config_path):
        """
        TODO: Needs more description
        Creates transform DAG using a configuration file. 
        
        Example YAML config:
            ```yaml
                dag_name: "bank_chase_transform"
                source_file: data/bank_chase/txn.csv

                steps:
                    - step: Parse
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

        config = TransformDAGConfig(IDagFactory.load_config(config_path))
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


class LoadDagFactory:
    pass

class EtlDagFactory:
    
    def create(self, name, config_path):
        # Create extract dag
        # Create transform dag
        # Create load dag

        pass