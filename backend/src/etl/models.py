""" """

from pydantic import BaseModel, Field, root_validator
from typing import List, Dict, Optional, Any, Union


class StepConfig(BaseModel):
    step: str = Field(..., description="The function name in the ETL library")
    args: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Arguments to pass to the function")
    enabled: bool = Field(default=True, description="Whether to run this step")
    description: Optional[str] = Field(None, description="Optional description of what this step does")

    @root_validator
    def validate_step(cls, values):
        if not values.get("step"):
            raise ValueError("Each step must define a 'step' name")
        return values


class DAGConfig(BaseModel):
    source_file: str = Field(..., description="Path to input file, e.g., CSV or PDF")
    output_path: Optional[str] = Field(None, description="Where to write final output (optional)")
    dag_name: Optional[str] = Field(None, description="Custom name for the DAG (optional)")
    steps: List[StepConfig] = Field(..., description="List of transformation steps")

    @root_validator
    def validate_steps(cls, values):
        steps = values.get("steps")
        if not steps or not isinstance(steps, list):
            raise ValueError("DAGConfig must have a list of steps")
        return values
