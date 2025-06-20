""" """

from beancount.core.account import is_valid
import inspect
import importlib
from pydantic import BaseModel, create_model, validator
import re
from typing import Dict, get_type_hints, List

# Dynamically import your step modules
modules = {
    # 'extract': importlib.import_module('.extract', package=__package__),
    'transform': importlib.import_module('.transform', package=__package__),
}

def get_all_step_classes_and_functions():
    step_map = {}
    for _, module in modules.items():
        for name, obj in inspect.getmembers(module):
            if not name.startswith("_"):
                if inspect.isfunction(obj):
                    step_map[name] = obj
                if inspect.isclass(obj):
                    step_map[name] = obj.__init__
    return step_map

def generate_pydantic_model_from_callable(fn_or_class):
    sig = inspect.signature(fn_or_class)
    hints = get_type_hints(fn_or_class)
    fields = {
        (param if param != 'in_' else 'in'): (hints[param], ...)
        for param in sig.parameters
        if param != 'self'
    }
    return create_model(f'{fn_or_class.__name__}Args', **fields)


step_map = get_all_step_classes_and_functions()

class Step(BaseModel):
    step: str
    args: Dict[str, str]

    @validator("args")
    def validate_args(cls, v, values):
        step_name = values["step"]
        if step_name not in step_map:
            raise ValueError(f"Unknown step: {step_name}")
        fn_or_class = step_map[step_name]
        model = generate_pydantic_model_from_callable(fn_or_class)
        model(**v)  # Will raise if invalid
        return v


class EtlDagConfig(BaseModel):
    account: str
    glob_pattern: str
    processed_uri: str
    steps: List[Step]
    
    @validator("account")
    def account_beancount_like(cls, v):
        if not is_valid(v):
            raise ValueError("Account must be a valid beancount account string")
        return v

    @validator("steps")
    def validate_steps(cls, values):
        steps = values.get("steps")
        if not steps or not isinstance(steps, list):
            raise ValueError("EtlDagConfig must have a list of steps")
        return values

# class TransformStepConfig(BaseModel):
#     step: str = Field(..., description="The function name in the ETL library")
#     args: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Arguments to pass to the function")
#     enabled: bool = Field(default=True, description="Whether to run this step")
#     description: Optional[str] = Field(None, description="Optional description of what this step does")

#     @root_validator
#     def validate_step(cls, values):
#         if not values.get("step"):
#             raise ValueError("Each step must define a 'step' name")
#         return values


# class TransformDAGConfig(BaseModel):
#     source_file: str = Field(..., description="Path to input file, e.g., CSV or PDF")
#     output_path: Optional[str] = Field(None, description="Where to write final output (optional)")
#     dag_name: Optional[str] = Field(None, description="Custom name for the DAG (optional)")
#     steps: List[TransformStepConfig] = Field(..., description="List of transformation steps")

#     @root_validator
#     def validate_steps(cls, values):
#         steps = values.get("steps")
#         if not steps or not isinstance(steps, list):
#             raise ValueError("DAGConfig must have a list of steps")
#         return values
