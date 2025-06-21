""" """

from beancount.core.account import is_valid
import inspect
import importlib
from pydantic import BaseModel, create_model, Field, field_validator, ValidationInfo
from typing import Any, Callable, Dict, get_type_hints, List

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

def generate_pydantic_model_from_callable(func: Callable) -> BaseModel:
    """
    Dynamically generate a Pydantic v2 model from a function signature and type hints

    Args:
        func (Callable): The function to generate the model from

    Returns:
        The generated Pydantic model
    """
    sig = inspect.signature(func)
    type_hints = get_type_hints(func)
    model_name = f"{func.__name__.capitalize()}Args"

    # Gather fields
    fields: Dict[str, tuple] = {}
    for name, param in sig.parameters.items():
        if name == "self":
            continue
        annotation = type_hints.get(name, Any)
        default = param.default if param.default is not inspect.Parameter.empty else ...
        fields[name] = (annotation, Field(default))
    
    # Create the model dynamically
    return create_model(model_name, **fields)


step_map = get_all_step_classes_and_functions()


class Step(BaseModel):
    step: str = Field(..., description="The function name in the ETL library")
    args: Dict[str, Any] = Field(..., description="Arguments to pass to the function")

    @field_validator("args")
    def validate_args(cls, v: Dict, info: ValidationInfo):
        step_name = info.data["step"]
        if step_name not in step_map:
            raise ValueError(f"Unknown step: {step_name}")
        fn_or_class = step_map[step_name]
        model = generate_pydantic_model_from_callable(fn_or_class)
        model(**v)  # Will raise if invalid
        return v


class EtlDagConfig(BaseModel):
    account: str = Field(..., description="The beancount account to use")
    glob_pattern: str = Field(..., description="The glob pattern to match input files")
    processed_uri: str = Field(..., description="The URI to store processed data")
    steps: List[Step] = Field(..., description="The list of steps to run")
    
    @field_validator("account")
    def account_beancount_like(cls, v):
        if not is_valid(v):
            raise ValueError("Account must be a valid beancount account string")
        return v
