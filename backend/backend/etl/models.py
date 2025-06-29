""" """

from beancount.core.account import is_valid
import inspect
import importlib
from pydantic import BaseModel, create_model, Field, field_validator, ValidationInfo
from typing import Any, Callable, Dict, get_type_hints, List


# Dynamically import your step modules
modules = {
    "transform": importlib.import_module(".transform", package=__package__),
}


def get_all_step_classes_and_functions() -> Dict[str, Callable]:
    """
    Iterate over the modules and gather all functions and classes (not starting with "_")
    and their __init__ methods into a dictionary. This is used to map the step names in
    the DAG config to their function or class.

    Returns:
        A dictionary where the key is the name of the step and the value is the callable.
    """
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


class TransformStep(BaseModel):
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


class ExtractDagConfig(BaseModel):
    type: str = Field(..., description="The type of data to extract")
    glob_pattern: str = Field(..., description="The glob pattern to match input files")
    args: Dict[str, Any] = Field(..., description="Arguments to pass to the function")


class TransformDagConfig(BaseModel):
    name: str = Field(..., description="The name of the DAG")
    steps: List[TransformStep] = Field(..., description="The list of steps to run")


class LoadDagConfig(BaseModel):
    type: str = Field(..., description="The type of data to load")
    uri: str = Field(..., description="The URI to store processed data")
    args: Dict[str, Any] = Field(..., description="Arguments to pass to the function")


class EtlDagConfig(BaseModel):
    extract: ExtractDagConfig = Field(..., description="The beancount account to use")
    transforms: TransformDagConfig = Field(..., description="The glob pattern to match input files")
    load: LoadDagConfig = Field(..., description="The URI to store processed data")
