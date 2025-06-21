import yaml
from . import transform_models


def test_load_step_from_yaml():
    yaml_str = """
    step: StringCleaner
    args:
      col: Description
      outcol: narration
      regex: "[^a-zA-Z0-9 ]"
      replacement: ""
    """
    data = yaml.safe_load(yaml_str)
    step = transform_models.TransformStep(**data)
    assert step.step == "StringCleaner"
