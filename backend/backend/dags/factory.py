
from abc import abstractmethod
import yaml


class IDagFactory:
    
    @abstractmethod
    def create(self, config):
        pass

    @classmethod
    def load_config(cls, config_path):
        with open(config_path, "r") as f:
            return yaml.safe_load(f)

