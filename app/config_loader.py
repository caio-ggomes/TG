import yaml
from typing import Dict, List



class ConfigLoader:
    def __init__(self, config_path: str) -> None:
        with open(config_path, "rb") as file:
            self.config = yaml.safe_load(file)

    @property
    def connection_host(self) -> str:
        return self.config["CONNECTION_HOST"]

    @property
    def connection_port(self) -> str:
        return self.config["CONNECTION_PORT"]

    @property
    def table_name(self) -> str:
        return self.config["TABLE_NAME"]

    @property
    def column_families_config(self) -> Dict[str, dict]:
        return self.config["COLUMN_FAMILIES_CONFIG"]

    @property
    def database_names(self) -> List[str]:
        return list(self.config["DATABASES_INFO"].keys())

    def get_sample_path(self, database_name: str) -> str:
        return self.config["DATABASES_INFO"][database_name]["SAMPLE_PATH"]

    def get_column_family_map(self, database_name: str) -> Dict[str, List[str]]:
        return self.config["DATABASES_INFO"][database_name]["COLUMN_FAMILY_MAP"]

    def get_loader_kwargs(self, database_name: str) -> dict:
        kwargs = dict(
            loader_name=database_name,
            sample_path=self.get_sample_path(database_name),
            column_family_map=self.get_column_family_map(database_name),
        )
        if database_name != "sisceab":
            kwargs.update(dict(sisceab_sample_path=self.get_sample_path("sisceab")))
        return kwargs
