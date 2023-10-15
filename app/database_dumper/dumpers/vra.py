import happybase
import pandas as pd

from typing import Dict, List
from database_dumper.interface import Dumper


class VraDumper(Dumper):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]], sisceab_sample_path: str) -> None:
        super().__init__(sample_path, column_family_map)
        self.sisceab_sample = pd.read_csv(sisceab_sample_path, delimiter=";")

    def dump(self, table: happybase.Table) -> None:
        ...
