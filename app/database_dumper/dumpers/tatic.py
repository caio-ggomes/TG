import happybase
import pandas as pd

from typing import Dict, List
from database_dumper.interface import Dumper


class TaticDumper(Dumper):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]], vra_sample_path: str) -> None:
        super().__init__(sample_path, column_family_map)
        self.vra_sample = pd.read_csv(vra_sample_path, delimiter=";")

    def dump(self, table: happybase.Table) -> None:
        ...

