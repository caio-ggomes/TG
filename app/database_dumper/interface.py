import happybase
import pandas as pd
from abc import ABC, abstractmethod
from typing import Dict, List


class Dumper(ABC):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]]) -> None:
        self.sample = pd.read_csv(sample_path, delimiter=";")
        self.column_family_map = column_family_map

    @abstractmethod
    def dump(self, table: happybase.Table) -> None:
        raise NotImplementedError
