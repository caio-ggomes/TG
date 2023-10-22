import happybase
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from datetime import datetime
from functools import cached_property
from typing import Dict, List


class DatabaseLoader(ABC):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]]) -> None:
        self.sample = pd.read_csv(sample_path, sep=";")
        self.column_family_map = column_family_map

    @abstractmethod
    def load(self, table: happybase.Table) -> None:
        raise NotImplementedError


class SisceabParsingDatabaseLoader(DatabaseLoader):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]], sisceab_sample_path: str) -> None:
        super().__init__(sample_path, column_family_map)
        self.sisceab_sample = pd.read_csv(sisceab_sample_path, sep=";")

    @cached_property
    def _id_voo(self) -> pd.Series:
        return self.sisceab_sample['co_indicativo']

    @cached_property
    def _horario_inicio(self) -> pd.Series:
        return pd.to_datetime(self.sisceab_sample['dh_inicio'])

    @cached_property
    def _horario_fim(self) -> pd.Series:
        return pd.to_datetime(self.sisceab_sample['dh_fim'])

    def _get_row_key(self, id_voo: str, horario_inicio: datetime, horario_fim: datetime) -> bytes:
        return (id_voo + " " +  horario_inicio.strftime("%Y-%m-%d %H:%M:%S")).encode()

    @staticmethod
    def _encode(object):
        if isinstance(object, str):
            return object.encode()
        if isinstance(object, np.int64):
            return str(object).encode()
        if object is None:
            return bytes()
        if np.isnan(object):
            return bytes()
        raise NotImplementedError(f"Encode method not implemented for object of type {type(object)}")


class SingleCellValueSisceabParsingDatabaseLoader(SisceabParsingDatabaseLoader):
    BATCH_SIZE = 500

    @abstractmethod
    def get_value(self, id_voo: str, horario_inicio: datetime, horario_fim: datetime, column: str):
        raise NotImplementedError

    def _get_column_values(self, id_voo: str, horario_inicio: datetime, horario_fim: datetime) -> dict:
        column_values = dict()
        for column_family, columns in self.column_family_map.items():
            column_values.update(
                {
                    self._encode(f'{column_family}:{column}'): (
                        self._encode(self.get_value(id_voo, horario_inicio, horario_fim, column))
                    )
                    for column in columns
                }
            )
        return column_values

    def load(self, table: happybase.Table) -> None:
        rows = self.sisceab_sample.index
        sliced_rows_indexes = [
            rows[counter:min(counter + self.BATCH_SIZE, len(rows) - 1)]
            for counter in range(0, len(rows), self.BATCH_SIZE)
        ]
        for sliced_rows_index in sliced_rows_indexes:
            with table.batch() as batch:
                for row in sliced_rows_index:
                    id_voo = self._id_voo.loc[row]
                    horario_inicio = self._horario_inicio.loc[row]
                    horario_fim = self._horario_fim.loc[row]

                    row_key = self._get_row_key(id_voo, horario_inicio, horario_fim)
                    column_values = self._get_column_values(id_voo, horario_inicio, horario_fim)

                    batch.put(row_key, column_values)

                batch.send()
