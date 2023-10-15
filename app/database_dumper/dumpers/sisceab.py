import happybase
import numpy as np
import pandas as pd

from functools import cached_property
from database_dumper.interface import Dumper


class SisceabDumper(Dumper):
    @cached_property
    def _id_voo(self) -> pd.Series:
        return self.sample['co_indicativo']

    @cached_property
    def _horario_partida(self) -> pd.Series:
        return pd.to_datetime(self.sample['dh_inicio'])

    def _get_row_key(self, row: int) -> pd.Series:
        return self._id_voo.loc[row] + " " +  self._horario_partida.loc[row].strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _encode(object):
        if isinstance(object, str):
            return object.encode()
        elif isinstance(object, np.int64):
            return str(object).encode()
        raise NotImplementedError(f"Encode method not implemented for object of type {type(object)}")

    def _get_column_values(self, row: int) -> dict:
        column_values = dict()
        for column_family, columns in self.column_family_map.items():
            column_values.update(
                {
                    self._encode(f'{column_family}:{column}'): self._encode(self.sample.loc[row, column])
                    for column in columns
                }
            )
        return column_values

    def dump(self, table: happybase.Table) -> None:
        with table.batch() as batch:
            rows = self.sample.index
            for row in rows:
                row_key = self._get_row_key(row)
                column_values = self._get_column_values(row)
                batch.put(row_key.encode(), column_values)
            batch.send()

