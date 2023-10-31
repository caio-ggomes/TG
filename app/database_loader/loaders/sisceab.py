import happybase
import pandas as pd

from functools import cached_property
from database_loader.interface import DatabaseLoader


class SisceabDatabaseLoader(DatabaseLoader):
    @cached_property
    def _id_voo(self) -> pd.Series:
        return self.sample['co_indicativo']

    @cached_property
    def _horario_partida(self) -> pd.Series:
        return pd.to_datetime(self.sample['dh_inicio'])

    def _get_row_key(self, row: int) -> pd.Series:
        return self._id_voo.loc[row] + " " +  self._horario_partida.loc[row].strftime("%Y-%m-%d %H:%M:%S")

    def _get_column_values(self, row: int) -> dict:
        column_values = dict()
        for column_family, columns in self.column_family_map.items():
            for column in columns:
                value = self.sample.loc[row, column]
                if pd.notnull(value):
                    encoded_key = self._encode(f'{column_family}:{column}')
                    encoded_value = self._encode(value)
                    column_values[encoded_key] = encoded_value
        return column_values

    def load(self, table: happybase.Table) -> None:
        with table.batch() as batch:
            rows = self.sample.index
            for row in rows:
                row_key = self._get_row_key(row)
                column_values = self._get_column_values(row)
                batch.put(row_key.encode(), column_values)
            batch.send()

