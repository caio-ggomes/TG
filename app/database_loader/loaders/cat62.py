from datetime import datetime
from typing import Dict, List

import happybase
import pandas as pd
from tqdm import tqdm

from database_loader.interface import SisceabParsingDatabaseLoader


class Cat62DatabaseLoader(SisceabParsingDatabaseLoader):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]], sisceab_sample_path: str) -> None:
        super().__init__(sample_path, column_family_map, sisceab_sample_path)
        self.sample['dt_radar'] = pd.to_datetime(self.sample['dt_radar'])

    def get_value(self, id_voo: str, timestamp: datetime, column: str):
        index = (
            (self.sample["ds_registration"] == id_voo)
            & (self.sample["dt_radar"] == timestamp)
        )
        slice = self.sample.loc[index, column]
        assert len(slice) <= 1, "Must return at most 1 value"
        value = (None if len(slice) == 0 else slice.values[0])
        return value

    def _get_column_values(self, id_voo: str, timestamp: datetime) -> dict:
        column_values = dict()
        for column_family, columns in self.column_family_map.items():
            column_values.update(
                {
                    self._encode(f'{column_family}:{column}'): (
                        self._encode(self.get_value(id_voo, timestamp, column))
                    )
                    for column in columns
                }
            )
        return column_values

    def load(self, table: happybase.Table) -> None:
        for timestamp in tqdm(self.sample['dt_radar'].drop_duplicates()):
            sisceab_sample_slice = self.sisceab_sample[
                (self._horario_inicio <= timestamp)
                & (self._horario_fim >= timestamp)
            ]
            with table.batch(timestamp=int(timestamp.timestamp())) as batch:
                for row in sisceab_sample_slice.index:
                    id_voo = self._id_voo.loc[row]
                    horario_inicio = self._horario_inicio.loc[row]
                    horario_fim = self._horario_fim.loc[row]

                    row_key = self._get_row_key(id_voo, horario_inicio, horario_fim)
                    column_values = self._get_column_values(id_voo, timestamp)

                    batch.put(row_key, column_values)

                batch.send()
