from datetime import datetime
from typing import Dict, List
import pandas as pd

from database_loader.interface import SingleCellValueSisceabParsingDatabaseLoader


class TaticDatabaseLoader(SingleCellValueSisceabParsingDatabaseLoader):
    def __init__(self, sample_path: str, column_family_map: Dict[str, List[str]], sisceab_sample_path: str) -> None:
        self.sample = pd.read_csv(sample_path, delimiter=";").sort_values(['dep', 'arr'])
        self.column_family_map = column_family_map
        self.sisceab_sample = pd.read_csv(sisceab_sample_path, delimiter=";")

    def _get_departure_value(self, id_voo: str, horario_inicio: datetime, horario_fim: datetime, column: str):
        same_flight_id = (self.sample['callsign'] == id_voo)
        departure_within_time = (
            (self.sample['eventtype'] == "DEP")
            & (pd.to_datetime(self.sample['dep']) >= horario_inicio)
            & (pd.to_datetime(self.sample['dep']) < horario_fim)
        )
        departure_row_match_index = (same_flight_id & departure_within_time)

        departure_values = self.sample.loc[departure_row_match_index, column]
        departure_value = (None if len(departure_values) == 0 else departure_values.values[0])

        return departure_value

    def _get_arrival_value(self, id_voo: str, horario_inicio: datetime, horario_fim: datetime, column: str):
        same_flight_id = (self.sample['callsign'] == id_voo)
        arrival_within_time = (
            (self.sample['eventtype'] == "ARR")
            & (pd.to_datetime(self.sample['arr']) > horario_inicio)
            & (pd.to_datetime(self.sample['arr']) <= horario_fim)
        )
        arrival_row_match_index = (same_flight_id & arrival_within_time)

        arrival_values = self.sample.loc[arrival_row_match_index, column]
        arrival_value = (None if len(arrival_values) == 0 else arrival_values.values[-1])

        return arrival_value

    def get_value(self, id_voo: str, horario_inicio: datetime, horario_fim: datetime, column: str):
        if column in ['id', 'flowid', 'createdat', 'locality', 'runway']:
            raise ValueError(f'Cannot resolve value for parameters {id_voo}, {horario_inicio}, {horario_fim}, {column}')
        if column in ['eobt', 'wpush', 'cpush',	'wtaxi', 'taxi', 'hold', 'crwy', 'cdep', 'dep']:
            return self._get_departure_value(id_voo, horario_inicio, horario_fim, column)
        if column in ['eta', 'arr', 'cpos']:
            return self._get_arrival_value(id_voo, horario_inicio, horario_fim, column)
        return self._get_departure_value(id_voo, horario_inicio, horario_fim, column)
