from typing import Dict, List

from database_dumper.dumpers.bimtra import BimtraDumper
from database_dumper.dumpers.cat62 import Cat62Dumper
from database_dumper.dumpers.seta import SetaDumper
from database_dumper.dumpers.sisceab import SisceabDumper
from database_dumper.dumpers.tatic import TaticDumper
from database_dumper.dumpers.vra import VraDumper

from database_dumper.interface import Dumper


class DumperMiddleware:
    DUMPER_MAP = {
        "bimtra": BimtraDumper,
        "cat62": Cat62Dumper,
        "seta": SetaDumper,
        "sisceab": SisceabDumper,
        "tatic": TaticDumper,
        "vra": VraDumper,
    }
    def get_dumper(self, dumper_name: str, sample_path: str, column_family_map: Dict[str, List[str]], **kwargs) -> Dumper:
        return self.DUMPER_MAP[dumper_name](sample_path, column_family_map, **kwargs)
