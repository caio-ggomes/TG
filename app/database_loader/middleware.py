from typing import Dict, List

from database_loader.loaders.bimtra import BimtraDatabaseLoader
from database_loader.loaders.cat62 import Cat62DatabaseLoader
from database_loader.loaders.seta import SetaDatabaseLoader
from database_loader.loaders.sisceab import SisceabDatabaseLoader
from database_loader.loaders.tatic import TaticDatabaseLoader
from database_loader.loaders.vra import VraDatabaseLoader

from database_loader.interface import DatabaseLoader


class DatabaseLoaderMiddleware:
    LOADER_MAP = {
        "bimtra": BimtraDatabaseLoader,
        "cat62": Cat62DatabaseLoader,
        "seta": SetaDatabaseLoader,
        "sisceab": SisceabDatabaseLoader,
        "tatic": TaticDatabaseLoader,
        "vra": VraDatabaseLoader,
    }
    def get_loader(self, loader_name: str, sample_path: str, column_family_map: Dict[str, List[str]], **kwargs) -> DatabaseLoader:
        return self.LOADER_MAP[loader_name](sample_path, column_family_map, **kwargs)
