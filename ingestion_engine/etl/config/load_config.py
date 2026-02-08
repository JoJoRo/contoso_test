import os
import json
from typing import Any, Dict, List, Optional, Sequence, Tuple


class LoadConfig:
    @staticmethod
    def load_raw_hub_config(config_rel_path: str = "../config/raw_hub.json") -> Dict[str, Any]:
        """
        Loads raw_hub.json from repo structure:
          repo_root/
            config/raw_hub.json
            utils/utils.py

        By default, it resolves relative to this file.
        """
        here = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.normpath(os.path.join(here, config_rel_path))
        with open(config_path, "r") as f:
            return json.load(f)

    @staticmethod
    def get_landing_path(cfg: Dict[str, Any]) -> str:
        """
        Returns landing base path from config. Expects key 'landing' at top-level.
        Example: "abfss://landing@contosostohierachical.dfs.core.windows.net"
        """
        if "landing" not in cfg or not str(cfg["landing"]).strip():
            raise KeyError("Config missing non-empty 'landing' key.")
        return str(cfg["landing"]).rstrip("/")

    @staticmethod
    def build_source_path(landing_path: str, folder: Optional[str], file_name: str) -> str:
        """
        Builds full path to a file under landing.
        """
        if not file_name or not str(file_name).strip():
            raise ValueError("file_name must be non-empty.")
        landing_path = landing_path.rstrip("/")

        folder_clean = (folder or "").strip().strip("/")
        if folder_clean:
            return f"{landing_path}/{folder_clean}/{file_name}"
        return f"{landing_path}/{file_name}"
