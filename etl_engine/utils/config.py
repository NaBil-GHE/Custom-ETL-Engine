import yaml
import os
import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)


class ETLConfig:
    """
    Loads and validates the ETL configuration from a YAML file.
    Provides typed access to config sections.
    """

    def __init__(self, config_path: str = "config/etl_config.yaml"):
        self.config_path = config_path
        self._config: Dict[str, Any] = {}
        self._load()

    def _load(self):
        if not os.path.exists(self.config_path):
            logger.warning(f"Config file not found: {self.config_path}. Using defaults.")
            self._config = self._defaults()
            return
        with open(self.config_path, "r", encoding="utf-8") as f:
            self._config = yaml.safe_load(f) or {}
        logger.info(f"Config loaded from: {self.config_path}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get a top-level config value."""
        return self._config.get(key, default)

    def section(self, name: str) -> Dict[str, Any]:
        """Get a config section as a dict."""
        return self._config.get(name, {})

    @property
    def sources(self) -> list:
        return self._config.get("sources", [])

    @property
    def cleaning(self) -> Dict[str, Any]:
        return self._config.get("cleaning", {})

    @property
    def deduplication(self) -> Dict[str, Any]:
        return self._config.get("deduplication", {})

    @property
    def validation_rules(self) -> Dict[str, Any]:
        return self._config.get("validation_rules", {})

    @property
    def output(self) -> Dict[str, Any]:
        return self._config.get("output", {})

    @staticmethod
    def _defaults() -> Dict[str, Any]:
        return {
            "sources": [],
            "cleaning": {"null_strategy": "fill"},
            "deduplication": {"keep": "first"},
            "validation_rules": {},
            "output": {
                "db_path": "data/processed/etl_output.db",
                "table_name": "etl_results",
                "export_csv": True,
                "export_json": False,
            },
        }
