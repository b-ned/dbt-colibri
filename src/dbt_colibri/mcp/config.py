"""
MCP Configuration Management

Handles storing and retrieving MCP server configuration.
"""

import json
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, asdict


@dataclass
class MCPConfig:
    """MCP server configuration"""
    manifest_path: str
    is_remote: bool = False
    cache_dir: Optional[str] = None

    @classmethod
    def get_config_path(cls) -> Path:
        """Get the path to the config file"""
        config_dir = Path.home() / ".config" / "dbt-colibri"
        config_dir.mkdir(parents=True, exist_ok=True)
        return config_dir / "mcp-config.json"

    @classmethod
    def load(cls) -> Optional["MCPConfig"]:
        """Load configuration from file"""
        config_path = cls.get_config_path()
        if not config_path.exists():
            return None

        try:
            with open(config_path) as f:
                data = json.load(f)
            return cls(**data)
        except (json.JSONDecodeError, TypeError):
            return None

    def save(self):
        """Save configuration to file"""
        config_path = self.get_config_path()
        with open(config_path, 'w') as f:
            json.dump(asdict(self), f, indent=2)

    @classmethod
    def delete(cls):
        """Delete the configuration file"""
        config_path = cls.get_config_path()
        if config_path.exists():
            config_path.unlink()

    def get_manifest_path(self) -> str:
        """
        Get the manifest path, downloading if remote.

        Returns:
            Path to the local manifest file
        """
        if not self.is_remote:
            return self.manifest_path

        # Import here to avoid requiring dependencies if not needed
        from .remote import download_manifest
        
        cache_dir = self.cache_dir or str(Path.home() / ".cache" / "dbt-colibri")
        return download_manifest(self.manifest_path, cache_dir)

    @staticmethod
    def is_remote_path(path: str) -> bool:
        """Check if a path is a remote URL"""
        return path.startswith(('http://', 'https://', 's3://', 'gs://'))


