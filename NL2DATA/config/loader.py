"""Load configuration from YAML file."""

import yaml
from pathlib import Path
from typing import Any, Dict, Optional


def find_config_file() -> Path:
    """Find config.yaml file in config directory."""
    config_dir = Path(__file__).parent
    config_file = config_dir / "config.yaml"
    
    if not config_file.exists():
        raise FileNotFoundError(
            f"config.yaml not found at {config_file}. "
            f"Please create the configuration file."
        )
    
    return config_file


def load_config() -> Dict[str, Any]:
    """
    Load configuration from config.yaml.
    
    Returns:
        dict: Configuration dictionary
        
    Raises:
        FileNotFoundError: If config.yaml is not found
        yaml.YAMLError: If YAML parsing fails
    """
    config_file = find_config_file()
    
    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    
    return config


def get_config(section: Optional[str] = None) -> Any:
    """
    Get configuration value(s).
    
    Args:
        section: Optional section name (e.g., "openai", "logging")
                 If None, returns entire config
        
    Returns:
        Configuration value or dictionary
    """
    config = load_config()
    
    if section is None:
        return config
    
    return config.get(section, {})

