#!/usr/bin/env python3
"""
Helper utilities for Data Platform.
Common functions used across the platform.
"""
import csv
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional


def read_csv(file_path: str) -> List[Dict]:
    """Read CSV file and return list of dictionaries."""
    rows = []
    with open(file_path, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(dict(row))
    return rows


def write_csv(data: List[Dict], file_path: str):
    """Write list of dictionaries to CSV file."""
    if not data:
        return
    
    fieldnames = list(data[0].keys())
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    
    with open(file_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)


def read_json(file_path: str) -> List[Dict]:
    """Read JSON file and return list of dictionaries."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def write_json(data: Any, file_path: str, indent: int = 2):
    """Write data to JSON file."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8') as f:
        if isinstance(data, (list, dict)):
            json.dump(data, f, indent=indent, default=str)
        else:
            json.dump(data, f, indent=indent)


def read_yaml(file_path: str) -> Dict:
    """Read YAML configuration file."""
    import yaml
    with open(file_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def get_env(key: str, default: str = None) -> str:
    """Get environment variable with optional default."""
    return os.environ.get(key, default)


def ensure_dir(path: str):
    """Ensure directory exists."""
    Path(path).mkdir(parents=True, exist_ok=True)


def get_timestamp() -> str:
    """Get current timestamp as ISO string."""
    return datetime.now().isoformat()


def parse_date(date_str: str) -> Optional[datetime]:
    """Parse date string with multiple format attempts."""
    formats = [
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%d-%m-%Y",
        "%Y/%m/%d",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    return None


def format_currency(value: float) -> str:
    """Format float as currency."""
    return f"${value:,.2f}"


def format_number(value: float) -> str:
    """Format number with thousand separators."""
    return f"{value:,.0f}"


def truncate(text: str, max_length: int = 50, suffix: str = "...") -> str:
    """Truncate text to maximum length."""
    if len(text) <= max_length:
        return text
    return text[:max_length - len(suffix)] + suffix


def chunk_list(items: List, chunk_size: int) -> List[List]:
    """Split list into chunks."""
    return [items[i:i + chunk_size] for i in range(0, len(items), chunk_size)]


def merge_dicts(*dicts: Dict) -> Dict:
    """Merge multiple dictionaries."""
    result = {}
    for d in dicts:
        result.update(d)
    return result


def flatten_dict(d: Dict, parent_key: str = '', sep: str = '_') -> Dict:
    """Flatten nested dictionary."""
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)


def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """Safely divide, returning default if denominator is zero."""
    if denominator == 0:
        return default
    return numerator / denominator


def percentage(part: float, whole: float, decimals: int = 1) -> float:
    """Calculate percentage."""
    return round(safe_divide(part * 100, whole), decimals)


# File path helpers
BASE_DIR = Path("/home/dados/Documents/Data Platform")


def get_data_path(*parts: str) -> Path:
    """Get path in data directory."""
    return BASE_DIR / "data-platform" / "lake" / Path(*parts)


def get_config_path(*parts: str) -> Path:
    """Get path in config directory."""
    return BASE_DIR / "data-platform" / "config" / Path(*parts)


def get_warehouse_path(*parts: str) -> Path:
    """Get path in warehouse directory."""
    return BASE_DIR / "data-platform" / "warehouse" / Path(*parts)


if __name__ == "__main__":
    # Test helpers
    print("Testing helpers...")
    print(f"Timestamp: {get_timestamp()}")
    print(f"Chunk [1,2,3,4,5]: {chunk_list([1,2,3,4,5], 2)}")