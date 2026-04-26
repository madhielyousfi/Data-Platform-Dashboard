#!/usr/bin/env python3
"""
Data Validator utility for Data Platform.
Validates data quality: nulls, schema, duplicates.
"""
import json
from pathlib import Path
from typing import Any


class Validator:
    """Data quality validator."""
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.errors = []
        self.warnings = []
    
    def validate_required_columns(self, df: list, required_columns: list) -> bool:
        """Check if all required columns exist."""
        if not df:
            self.errors.append("Data is empty")
            return False
        
        if not required_columns:
            return True
        
        columns = set(df[0].keys()) if df else set()
        missing = set(required_columns) - columns
        
        if missing:
            self.errors.append(f"Missing columns: {missing}")
            return False
        return True
    
    def validate_nulls(self, df: list, required_fields: list) -> tuple:
        """Check for null values in required fields.
        Returns (is_valid, null_count_by_field)
        """
        null_counts = {}
        is_valid = True
        
        for field in required_fields:
            null_count = sum(1 for row in df if not row.get(field))
            if null_count > 0:
                null_counts[field] = null_count
                self.warnings.append(f"Null values in {field}: {null_count}")
                is_valid = False
        
        return is_valid, null_counts
    
    def validate_duplicates(self, df: list, key_field: str) -> tuple:
        """Check for duplicates based on key field.
        Returns (is_valid, duplicate_count)
        """
        if not df:
            return True, 0
        
        keys = [row.get(key_field) for row in df]
        unique_keys = set(keys)
        duplicate_count = len(keys) - len(unique_keys)
        
        if duplicate_count > 0:
            self.warnings.append(f"Duplicates in {key_field}: {duplicate_count}")
            return False, duplicate_count
        
        return True, 0
    
    def validate_data_types(self, df: list, type_specs: dict) -> bool:
        """Validate data types of fields.
        type_specs: {"field": "type"} where type is int, float, str, date
        """
        is_valid = True
        
        for row in df:
            for field, expected_type in type_specs.items():
                value = row.get(field)
                if value is None:
                    continue
                    
                if expected_type == "int":
                    try:
                        int(value)
                    except (ValueError, TypeError):
                        self.errors.append(f"Invalid int in {field}: {value}")
                        is_valid = False
                        
                elif expected_type == "float":
                    try:
                        float(value)
                    except (ValueError, TypeError):
                        self.errors.append(f"Invalid float in {field}: {value}")
                        is_valid = False
        
        return is_valid
    
    def validate_range(self, df: list, field: str, min_val: float = None, max_val: float = None) -> tuple:
        """Validate numeric values are within range."""
        if not min_val and not max_val:
            return True, 0
        
        out_of_range = 0
        for row in df:
            value = row.get(field)
            if value is None:
                continue
            try:
                val = float(value)
                if min_val is not None and val < min_val:
                    out_of_range += 1
                if max_val is not None and val > max_val:
                    out_of_range += 1
            except (ValueError, TypeError):
                pass
        
        if out_of_range > 0:
            self.warnings.append(f"Values out of range for {field}: {out_of_range}")
            return False, out_of_range
        
        return True, 0
    
    def get_report(self) -> dict:
        """Get validation report."""
        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "is_valid": len(self.errors) == 0
        }
    
    def reset(self):
        """Reset errors and warnings."""
        self.errors = []
        self.warnings = []


def validate_data(df: list, schema: dict) -> dict:
    """Quick validation function."""
    validator = Validator()
    
    # Check required columns
    required = schema.get("required_columns", [])
    if required:
        validator.validate_required_columns(df, required)
    
    # Check nulls
    null_fields = schema.get("not_null", [])
    if null_fields:
        validator.validate_nulls(df, null_fields)
    
    # Check duplicates
    key_field = schema.get("unique_key")
    if key_field:
        validator.validate_duplicates(df, key_field)
    
    return validator.get_report()


if __name__ == "__main__":
    # Test validator
    test_data = [
        {"order_id": "1", "sales": 100, "customer_id": "C1"},
        {"order_id": "2", "sales": 200, "customer_id": "C2"},
        {"order_id": "1", "sales": 150, "customer_id": "C3"},  # duplicate
    ]
    
    schema = {
        "required_columns": ["order_id", "sales"],
        "not_null": ["order_id", "sales"],
        "unique_key": "order_id"
    }
    
    report = validate_data(test_data, schema)
    print(json.dumps(report, indent=2))