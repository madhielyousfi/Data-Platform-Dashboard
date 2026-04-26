#!/usr/bin/env python3
"""
Data Transformation for Data Platform.
Applies transformations to data.
"""
import json
from pathlib import Path
from typing import Any, Dict, List, Callable, Optional
from utils.helpers import read_json, write_json, parse_date
from utils.logger import get_logger, log_start, log_end
from utils.validator import Validator


class DataTransformer:
    """Transform data between zones."""
    
    def __init__(self):
        self.logger = get_logger("transform")
    
    def apply_mapping(self, data: List[Dict], mapping: Dict[str, str]) -> List[Dict]:
        """Apply field mapping.
        
        Args:
            data: Input records
            mapping: {"source_field": "target_field"}
            
        Returns:
            Transformed records
        """
        result = []
        for row in data:
            new_row = {}
            for source, target in mapping.items():
                new_row[target] = row.get(source)
            result.append(new_row)
        return result
    
    def add_computed_fields(self, data: List[Dict], fields: Dict[str, Callable]) -> List[Dict]:
        """Add computed fields.
        
        Args:
            data: Input records
            fields: {"field_name": function(row) -> value}
            
        Returns:
            Records with new fields
        """
        result = []
        for row in data:
            new_row = row.copy()
            for field_name, func in fields.items():
                try:
                    new_row[field_name] = func(row)
                except Exception:
                    new_row[field_name] = None
            result.append(new_row)
        return result
    
    def filter_records(self, data: List[Dict], condition: Callable) -> List[Dict]:
        """Filter records by condition.
        
        Args:
            data: Input records
            condition: function(row) -> bool
            
        Returns:
            Filtered records
        """
        return [row for row in data if condition(row)]
    
    def join_data(self, left: List[Dict], right: List[Dict], key: str, how: str = "inner") -> List[Dict]:
        """Join two datasets.
        
        Args:
            left: Left dataset
            right: Right dataset  
            key: Join key field
            how: Join type (inner, left, right)
            
        Returns:
            Joined records
        """
        # Build lookup
        right_lookup = {row[key]: row for row in right if key in row}
        
        result = []
        for row in left:
            if key not in row:
                continue
            match = right_lookup.get(row[key])
            if match:
                merged = {**row, **match}
                result.append(merged)
            elif how == "left":
                result.append(row)
        
        return result
    
    def pivot_data(self, data: List[Dict], index: str, columns: str, values: str, aggfunc: str = "sum") -> List[Dict]:
        """Pivot data.
        
        Args:
            data: Input records
            index: Field for index
            columns: Field for columns
            values: Field for values
            aggfunc: Aggregation function
            
        Returns:
            Pivoted records
        """
        pivot = {}
        
        for row in data:
            idx = row.get(index)
            col = row.get(columns)
            val = row.get(values)
            
            if idx and col:
                key = (idx, col)
                if key not in pivot:
                    pivot[key] = []
                pivot[key].append(val)
        
        result = []
        for (idx, col), vals in pivot.items():
            row = {index: idx, columns: col}
            if aggfunc == "sum":
                row[values] = sum(v for v in vals if v)
            elif aggfunc == "avg":
                row[values] = sum(v for v in vals if v) / len(vals)
            elif aggfunc == "count":
                row[values] = len(vals)
            elif aggfunc == "max":
                row[values] = max(v for v in vals if v)
            result.append(row)
        
        return result


def transform_data(input_path: str, output_path: str, transforms: List[Dict]) -> dict:
    """Apply transformations to data.
    
    Args:
        input_path: Input file path
        output_path: Output file path
        transforms: List of transformation configs
        
    Returns:
        dict with status
    """
    log_start("Transform Data")
    
    try:
        data = read_json(input_path)
        
        transformer = DataTransformer()
        
        for transform in transforms:
            transform_type = transform.get("type")
            
            if transform_type == "map":
                mapping = transform.get("mapping", {})
                data = transformer.apply_mapping(data, mapping)
                
            elif transform_type == "computed":
                fields = transform.get("fields", {})
                data = transformer.add_computed_fields(data, fields)
                
            elif transform_type == "filter":
                # Simple filter - field equals value
                field = transform.get("field")
                value = transform.get("value")
                if field:
                    data = transformer.filter_records(
                        data, 
                        lambda r: r.get(field) == value
                    )
        
        write_json(data, output_path)
        log_end("Transform Data", len(data))
        
        return {"status": "success", "records": len(data)}
        
    except Exception as e:
        return {"status": "error", "error": str(e)}


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 3:
        print("Usage: python transform.py <input.json> <output.json>")
        sys.exit(1)
    
    result = transform_data(sys.argv[1], sys.argv[2], [])
    print(json.dumps(result, indent=2))