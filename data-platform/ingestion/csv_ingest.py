#!/usr/bin/env python3
"""
CSV Ingestion for Data Platform.
Loads data from CSV files to the lake.
"""
import csv
import json
from pathlib import Path
from utils.helpers import read_csv, write_json, ensure_dir
from utils.logger import get_logger, log_start, log_end


class CSVIngest:
    """Ingest data from CSV files."""
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.logger = get_logger("csv_ingest")
    
    def load_file(self, file_path: str, output_zone: str = "bronze") -> dict:
        """Load CSV file and save to lake zone.
        
        Args:
            file_path: Path to CSV file
            output_zone: Target zone (bronze, silver, gold)
            
        Returns:
            dict with record count and status
        """
        log_start(f"CSV Ingest: {file_path}")
        
        try:
            # Read CSV
            rows = read_csv(file_path)
            record_count = len(rows)
            
            # Determine output path
            file_name = Path(file_path).stem
            base_path = Path("/home/dados/Documents/Data Platform/data-platform/lake")
            output_path = base_path / output_zone / f"{file_name}.json"
            ensure_dir(str(output_path.parent))
            
            # Write to JSON
            write_json(rows, str(output_path))
            
            log_end(f"CSV Ingest: {file_path}", record_count)
            
            return {
                "status": "success",
                "records": record_count,
                "output": str(output_path)
            }
            
        except Exception as e:
            self.logger.error(f"Error: {e}")
            return {"status": "error", "error": str(e)}
    
    def load_directory(self, dir_path: str, output_zone: str = "bronze") -> list:
        """Load all CSV files from directory.
        
        Args:
            dir_path: Directory containing CSV files
            output_zone: Target zone
            
        Returns:
            list of results
        """
        results = []
        dir_path = Path(dir_path)
        
        for csv_file in dir_path.glob("*.csv"):
            result = self.load_file(str(csv_file), output_zone)
            results.append(result)
        
        return results


def main():
    """CLI entry point."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python -m ingestion.csv_ingest <csv_file> [bronze|silver|gold]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    output_zone = sys.argv[2] if len(sys.argv) > 2 else "bronze"
    
    ingest = CSVIngest()
    result = ingest.load_file(file_path, output_zone)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()