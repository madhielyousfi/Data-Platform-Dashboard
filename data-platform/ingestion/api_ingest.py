#!/usr/bin/env python3
"""
API Ingestion for Data Platform.
Fetches data from external APIs.
"""
import json
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError
from utils.helpers import write_json, ensure_dir
from utils.logger import get_logger, log_start, log_end


class APIIngest:
    """Ingest data from external APIs."""
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.logger = get_logger("api_ingest")
    
    def fetch_json(self, url: str, headers: dict = None) -> dict:
        """Fetch JSON from URL.
        
        Args:
            url: API endpoint URL
            headers: Optional HTTP headers
            
        Returns:
            dict with data or error
        """
        log_start(f"API Ingest: {url}")
        
        try:
            # Build request
            request = Request(url)
            request.add_header("Accept", "application/json")
            
            if headers:
                for key, value in headers.items():
                    request.add_header(key, value)
            
            # Make request
            with urlopen(request, timeout=30) as response:
                data = json.loads(response.read().decode())
                log_end(f"API Ingest: {url}")
                return {"status": "success", "data": data}
                
        except URLError as e:
            self.logger.error(f"URL Error: {e}")
            return {"status": "error", "error": str(e)}
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON Parse Error: {e}")
            return {"status": "error", "error": str(e)}
        except Exception as e:
            self.logger.error(f"Error: {e}")
            return {"status": "error", "error": str(e)}
    
    def fetch_and_save(self, url: str, output_name: str, output_zone: str = "bronze", headers: dict = None) -> dict:
        """Fetch from API and save to lake.
        
        Args:
            url: API endpoint
            output_name: Name for output file
            output_zone: Target zone
            headers: Optional HTTP headers
            
        Returns:
            dict with status
        """
        result = self.fetch_json(url, headers)
        
        if result.get("status") == "success":
            base_path = Path("/home/dados/Documents/Data Platform/data-platform/lake")
            output_path = base_path / output_zone / f"{output_name}.json"
            ensure_dir(str(output_path.parent))
            
            data = result.get("data", [])
            if not isinstance(data, list):
                data = [data]
            
            write_json(data, str(output_path))
            result["output"] = str(output_path)
            result["records"] = len(data)
        
        return result


# Example API endpoints to try
EXAMPLE_APIS = {
    "products": "https://fakestoreapi.com/products",
    "carts": "https://fakestoreapi.com/carts",
    "users": "https://fakestoreapi.com/users",
}


def main():
    """CLI entry point."""
    import sys
    
    if len(sys.argv) < 3:
        print(f"Usage: python -m ingestion.api_ingest <endpoint> <output_name>")
        print(f"Example APIs: {list(EXAMPLE_APIS.keys())}")
        sys.exit(1)
    
    endpoint = sys.argv[1]
    output_name = sys.argv[2]
    output_zone = sys.argv[3] if len(sys.argv) > 3 else "bronze"
    
    # Build URL
    url = endpoint if endpoint.startswith("http") else EXAMPLE_APIS.get(endpoint, "")
    
    if not url:
        print(f"Unknown endpoint: {endpoint}")
        sys.exit(1)
    
    ingest = APIIngest()
    result = ingest.fetch_and_save(url, output_name, output_zone)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()