#!/usr/bin/env python3
"""
FastAPI application for Data Platform.
REST API to serve data from the warehouse.
"""
import os
import sys
from pathlib import Path

# Add to path
sys.path.insert(0, str(Path(__file__).parent.parent))

BASE_PATH = Path("/home/dados/Documents/Data Platform")


def get_db_connection():
    """Get database connection."""
    # Try PostgreSQL first
    try:
        from sqlalchemy import create_engine, text
        import yaml
        
        config_path = BASE_PATH / "data-platform" / "config" / "config.yaml"
        if config_path.exists():
            with open(config_path) as f:
                config = yaml.safe_load(f)
            
            db = config.get("database", {})
            host = os.environ.get("DB_HOST", db.get("host", "localhost"))
            port = os.environ.get("DB_PORT", str(db.get("port", "5432")))
            name = os.environ.get("DB_NAME", db.get("name", "datawarehouse"))
            user = os.environ.get("DB_USER", db.get("user", "postgres"))
            password = os.environ.get("DB_PASSWORD", db.get("password", "postgres"))
            
            conn_str = f"postgresql://{user}:{password}@{host}:{port}/{name}"
            return create_engine(conn_str)
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
    
    # Fallback to SQLite
    sqlite_path = BASE_PATH / "data-platform" / "warehouse" / "datawarehouse.db"
    if sqlite_path.exists():
        import sqlite3
        conn = sqlite3.connect(str(sqlite_path))
        conn.row_factory = sqlite3.Row
        return conn
    
    return None


def query_db(sql: str):
    """Execute query and return results."""
    conn = get_db_connection()
    if conn is None:
        return {"error": "No database connection"}
    
    try:
        # Check if SQLAlchemy or sqlite3 connection
        if hasattr(conn, 'execute'):
            # sqlite3
            cursor = conn.cursor()
            cursor.execute(sql)
            rows = cursor.fetchall()
            conn.close()
            return [dict(r) for r in rows]
        else:
            # SQLAlchemy
            with conn.connect() as c:
                result = c.execute(text(sql))
                return [dict(r) for r in result]
    except Exception as e:
        return {"error": str(e)}


# FastAPI app or fallback to simple HTTP
try:
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    print("FastAPI not installed. Using simple HTTP fallback.")


if HAS_FASTAPI:
    app = FastAPI(
        title="Data Platform API",
        description="REST API for Data Platform",
        version="1.0.0"
    )
    
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    @app.get("/")
    def root():
        return {"message": "Data Platform API", "version": "1.0.0"}
    
    @app.get("/api/health")
    def health():
        return {"status": "healthy"}
    
    @app.get("/api/sales")
    def get_sales(limit: int = 100):
        sql = f"SELECT * FROM fact_sales ORDER BY order_date DESC LIMIT {limit}"
        return query_db(sql)
    
    @app.get("/api/products")
    def get_products(limit: int = 50):
        sql = f"SELECT * FROM dim_products ORDER BY product_id LIMIT {limit}"
        return query_db(sql)
    
    @app.get("/api/customers")
    def get_customers(limit: int = 50):
        sql = f"SELECT * FROM dim_customers ORDER BY customer_id LIMIT {limit}"
        return query_db(sql)
    
    @app.get("/api/kpis/category")
    def get_kpis_category():
        sql = "SELECT * FROM kpi_category ORDER BY total_sales DESC"
        return query_db(sql)
    
    @app.get("/api/kpis/date")
    def get_kpis_date():
        sql = "SELECT * FROM kpi_date ORDER BY year_month DESC"
        return query_db(sql)
    
    @app.get("/api/kpis/product")
    def get_kpis_product(limit: int = 10):
        sql = f"SELECT * FROM kpi_product ORDER BY total_sales DESC LIMIT {limit}"
        return query_db(sql)
    
    @app.get("/api/kpis/customer")
    def get_kpis_customer(limit: int = 10):
        sql = f"SELECT * FROM kpi_customer ORDER BY total_sales DESC LIMIT {limit}"
        return query_db(sql)
    
    @app.get("/api/stats")
    def get_stats():
        results = {
            "customers": query_db("SELECT COUNT(*) as cnt FROM dim_customers"),
            "products": query_db("SELECT COUNT(*) as cnt FROM dim_products"),
            "sales": query_db("SELECT COUNT(*) as cnt FROM fact_sales"),
        }
        return results
    
    def main():
        import uvicorn
        config_path = BASE_PATH / "data-platform" / "config" / "config.yaml"
        
        host = "0.0.0.0"
        port = 8000
        
        if config_path.exists():
            try:
                import yaml
                with open(config_path) as f:
                    cfg = yaml.safe_load(f)
                    api_cfg = cfg.get("api", {})
                    host = api_cfg.get("host", "0.0.0.0")
                    port = api_cfg.get("port", 8000)
            except:
                pass
        
        print(f"Starting API server on http://{host}:{port}")
        uvicorn.run(app, host=host, port=port)
    
    if __name__ == "__main__":
        main()

else:
    # Simple fallback using http.server
    import http.server
    import json
    
    class SimpleAPIHandler(http.server.BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass
        
        def do_GET(self):
            if self.path == "/" or self.path == "/api":
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps({"message": "Data Platform API"}).encode())
            elif self.path == "/api/sales":
                rows = query_db("SELECT * FROM fact_sales LIMIT 100")
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(rows).encode())
            elif self.path == "/api/products":
                rows = query_db("SELECT * FROM dim_products LIMIT 50")
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(rows).encode())
            elif self.path == "/api/customers":
                rows = query_db("SELECT * FROM dim_customers LIMIT 50")
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(rows).encode())
            elif self.path == "/api/kpis/category":
                rows = query_db("SELECT * FROM kpi_category ORDER BY total_sales DESC")
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(rows).encode())
            else:
                self.send_response(404)
                self.end_headers()
    
    def main():
        print("Starting API server on http://0.0.0.0:8000")
        server = http.server.HTTPServer(("0.0.0.0", 8000), SimpleAPIHandler)
        server.serve_forever()
    
    if __name__ == "__main__":
        main()