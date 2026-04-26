#!/usr/bin/env python3
"""
Data Platform Dashboard - Simple Web UI
For upgraded data-platform with lake structure.
"""
import http.server
import json
import sqlite3
from datetime import datetime
from pathlib import Path
from html import escape

BASE_PATH = Path("/home/dados/Documents/Data Platform")
DB_PATH = BASE_PATH / "data-platform" / "warehouse" / "datawarehouse.db"
LAKE_PATH = BASE_PATH / "data-platform" / "lake"


def get_db():
    """Get SQLite database connection."""
    if not DB_PATH.exists():
        return None
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def query_db(sql):
    """Execute query and return results."""
    conn = get_db()
    if conn is None:
        return []
    try:
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except sqlite3.Error as e:
        print(f"Query error: {e}")
        return []


def get_stats():
    """Get dashboard statistics."""
    stats = {"customers": 0, "products": 0, "sales": 0, "total_sales": 0, "total_profit": 0}
    
    conn = get_db()
    if conn:
        cur = conn.cursor()
        try:
            cur.execute("SELECT COUNT(*) FROM dim_customers")
            stats["customers"] = cur.fetchone()[0]
        except:
            pass
        try:
            cur.execute("SELECT COUNT(*) FROM dim_products")
            stats["products"] = cur.fetchone()[0]
        except:
            pass
        try:
            cur.execute("SELECT COUNT(*) FROM fact_sales")
            stats["sales"] = cur.fetchone()[0]
        except:
            pass
        try:
            cur.execute("SELECT SUM(total_sales) FROM kpi_category")
            stats["total_sales"] = cur.fetchone()[0] or 0
        except:
            pass
        try:
            cur.execute("SELECT SUM(total_profit) FROM kpi_category")
            stats["total_profit"] = cur.fetchone()[0] or 0
        except:
            pass
        conn.close()
    
    return stats


def table_html(rows, cols=None):
    """Render rows as HTML table."""
    if not rows:
        return "<p>No data</p>"
    
    if cols is None:
        cols = list(rows[0].keys())
    
    html = '<table class="data-table">\n<thead><tr>'
    for col in cols:
        html += f"<th>{escape(col)}</th>"
    html += "</tr></thead>\n<tbody>\n"
    
    for row in rows:
        html += "<tr>"
        for col in cols:
            val = row.get(col, "")
            if val is None:
                val = ""
            html += f"<td>{escape(str(val))}</td>"
        html += "</tr>\n"
    html += "</tbody>\n</table>"
    return html


CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; background: #0f1419; color: #e7e9ea; min-height: 100vh; }
.header { background: linear-gradient(135deg, #1a1a2e, #16213e); padding: 20px 30px; border-bottom: 1px solid #2d333b; }
.header h1 { font-size: 24px; font-weight: 700; color: #fff; }
.header p { color: #8b949e; font-size: 14px; margin-top: 4px; }
.nav { background: #161b22; padding: 0 30px; display: flex; gap: 0; border-bottom: 1px solid #2d333b; }
.nav a { display: block; padding: 16px 20px; color: #8b949e; text-decoration: none; border-bottom: 2px solid transparent; }
.nav a:hover { color: #fff; background: rgba(255,255,255,0.05); }
.nav a.active { color: #fff; border-bottom-color: #f97316; }
.container { padding: 30px; max-width: 1400px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin-bottom: 30px; }
.stat-box { background: #161b22; border: 1px solid #2d333b; border-radius: 12px; padding: 24px; text-align: center; }
.stat-box .label { color: #8b949e; font-size: 13px; text-transform: uppercase; letter-spacing: 0.5px; }
.stat-box .value { font-size: 36px; font-weight: 700; color: #fff; margin: 10px 0; }
.stat-box .sub { color: #22c55e; font-size: 14px; }
.card { background: #161b22; border: 1px solid #2d333b; border-radius: 12px; padding: 24px; margin-bottom: 24px; }
.card h2 { font-size: 18px; font-weight: 600; margin-bottom: 16px; padding-bottom: 12px; border-bottom: 1px solid #2d333b; }
.data-table { width: 100%; border-collapse: collapse; }
.data-table th { background: #21262d; color: #8b949e; padding: 12px; text-align: left; font-weight: 500; font-size: 13px; }
.data-table td { padding: 12px; border-bottom: 1px solid #21262d; font-size: 14px; }
.data-table tr:hover { background: rgba(255,255,255,0.02); }
.btn { background: #f97316; color: white; border: none; padding: 12px 24px; border-radius: 8px; cursor: pointer; font-size: 14px; font-weight: 500; }
.btn:hover { background: #ea580c; }
.log { background: #0d1117; color: #22c55e; padding: 16px; border-radius: 8px; font-family: monospace; font-size: 13px; white-space: pre-wrap; max-height: 200px; overflow: auto; }
"""


def render_page(title, content):
    """Render a complete page."""
    return f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{CSS}</style>
</head>
<body>
    <div class="header">
        <h1>📊 Data Platform Dashboard</h1>
        <p>ETL Pipeline | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    <div class="nav">
        <a href="/" class="active">Dashboard</a>
        <a href="/data">Data Explorer</a>
        <a href="/run">Pipeline</a>
        <a href="/lake">Data Lake</a>
    </div>
    <div class="container">
        {content}
    </div>
</body>
</html>"""


def render_dashboard():
    """Render main dashboard."""
    stats = get_stats()
    
    content = f'''
    <div class="stats">
        <div class="stat-box"><div class="label">Customers</div><div class="value">{stats["customers"]}</div></div>
        <div class="stat-box"><div class="label">Products</div><div class="value">{stats["products"]}</div></div>
        <div class="stat-box"><div class="label">Sales Records</div><div class="value">{stats["sales"]}</div></div>
        <div class="stat-box"><div class="label">Total Sales</div><div class="value">${stats["total_sales"]:,.0f}</div><div class="stat-box .sub">Profit: ${stats["total_profit"]:,.0f}</div></div>
    </div>
    '''
    
    # Category KPIs
    rows = query_db("SELECT category, total_sales, total_profit FROM kpi_category ORDER BY total_sales DESC")
    if rows:
        content += f'<div class="card"><h2>📦 Sales by Category</h2>{table_html(rows)}</div>'
    
    # Top Products
    rows = query_db("SELECT product_name, category, total_sales FROM kpi_product ORDER BY total_sales DESC LIMIT 10")
    if rows:
        content += f'<div class="card"><h2>🏆 Top Products</h2>{table_html(rows)}</div>'
    
    # Top Customers
    rows = query_db("SELECT customer_name, segment, total_sales FROM kpi_customer ORDER BY total_sales DESC LIMIT 10")
    if rows:
        content += f'<div class="card"><h2>👥 Top Customers</h2>{table_html(rows)}</div>'
    
    # Monthly Trend - try different table name
    rows = query_db("SELECT * FROM kpi_date ORDER BY year_month DESC LIMIT 12")
    if not rows:
        rows = query_db("SELECT year_month, total_sales, order_count FROM kpi_date ORDER BY year_month DESC LIMIT 12")
    if rows:
        content += f'<div class="card"><h2>📈 Monthly Trend</h2>{table_html(rows)}</div>'
    
    return render_page("Data Platform - Dashboard", content)


def render_data():
    """Render data explorer."""
    content = '<div class="card"><h2>Customers</h2>'
    rows = query_db("SELECT customer_name, segment, city, region FROM dim_customers LIMIT 20")
    content += table_html(rows) + '</div>'
    
    content += '<div class="card"><h2>Products</h2>'
    rows = query_db("SELECT product_name, category, sub_category FROM dim_products LIMIT 20")
    content += table_html(rows) + '</div>'
    
    content += '<div class="card"><h2>Sales</h2>'
    rows = query_db("SELECT order_id, order_date, customer_id, product_id, sales, profit FROM fact_sales LIMIT 20")
    content += table_html(rows) + '</div>'
    
    return render_page("Data Platform - Data Explorer", content)


def render_run():
    """Render pipeline controls."""
    content = '''
    <div class="card">
        <h2>▶️ Run Pipeline</h2>
        <p style="color: #8b949e; margin-bottom: 16px;">Click below to run the full ETL pipeline:</p>
        <button class="btn" onclick="runPipeline()">Run Pipeline</button>
        <div id="log" class="log" style="display: none; margin-top: 16px;"></div>
    </div>
    <script>
        async function runPipeline() {
            const btn = document.querySelector('.btn');
            const log = document.getElementById('log');
            btn.disabled = true;
            btn.textContent = 'Running...';
            log.style.display = 'block';
            log.textContent = 'Starting pipeline...';
            
            try {{
                const resp = await fetch('/api/run');
                const data = await resp.json();
                log.textContent = data.log || 'Done!';
            }} catch(e) {{
                log.textContent = 'Error: ' + e;
            }}
            
            btn.disabled = false;
            btn.textContent = 'Run Pipeline';
            setTimeout(() => location.reload(), 2000);
        }}
    </script>
    '''
    return render_page("Data Platform - Pipeline", content)


def render_lake():
    """Render data lake view."""
    content = '<div class="card"><h2>Data Lake Zones</h2>'
    
    zones = [
        ("Raw", LAKE_PATH / "raw"),
        ("Bronze", LAKE_PATH / "bronze"),
        ("Silver", LAKE_PATH / "silver"),
        ("Gold", LAKE_PATH / "gold"),
    ]
    
    for name, path in zones:
        files = list(path.glob("*")) if path.exists() else []
        file_list = ", ".join([f.name for f in files]) if files else "empty"
        content += f'<p><strong>{name}</strong>: {file_list}</p>'
    
    content += '</div>'
    return render_page("Data Platform - Data Lake", content)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        if self.path == "/" or self.path == "/dashboard":
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(render_dashboard().encode("utf-8"))
        elif self.path == "/data":
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(render_data().encode("utf-8"))
        elif self.path == "/run":
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(render_run().encode("utf-8"))
        elif self.path == "/lake":
            self.send_response(200)
            self.send_header("Content-type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(render_lake().encode("utf-8"))
        elif self.path == "/api/run":
            import subprocess
            result = subprocess.run(
                ["python3", "data-platform/ingestion/generate_sample.py"],
                capture_output=True, text=True, cwd=str(BASE_PATH)
            )
            result += subprocess.run(
                ["python3", "data-platform/ingestion/load_raw.py"],
                capture_output=True, text=True, cwd=str(BASE_PATH)
            )
            result += subprocess.run(
                ["python3", "data-platform/processing/clean.py"],
                capture_output=True, text=True, cwd=str(BASE_PATH)
            )
            result += subprocess.run(
                ["python3", "data-platform/processing/aggregate.py"],
                capture_output=True, text=True, cwd=str(BASE_PATH)
            )
            result += subprocess.run(
                ["python3", "data-platform/warehouse/load_postgres.py"],
                capture_output=True, text=True, cwd=str(BASE_PATH)
            )
            output = result.stdout
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"success": True, "log": output}).encode())
        else:
            self.send_response(404)
            self.end_headers()


def main():
    port = 8888
    server = http.server.HTTPServer(("0.0.0.0", port), Handler)
    print(f"Data Platform Dashboard: http://localhost:{port}")
    print(f"Database: {DB_PATH}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
        server.close()


if __name__ == "__main__":
    main()