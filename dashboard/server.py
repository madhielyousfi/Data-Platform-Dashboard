#!/usr/bin/env python3
"""
Web Interface for Data Platform Dashboard
Uses Python stdlib only (http.server + sqlite3)
"""
import http.server
import json
import sqlite3
import subprocess
import sys
import urllib.parse
from datetime import datetime
from pathlib import Path
from html import escape

BASE_PATH = Path("/home/dados/Documents/Data Platform")
DB_PATH = BASE_PATH / "warehouse" / "datawarehouse.db"


def get_db():
    return sqlite3.connect(str(DB_PATH))


def query_db(sql):
    conn = get_db()
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def table_html(rows, cols=None):
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
            if isinstance(val, (int, float)):
                if isinstance(val, float):
                    val = f"{val:,.2f}"
                html += f"<td>{escape(str(val))}</td>"
            else:
                html += f"<td>{escape(str(val))}</td>"
        html += "</tr>\n"
    html += "</tbody>\n</table>"
    return html


def get_stats():
    stats = {}
    conn = get_db()
    cur = conn.cursor()
    
    cur.execute("SELECT COUNT(*) as cnt FROM dim_customers")
    stats["customers"] = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) as cnt FROM dim_products")
    stats["products"] = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) as cnt FROM fact_sales")
    stats["sales"] = cur.fetchone()[0]
    
    cur.execute("SELECT SUM(total_sales) as total FROM kpi_category")
    stats["total_sales"] = cur.fetchone()[0] or 0
    
    cur.execute("SELECT SUM(total_profit) as total FROM kpi_category")
    stats["total_profit"] = cur.fetchone()[0] or 0
    
    conn.close()
    return stats


CSS = """
* { box-sizing: border-box; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 0; background: #f5f5f5; }
.header { background: linear-gradient(135deg, #1a1a2e, #16213e); color: white; padding: 20px; }
.header h1 { margin: 0; font-size: 24px; }
.header p { margin: 5px 0 0; opacity: 0.7; font-size: 14px; }
.nav { background: #0f3460; padding: 0 20px; }
.nav a { display: inline-block; padding: 15px 20px; color: white; text-decoration: none; }
.nav a:hover, .nav a.active { background: #e94560; }
.container { padding: 20px; max-width: 1400px; margin: 0 auto; }
.card { background: white; border-radius: 8px; padding: 20px; margin-bottom: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
.card h2 { margin: 0 0 15px; color: #1a1a2e; font-size: 18px; border-bottom: 2px solid #e94560; padding-bottom: 10px; }
.stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
.stat-box { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); text-align: center; }
.stat-box .label { color: #666; font-size: 14px; text-transform: uppercase; }
.stat-box .value { font-size: 32px; font-weight: bold; color: #1a1a2e; margin: 10px 0; }
.stat-box .sub { font-size: 12px; color: #27ae60; }
.btn { background: #e94560; color: white; border: none; padding: 12px 24px; border-radius: 4px; cursor: pointer; font-size: 16px; }
.btn:hover { background: #c73e54; }
.btn:disabled { background: #999; cursor: not-allowed; }
.btn-secondary { background: #27ae60 !important; }
.btn-secondary:hover { background: #1e8449 !important; }
.data-table { width: 100%; border-collapse: collapse; font-size: 14px; }
.data-table th { background: #1a1a2e; color: white; padding: 12px; text-align: left; }
.data-table td { padding: 10px 12px; border-bottom: 1px solid #eee; }
.data-table tr:hover { background: #f9f9f9; }
.tab-content { display: none; }
.tab-content.active { display: block; }
.tab-btn { background: #ddd; border: none; padding: 10px 20px; margin-right: 5px; cursor: pointer; }
.tab-btn.active { background: #e94560; color: white; }
.log-output { background: #1a1a2e; color: #0f0; padding: 15px; border-radius: 4px; font-family: monospace; font-size: 13px; max-height: 300px; overflow: auto; white-space: pre-wrap; }
"""


def render_page(title, content):
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <meta charset="utf-8">
    <style>{CSS}</style>
</head>
<body>
    <div class="header">
        <h1>Data Platform Dashboard</h1>
        <p>ETL Pipeline | {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    </div>
    <div class="nav">
        <a href="/">Dashboard</a>
        <a href="/data">Data</a>
        <a href="/run">Run Pipeline</a>
    </div>
    <div class="container">
        {content}
    </div>
<script>
        function runPipeline() {{
            const btn = document.querySelector('.btn');
            btn.disabled = true;
            btn.textContent = 'Running...';
            const logDiv = document.getElementById('log');
            logDiv.style.display = 'block';
            logDiv.textContent = 'Running pipeline...';
            fetch('/api/run').then(r => r.json()).then(d => {{
                logDiv.textContent = d.log || 'Done!';
                btn.disabled = false;
                btn.textContent = 'Run Pipeline';
                setTimeout(() => location.reload(), 1500);
            }});
        }}
        function exportCSV() {{
            const btn = document.querySelector('.btn-secondary');
            btn.disabled = true;
            btn.textContent = 'Exporting...';
            window.location.href = '/api/export';
        }}
    </script>
</body>
</html>"""
    return html


def render_dashboard():
    stats = get_stats()
    
    content = f'''
    <div class="stats">
        <div class="stat-box"><div class="label">Customers</div><div class="value">{stats["customers"]}</div></div>
        <div class="stat-box"><div class="label">Products</div><div class="value">{stats["products"]}</div></div>
        <div class="stat-box"><div class="label">Sales</div><div class="value">{stats["sales"]}</div></div>
        <div class="stat-box"><div class="label">Total Sales</div><div class="value">${stats["total_sales"]:,.0f}</div><div class="stat-box .sub">Profit: ${stats["total_profit"]:,.0f}</div></div>
    </div>
    '''
    
    rows = query_db("SELECT customer_name, segment, total_sales, order_count FROM kpi_customer ORDER BY total_sales DESC LIMIT 5")
    content += f'<div class="card"><h2>Top Customers</h2>{table_html(rows, ["customer_name", "segment", "total_sales", "order_count"])}</div>'
    
    rows = query_db("SELECT product_name, category, total_sales FROM kpi_product ORDER BY total_sales DESC LIMIT 5")
    content += f'<div class="card"><h2>Top Products</h2>{table_html(rows, ["product_name", "category", "total_sales"])}</div>'
    
    rows = query_db("SELECT category, total_sales, total_profit FROM kpi_category ORDER BY total_sales DESC")
    content += f'<div class="card"><h2>By Category</h2>{table_html(rows, ["category", "total_sales", "total_profit"])}</div>'
    
    rows = query_db("SELECT year_month, total_sales, order_count FROM kpi_date ORDER BY year_month DESC LIMIT 12")
    content += f'<div class="card"><h2>Monthly Trend</h2>{table_html(rows)}</div>'
    
    return render_page("Data Platform - Dashboard", content)


def render_data():
    content = '<div class="card"><h2>Customers</h2>'
    rows = query_db("SELECT customer_name, segment, city, region FROM dim_customers LIMIT 20")
    content += table_html(rows) + '</div>'
    
    content += '<div class="card"><h2>Products</h2>'
    rows = query_db("SELECT product_name, category, sub_category FROM dim_products LIMIT 20")
    content += table_html(rows) + '</div>'
    
    content += '<div class="card"><h2>Sales</h2>'
    rows = query_db("SELECT order_date, sales, quantity, profit FROM fact_sales LIMIT 20")
    content += table_html(rows) + '</div>'
    
    return render_page("Data Platform - Data", content)


def render_run():
    content = '''
    <div class="card">
        <h2>Pipeline Actions</h2>
        <div style="display: flex; gap: 15px; margin-top: 15px;">
            <button class="btn" onclick="runPipeline()">Run Pipeline</button>
            <button class="btn btn-secondary" onclick="exportCSV()" style="background: #27ae60;">Export CSV</button>
        </div>
        <div id="log" class="log-output" style="margin-top: 20px; display: none;"></div>
    </div>
    '''
    return render_page("Data Platform - Run", content)


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
        elif self.path == "/api/run":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            
            result = subprocess.run(
                [sys.executable, str(BASE_PATH / "run_pipeline.py")],
                capture_output=True,
                text=True
            )
            self.wfile.write(json.dumps({"success": result.returncode == 0, "log": result.stdout}).encode())
        elif self.path == "/api/export":
            conn = get_db()
            cur = conn.cursor()
            cur.execute("""
                SELECT 
                    f.order_id,
                    f.order_date,
                    f.ship_date,
                    f.ship_mode,
                    c.customer_name,
                    c.segment,
                    p.product_name,
                    p.category,
                    f.quantity,
                    f.sales,
                    f.discount,
                    f.profit
                FROM fact_sales f
                JOIN dim_customers c ON f.customer_id = c.customer_id
                JOIN dim_products p ON f.product_id = p.product_id
                ORDER BY f.order_date DESC
            """)
            rows = cur.fetchall()
            conn.close()
            
            csv = "order_id,order_date,ship_date,ship_mode,customer_name,segment,product_name,category,quantity,sales,discount,profit\n"
            for row in rows:
                csv += f"{row[0]},{row[1]},{row[2]},{row[3]},{row[4]},{row[5]},{row[6]},{row[7]},{row[8]},{row[9]},{row[10]},{row[11]}\n"
            
            self.send_response(200)
            self.send_header("Content-Type", "text/csv")
            self.send_header("Content-Disposition", "attachment; filename=sales_export.csv")
            self.end_headers()
            self.wfile.write(csv.encode("utf-8"))
        else:
            self.send_response(404)
            self.end_headers()


def main():
    PORT = 8888
    server = http.server.HTTPServer(("0.0.0.0", PORT), Handler)
    print(f"Data Platform: http://localhost:{PORT}")
    print(f"Database: {DB_PATH}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        server.close()


if __name__ == "__main__":
    main()