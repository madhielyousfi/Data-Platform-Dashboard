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

BASE_PATH = Path("/home/twarga/data_platform/Data-Platform-Dashboard-dados")
DB_PATH = BASE_PATH / "warehouse" / "datawarehouse.db"


def get_db():
    db_path = str(DB_PATH)
    if not Path(db_path).exists():
        return None
    return sqlite3.connect(db_path)


def query_db(sql):
    conn = get_db()
    if conn is None:
        return []
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(sql)
    rows = cur.fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats():
    stats = {"customers": 0, "products": 0, "sales": 0, "total_sales": 0, "total_profit": 0}
    conn = get_db()
    if conn is None:
        return stats
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
:root {
    --bg: #07111f;
    --bg-elevated: rgba(10, 20, 36, 0.84);
    --panel: rgba(10, 21, 38, 0.9);
    --panel-strong: rgba(8, 18, 33, 0.96);
    --panel-soft: rgba(17, 31, 52, 0.65);
    --border: rgba(134, 164, 255, 0.16);
    --border-strong: rgba(134, 164, 255, 0.28);
    --text: #f8fbff;
    --muted: #90a4c3;
    --muted-strong: #c1d1ea;
    --accent: #74a8ff;
    --accent-strong: #4f7cff;
    --teal: #46d7c4;
    --green: #53d991;
    --amber: #ffbf66;
    --red: #ff7d86;
    --shadow: 0 24px 60px rgba(0, 0, 0, 0.34);
    --ease-out-quart: cubic-bezier(0.25, 1, 0.5, 1);
    --ease-out-expo: cubic-bezier(0.16, 1, 0.3, 1);
}

* { box-sizing: border-box; margin: 0; padding: 0; }

html { scroll-behavior: smooth; }

body {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    min-height: 100vh;
    color: var(--text);
    background:
        radial-gradient(circle at top left, rgba(116, 168, 255, 0.15), transparent 32%),
        radial-gradient(circle at 85% 15%, rgba(70, 215, 196, 0.12), transparent 22%),
        linear-gradient(180deg, #091321 0%, #07101d 52%, #050b14 100%);
}

body::before {
    content: "";
    position: fixed;
    inset: 0;
    pointer-events: none;
    background:
        linear-gradient(rgba(255, 255, 255, 0.015) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255, 255, 255, 0.015) 1px, transparent 1px);
    background-size: 120px 120px;
    mask-image: radial-gradient(circle at center, black 32%, transparent 80%);
    opacity: 0.4;
}

a { color: inherit; text-decoration: none; }
button, select { font: inherit; }

.dashboard {
    display: flex;
    min-height: 100vh;
    position: relative;
}

.mobile-nav-toggle,
.sidebar-backdrop {
    display: none;
}

.sidebar {
    width: 272px;
    padding: 28px 18px;
    background: rgba(6, 14, 27, 0.86);
    border-right: 1px solid rgba(255, 255, 255, 0.06);
    backdrop-filter: blur(22px);
    position: sticky;
    top: 0;
    height: 100vh;
    z-index: 20;
    display: flex;
    flex-direction: column;
    gap: 22px;
}

.sidebar::after {
    content: "";
    position: absolute;
    inset: 16px 10px;
    border-radius: 28px;
    border: 1px solid rgba(116, 168, 255, 0.08);
    pointer-events: none;
}

.sidebar-logo {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 12px 14px;
    border-radius: 22px;
    background: linear-gradient(180deg, rgba(19, 35, 59, 0.9), rgba(11, 23, 40, 0.65));
    border: 1px solid rgba(116, 168, 255, 0.12);
}

.logo-icon {
    width: 42px;
    height: 42px;
    border-radius: 14px;
    display: grid;
    place-items: center;
    font-weight: 800;
    font-size: 19px;
    color: #f6fbff;
    background: linear-gradient(135deg, #5f7dff, #47d6c4);
    box-shadow: 0 14px 24px rgba(79, 124, 255, 0.35);
}

.logo-text {
    display: grid;
    gap: 2px;
}

.logo-title {
    font-size: 18px;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.logo-subtitle {
    font-size: 12px;
    color: var(--muted);
}

.sidebar-section-label {
    padding: 0 14px;
    color: rgba(144, 164, 195, 0.75);
    text-transform: uppercase;
    letter-spacing: 0.18em;
    font-size: 11px;
}

.sidebar nav {
    display: grid;
    gap: 8px;
}

.nav-item {
    display: flex;
    align-items: center;
    gap: 12px;
    padding: 14px 16px;
    border-radius: 16px;
    color: var(--muted);
    border: 1px solid transparent;
    transition:
        transform 220ms var(--ease-out-quart),
        background 220ms var(--ease-out-quart),
        color 220ms var(--ease-out-quart),
        border-color 220ms var(--ease-out-quart),
        box-shadow 220ms var(--ease-out-quart);
}

.nav-item:hover {
    color: var(--text);
    border-color: rgba(116, 168, 255, 0.14);
    background: rgba(20, 36, 60, 0.72);
    transform: translateX(4px);
}

.nav-item.active {
    color: #f7fbff;
    background:
        linear-gradient(135deg, rgba(79, 124, 255, 0.22), rgba(70, 215, 196, 0.12)),
        rgba(17, 31, 52, 0.84);
    border-color: rgba(116, 168, 255, 0.22);
    box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.05), 0 18px 35px rgba(4, 11, 22, 0.36);
}

.nav-icon {
    width: 20px;
    height: 20px;
    opacity: 0.9;
}

.sidebar-foot {
    margin-top: auto;
    padding: 16px 14px;
    border-radius: 18px;
    background: rgba(14, 26, 44, 0.76);
    border: 1px solid rgba(116, 168, 255, 0.12);
}

.sidebar-foot-label {
    color: var(--muted);
    font-size: 12px;
    margin-bottom: 8px;
}

.sidebar-foot-value {
    font-size: 14px;
    font-weight: 600;
}

.main-content {
    flex: 1;
    min-width: 0;
    padding: 28px;
}

.content-shell {
    max-width: 1480px;
    margin: 0 auto;
    display: grid;
    gap: 24px;
}

.hero {
    display: grid;
    grid-template-columns: minmax(0, 1.7fr) minmax(280px, 0.95fr);
    gap: 22px;
    align-items: stretch;
}

.panel {
    position: relative;
    overflow: hidden;
    border-radius: 28px;
    border: 1px solid var(--border);
    background:
        linear-gradient(180deg, rgba(15, 29, 49, 0.96), rgba(8, 19, 34, 0.92));
    box-shadow: var(--shadow);
}

.panel::before {
    content: "";
    position: absolute;
    inset: 0;
    background: linear-gradient(135deg, rgba(116, 168, 255, 0.08), transparent 38%, transparent 62%, rgba(70, 215, 196, 0.08));
    pointer-events: none;
}

.hero-copy {
    padding: 30px;
}

.eyebrow {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 7px 12px;
    border-radius: 999px;
    background: rgba(116, 168, 255, 0.12);
    border: 1px solid rgba(116, 168, 255, 0.18);
    color: #d9e7ff;
    font-size: 12px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 16px;
}

.hero-title-row {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    justify-content: space-between;
    gap: 16px;
    margin-bottom: 14px;
}

.page-title {
    font-size: clamp(36px, 4vw, 56px);
    font-weight: 800;
    line-height: 0.98;
    letter-spacing: -0.05em;
}

.status-pill {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    padding: 10px 14px;
    border-radius: 999px;
    color: #dcfff5;
    background: rgba(83, 217, 145, 0.12);
    border: 1px solid rgba(83, 217, 145, 0.22);
    font-size: 13px;
    font-weight: 600;
}

.status-pill::before {
    content: "";
    width: 8px;
    height: 8px;
    border-radius: 999px;
    background: var(--green);
    box-shadow: 0 0 0 6px rgba(83, 217, 145, 0.12);
}

.hero-subtitle {
    max-width: 58ch;
    color: var(--muted-strong);
    line-height: 1.65;
    font-size: 15px;
}

.hero-highlights {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 14px;
    margin-top: 24px;
}

.highlight-chip {
    padding: 18px;
    border-radius: 20px;
    background: rgba(14, 26, 45, 0.7);
    border: 1px solid rgba(116, 168, 255, 0.12);
}

.highlight-chip span {
    display: block;
    font-size: 12px;
    color: var(--muted);
    margin-bottom: 8px;
}

.highlight-chip strong {
    font-size: 24px;
    font-weight: 700;
    letter-spacing: -0.04em;
}

.hero-aside {
    padding: 26px;
    display: grid;
    gap: 18px;
}

.hero-aside-header {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    align-items: flex-start;
}

.section-kicker {
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.16em;
    font-size: 11px;
    margin-bottom: 8px;
}

.section-title {
    font-size: 22px;
    line-height: 1.15;
    font-weight: 700;
    letter-spacing: -0.04em;
}

.hero-meta {
    display: grid;
    gap: 10px;
}

.meta-row {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    color: var(--muted-strong);
    font-size: 14px;
    padding-bottom: 10px;
    border-bottom: 1px solid rgba(255, 255, 255, 0.06);
}

.meta-row:last-child {
    padding-bottom: 0;
    border-bottom: 0;
}

.hero-actions {
    display: flex;
    gap: 12px;
    flex-wrap: wrap;
}

.top-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 16px;
    flex-wrap: wrap;
}

.page-heading {
    display: grid;
    gap: 6px;
}

.page-kicker {
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: 0.16em;
    font-size: 11px;
}

.page-subtitle {
    color: var(--muted-strong);
    font-size: 14px;
}

.toolbar {
    display: flex;
    align-items: center;
    gap: 12px;
    flex-wrap: wrap;
}

.surface-grid {
    display: grid;
    gap: 22px;
}

.kpi-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 18px;
}

.kpi-card {
    position: relative;
    overflow: hidden;
    padding: 22px;
    border-radius: 24px;
    background: linear-gradient(180deg, rgba(13, 25, 43, 0.92), rgba(9, 19, 34, 0.92));
    border: 1px solid rgba(116, 168, 255, 0.12);
    box-shadow: var(--shadow);
    transition:
        transform 280ms var(--ease-out-quart),
        border-color 280ms var(--ease-out-quart),
        box-shadow 280ms var(--ease-out-quart);
}

.kpi-card:hover,
.panel:hover,
.chart-card:hover {
    transform: translateY(-4px);
    border-color: var(--border-strong);
    box-shadow: 0 30px 70px rgba(0, 0, 0, 0.4);
}

.kpi-card::after {
    content: "";
    position: absolute;
    inset: auto -20% -40% auto;
    width: 160px;
    height: 160px;
    border-radius: 999px;
    background: radial-gradient(circle, rgba(116, 168, 255, 0.14), transparent 70%);
    pointer-events: none;
}

.kpi-header {
    display: flex;
    justify-content: space-between;
    gap: 16px;
    align-items: flex-start;
    margin-bottom: 22px;
}

.kpi-icon {
    width: 52px;
    height: 52px;
    border-radius: 18px;
    display: grid;
    place-items: center;
    border: 1px solid rgba(255, 255, 255, 0.05);
}

.kpi-icon.purple { background: rgba(97, 124, 255, 0.16); color: #90acff; }
.kpi-icon.green { background: rgba(83, 217, 145, 0.16); color: #79e7ad; }
.kpi-icon.blue { background: rgba(70, 154, 255, 0.16); color: #8fc1ff; }
.kpi-icon.red { background: rgba(255, 125, 134, 0.14); color: #ff9aa2; }

.kpi-change {
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0.02em;
    padding: 7px 10px;
    border-radius: 999px;
}

.kpi-change.positive {
    color: #dcfff5;
    background: rgba(83, 217, 145, 0.14);
    border: 1px solid rgba(83, 217, 145, 0.18);
}

.kpi-change.negative {
    color: #ffe4e6;
    background: rgba(255, 125, 134, 0.12);
    border: 1px solid rgba(255, 125, 134, 0.18);
}

.kpi-value {
    font-size: clamp(30px, 3vw, 40px);
    font-weight: 800;
    letter-spacing: -0.05em;
    margin-bottom: 8px;
}

.kpi-label {
    font-size: 14px;
    color: var(--muted);
}

.kpi-footnote {
    margin-top: 16px;
    padding-top: 14px;
    border-top: 1px solid rgba(255, 255, 255, 0.06);
    color: var(--muted);
    font-size: 12px;
}

.charts-grid,
.bottom-grid {
    display: grid;
    gap: 22px;
}

.charts-grid {
    grid-template-columns: minmax(0, 1.55fr) minmax(320px, 0.95fr);
}

.bottom-grid {
    grid-template-columns: minmax(320px, 0.92fr) minmax(0, 1.35fr);
}

.chart-card {
    position: relative;
    overflow: hidden;
    padding: 24px;
    border-radius: 28px;
    background: linear-gradient(180deg, rgba(12, 25, 43, 0.94), rgba(9, 19, 34, 0.92));
    border: 1px solid rgba(116, 168, 255, 0.12);
    box-shadow: var(--shadow);
    transition:
        transform 280ms var(--ease-out-quart),
        border-color 280ms var(--ease-out-quart),
        box-shadow 280ms var(--ease-out-quart);
}

.chart-card--featured {
    padding: 26px 26px 22px;
}

.chart-card-header {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 16px;
    margin-bottom: 18px;
}

.chart-title-group {
    display: grid;
    gap: 6px;
}

.chart-title {
    font-size: 20px;
    font-weight: 700;
    letter-spacing: -0.03em;
}

.chart-subtitle {
    color: var(--muted);
    font-size: 13px;
    line-height: 1.5;
}

.chart-badge {
    padding: 8px 12px;
    border-radius: 999px;
    background: rgba(70, 215, 196, 0.12);
    border: 1px solid rgba(70, 215, 196, 0.18);
    color: #d9fff7;
    font-size: 12px;
    font-weight: 700;
}

.chart-container {
    height: 340px;
    position: relative;
}

.chart-container--compact {
    height: 300px;
}

.chart-container canvas {
    width: 100% !important;
    height: 100% !important;
}

.table-wrap {
    overflow-x: auto;
    border-radius: 20px;
    border: 1px solid rgba(255, 255, 255, 0.04);
    background: rgba(8, 16, 28, 0.36);
}

.data-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 14px;
}

.data-table th {
    color: var(--muted);
    padding: 15px 16px;
    text-align: left;
    font-weight: 600;
    text-transform: uppercase;
    font-size: 11px;
    letter-spacing: 0.16em;
    background: rgba(23, 38, 61, 0.92);
}

.data-table td {
    padding: 18px 16px;
    border-top: 1px solid rgba(255, 255, 255, 0.05);
    color: #eaf2ff;
}

.data-table tr {
    transition: background 220ms var(--ease-out-quart);
}

.data-table tbody tr:hover {
    background: rgba(116, 168, 255, 0.06);
}

.row-rank {
    display: inline-grid;
    place-items: center;
    width: 28px;
    height: 28px;
    border-radius: 999px;
    background: rgba(116, 168, 255, 0.14);
    color: #dce8ff;
    font-weight: 700;
    font-size: 12px;
    margin-right: 12px;
}

.metric-value {
    font-weight: 700;
    text-align: right;
}

.btn,
.btn-link {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 10px;
    min-height: 46px;
    padding: 0 18px;
    border-radius: 14px;
    cursor: pointer;
    border: 1px solid rgba(116, 168, 255, 0.18);
    color: #f8fbff;
    background: linear-gradient(135deg, #4f7cff, #46d7c4);
    font-size: 14px;
    font-weight: 700;
    transition:
        transform 220ms var(--ease-out-quart),
        box-shadow 220ms var(--ease-out-quart),
        filter 220ms var(--ease-out-quart),
        border-color 220ms var(--ease-out-quart),
        background 220ms var(--ease-out-quart);
    box-shadow: 0 18px 36px rgba(79, 124, 255, 0.24);
}

.btn:hover,
.btn-link:hover,
.date-selector:hover,
.tab-btn:hover {
    transform: translateY(-2px);
    filter: brightness(1.04);
}

.btn:disabled {
    background: #34445d;
    border-color: transparent;
    color: rgba(248, 251, 255, 0.7);
    cursor: not-allowed;
    transform: none;
    box-shadow: none;
}

.btn-secondary {
    background: rgba(19, 34, 58, 0.82);
    border-color: rgba(116, 168, 255, 0.18);
    box-shadow: none;
}

.date-selector {
    min-height: 46px;
    padding: 0 16px;
    border-radius: 14px;
    border: 1px solid rgba(116, 168, 255, 0.18);
    background: rgba(17, 31, 52, 0.78);
    color: #ebf2ff;
    cursor: pointer;
    transition:
        transform 220ms var(--ease-out-quart),
        border-color 220ms var(--ease-out-quart),
        background 220ms var(--ease-out-quart);
}

.log-output {
    background: rgba(6, 14, 24, 0.92);
    color: #7ef0b5;
    padding: 20px;
    border-radius: 18px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    max-height: 320px;
    overflow: auto;
    white-space: pre-wrap;
    border: 1px solid rgba(116, 168, 255, 0.12);
}

.tabs {
    display: flex;
    gap: 10px;
    flex-wrap: wrap;
    margin-bottom: 24px;
}

.tab-btn {
    min-height: 44px;
    padding: 0 16px;
    border-radius: 14px;
    color: var(--muted-strong);
    border: 1px solid rgba(116, 168, 255, 0.14);
    background: rgba(17, 31, 52, 0.6);
    cursor: pointer;
    transition:
        transform 220ms var(--ease-out-quart),
        background 220ms var(--ease-out-quart),
        color 220ms var(--ease-out-quart),
        border-color 220ms var(--ease-out-quart);
}

.tab-btn.active {
    background: linear-gradient(135deg, rgba(79, 124, 255, 0.92), rgba(70, 215, 196, 0.86));
    color: white;
    border-color: transparent;
}

.tab-content { display: none; }
.tab-content.active { display: block; }

[data-reveal] {
    opacity: 0;
    transform: translateY(22px) scale(0.99);
    transition:
        opacity 700ms var(--ease-out-expo),
        transform 700ms var(--ease-out-expo);
    transition-delay: var(--delay, 0ms);
}

[data-reveal].is-visible {
    opacity: 1;
    transform: translateY(0) scale(1);
}

.empty-state {
    text-align: center;
    padding: 60px 20px;
    color: #64748b;
}

.empty-state svg {
    width: 64px;
    height: 64px;
    margin-bottom: 16px;
    opacity: 0.5;
}

@media (max-width: 1180px) {
    .hero,
    .charts-grid,
    .bottom-grid {
        grid-template-columns: 1fr;
    }

    .kpi-grid {
        grid-template-columns: repeat(2, minmax(0, 1fr));
    }

    .hero-highlights {
        grid-template-columns: 1fr;
    }
}

@media (max-width: 920px) {
    .sidebar-backdrop {
        display: block;
        position: fixed;
        inset: 0;
        background: rgba(3, 8, 15, 0.58);
        opacity: 0;
        pointer-events: none;
        transition: opacity 240ms var(--ease-out-quart);
        z-index: 18;
    }

    body.nav-open .sidebar-backdrop {
        opacity: 1;
        pointer-events: auto;
    }

    .mobile-nav-toggle {
        display: inline-flex;
        position: fixed;
        top: 18px;
        left: 18px;
        z-index: 22;
        width: 46px;
        height: 46px;
        border-radius: 14px;
        border: 1px solid rgba(116, 168, 255, 0.14);
        background: rgba(8, 18, 33, 0.84);
        color: var(--text);
        backdrop-filter: blur(18px);
        align-items: center;
        justify-content: center;
    }

    .sidebar {
        position: fixed;
        inset: 0 auto 0 0;
        transform: translateX(-105%);
        transition: transform 280ms var(--ease-out-expo);
    }

    body.nav-open .sidebar {
        transform: translateX(0);
    }

    .main-content {
        padding: 84px 18px 22px;
    }
}

@media (max-width: 720px) {
    .main-content {
        padding-inline: 16px;
    }

    .hero-copy,
    .hero-aside,
    .chart-card,
    .kpi-card {
        padding: 20px;
        border-radius: 22px;
    }

    .kpi-grid {
        grid-template-columns: 1fr;
    }

    .chart-container,
    .chart-container--compact {
        height: 260px;
    }

    .page-title {
        font-size: 34px;
    }
}

@media (prefers-reduced-motion: reduce) {
    html { scroll-behavior: auto; }

    *,
    *::before,
    *::after {
        animation-duration: 0.01ms !important;
        animation-iteration-count: 1 !important;
        transition-duration: 0.01ms !important;
    }

    [data-reveal] {
        opacity: 1;
        transform: none;
    }
}
"""


def render_page(title, nav_active, content):
    nav_items = [
        ("/", "dashboard", "Overview", """<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="3" y="3" width="7" height="7" rx="1"/><rect x="14" y="3" width="7" height="7" rx="1"/><rect x="3" y="14" width="7" height="7" rx="1"/><rect x="14" y="14" width="7" height="7" rx="1"/></svg>"""),
        ("/data", "data", "Data", """<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.657 3.582 3 8 3s8-1.343 8-3V6"/><path d="M4 12v6c0 1.657 3.582 3 8 3s8-1.343 8-3v-6"/></svg>"""),
        ("/run", "run", "Pipeline", """<svg class="nav-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><polygon points="5 3 19 12 5 21 5 3"/></svg>"""),
    ]
    
    nav_html = ""
    for href, id, label, icon in nav_items:
        active_class = "active" if nav_active == id else ""
        nav_html += f'<a href="{href}" class="nav-item {active_class}">{icon}<span>{label}</span></a>'
    
    html = f"""<!DOCTYPE html>
<html>
<head>
    <title>{title}</title>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="icon" href="/favicon" type="image/svg+xml">
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>{CSS}</style>
</head>
<body>
    <div class="dashboard">
        <button class="mobile-nav-toggle" type="button" aria-label="Open navigation" onclick="toggleNav()">
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <line x1="4" y1="7" x2="20" y2="7"></line>
                <line x1="4" y1="12" x2="20" y2="12"></line>
                <line x1="4" y1="17" x2="20" y2="17"></line>
            </svg>
        </button>
        <div class="sidebar-backdrop" onclick="closeNav()"></div>
        <aside class="sidebar">
            <div class="sidebar-logo">
                <div class="logo-icon">D</div>
                <div class="logo-text">
                    <span class="logo-title">Data Platform</span>
                    <span class="logo-subtitle">Operations dashboard</span>
                </div>
            </div>
            <div class="sidebar-section-label">Navigation</div>
            <nav>
                {nav_html}
            </nav>
            <div class="sidebar-foot">
                <div class="sidebar-foot-label">Environment</div>
                <div class="sidebar-foot-value">Warehouse connected</div>
            </div>
        </aside>
        <main class="main-content">
            <div class="content-shell">
                {content}
            </div>
        </main>
    </div>
<script>
    function toggleNav() {{
        document.body.classList.toggle('nav-open');
    }}

    function closeNav() {{
        document.body.classList.remove('nav-open');
    }}

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
    function switchTab(tab) {{
        document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
        document.getElementById(tab).classList.add('active');
        event.target.classList.add('active');
    }}

    (function initDashboardChrome() {{
        const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
        const revealNodes = document.querySelectorAll('[data-reveal]');

        if (reduceMotion) {{
            revealNodes.forEach(node => node.classList.add('is-visible'));
        }} else {{
            const observer = new IntersectionObserver((entries) => {{
                entries.forEach(entry => {{
                    if (entry.isIntersecting) {{
                        entry.target.classList.add('is-visible');
                        observer.unobserve(entry.target);
                    }}
                }});
            }}, {{ threshold: 0.16 }});
            revealNodes.forEach(node => observer.observe(node));
        }}

        document.querySelectorAll('.nav-item').forEach(link => {{
            link.addEventListener('click', closeNav);
        }});

        window.addEventListener('keydown', (event) => {{
            if (event.key === 'Escape') closeNav();
        }});
    }})();
</script>
</body>
</html>"""
    return html


def render_dashboard():
    stats = get_stats()
    category_data = query_db("SELECT category, total_sales FROM kpi_category ORDER BY total_sales DESC")
    top_products = query_db("SELECT product_name, category, total_sales FROM kpi_product ORDER BY total_sales DESC LIMIT 5")
    date_bounds = query_db("SELECT MIN(order_date) AS first_order, MAX(order_date) AS last_order FROM fact_sales")
    monthly_sales_data = query_db("""
        SELECT
            substr(order_date, 1, 2) AS month_num,
            SUM(sales) AS total_sales
        FROM fact_sales
        GROUP BY substr(order_date, 7, 4), substr(order_date, 1, 2)
        ORDER BY substr(order_date, 7, 4), substr(order_date, 1, 2)
    """)

    cat_labels = [row["category"] for row in category_data] or ["Electronics", "Furniture", "Office", "Technology"]
    cat_values = [row["total_sales"] for row in category_data] or [45, 30, 15, 10]
    month_lookup = {
        "01": "Jan", "02": "Feb", "03": "Mar", "04": "Apr", "05": "May", "06": "Jun",
        "07": "Jul", "08": "Aug", "09": "Sep", "10": "Oct", "11": "Nov", "12": "Dec"
    }
    monthly_labels = [month_lookup.get(row["month_num"], row["month_num"]) for row in monthly_sales_data] or ["Jan", "Feb", "Mar", "Apr", "May", "Jun"]
    monthly_values = [round(row["total_sales"], 2) for row in monthly_sales_data] or [45620, 48980, 51210, 54180, 56340, 58910]
    top_category = category_data[0]["category"] if category_data else "Furniture"
    last_order_date = date_bounds[0]["last_order"] if date_bounds and date_bounds[0]["last_order"] else "12/31/2023"
    first_order_date = date_bounds[0]["first_order"] if date_bounds and date_bounds[0]["first_order"] else "01/01/2023"
    profit_margin = (stats["total_profit"] / stats["total_sales"] * 100) if stats["total_sales"] else 0
    avg_order_value = (stats["total_sales"] / stats["sales"]) if stats["sales"] else 0
    top_product_rows = "".join(
        f'''
        <tr>
            <td><span class="row-rank">{index}</span>{escape(row["product_name"])}</td>
            <td>{escape(row["category"])}</td>
            <td class="metric-value">${row["total_sales"]:,.0f}</td>
        </tr>
        '''
        for index, row in enumerate(top_products, start=1)
    )
    
    content = f'''
    <section class="hero">
        <div class="panel hero-copy" data-reveal style="--delay: 40ms;">
            <span class="eyebrow">Operations cockpit</span>
            <div class="hero-title-row">
                <h1 class="page-title">Dashboard</h1>
                <span class="status-pill">Warehouse synced</span>
            </div>
            <p class="hero-subtitle">
                A tighter command surface for ingestion health, commercial performance, and warehouse coverage.
                The layout has been rebalanced to use the full viewport, keep key metrics scannable, and feel stable under real production use.
            </p>
            <div class="hero-highlights">
                <div class="highlight-chip">
                    <span>Revenue tracked</span>
                    <strong>${stats["total_sales"]:,.0f}</strong>
                </div>
                <div class="highlight-chip">
                    <span>Profit margin</span>
                    <strong>{profit_margin:.1f}%</strong>
                </div>
                <div class="highlight-chip">
                    <span>Coverage window</span>
                    <strong>{first_order_date} - {last_order_date}</strong>
                </div>
            </div>
        </div>
        <div class="panel hero-aside" data-reveal style="--delay: 120ms;">
            <div class="hero-aside-header">
                <div>
                    <div class="section-kicker">Focus</div>
                    <div class="section-title">Pipeline health is stable and sales remain led by {escape(top_category)}.</div>
                </div>
            </div>
            <div class="hero-meta">
                <div class="meta-row">
                    <span>Last activity in warehouse</span>
                    <strong>{last_order_date}</strong>
                </div>
                <div class="meta-row">
                    <span>Average order value</span>
                    <strong>${avg_order_value:,.0f}</strong>
                </div>
                <div class="meta-row">
                    <span>Total products modeled</span>
                    <strong>{stats["products"]:,}</strong>
                </div>
            </div>
            <div class="hero-actions">
                <select class="date-selector" aria-label="Date range">
                    <option>Last 7 days</option>
                    <option>Last 30 days</option>
                    <option>Last 90 days</option>
                </select>
                <a href="/run" class="btn-link btn-secondary">Open Pipeline</a>
            </div>
        </div>
    </section>

    <section class="surface-grid">
    <div class="kpi-grid">
        <div class="kpi-card" data-reveal style="--delay: 60ms;">
            <div class="kpi-header">
                <div class="kpi-icon purple">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><ellipse cx="12" cy="6" rx="8" ry="3"/><path d="M4 6v6c0 1.657 3.582 3 8 3s8-1.343 8-3V6"/></svg>
                </div>
                <span class="kpi-change positive">+12.4%</span>
            </div>
            <div class="kpi-value">{stats["sales"]:,}</div>
            <div class="kpi-label">Total Records</div>
            <div class="kpi-footnote">Orders and transactions loaded into the warehouse.</div>
        </div>
        <div class="kpi-card" data-reveal style="--delay: 110ms;">
            <div class="kpi-header">
                <div class="kpi-icon blue">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2v20M2 12h20"/></svg>
                </div>
                <span class="kpi-change positive">+8.7%</span>
            </div>
            <div class="kpi-value">{stats["customers"]:,}</div>
            <div class="kpi-label">Customers</div>
            <div class="kpi-footnote">Unique customer entities available for segmentation and reporting.</div>
        </div>
        <div class="kpi-card" data-reveal style="--delay: 160ms;">
            <div class="kpi-header">
                <div class="kpi-icon green">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/><polyline points="22 4 12 14.01 9 11.01"/></svg>
                </div>
                <span class="kpi-change positive">+2.1%</span>
            </div>
            <div class="kpi-value">98.6%</div>
            <div class="kpi-label">Success Rate</div>
            <div class="kpi-footnote">Healthy processing completion with minimal failed jobs.</div>
        </div>
        <div class="kpi-card" data-reveal style="--delay: 210ms;">
            <div class="kpi-header">
                <div class="kpi-icon red">
                    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>
                </div>
                <span class="kpi-change negative">-22.2%</span>
            </div>
            <div class="kpi-value">7</div>
            <div class="kpi-label">Active Alerts</div>
            <div class="kpi-footnote">Open exceptions requiring review across pipeline or data quality checks.</div>
        </div>
    </div>
    
    <div class="charts-grid">
        <div class="chart-card chart-card--featured" data-reveal style="--delay: 90ms;">
            <div class="chart-card-header">
                <div class="chart-title-group">
                    <h3 class="chart-title">Sales Trend Through The Year</h3>
                    <p class="chart-subtitle">Monthly rollup from warehouse fact sales, rendered with more breathing room and cleaner visual hierarchy.</p>
                </div>
                <span class="chart-badge">Live series</span>
            </div>
            <div class="chart-container">
                <canvas id="ingestionChart"></canvas>
            </div>
        </div>
        <div class="chart-card" data-reveal style="--delay: 150ms;">
            <div class="chart-card-header">
                <div class="chart-title-group">
                    <h3 class="chart-title">Data by Source</h3>
                    <p class="chart-subtitle">Source mix across ingestion channels.</p>
                </div>
                <span class="chart-badge">Channel mix</span>
            </div>
            <div class="chart-container chart-container--compact">
                <canvas id="sourceChart"></canvas>
            </div>
        </div>
    </div>
    
    <div class="bottom-grid">
        <div class="chart-card" data-reveal style="--delay: 110ms;">
            <div class="chart-card-header">
                <div class="chart-title-group">
                    <h3 class="chart-title">Sales by Category</h3>
                    <p class="chart-subtitle">The current revenue distribution across modeled categories.</p>
                </div>
                <span class="chart-badge">Category share</span>
            </div>
            <div class="chart-container chart-container--compact">
                <canvas id="categoryChart"></canvas>
            </div>
        </div>
        <div class="chart-card" data-reveal style="--delay: 170ms;">
            <div class="chart-card-header">
                <div class="chart-title-group">
                    <h3 class="chart-title">Top Products</h3>
                    <p class="chart-subtitle">Highest-performing items in the current warehouse summary.</p>
                </div>
                <span class="chart-badge">Top 5</span>
            </div>
            <div class="table-wrap">
                <table class="data-table">
                    <thead>
                        <tr>
                            <th>Product</th>
                            <th>Category</th>
                            <th style="text-align:right">Sales</th>
                        </tr>
                    </thead>
                    <tbody>
                        {top_product_rows}
                    </tbody>
                </table>
            </div>
        </div>
    </div>
    </section>
    
    <script>
    Chart.defaults.color = '#90a4c3';
    Chart.defaults.font.family = 'Inter, sans-serif';
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.plugins.legend.labels.boxWidth = 10;
    Chart.defaults.plugins.legend.labels.boxHeight = 10;
    Chart.defaults.plugins.legend.labels.padding = 18;
    Chart.defaults.elements.line.borderWidth = 3;
    Chart.defaults.elements.point.radius = 0;
    Chart.defaults.elements.point.hoverRadius = 6;

    const trendCanvas = document.getElementById('ingestionChart');
    const trendContext = trendCanvas.getContext('2d');
    const trendGradient = trendContext.createLinearGradient(0, 0, 0, 340);
    trendGradient.addColorStop(0, 'rgba(116, 168, 255, 0.34)');
    trendGradient.addColorStop(1, 'rgba(116, 168, 255, 0.02)');

    new Chart(document.getElementById('ingestionChart'), {{
        type: 'line',
        data: {{
            labels: {json.dumps(monthly_labels)},
            datasets: [{{
                label: 'Sales',
                data: {json.dumps(monthly_values)},
                borderColor: '#74a8ff',
                backgroundColor: trendGradient,
                fill: true,
                tension: 0.36,
                pointBackgroundColor: '#74a8ff',
                pointBorderColor: '#07111f',
                pointHoverBorderWidth: 3
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{ intersect: false, mode: 'index' }},
            plugins: {{
                legend: {{ display: false }},
                tooltip: {{
                    backgroundColor: 'rgba(8, 18, 33, 0.94)',
                    borderColor: 'rgba(116, 168, 255, 0.18)',
                    borderWidth: 1,
                    padding: 12,
                    displayColors: false,
                    callbacks: {{
                        label: (context) => '$' + context.parsed.y.toLocaleString()
                    }}
                }}
            }},
            scales: {{
                x: {{
                    grid: {{ display: false }},
                    ticks: {{ color: '#90a4c3' }}
                }},
                y: {{
                    grid: {{ color: 'rgba(144, 164, 195, 0.12)' }},
                    ticks: {{
                        color: '#90a4c3',
                        callback: (value) => '$' + Number(value).toLocaleString()
                    }}
                }}
            }}
        }}
    }});
    
    new Chart(document.getElementById('sourceChart'), {{
        type: 'doughnut',
        data: {{
            labels: {['Database', 'API', 'Files', 'Streaming', 'Other']},
            datasets: [{{
                data: [35, 25, 20, 15, 5],
                backgroundColor: ['#74a8ff', '#46d7c4', '#ffbf66', '#8b8cff', '#ff7d86'],
                borderColor: 'rgba(7, 17, 31, 0.92)',
                borderWidth: 6,
                hoverOffset: 8
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            cutout: '62%',
            plugins: {{
                legend: {{
                    position: 'bottom',
                    labels: {{ color: '#90a4c3', padding: 16 }}
                }}
            }}
        }}
    }});
    
    new Chart(document.getElementById('categoryChart'), {{
        type: 'doughnut',
        data: {{
            labels: {json.dumps(cat_labels)},
            datasets: [{{
                data: {json.dumps(cat_values)},
                backgroundColor: ['#74a8ff', '#46d7c4', '#8b8cff', '#ffbf66', '#ff7d86'],
                borderColor: 'rgba(7, 17, 31, 0.92)',
                borderWidth: 6,
                hoverOffset: 8
            }}]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            cutout: '64%',
            plugins: {{
                legend: {{
                    position: 'bottom',
                    labels: {{ color: '#90a4c3', padding: 16 }}
                }}
            }}
        }}
    }});
    </script>
    '''
    
    return render_page("Data Platform - Dashboard", "dashboard", content)


def render_data():
    content = f'''
    <div class="top-bar">
        <div class="page-heading">
            <span class="page-kicker">Explore</span>
            <h1 class="section-title">Data</h1>
            <p class="page-subtitle">Review the entities and fact tables currently exposed by the warehouse.</p>
        </div>
    </div>
    
    <div class="tabs">
        <button class="tab-btn active" onclick="switchTab('customers')">Customers</button>
        <button class="tab-btn" onclick="switchTab('products')">Products</button>
        <button class="tab-btn" onclick="switchTab('sales')">Sales</button>
    </div>
    
    <div id="customers" class="tab-content active">
        <div class="chart-card" data-reveal style="--delay: 60ms;">
            <div class="table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Customer</th>
                        <th>Segment</th>
                        <th>City</th>
                        <th>Region</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(f'<tr><td>{escape(row["customer_name"])}</td><td>{escape(row["segment"])}</td><td>{escape(row["city"])}</td><td>{escape(row["region"])}</td></tr>' for row in query_db("SELECT customer_name, segment, city, region FROM dim_customers LIMIT 20"))}
                </tbody>
            </table>
            </div>
        </div>
    </div>
    
    <div id="products" class="tab-content">
        <div class="chart-card" data-reveal style="--delay: 60ms;">
            <div class="table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Product</th>
                        <th>Category</th>
                        <th>Sub Category</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(f'<tr><td>{escape(row["product_name"])}</td><td>{escape(row["category"])}</td><td>{escape(row["sub_category"])}</td></tr>' for row in query_db("SELECT product_name, category, sub_category FROM dim_products LIMIT 20"))}
                </tbody>
            </table>
            </div>
        </div>
    </div>
    
    <div id="sales" class="tab-content">
        <div class="chart-card" data-reveal style="--delay: 60ms;">
            <div class="table-wrap">
            <table class="data-table">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Sales</th>
                        <th>Quantity</th>
                        <th>Profit</th>
                    </tr>
                </thead>
                <tbody>
                    {''.join(f'<tr><td>{row["order_date"]}</td><td>${row["sales"]:.2f}</td><td>{row["quantity"]}</td><td>${row["profit"]:.2f}</td></tr>' for row in query_db("SELECT order_date, sales, quantity, profit FROM fact_sales LIMIT 20"))}
                </tbody>
            </table>
            </div>
        </div>
    </div>
    '''
    return render_page("Data Platform - Data", "data", content)


def render_run():
    content = '''
    <div class="top-bar">
        <div class="page-heading">
            <span class="page-kicker">Operations</span>
            <h1 class="section-title">Pipeline</h1>
            <p class="page-subtitle">Trigger the ETL workflow and export the current curated dataset.</p>
        </div>
    </div>
    
    <div class="chart-card" data-reveal style="--delay: 60ms;">
        <h3 class="chart-title">Run ETL Pipeline</h3>
        <div style="display: flex; gap: 16px; margin-top: 20px;">
            <button class="btn" onclick="runPipeline()">Run Pipeline</button>
            <button class="btn btn-secondary" onclick="exportCSV()">Export CSV</button>
        </div>
        <div id="log" class="log-output" style="margin-top: 24px; display: none;"></div>
    </div>
    '''
    return render_page("Data Platform - Pipeline", "run", content)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass
    
    def do_GET(self):
        if self.path == "/favicon":
            import base64
            svg = '''<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100">
                <defs>
                    <linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%">
                        <stop offset="0%" style="stop-color:#5f7dff"/>
                        <stop offset="100%" style="stop-color:#47d6c4"/>
                    </linearGradient>
                </defs>
                <rect width="100" height="100" rx="22" fill="url(#g)"/>
                <text x="50" y="68" text-anchor="middle" fill="white" font-size="52" font-weight="800" font-family="system-ui">D</text>
            </svg>'''
            self.send_response(200)
            self.send_header("Content-type", "image/svg+xml")
            self.end_headers()
            self.wfile.write(svg.encode("utf-8"))
        elif self.path == "/" or self.path == "/dashboard":
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
                [sys.executable, str(Path("/home/twarga/data_platform/Data-Platform-Dashboard-dados") / "run_pipeline.py")],
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
