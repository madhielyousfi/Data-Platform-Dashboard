#!/usr/bin/env python3
"""
Ingestion module for Data Platform.
API for data ingestion.
"""
from .csv_ingest import CSVIngest
from .api_ingest import APIIngest

__all__ = ['CSVIngest', 'APIIngest']