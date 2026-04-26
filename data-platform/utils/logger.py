#!/usr/bin/env python3
"""
Logger utility for Data Platform.
Provides logging with timestamps, file output, and console output.
"""
import logging
import os
from datetime import datetime
from pathlib import Path


class Logger:
    """Simple logger for the data platform."""
    
    def __init__(self, name: str = "data-platform", log_dir: str = "logs"):
        self.name = name
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        
        if not self.logger.handlers:
            # File handler
            log_file = self.log_dir / f"{name}.log"
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(logging.INFO)
            
            # Console handler
            console_handler = logging.StreamHandler()
            console_handler.setLevel(logging.INFO)
            
            # Format
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )
            file_handler.setFormatter(formatter)
            console_handler.setFormatter(formatter)
            
            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)
    
    def info(self, message: str):
        self.logger.info(message)
    
    def error(self, message: str):
        self.logger.error(message)
    
    def warning(self, message: str):
        self.logger.warning(message)
    
    def debug(self, message: str):
        self.logger.debug(message)


def get_logger(name: str = "data-platform") -> Logger:
    """Get or create a logger instance."""
    return Logger(name)


# Convenience functions
def log_start(step: str):
    """Log the start of a pipeline step."""
    logger = get_logger()
    logger.info(f"=" * 50)
    logger.info(f"START: {step}")
    logger.info(f"=" * 50)


def log_end(step: str, record_count: int = None):
    """Log the end of a pipeline step."""
    logger = get_logger()
    msg = f"END: {step}"
    if record_count is not None:
        msg += f" - {record_count} records"
    logger.info(msg)
    logger.info("-" * 50)


def log_error(step: str, error: Exception):
    """Log an error during pipeline execution."""
    logger = get_logger()
    logger.error(f"ERROR in {step}: {str(error)}")


def log_stats(data: dict):
    """Log statistics as key-value pairs."""
    logger = get_logger()
    logger.info("STATS:")
    for key, value in data.items():
        logger.info(f"  {key}: {value}")


if __name__ == "__main__":
    # Test the logger
    log = get_logger()
    log.info("Logger test message")
    log_start("Test Step")
    log_end("Test Step", 100)
    log_stats({"customers": 500, "products": 100, "sales": 5000})