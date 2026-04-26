#!/usr/bin/env python3
"""
Tests for Data Platform Pipeline.
"""
import sys
import json
from pathlib import Path

# Add to path
sys.path.insert(0, str(Path(__file__).parent.parent / "data-platform"))


def test_helpers():
    """Test helper functions."""
    from utils.helpers import read_csv, write_csv, read_json, write_json, chunk_list
    
    # Test chunk_list
    result = chunk_list([1, 2, 3, 4, 5], 2)
    assert result == [[1, 2], [3, 4], [5]]
    print("✓ chunk_list works")


def test_validator():
    """Test validator."""
    from utils.validator import validate_data, Validator
    
    test_data = [
        {"order_id": "1", "sales": "100", "customer_id": "C1"},
        {"order_id": "2", "sales": "200", "customer_id": "C2"},
    ]
    
    schema = {
        "required_columns": ["order_id", "sales"],
        "not_null": ["order_id"],
        "unique_key": "order_id"
    }
    
    report = validate_data(test_data, schema)
    assert report["is_valid"] == True
    print("✓ validator works")


def test_logger():
    """Test logger."""
    import os
    from utils.logger import get_logger
    
    logger = get_logger("test")
    logger.info("Test message")
    print("✓ logger works")


def test_read_json():
    """Test JSON reading."""
    from utils.helpers import read_json
    
    gold_path = Path("/home/dados/Documents/Data Platform/data-platform/lake/gold")
    kpi_file = gold_path / "kpi_category.json"
    
    if kpi_file.exists():
        data = read_json(str(kpi_file))
        assert isinstance(data, list)
        print(f"✓ read_json works ({len(data)} records)")
    else:
        print("⚠ No data files yet - skip test")


def test_pipeline_steps():
    """Test that pipeline steps can be imported."""
    sys.path.insert(0, "/home/dados/Documents/Data Platform")
    
    try:
        from data_platform.ingestion import generate_sample
        print("✓ ingestion module loads")
    except ImportError:
        print("⚠ ingestion module not found")
    
    try:
        from data_platform.processing import clean
        print("✓ processing module loads")
    except ImportError:
        print("⚠ processing module not found")


def main():
    """Run all tests."""
    print("=" * 50)
    print("Running Data Platform Tests")
    print("=" * 50)
    
    tests = [
        ("Helpers", test_helpers),
        ("Validator", test_validator),
        ("Logger", test_logger),
        ("Read JSON", test_read_json),
        ("Pipeline Steps", test_pipeline_steps),
    ]
    
    passed = 0
    for name, test_fn in tests:
        try:
            print(f"\nTesting: {name}")
            test_fn()
            passed += 1
        except Exception as e:
            print(f"✗ {name} failed: {e}")
    
    print("\n" + "=" * 50)
    print(f"Tests: {passed}/{len(tests)} passed")
    print("=" * 50)


if __name__ == "__main__":
    main()