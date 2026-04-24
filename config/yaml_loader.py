#!/usr/bin/env python3
"""
Simple YAML parser using Python stdlib.
"""
import re
import json
from pathlib import Path


class YAML:
    @staticmethod
    def load(path):
        with open(path, "r") as f:
            return YAML._parse(f.read())
    
    @staticmethod
    def _parse(text):
        result = {}
        current_key = None
        current_dict = result
        
        stack = [(result, 0)]
        
        for line in text.split("\n"):
            line = line.rstrip()
            if not line or line.startswith("#"):
                continue
            
            indent = len(line) - len(line.lstrip())
            line = line.lstrip()
            
            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()
                
                if not value:
                    new_dict = {}
                    stack[-1][0][key] = new_dict
                    stack.append((new_dict, indent))
                else:
                    if value.startswith("[") and value.endswith("]"):
                        stack[-1][0][key] = [x.strip() for x in value[1:-1].split(",")]
                    elif value.isdigit():
                        stack[-1][0][key] = int(value)
                    else:
                        stack[-1][0][key] = value
        
        return result


def load_yaml(path):
    return YAML.load(path)