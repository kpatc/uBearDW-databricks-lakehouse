"""
uBear Eats Data Warehouse - Expectations Package
Règles de qualité de données pour Delta Live Tables
"""

from .data_quality import (
    BRONZE_EXPECTATIONS,
    SILVER_EXPECTATIONS,
    GOLD_EXPECTATIONS,
    apply_expectations,
    get_quarantine_expectations
)

__all__ = [
    'BRONZE_EXPECTATIONS',
    'SILVER_EXPECTATIONS',
    'GOLD_EXPECTATIONS',
    'apply_expectations',
    'get_quarantine_expectations'
]

__version__ = '1.0.0'
