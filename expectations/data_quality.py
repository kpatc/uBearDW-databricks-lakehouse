"""
Règles de qualité de données (DLT Expectations)
uBear Eats Data Warehouse - Databricks Lakehouse

Définit toutes les contraintes de qualité pour les pipelines DLT.
"""

# =============================================================================
# BRONZE LAYER EXPECTATIONS
# =============================================================================

BRONZE_EXPECTATIONS = {
    'trip_events_bronze': [
        {
            'name': 'valid_order_id',
            'constraint': 'order_id IS NOT NULL',
            'action': 'drop'  # drop, fail, or None (just count)
        },
        {
            'name': 'valid_event_time',
            'constraint': 'event_time IS NOT NULL',
            'action': 'drop'
        }
    ],
    'eater_bronze': [
        {
            'name': 'valid_eater_id',
            'constraint': 'eater_id IS NOT NULL',
            'action': 'drop'
        }
    ],
    'merchant_bronze': [
        {
            'name': 'valid_merchant_id',
            'constraint': 'merchant_id IS NOT NULL',
            'action': 'drop'
        }
    ],
    'courier_bronze': [
        {
            'name': 'valid_courier_id',
            'constraint': 'courier_id IS NOT NULL',
            'action': 'drop'
        }
    ]
}


# =============================================================================
# SILVER LAYER EXPECTATIONS
# =============================================================================

SILVER_EXPECTATIONS = {
    'trip_events_silver': [
        {
            'name': 'valid_order_id',
            'constraint': 'order_id IS NOT NULL',
            'action': 'drop'
        },
        {
            'name': 'valid_total_amount',
            'constraint': 'total_amount >= 0',
            'action': 'drop'
        },
        {
            'name': 'valid_eater_id',
            'constraint': 'eater_id IS NOT NULL',
            'action': None  # Log only
        },
        {
            'name': 'valid_merchant_id',
            'constraint': 'merchant_id IS NOT NULL',
            'action': None
        },
        {
            'name': 'valid_distance',
            'constraint': 'distance_miles IS NULL OR (distance_miles >= 0 AND distance_miles < 100)',
            'action': None
        },
        {
            'name': 'valid_ratings',
            'constraint': 'eater_rating IS NULL OR (eater_rating >= 1 AND eater_rating <= 5)',
            'action': None
        }
    ],
    'eater_silver': [
        {
            'name': 'valid_eater_id',
            'constraint': 'eater_id IS NOT NULL',
            'action': 'drop'
        },
        {
            'name': 'valid_email',
            'constraint': "email IS NOT NULL AND email LIKE '%@%'",
            'action': 'drop'
        },
        {
            'name': 'valid_name',
            'constraint': 'first_name IS NOT NULL AND last_name IS NOT NULL',
            'action': None
        },
        {
            'name': 'valid_email_format',
            'constraint': "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Z|a-z]{2,}$'",
            'action': None
        }
    ],
    'merchant_silver': [
        {
            'name': 'valid_merchant_id',
            'constraint': 'merchant_id IS NOT NULL',
            'action': 'drop'
        },
        {
            'name': 'valid_merchant_name',
            'constraint': 'merchant_name IS NOT NULL',
            'action': None
        },
        {
            'name': 'valid_city',
            'constraint': 'city IS NOT NULL',
            'action': None
        }
    ],
    'courier_silver': [
        {
            'name': 'valid_courier_id',
            'constraint': 'courier_id IS NOT NULL',
            'action': 'drop'
        },
        {
            'name': 'valid_courier_name',
            'constraint': 'first_name IS NOT NULL AND last_name IS NOT NULL',
            'action': None
        },
        {
            'name': 'valid_vehicle',
            'constraint': 'vehicle_type IS NOT NULL',
            'action': None
        }
    ]
}


# =============================================================================
# GOLD LAYER EXPECTATIONS
# =============================================================================

GOLD_EXPECTATIONS = {
    'dim_eater': [
        {
            'name': 'valid_eater_id',
            'constraint': 'eater_id IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_scd2_dates',
            'constraint': 'effective_start_date IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_current_flag',
            'constraint': 'is_current IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_version',
            'constraint': 'version_number > 0',
            'action': 'fail'
        }
    ],
    'dim_merchant': [
        {
            'name': 'valid_merchant_id',
            'constraint': 'merchant_id IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_scd2_dates',
            'constraint': 'effective_start_date IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_current_flag',
            'constraint': 'is_current IS NOT NULL',
            'action': 'fail'
        }
    ],
    'dim_courier': [
        {
            'name': 'valid_courier_id',
            'constraint': 'courier_id IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_scd2_dates',
            'constraint': 'effective_start_date IS NOT NULL',
            'action': 'fail'
        }
    ],
    'trip_fact': [
        {
            'name': 'valid_trip_id',
            'constraint': 'order_id IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_foreign_keys',
            'constraint': 'eater_id IS NOT NULL AND merchant_id IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'valid_total_amount',
            'constraint': 'total_amount >= 0',
            'action': 'fail'
        },
        {
            'name': 'valid_timestamps',
            'constraint': 'order_placed_at IS NOT NULL',
            'action': None
        },
        {
            'name': 'logical_time_sequence',
            'constraint': 'delivered_at IS NULL OR delivered_at >= order_placed_at',
            'action': None
        }
    ],
    'dim_date': [
        {
            'name': 'valid_date_key',
            'constraint': 'date_key IS NOT NULL',
            'action': 'fail'
        },
        {
            'name': 'unique_date',
            'constraint': 'full_date IS NOT NULL',
            'action': 'fail'
        }
    ],
    'dim_time': [
        {
            'name': 'valid_time_key',
            'constraint': 'time_key IS NOT NULL',
            'action': 'fail'
        }
    ]
}


# =============================================================================
# FONCTIONS HELPER
# =============================================================================

def apply_expectations(dlt_decorator, table_name: str, layer: str):
    """
    Applique les expectations DLT à une table.
    
    Usage:
        @apply_expectations(dlt, 'eater_silver', 'silver')
        @dlt.table(name="eater_silver")
        def eater_silver():
            ...
    
    Args:
        dlt_decorator: Module dlt
        table_name: Nom de la table
        layer: Layer (bronze, silver, gold)
    """
    expectations_map = {
        'bronze': BRONZE_EXPECTATIONS,
        'silver': SILVER_EXPECTATIONS,
        'gold': GOLD_EXPECTATIONS
    }
    
    expectations = expectations_map.get(layer, {}).get(table_name, [])
    
    def decorator(func):
        for exp in expectations:
            if exp['action'] == 'drop':
                func = dlt_decorator.expect_or_drop(exp['name'], exp['constraint'])(func)
            elif exp['action'] == 'fail':
                func = dlt_decorator.expect_or_fail(exp['name'], exp['constraint'])(func)
            else:
                func = dlt_decorator.expect(exp['name'], exp['constraint'])(func)
        return func
    
    return decorator


def get_quarantine_expectations():
    """
    Retourne les expectations pour les tables de quarantaine.
    Tables de quarantaine contiennent les records rejetés.
    """
    return {
        'name': 'has_rejection_reason',
        'constraint': '_rescued_data IS NOT NULL OR _dlt_error IS NOT NULL'
    }
