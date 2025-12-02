"""
Fonctions utilitaires pour transformations de données
uBear Eats Data Warehouse - Databricks Lakehouse
"""

from pyspark.sql.functions import (
    col, sha2, concat_ws, current_timestamp, lit, 
    to_date, trim, upper, regexp_replace, when, coalesce
)
from pyspark.sql import DataFrame
from typing import List


def add_row_hash(df: DataFrame, columns: List[str]) -> DataFrame:
    """
    Ajoute une colonne row_hash calculée à partir des colonnes spécifiées.
    Utilisé pour la détection de changements dans SCD2.
    
    Args:
        df: DataFrame source
        columns: Liste des colonnes à inclure dans le hash
        
    Returns:
        DataFrame avec la colonne row_hash ajoutée
    """
    return df.withColumn(
        'row_hash',
        sha2(concat_ws('||', *[col(c).cast('string') for c in columns]), 256)
    )


def add_scd2_columns(df: DataFrame, version: int = 1) -> DataFrame:
    """
    Ajoute les colonnes standard SCD2 à un DataFrame.
    
    Args:
        df: DataFrame source
        version: Numéro de version initial (défaut: 1)
        
    Returns:
        DataFrame avec colonnes SCD2 ajoutées
    """
    return (df
        .withColumn('effective_start_date', current_timestamp())
        .withColumn('effective_end_date', lit(None).cast('timestamp'))
        .withColumn('is_current', lit(True))
        .withColumn('version_number', lit(version))
    )


def clean_address_data(df: DataFrame) -> DataFrame:
    """
    Nettoie et normalise les données d'adresse.
    
    Args:
        df: DataFrame avec colonnes d'adresse
        
    Returns:
        DataFrame avec adresses nettoyées
    """
    address_columns = [
        'address_line_1', 'address_line_2', 
        'city', 'state_province', 'country'
    ]
    
    result_df = df
    
    for addr_col in address_columns:
        if addr_col in df.columns:
            result_df = result_df.withColumn(
                addr_col,
                trim(col(addr_col))
            )
    
    # Normaliser postal_code (enlever caractères non-numériques)
    if 'postal_code' in df.columns:
        result_df = result_df.withColumn(
            'postal_code',
            regexp_replace(col('postal_code'), '[^0-9]', '')
        )
    
    # Normaliser country en majuscules
    if 'country' in df.columns:
        result_df = result_df.withColumn(
            'country',
            upper(trim(col('country')))
        )
    
    return result_df


def clean_email(df: DataFrame, email_col: str = 'email') -> DataFrame:
    """
    Nettoie et normalise les adresses email.
    
    Args:
        df: DataFrame avec colonne email
        email_col: Nom de la colonne email
        
    Returns:
        DataFrame avec emails nettoyés
    """
    return df.withColumn(
        email_col,
        trim(upper(col(email_col)))
    )


def add_date_partition(df: DataFrame, timestamp_col: str) -> DataFrame:
    """
    Ajoute une colonne date_partition dérivée d'un timestamp.
    
    Args:
        df: DataFrame source
        timestamp_col: Nom de la colonne timestamp
        
    Returns:
        DataFrame avec date_partition ajoutée
    """
    return df.withColumn(
        'date_partition',
        to_date(col(timestamp_col))
    )


def calculate_metrics_columns(df: DataFrame) -> DataFrame:
    """
    Calcule les colonnes de métriques dérivées pour trip_fact.
    
    Args:
        df: DataFrame avec colonnes de montants et timestamps
        
    Returns:
        DataFrame avec métriques calculées
    """
    result_df = df
    
    # Calculer total_amount si non présent
    if 'total_amount' not in df.columns and all(
        col_name in df.columns 
        for col_name in ['subtotal_amount', 'delivery_fee', 'service_fee', 'tax_amount', 'tip_amount']
    ):
        result_df = result_df.withColumn(
            'total_amount',
            coalesce(col('subtotal_amount'), lit(0)) +
            coalesce(col('delivery_fee'), lit(0)) +
            coalesce(col('service_fee'), lit(0)) +
            coalesce(col('tax_amount'), lit(0)) +
            coalesce(col('tip_amount'), lit(0)) -
            coalesce(col('discount_amount'), lit(0))
        )
    
    return result_df


def derive_region_partition(df: DataFrame, city_col: str = 'city') -> DataFrame:
    """
    Dérive region_partition à partir de la ville.
    Logique simplifiée - à adapter selon vos régions réelles.
    
    Args:
        df: DataFrame avec colonne city
        city_col: Nom de la colonne city
        
    Returns:
        DataFrame avec region_partition ajoutée
    """
    return df.withColumn(
        'region_partition',
        when(col(city_col).isin(['New York', 'Boston', 'Philadelphia']), 'NORTHEAST')
        .when(col(city_col).isin(['San Francisco', 'Los Angeles', 'Seattle']), 'WEST')
        .when(col(city_col).isin(['Chicago', 'Detroit', 'Minneapolis']), 'MIDWEST')
        .when(col(city_col).isin(['Atlanta', 'Miami', 'Dallas']), 'SOUTH')
        .otherwise('OTHER')
    )


def parse_debezium_envelope(df: DataFrame, schema) -> DataFrame:
    """
    Parse l'enveloppe Debezium CDC et extrait payload.after.
    
    Args:
        df: DataFrame avec colonne json_str (message Kafka)
        schema: StructType définissant le schéma du payload
        
    Returns:
        DataFrame avec données parsées
    """
    from pyspark.sql.functions import from_json, get_json_object
    
    return (df
        .withColumn('payload_after', get_json_object(col('json_str'), '$.payload.after'))
        .withColumn('parsed', from_json(col('payload_after'), schema))
    )


def safe_cast_numeric(df: DataFrame, column: str, target_type: str = 'decimal(12,2)') -> DataFrame:
    """
    Cast sécurisé d'une colonne vers un type numérique avec gestion des nulls.
    
    Args:
        df: DataFrame source
        column: Nom de la colonne à caster
        target_type: Type cible (ex: 'decimal(12,2)', 'bigint')
        
    Returns:
        DataFrame avec colonne castée
    """
    return df.withColumn(
        column,
        col(column).cast(target_type)
    )
