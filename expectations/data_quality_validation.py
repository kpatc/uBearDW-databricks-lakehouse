"""
Validation de la qualité des données pour les tables Gold
Vérifie l'intégrité, la complétude et la cohérence des données après le traitement batch
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, max as _max, min as _min, when, lit
from datetime import datetime
import json

def validate_gold_tables(catalog: str, schema: str):
    """
    Exécute les validations de qualité sur toutes les tables Gold
    
    Args:
        catalog: Nom du catalog Unity Catalog
        schema: Nom du schéma Gold
    
    Returns:
        dict: Résultats des validations avec statut et métriques
    """
    spark = SparkSession.builder.getOrCreate()
    
    print(f"🔍 Début de la validation qualité des données Gold: {catalog}.{schema}")
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    validation_results = {
        "timestamp": datetime.now().isoformat(),
        "catalog": catalog,
        "schema": schema,
        "tables": {}
    }
    
    # ========== 1. Validation dim_eater ==========
    print("📊 Validation dim_eater...")
    try:
        df_eater = spark.table(f"{catalog}.{schema}.dim_eater")
        
        total_eaters = df_eater.count()
        current_eaters = df_eater.filter(col("is_current") == True).count()
        null_eater_ids = df_eater.filter(col("eater_id").isNull()).count()
        invalid_loyalty_tier = df_eater.filter(
            ~col("loyalty_tier").isin(["bronze", "silver", "gold", "platinum"])
        ).count()
        
        validation_results["tables"]["dim_eater"] = {
            "total_records": total_eaters,
            "current_records": current_eaters,
            "checks": {
                "no_null_eater_id": null_eater_ids == 0,
                "valid_loyalty_tiers": invalid_loyalty_tier == 0,
                "has_data": total_eaters > 0
            },
            "metrics": {
                "null_eater_ids": null_eater_ids,
                "invalid_loyalty_tier": invalid_loyalty_tier
            },
            "status": "PASS" if (null_eater_ids == 0 and invalid_loyalty_tier == 0 and total_eaters > 0) else "FAIL"
        }
        
        print(f"   ✅ Total: {total_eaters}, Current: {current_eaters}")
        print(f"   {'✅' if null_eater_ids == 0 else '❌'} Null eater_id: {null_eater_ids}")
        print(f"   {'✅' if invalid_loyalty_tier == 0 else '❌'} Invalid loyalty_tier: {invalid_loyalty_tier}")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["dim_eater"] = {"status": "ERROR", "error": str(e)}
    
    # ========== 2. Validation dim_merchant ==========
    print("\n📊 Validation dim_merchant...")
    try:
        df_merchant = spark.table(f"{catalog}.{schema}.dim_merchant")
        
        total_merchants = df_merchant.count()
        current_merchants = df_merchant.filter(col("is_current") == True).count()
        null_merchant_ids = df_merchant.filter(col("merchant_id").isNull()).count()
        invalid_ratings = df_merchant.filter(
            (col("overall_rating") < 0) | (col("overall_rating") > 5)
        ).count()
        
        validation_results["tables"]["dim_merchant"] = {
            "total_records": total_merchants,
            "current_records": current_merchants,
            "checks": {
                "no_null_merchant_id": null_merchant_ids == 0,
                "valid_ratings": invalid_ratings == 0,
                "has_data": total_merchants > 0
            },
            "metrics": {
                "null_merchant_ids": null_merchant_ids,
                "invalid_ratings": invalid_ratings
            },
            "status": "PASS" if (null_merchant_ids == 0 and invalid_ratings == 0 and total_merchants > 0) else "FAIL"
        }
        
        print(f"   ✅ Total: {total_merchants}, Current: {current_merchants}")
        print(f"   {'✅' if null_merchant_ids == 0 else '❌'} Null merchant_id: {null_merchant_ids}")
        print(f"   {'✅' if invalid_ratings == 0 else '❌'} Invalid ratings (0-5): {invalid_ratings}")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["dim_merchant"] = {"status": "ERROR", "error": str(e)}
    
    # ========== 3. Validation dim_courier ==========
    print("\n📊 Validation dim_courier...")
    try:
        df_courier = spark.table(f"{catalog}.{schema}.dim_courier")
        
        total_couriers = df_courier.count()
        current_couriers = df_courier.filter(col("is_current") == True).count()
        null_courier_ids = df_courier.filter(col("courier_id").isNull()).count()
        invalid_rates = df_courier.filter(
            (col("on_time_delivery_rate") < 0) | (col("on_time_delivery_rate") > 100)
        ).count()
        
        validation_results["tables"]["dim_courier"] = {
            "total_records": total_couriers,
            "current_records": current_couriers,
            "checks": {
                "no_null_courier_id": null_courier_ids == 0,
                "valid_rates": invalid_rates == 0,
                "has_data": total_couriers > 0
            },
            "metrics": {
                "null_courier_ids": null_courier_ids,
                "invalid_rates": invalid_rates
            },
            "status": "PASS" if (null_courier_ids == 0 and invalid_rates == 0 and total_couriers > 0) else "FAIL"
        }
        
        print(f"   ✅ Total: {total_couriers}, Current: {current_couriers}")
        print(f"   {'✅' if null_courier_ids == 0 else '❌'} Null courier_id: {null_courier_ids}")
        print(f"   {'✅' if invalid_rates == 0 else '❌'} Invalid rates (0-100%): {invalid_rates}")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["dim_courier"] = {"status": "ERROR", "error": str(e)}
    
    # ========== 4. Validation dim_location ==========
    print("\n📊 Validation dim_location...")
    try:
        df_location = spark.table(f"{catalog}.{schema}.dim_location")
        
        total_locations = df_location.count()
        null_location_ids = df_location.filter(col("location_id").isNull()).count()
        invalid_coords = df_location.filter(
            (col("latitude") < -90) | (col("latitude") > 90) |
            (col("longitude") < -180) | (col("longitude") > 180)
        ).count()
        null_geohash = df_location.filter(col("geohash").isNull()).count()
        
        validation_results["tables"]["dim_location"] = {
            "total_records": total_locations,
            "checks": {
                "no_null_location_id": null_location_ids == 0,
                "valid_coordinates": invalid_coords == 0,
                "has_geohash": null_geohash == 0,
                "has_data": total_locations > 0
            },
            "metrics": {
                "null_location_ids": null_location_ids,
                "invalid_coords": invalid_coords,
                "null_geohash": null_geohash
            },
            "status": "PASS" if (null_location_ids == 0 and invalid_coords == 0 and total_locations > 0) else "FAIL"
        }
        
        print(f"   ✅ Total: {total_locations}")
        print(f"   {'✅' if null_location_ids == 0 else '❌'} Null location_id: {null_location_ids}")
        print(f"   {'✅' if invalid_coords == 0 else '❌'} Invalid coordinates: {invalid_coords}")
        print(f"   {'✅' if null_geohash == 0 else '❌'} Null geohash: {null_geohash}")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["dim_location"] = {"status": "ERROR", "error": str(e)}
    
    # ========== 5. Validation trip_fact ==========
    print("\n📊 Validation trip_fact...")
    try:
        df_fact = spark.table(f"{catalog}.{schema}.trip_fact")
        
        total_trips = df_fact.count()
        null_trip_ids = df_fact.filter(col("trip_id").isNull()).count()
        null_foreign_keys = df_fact.filter(
            col("eater_id").isNull() | col("merchant_id").isNull()
        ).count()
        negative_amounts = df_fact.filter(
            (col("order_total_amount") < 0) | (col("delivery_fee") < 0)
        ).count()
        
        # Vérifier l'intégrité référentielle avec dim_eater
        df_eater_current = spark.table(f"{catalog}.{schema}.dim_eater").filter(col("is_current") == True)
        orphan_eaters = df_fact.join(
            df_eater_current,
            df_fact.eater_id == df_eater_current.eater_id,
            "left_anti"
        ).count()
        
        validation_results["tables"]["trip_fact"] = {
            "total_records": total_trips,
            "checks": {
                "no_null_trip_id": null_trip_ids == 0,
                "no_null_foreign_keys": null_foreign_keys == 0,
                "no_negative_amounts": negative_amounts == 0,
                "referential_integrity": orphan_eaters == 0,
                "has_data": total_trips > 0
            },
            "metrics": {
                "null_trip_ids": null_trip_ids,
                "null_foreign_keys": null_foreign_keys,
                "negative_amounts": negative_amounts,
                "orphan_eaters": orphan_eaters
            },
            "status": "PASS" if (null_trip_ids == 0 and null_foreign_keys == 0 and 
                                negative_amounts == 0 and total_trips > 0) else "FAIL"
        }
        
        print(f"   ✅ Total trips: {total_trips}")
        print(f"   {'✅' if null_trip_ids == 0 else '❌'} Null trip_id: {null_trip_ids}")
        print(f"   {'✅' if null_foreign_keys == 0 else '❌'} Null foreign keys: {null_foreign_keys}")
        print(f"   {'✅' if negative_amounts == 0 else '❌'} Negative amounts: {negative_amounts}")
        print(f"   {'✅' if orphan_eaters == 0 else '❌'} Orphan eaters: {orphan_eaters}")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["trip_fact"] = {"status": "ERROR", "error": str(e)}
    
    # ========== 6. Validation dim_date ==========
    print("\n📊 Validation dim_date...")
    try:
        df_date = spark.table(f"{catalog}.{schema}.dim_date")
        
        total_dates = df_date.count()
        expected_dates = 3653  # 2020-2030 (11 ans)
        
        validation_results["tables"]["dim_date"] = {
            "total_records": total_dates,
            "expected_records": expected_dates,
            "checks": {
                "correct_count": total_dates == expected_dates,
                "has_data": total_dates > 0
            },
            "status": "PASS" if total_dates == expected_dates else "FAIL"
        }
        
        print(f"   {'✅' if total_dates == expected_dates else '❌'} Total: {total_dates} (expected: {expected_dates})")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["dim_date"] = {"status": "ERROR", "error": str(e)}
    
    # ========== 7. Validation dim_time ==========
    print("\n📊 Validation dim_time...")
    try:
        df_time = spark.table(f"{catalog}.{schema}.dim_time")
        
        total_times = df_time.count()
        expected_times = 1440  # Minutes in a day
        
        validation_results["tables"]["dim_time"] = {
            "total_records": total_times,
            "expected_records": expected_times,
            "checks": {
                "correct_count": total_times == expected_times,
                "has_data": total_times > 0
            },
            "status": "PASS" if total_times == expected_times else "FAIL"
        }
        
        print(f"   {'✅' if total_times == expected_times else '❌'} Total: {total_times} (expected: {expected_times})")
        
    except Exception as e:
        print(f"   ❌ Erreur: {str(e)}")
        validation_results["tables"]["dim_time"] = {"status": "ERROR", "error": str(e)}
    
    # ========== Résumé final ==========
    print("\n" + "="*80)
    print("📊 RÉSUMÉ DE LA VALIDATION")
    print("="*80)
    
    total_tables = len(validation_results["tables"])
    passed_tables = sum(1 for t in validation_results["tables"].values() if t.get("status") == "PASS")
    failed_tables = sum(1 for t in validation_results["tables"].values() if t.get("status") == "FAIL")
    error_tables = sum(1 for t in validation_results["tables"].values() if t.get("status") == "ERROR")
    
    validation_results["summary"] = {
        "total_tables": total_tables,
        "passed": passed_tables,
        "failed": failed_tables,
        "errors": error_tables,
        "overall_status": "PASS" if failed_tables == 0 and error_tables == 0 else "FAIL"
    }
    
    print(f"✅ Passed: {passed_tables}/{total_tables}")
    print(f"❌ Failed: {failed_tables}/{total_tables}")
    print(f"⚠️  Errors: {error_tables}/{total_tables}")
    
    print("\nDétails par table:")
    for table_name, result in validation_results["tables"].items():
        status_icon = "✅" if result["status"] == "PASS" else "❌" if result["status"] == "FAIL" else "⚠️"
        print(f"  {status_icon} {table_name}: {result['status']}")
    
    print(f"\n{'✅ VALIDATION GLOBALE: PASS' if validation_results['summary']['overall_status'] == 'PASS' else '❌ VALIDATION GLOBALE: FAIL'}")
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Sauvegarder les résultats dans une table de logs
    try:
        results_json = json.dumps(validation_results, indent=2)
        print(f"\n💾 Résultats de validation:\n{results_json}")
    except Exception as e:
        print(f"\n⚠️ Impossible de sérialiser les résultats: {str(e)}")
    
    return validation_results


def main():
    """Point d'entrée principal pour Databricks Job"""
    # Récupérer les paramètres depuis dbutils
    spark = SparkSession.builder.getOrCreate()
    
    try:
        dbutils = spark.conf.get("spark.databricks.service.client.enabled")
        from pyspark.dbutils import DBUtils
        dbutils = DBUtils(spark)
        
        catalog = dbutils.widgets.get("catalog")
        gold_schema = dbutils.widgets.get("gold_schema")
    except:
        # Valeurs par défaut si pas de widgets
        catalog = "ubear_catalog"
        gold_schema = "ubear_gold"
    
    # Exécuter la validation
    results = validate_gold_tables(catalog, gold_schema)
    
    # Lever une exception si la validation échoue
    if results["summary"]["overall_status"] == "FAIL":
        failed_tables = [t for t, r in results["tables"].items() if r["status"] != "PASS"]
        raise Exception(f"Validation failed for tables: {', '.join(failed_tables)}")


if __name__ == "__main__":
    main()
