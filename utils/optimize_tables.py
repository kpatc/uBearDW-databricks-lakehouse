"""
Script d'optimisation des tables Delta Lake dans le schéma Gold
Exécute OPTIMIZE et ZORDER BY sur toutes les tables Gold
"""

import argparse
from pyspark.sql import SparkSession
from datetime import datetime

def optimize_gold_tables(catalog: str, schema: str):
    """
    Optimise toutes les tables du schéma Gold avec OPTIMIZE et ZORDER BY
    
    Args:
        catalog: Nom du catalog Unity Catalog
        schema: Nom du schéma (ubear_gold)
    """
    spark = SparkSession.builder.getOrCreate()
    
    print(f"🔧 Début de l'optimisation des tables Gold: {catalog}.{schema}")
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Configuration des tables avec leurs colonnes de ZORDER
    tables_config = {
        "dim_eater": ["eater_id"],
        "dim_merchant": ["merchant_id"],
        "dim_courier": ["courier_id"],
        "dim_location": ["location_id", "region_zone"],
        "dim_date": ["date_id"],
        "dim_time": ["time_id"],
        "trip_fact": ["order_placed_at", "eater_id", "merchant_id", "courier_id"]
    }
    
    results = []
    
    for table_name, zorder_columns in tables_config.items():
        try:
            full_table_name = f"{catalog}.{schema}.{table_name}"
            
            print(f"\n📊 Optimisation de {table_name}...")
            
            # Vérifier si la table existe
            if not spark.catalog.tableExists(full_table_name):
                print(f"⚠️  Table {full_table_name} n'existe pas, skip")
                results.append({
                    "table": table_name,
                    "status": "SKIPPED",
                    "reason": "Table does not exist"
                })
                continue
            
            # Compter les fichiers avant optimisation
            df_before = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
            num_files_before = df_before.select("numFiles").first()[0]
            size_before_mb = df_before.select("sizeInBytes").first()[0] / (1024 * 1024)
            
            print(f"   Fichiers avant: {num_files_before}, Taille: {size_before_mb:.2f} MB")
            
            # OPTIMIZE avec ZORDER BY
            zorder_cols = ", ".join(zorder_columns)
            optimize_query = f"OPTIMIZE {full_table_name} ZORDER BY ({zorder_cols})"
            
            print(f"   Exécution: OPTIMIZE ... ZORDER BY ({zorder_cols})")
            spark.sql(optimize_query)
            
            # Compter les fichiers après optimisation
            df_after = spark.sql(f"DESCRIBE DETAIL {full_table_name}")
            num_files_after = df_after.select("numFiles").first()[0]
            size_after_mb = df_after.select("sizeInBytes").first()[0] / (1024 * 1024)
            
            files_reduced = num_files_before - num_files_after
            reduction_pct = (files_reduced / num_files_before * 100) if num_files_before > 0 else 0
            
            print(f"   ✅ Fichiers après: {num_files_after}, Taille: {size_after_mb:.2f} MB")
            print(f"   📉 Réduction: {files_reduced} fichiers ({reduction_pct:.1f}%)")
            
            # ANALYZE TABLE pour mettre à jour les statistiques
            print(f"   Mise à jour des statistiques...")
            spark.sql(f"ANALYZE TABLE {full_table_name} COMPUTE STATISTICS FOR ALL COLUMNS")
            
            results.append({
                "table": table_name,
                "status": "SUCCESS",
                "files_before": num_files_before,
                "files_after": num_files_after,
                "files_reduced": files_reduced,
                "reduction_pct": round(reduction_pct, 2),
                "size_mb": round(size_after_mb, 2)
            })
            
        except Exception as e:
            print(f"   ❌ Erreur lors de l'optimisation de {table_name}: {str(e)}")
            results.append({
                "table": table_name,
                "status": "ERROR",
                "error": str(e)
            })
    
    # Résumé final
    print("\n" + "="*80)
    print("📊 RÉSUMÉ DE L'OPTIMISATION")
    print("="*80)
    
    success_count = sum(1 for r in results if r["status"] == "SUCCESS")
    error_count = sum(1 for r in results if r["status"] == "ERROR")
    skipped_count = sum(1 for r in results if r["status"] == "SKIPPED")
    
    print(f"✅ Succès: {success_count}")
    print(f"❌ Erreurs: {error_count}")
    print(f"⚠️  Skipped: {skipped_count}")
    
    if success_count > 0:
        total_files_reduced = sum(r.get("files_reduced", 0) for r in results if r["status"] == "SUCCESS")
        total_size_mb = sum(r.get("size_mb", 0) for r in results if r["status"] == "SUCCESS")
        print(f"\n📉 Total fichiers réduits: {total_files_reduced}")
        print(f"💾 Taille totale des tables optimisées: {total_size_mb:.2f} MB")
    
    print("\nDétails par table:")
    for result in results:
        if result["status"] == "SUCCESS":
            print(f"  • {result['table']}: {result['files_before']} → {result['files_after']} fichiers "
                  f"(-{result['reduction_pct']}%), {result['size_mb']} MB")
        elif result["status"] == "ERROR":
            print(f"  • {result['table']}: ❌ {result['error']}")
        else:
            print(f"  • {result['table']}: ⚠️ {result['reason']}")
    
    print("\n✅ Optimisation terminée!")
    print(f"⏰ Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Retourner les résultats pour validation
    return results


def main():
    """Point d'entrée principal du script"""
    parser = argparse.ArgumentParser(description="Optimise les tables Gold avec OPTIMIZE et ZORDER BY")
    parser.add_argument("--catalog", required=True, help="Nom du catalog Unity Catalog")
    parser.add_argument("--schema", required=True, help="Nom du schéma Gold")
    
    args = parser.parse_args()
    
    # Exécuter l'optimisation
    results = optimize_gold_tables(args.catalog, args.schema)
    
    # Vérifier si des erreurs sont survenues
    error_count = sum(1 for r in results if r["status"] == "ERROR")
    if error_count > 0:
        raise Exception(f"L'optimisation a échoué pour {error_count} table(s)")


if __name__ == "__main__":
    main()
