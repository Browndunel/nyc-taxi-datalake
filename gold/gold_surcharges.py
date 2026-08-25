"""
Gold 1 — Évolution des suppléments tarifaires par type de véhicule.

Répond à : "pour chaque supplément apparu progressivement, montrer son
évolution (montant total ou moyen) par type de véhicule."

Point clé : on ne moyenne QUE sur les lignes où le supplément était
effectivement disponible ce mois-là (`<supplement>_disponible = true`) —
sinon les NULL des mois "avant introduction" fausseraient une moyenne
classique (Spark les ignore par défaut dans un avg(), ce qui est déjà
correct, mais on filtre quand même explicitement pour que ce soit lisible
et pour pouvoir aussi publier, à titre indicatif, la proportion de mois où
le supplément existait).
"""
import sys
from pyspark.sql import SparkSession, functions as F

HDFS_URI = "hdfs://namenode:9000"
SILVER_TAXI = f"{HDFS_URI}/silver/taxi"
GOLD_OUT = f"{HDFS_URI}/gold/surcharges_evolution"

SURCHARGES = ["improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee"]


def main():
    spark = SparkSession.builder.appName("gold-surcharges").master("spark://spark-master:7077").getOrCreate()
    taxi = spark.read.parquet(SILVER_TAXI)

    results = []
    for surcharge in SURCHARGES:
        agg = (
            taxi.filter(F.col(f"{surcharge}_disponible") == True)  # noqa: E712
            .groupBy("vehicle_type", "year", "month")
            .agg(
                F.avg(surcharge).alias("montant_moyen"),
                F.sum(surcharge).alias("montant_total"),
                F.count("*").alias("nb_courses"),
            )
            .withColumn("supplement", F.lit(surcharge))
        )
        results.append(agg)

    out = results[0]
    for r in results[1:]:
        out = out.unionByName(r)

    out.write.mode("overwrite").partitionBy("supplement").parquet(GOLD_OUT)
    print(f"Gold surcharges écrit : {GOLD_OUT}")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
