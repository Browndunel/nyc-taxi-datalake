"""
Gold 2 — Flux pickup/dropoff (base du chord diagram).

Une table agrégée zone_depart x zone_arrivee x nb_courses suffit : le chord
diagram lui-même se dessine côté notebook (plotly/holoviews) à partir de
cette matrice, pas besoin de Spark pour le rendu visuel.

On agrège par arrondissement (Borough) plutôt que par zone fine (265
zones) par défaut : un chord diagram à 265 noeuds est illisible. On garde
quand même le détail par zone dans une seconde table, pour qui veut
zoomer.
"""
import sys
from pyspark.sql import SparkSession, functions as F

HDFS_URI = "hdfs://namenode:9000"
SILVER_TAXI = f"{HDFS_URI}/silver/taxi"
ZONES_LOOKUP = f"{HDFS_URI}/bronze/reference/taxi_zones/taxi_zone_lookup.csv"
GOLD_OUT_ZONE = f"{HDFS_URI}/gold/flows_by_zone"
GOLD_OUT_BOROUGH = f"{HDFS_URI}/gold/flows_by_borough"


def main():
    spark = SparkSession.builder.appName("gold-flows").master("spark://spark-master:7077").getOrCreate()
    taxi = spark.read.parquet(SILVER_TAXI)
    zones = spark.read.option("header", True).csv(ZONES_LOOKUP)

    flows_zone = (
        taxi.filter(F.col("pickup_zone_id").isNotNull() & F.col("dropoff_zone_id").isNotNull())
        .groupBy("vehicle_type", "pickup_zone_id", "dropoff_zone_id")
        .agg(F.count("*").alias("nb_courses"))
    )
    flows_zone.write.mode("overwrite").parquet(GOLD_OUT_ZONE)

    pu_borough = zones.select(F.col("LocationID").alias("pickup_zone_id"), F.col("Borough").alias("pickup_borough"))
    do_borough = zones.select(F.col("LocationID").alias("dropoff_zone_id"), F.col("Borough").alias("dropoff_borough"))

    flows_borough = (
        flows_zone.join(pu_borough, on="pickup_zone_id", how="left")
        .join(do_borough, on="dropoff_zone_id", how="left")
        .groupBy("vehicle_type", "pickup_borough", "dropoff_borough")
        .agg(F.sum("nb_courses").alias("nb_courses"))
    )
    flows_borough.write.mode("overwrite").parquet(GOLD_OUT_BOROUGH)

    print(f"Gold flows écrits : {GOLD_OUT_ZONE} et {GOLD_OUT_BOROUGH}")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
