"""
Gold 4 — table pour la fonction heatmap(annee) : fréquence des courses par
jour de la semaine x heure de la journée, par type de véhicule.

On matérialise une seule table Gold couvrant TOUTES les années (petite :
7 jours x 24h x 4 types x N années lignes), et la fonction heatmap(annee)
côté notebook ne fait qu'un filtre + un pivot pandas dessus — pas besoin
de relancer Spark à chaque appel de la fonction.
"""
import sys
from pyspark.sql import SparkSession, functions as F

HDFS_URI = "hdfs://namenode:9000"
SILVER_TAXI = f"{HDFS_URI}/silver/taxi"
GOLD_OUT = f"{HDFS_URI}/gold/rides_by_dow_hour"


def main():
    spark = SparkSession.builder.appName("gold-heatmap").master("spark://spark-master:7077").getOrCreate()
    taxi = spark.read.parquet(SILVER_TAXI)

    agg = (
        taxi.filter(F.col("pickup_datetime").isNotNull())
        .withColumn("day_of_week", F.dayofweek("pickup_datetime"))  # 1=dimanche ... 7=samedi (convention Spark)
        .withColumn("hour_of_day", F.hour("pickup_datetime"))
        .groupBy("vehicle_type", "year", "day_of_week", "hour_of_day")
        .agg(F.count("*").alias("nb_courses"))
    )

    agg.write.mode("overwrite").partitionBy("year").parquet(GOLD_OUT)
    print(f"Gold heatmap écrit : {GOLD_OUT}")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
