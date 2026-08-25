"""
Gold 3 — Évolution du prix par km, par zone de départ, au fil des années
(données brutes pour un ridgeline plot).

Un ridgeline plot a besoin de la DISTRIBUTION complète des prix par
(zone, année), pas juste d'une moyenne — donc on ne peut pas agréger à
une seule ligne par groupe comme pour les autres analyses. Pour rester
gérable côté notebook (pandas + joypy), on :
  - ne garde que les zones de départ avec un volume significatif
    (>= MIN_COURSES courses sur toute la période, sinon la distribution
    n'a pas de sens statistique) ;
  - échantillonne les lignes par (zone, année) pour ne pas ramener des
    dizaines de millions de valeurs individuelles en mémoire pandas.
"""
import sys
from pyspark.sql import SparkSession, functions as F

HDFS_URI = "hdfs://namenode:9000"
SILVER_TAXI = f"{HDFS_URI}/silver/taxi"
GOLD_OUT = f"{HDFS_URI}/gold/price_per_km_by_zone_year"

MIN_COURSES = 5000
SAMPLE_FRACTION = 0.01
MAX_PRICE_PER_KM = 50  # filtre anti-outliers grossiers (erreurs de saisie, trajets à 0 distance)


def main():
    spark = SparkSession.builder.appName("gold-price-ridgeline").master("spark://spark-master:7077").getOrCreate()
    taxi = spark.read.parquet(SILVER_TAXI)

    base = taxi.filter(
        F.col("price_per_km").isNotNull()
        & (F.col("price_per_km") > 0)
        & (F.col("price_per_km") < MAX_PRICE_PER_KM)
        & F.col("pickup_zone_id").isNotNull()
    ).select("pickup_zone_id", "year", "price_per_km")

    volumes = base.groupBy("pickup_zone_id").agg(F.count("*").alias("nb_courses"))
    eligible_zones = volumes.filter(F.col("nb_courses") >= MIN_COURSES).select("pickup_zone_id")

    sampled = (
        base.join(F.broadcast(eligible_zones), on="pickup_zone_id", how="inner")
        .sampleBy("year", fractions={y: SAMPLE_FRACTION for y in range(2009, 2027)}, seed=42)
    )

    sampled.write.mode("overwrite").partitionBy("pickup_zone_id").parquet(GOLD_OUT)
    print(f"Gold price ridgeline écrit : {GOLD_OUT} ({sampled.count()} lignes échantillonnées)")
    spark.stop()


if __name__ == "__main__":
    sys.exit(main())
