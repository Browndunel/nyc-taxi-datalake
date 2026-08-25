"""
Convention de nommage Bronze + petits utilitaires HDFS communs à tous les
scripts d'ingestion (taxi, météo, référentiel zones).

Convention (répond aux 3 questions du sujet SANS lire le contenu des
fichiers, seulement en listant des chemins) :

  /bronze/taxi/vehicle_type=<yellow|green|fhv|fhvhv>/year=<YYYY>/month=<MM>/
      <vehicle_type>_tripdata_<YYYY>-<MM>.parquet
      _SUCCESS                      <- marker JSON (source, date de
                                        téléchargement, taille, nb de lignes)

  /bronze/weather/year=<YYYY>/month=<MM>/
      weather_<YYYY>-<MM>.parquet
      _SUCCESS

  /bronze/reference/taxi_zones/
      taxi_zone_lookup.csv
      taxi_zones.zip
      _SUCCESS

- "Quels types de véhicules ai-je déjà ingérés ?"
      -> lister les sous-dossiers de /bronze/taxi/
- "Pour quelle période ai-je des données pour un type donné ?"
      -> lister les sous-dossiers year=/month= sous /bronze/taxi/vehicle_type=X/
- "Un mois précis d'un type précis a-t-il déjà été ingéré ?"
      -> tester l'existence du fichier _SUCCESS dans la partition correspondante

Aucune de ces réponses ne nécessite d'ouvrir un fichier de données.
"""
import json
import os
from datetime import datetime, timezone

from hdfs import InsecureClient

NAMENODE_HOST = os.environ.get("HDFS_NAMENODE_HOST", "namenode")
WEBHDFS_PORT = os.environ.get("HDFS_NAMENODE_WEBHDFS_PORT", "9870")
HDFS_USER = os.environ.get("HDFS_USER", "root")

BRONZE_ROOT = "/bronze"


def get_client():
    url = f"http://{NAMENODE_HOST}:{WEBHDFS_PORT}"
    return InsecureClient(url, user=HDFS_USER)


def taxi_partition_dir(vehicle_type, year, month):
    return f"{BRONZE_ROOT}/taxi/vehicle_type={vehicle_type}/year={year:04d}/month={month:02d}"


def weather_partition_dir(year, month):
    return f"{BRONZE_ROOT}/weather/year={year:04d}/month={month:02d}"


def reference_zones_dir():
    return f"{BRONZE_ROOT}/reference/taxi_zones"


def is_ingested(client, partition_dir):
    """Une partition est considérée ingérée si son marker _SUCCESS existe.
    C'est la SEULE chose qu'on lit pour décider de retélécharger ou pas."""
    marker = f"{partition_dir}/_SUCCESS"
    return client.status(marker, strict=False) is not None


def write_success_marker(client, partition_dir, metadata):
    """Ecrit un marker _SUCCESS contenant des métadonnées utiles (source,
    date d'ingestion, nb de lignes, taille) — jamais les données elles-mêmes,
    c'est juste un marqueur + un peu de traçabilité pour le débogage."""
    metadata = dict(metadata)
    metadata["ingested_at"] = datetime.now(timezone.utc).isoformat()
    marker = f"{partition_dir}/_SUCCESS"
    with client.write(marker, encoding="utf-8", overwrite=True) as writer:
        writer.write(json.dumps(metadata, indent=2))


def ensure_dir(client, path):
    client.makedirs(path)
