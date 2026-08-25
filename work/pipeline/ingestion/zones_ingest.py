"""
Ingestion du référentiel des zones TLC (taxi_zone_lookup.csv + géométries
taxi_zones.zip). C'est cette table qui permet, en Silver, de rattacher une
course à une zone/arrondissement quelle que soit son époque :
  - post-juillet 2016 : la course référence déjà un LocationID -> jointure
    directe avec taxi_zone_lookup.csv.
  - pré-juillet 2016 : la course a des coordonnées GPS brutes -> on fait un
    point-in-polygon avec les géométries de taxi_zones.zip pour retrouver
    le LocationID correspondant, et on retombe ensuite sur la même table
    de référence. C'est ce qui unifie les deux époques sous un même concept
    de zone.

Donnée de référence quasi-statique (stable depuis 2016) : pas de
partitionnement par date, une seule ingestion suffit, ré-exécutable sans
risque (idempotente via le même mécanisme de marker _SUCCESS).
"""
import logging
import os
import sys

import requests
from hdfs_paths import get_client, reference_zones_dir, is_ingested, write_success_marker, ensure_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("zones_ingest")

TLC_MISC_URL = "https://d37ci6vzurychx.cloudfront.net/misc"
FILES = ["taxi_zone_lookup.csv", "taxi_zones.zip"]
CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp/nyc_taxi_cache")
os.makedirs(CACHE_DIR, exist_ok=True)


def main():
    client = get_client()
    partition_dir = reference_zones_dir()

    if is_ingested(client, partition_dir):
        log.info("Référentiel zones déjà ingéré, rien à faire (utiliser --force via un appel Python direct si besoin).")
        return

    ensure_dir(client, partition_dir)
    for fname in FILES:
        url = f"{TLC_MISC_URL}/{fname}"
        log.info("Téléchargement %s...", url)
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        local_path = f"{CACHE_DIR}/{fname}"
        with open(local_path, "wb") as f:
            f.write(resp.content)
        client.upload(f"{partition_dir}/{fname}", local_path, overwrite=True)

    write_success_marker(client, partition_dir, {"files": FILES, "source": TLC_MISC_URL})
    log.info("Référentiel zones ingéré avec succès.")


if __name__ == "__main__":
    sys.exit(main())
