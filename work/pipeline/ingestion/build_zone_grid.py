"""
Script offline (à lancer une fois, pas dans le chemin critique du
docker-compose) qui transforme les géométries de taxi_zones.zip en une
table de correspondance "grille -> LocationID", pour pouvoir rattacher les
courses pré-juillet-2016 (coordonnées GPS brutes) à un LocationID sans
faire du point-in-polygon coûteux ligne à ligne dans Spark.

Principe : on découpe la bounding box de New York en cellules de
0.001° (~110m à cette latitude, largement plus fin que la plupart des
zones TLC), et pour chaque cellule on calcule dans quelle zone tombe son
centre (point-in-polygon avec geopandas, une seule fois, hors Spark).
Le résultat (quelques dizaines de milliers de lignes) est stocké en
Parquet dans /bronze/reference/taxi_zones/grid_lookup.parquet : Silver
n'aura plus qu'à faire un join classique (arrondi lat/lon -> LocationID),
une opération triviale à l'échelle de Spark.

Prérequis : que zones_ingest.py ait déjà déposé taxi_zones.zip dans HDFS.
"""
import io
import logging
import os
import sys
import zipfile

import geopandas as gpd
import numpy as np
import pandas as pd
from hdfs_paths import get_client, reference_zones_dir

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("build_zone_grid")

CACHE_DIR = os.environ.get("CACHE_DIR", "/tmp/nyc_taxi_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

GRID_RESOLUTION = 0.001

# Bounding box large autour de NYC (couvre largement les 5 boroughs +
# marge, pour ne rater aucune course même légèrement excentrée/bruitée).
LON_MIN, LON_MAX = -74.30, -73.65
LAT_MIN, LAT_MAX = 40.45, 40.95


def main():
    client = get_client()
    partition_dir = reference_zones_dir()

    local_zip = f"{CACHE_DIR}/taxi_zones.zip"
    client.download(f"{partition_dir}/taxi_zones.zip", local_zip, overwrite=True)

    # engine="pyogrio" explicite : le moteur par défaut (fiona) plante avec
    # geopandas 0.14.3 + fiona >= 1.10 ("module 'fiona' has no attribute
    # 'path'") — fiona 1.10 a retiré un module interne que cette version de
    # geopandas attend encore. pyogrio (backend GDAL alternatif) n'a pas ce
    # problème.
    #
    # En revanche, contrairement à fiona, pyogrio (GDAL/OGR) ne détecte pas
    # automatiquement un shapefile logé dans un sous-dossier à l'intérieur
    # du zip — et c'est justement le cas de taxi_zones.zip (TLC), dont le
    # contenu est sous "taxi_zones/taxi_zones.shp" et non à la racine du
    # zip. Sans chemin explicite, GDAL renvoie "not recognized as a
    # supported file format" en croyant que le zip lui-même n'est pas un
    # format supporté. On liste donc le zip pour trouver le .shp où qu'il
    # soit (racine ou sous-dossier), et on le référence explicitement via
    # la syntaxe "zip://<zip>!<chemin_interne>".
    with zipfile.ZipFile(local_zip) as zf:
        shp_candidates = [n for n in zf.namelist() if n.lower().endswith(".shp")]
    if not shp_candidates:
        raise RuntimeError(f"Aucun fichier .shp trouvé dans {local_zip}")
    shp_inner_path = shp_candidates[0]
    zones = gpd.read_file(f"zip://{local_zip}!{shp_inner_path}", engine="pyogrio")
    zones = zones.to_crs(epsg=4326)  # coordonnées GPS standard (lat/lon)

    lons = np.arange(LON_MIN, LON_MAX, GRID_RESOLUTION)
    lats = np.arange(LAT_MIN, LAT_MAX, GRID_RESOLUTION)
    log.info("Grille : %d x %d = %d cellules à résoudre", len(lons), len(lats), len(lons) * len(lats))

    grid_lon, grid_lat = np.meshgrid(lons, lats)
    # Indices ENTIERS de grille (round(lon/résolution), round(lat/résolution)),
    # pas les floats bruts : Silver joindra là-dessus. Un join sur des floats
    # arrondis se casserait silencieusement (ex: round(-73.978/0.001)*0.001
    # ne redonne PAS exactement -73.978 en binaire — l'égalité échoue sans
    # erreur, la ligne tombe juste hors du join). Avec des entiers, aucune
    # ambiguïté de représentation possible.
    grid_lon_idx = np.round(grid_lon / GRID_RESOLUTION).astype(np.int64)
    grid_lat_idx = np.round(grid_lat / GRID_RESOLUTION).astype(np.int64)
    grid_points = gpd.GeoDataFrame(
        {"grid_lon_idx": grid_lon_idx.ravel(), "grid_lat_idx": grid_lat_idx.ravel()},
        geometry=gpd.points_from_xy(grid_lon.ravel(), grid_lat.ravel()),
        crs="EPSG:4326",
    )

    joined = gpd.sjoin(grid_points, zones[["LocationID", "geometry"]], how="inner", predicate="within")
    result = joined[["grid_lon_idx", "grid_lat_idx", "LocationID"]].reset_index(drop=True)
    log.info("%d cellules rattachées à une zone (sur %d, le reste tombe hors polygones = hors NYC/eau).",
              len(result), len(grid_points))

    local_out = f"{CACHE_DIR}/grid_lookup.parquet"
    result.to_parquet(local_out, index=False)
    client.upload(f"{partition_dir}/grid_lookup.parquet", local_out, overwrite=True)
    log.info("grid_lookup.parquet déposé dans %s", partition_dir)


if __name__ == "__main__":
    sys.exit(main())