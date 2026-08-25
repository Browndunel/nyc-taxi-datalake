"""
Petits utilitaires partagés par les cellules du notebook : lire une table
Gold (Parquet, potentiellement partitionné) depuis HDFS vers un DataFrame
pandas, sans réinventer la logique de téléchargement dans chaque cellule.
"""
import os
import shutil

import pandas as pd
from hdfs import InsecureClient

HDFS_WEBHDFS_URL = "http://namenode:9870"
LOCAL_CACHE_ROOT = "/home/jovyan/work/.gold_cache"


def read_gold_table(hdfs_path, refresh=False):
    """hdfs_path ex: '/gold/rides_by_dow_hour'. Télécharge (une fois, sauf
    refresh=True) le dossier Parquet complet en local puis le lit avec
    pandas (via pyarrow, qui gère nativement les sous-dossiers de
    partitionnement type year=2019/month=03/...)."""
    local_dir = os.path.join(LOCAL_CACHE_ROOT, hdfs_path.strip("/").replace("/", "_"))
    if refresh and os.path.exists(local_dir):
        shutil.rmtree(local_dir)
    if not os.path.exists(local_dir):
        os.makedirs(LOCAL_CACHE_ROOT, exist_ok=True)
        client = InsecureClient(HDFS_WEBHDFS_URL, user="root")
        client.download(hdfs_path, local_dir, overwrite=True)
    return pd.read_parquet(local_dir)
