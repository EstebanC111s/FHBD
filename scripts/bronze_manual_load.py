"""
bronze_manual_load.py
─────────────────────
Carga manual de datos Bronze desde S3 público de ClickHouse:
  - posts 2020 y 2021 (archivos particionados por año)
  - users 2020

URLs fuente:
  Posts: https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/{year}.parquet
  Users: https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet

Se ejecuta ANTES del DAG de Airflow.
Solo users_2021 pasa por el pipeline.
"""

import os
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

MINIO_ENDPOINT = os.getenv("STORAGE_URI", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")

LIMIT_POSTS = int(os.getenv("BRONZE_LIMIT_POSTS", "50000"))
LIMIT_USERS = int(os.getenv("BRONZE_LIMIT_USERS", "50000"))

POSTS_URL_TEMPLATE = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/posts/{year}.parquet"
USERS_URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet"


def upload_parquet_to_minio(df: pd.DataFrame, bucket: str, key: str):
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name="us-east-1",
    )
    buf = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)
    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())
    print(f"  Subido s3://{bucket}/{key} ({len(df)} filas)")


def load_posts(year: str, limit: int = LIMIT_POSTS):
    import requests, tempfile, os

    url = POSTS_URL_TEMPLATE.format(year=year)
    print(f"\n── Cargando posts {year} ──")
    print(f"  Descargando {url} a disco temporal ...")

    # 1. Descargar a archivo temporal en disco (no usa RAM)
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    try:
        with requests.get(url, stream=True, timeout=300) as r:
            r.raise_for_status()
            downloaded = 0
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):  # 8MB por chunk
                tmp.write(chunk)
                downloaded += len(chunk)
                print(f"  {downloaded / 1024 / 1024:.0f} MB...", end="\r")
        tmp.close()
        print(f"\n  Descarga completa.")

        # 2. Leer solo las primeras N filas con pyarrow (eficiente en memoria)
        pf = pq.ParquetFile(tmp.name)
        batches = []
        count = 0
        for batch in pf.iter_batches(batch_size=10000):
            batches.append(batch.to_pandas())
            count += len(batches[-1])
            if count >= limit:
                break

        df = pd.concat(batches).head(limit)
        print(f"  Filas cargadas: {len(df)}")

    finally:
        os.unlink(tmp.name)  # Borra el archivo temporal

    key = f"posts/{year}/posts_{year}.parquet"
    upload_parquet_to_minio(df, "bronze", key)

def load_users(year: str, limit: int = LIMIT_USERS):
    import requests, tempfile, os

    print(f"\n── Cargando users {year} ──")
    print(f"  Descargando {USERS_URL} a disco temporal ...")

    # 1. Descargar a disco en chunks
    tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
    try:
        with requests.get(USERS_URL, stream=True, timeout=300) as r:
            r.raise_for_status()
            downloaded = 0
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                tmp.write(chunk)
                downloaded += len(chunk)
                print(f"  {downloaded / 1024 / 1024:.0f} MB...", end="\r")
        tmp.close()
        print(f"\n  Descarga completa.")

        # 2. Leer por batches y filtrar por año sin cargar todo
        pf = pq.ParquetFile(tmp.name)
        batches = []
        count = 0
        for batch in pf.iter_batches(batch_size=50000):
            df_batch = batch.to_pandas()
            df_batch["CreationDate"] = pd.to_datetime(df_batch["CreationDate"], errors="coerce")
            df_filtered = df_batch[df_batch["CreationDate"].dt.year == int(year)]
            batches.append(df_filtered)
            count += len(df_filtered)
            print(f"  Filas {year} encontradas: {count}...", end="\r")
            if count >= limit:
                break

        df = pd.concat(batches).head(limit)
        print(f"\n  Filas para año {year}: {len(df)}")

    finally:
        os.unlink(tmp.name)

    key = f"users/{year}/users_{year}.parquet"
    upload_parquet_to_minio(df, "bronze", key)

if __name__ == "__main__":
    print("=" * 60)
    print("  CARGA MANUAL BRONZE — posts 2020, posts 2021, users 2020")
    print("=" * 60)

    load_posts("2020")
    load_posts("2021")
    load_users("2020")

    print("\n Carga manual Bronze completada.")
    print("   Ahora puedes ejecutar el DAG de Airflow para users_2021.")