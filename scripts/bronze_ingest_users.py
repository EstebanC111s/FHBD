"""
bronze_ingest_users.py
──────────────────────
Ingesta Bronze: descarga users del dataset StackOverflow desde S3 público
de ClickHouse, filtra por año y sube como Parquet a MinIO bronze/.

URL fuente:
  https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet

Uso desde Airflow: se importa run() con PythonOperator.
Uso manual: python bronze_ingest_users.py
"""

import os
import time
import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# ── Config ──
MINIO_ENDPOINT = os.getenv("STORAGE_URI", "http://minio:9000")
AWS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "admin")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
YEAR = os.getenv("BRONZE_YEAR", "2021")
LIMIT = int(os.getenv("BRONZE_LIMIT", "20000"))

USERS_URL = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet"


def upload_parquet_to_minio(df: pd.DataFrame, bucket: str, key: str):
    """Sube un DataFrame como Parquet a MinIO."""
    
    df = df.reset_index(drop=True)

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SECRET,
        region_name="us-east-1",
    )

    buf = BytesIO()
    table = pa.Table.from_pandas(df)

    pq.write_table(table, buf, compression="snappy")

    buf.seek(0)

    s3.put_object(Bucket=bucket, Key=key, Body=buf.getvalue())

    print(f"  Subido s3://{bucket}/{key} ({len(df)} filas)")


def download_users_for_year(year: str, limit: int) -> pd.DataFrame:
    """Descarga users.parquet de forma segura (columnas + retry + control memoria)"""

    print(f"  Descargando {USERS_URL} ...")
    print(f"  (Optimizado: columnas + retry + control de memoria)")

    columns = ["Id", "CreationDate", "Reputation"]

    df = None

    for attempt in range(3):
        try:
            df = pd.read_parquet(
                USERS_URL,
                columns=columns,
                engine="pyarrow"   # más estable
            )
            print(f"  Descarga exitosa en intento {attempt + 1}")
            break

        except Exception as e:
            print(f"  Error intento {attempt + 1}: {e}")
            time.sleep(5)

    if df is None:
        raise Exception("No se pudo descargar el dataset después de 3 intentos")

    print(f"  Total filas descargadas: {len(df)}")

    df["CreationDate"] = pd.to_datetime(df["CreationDate"], errors="coerce")

    df_year = df[df["CreationDate"].dt.year == int(year)]

    df_year = df_year.dropna(subset=["CreationDate"])

    print(f"  Filas para año {year}: {len(df_year)}")

    if limit and len(df_year) > limit:
        df_year = df_year.head(limit)
        print(f"  Limitado a {limit} filas")

    return df_year


def run(**kwargs):
    year = kwargs.get("year", YEAR)
    limit = kwargs.get("limit", LIMIT)

    print(f"\n{'='*60}")
    print(f"  BRONZE INGEST — users_{year}")
    print(f"{'='*60}")

    print(f"\n[1/2] Descargando y filtrando users año {year} ...")
    df = download_users_for_year(year, limit)

    key = f"users/{year}/users_{year}.parquet"

    print(f"\n[2/2] Subiendo a MinIO bronze/{key} ...")
    upload_parquet_to_minio(df, "bronze", key)

    print(f"\n  Bronze ingest users_{year} completado.\n")

    return f"bronze/{key}"


if __name__ == "__main__":
    run()