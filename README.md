# Proyecto 2 — Arquitectura Lakehouse con Pipeline Medallion

## Integrantes
- Carlos Orozco
- Samuel Uribe
- Esteban Cobo
- Jose David Mesa
- Juan Pablo Lopez

## Descripción
Arquitectura Lakehouse desplegada con Docker Compose que implementa el patrón Medallion (Bronze → Silver → Gold) sobre el dataset de **StackOverflow** (datos públicos de ClickHouse).

## Arquitectura

| Capa | Formato | Escritura | Contenido |
|------|---------|-----------|-----------|
| Bronze | Parquet | Override | Datos crudos anuales (posts 2020/2021, users 2020/2021) |
| Silver | Iceberg | Merge (upsert) | Tablas históricas: `posts_hist`, `users_hist` + fecha_cargue |
| Gold | Iceberg | Merge (upsert) | `post_counts_by_user` (métricas/KPIs) + fecha_cargue |

## Servicios

| Servicio | Puerto | URL |
|----------|--------|-----|
| Airflow Web | 8080 | http://localhost:8080 (admin/admin) |
| Spark Master UI | 9090 | http://localhost:9090 |
| Jupyter Lab | 8888 | http://localhost:8888 |
| MinIO Console | 9001 | http://localhost:9001 (admin/password) |
| MinIO API | 9000 | http://localhost:9000 |
| Nessie | 19120 | http://localhost:19120 |
| Dremio | 9047 | http://localhost:9047 |
| Trino | 8085 | http://localhost:8085 |

## Despliegue

### 1. Configurar AIRFLOW_UID
En Linux/WSL:
```bash
echo "AIRFLOW_UID=$(id -u)" > .env
echo "_AIRFLOW_WWW_USER_USERNAME=airflow" >> .env
echo "_AIRFLOW_WWW_USER_PASSWORD=airflow" >> .env
```
En Windows el archivo `.env` ya viene configurado con UID 50000.

### 2. Construir y levantar
```bash
docker compose build
docker compose up -d
```
Esperar ~2-3 minutos a que todos los servicios estén listos.

### 3. Verificar servicios
```bash
docker compose ps
```

## Ejecución del Pipeline

### Paso 1: Carga manual de datos Bronze
Antes de ejecutar el DAG, cargar manualmente posts (2020, 2021) y users (2020):
```bash
docker compose exec airflow-worker python /opt/airflow/scripts/bronze_manual_load.py
```

### Paso 2: Carga manual de Silver posts_hist
Crear la tabla `posts_hist` en Silver (sin columna Body para optimizar memoria):
```
docker compose exec airflow-worker bash -c "/home/airflow/.local/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --jars /opt/spark-jars/hadoop-aws-3.3.4.jar,/opt/spark-jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark-jars/bundle-2.24.8.jar,/opt/spark-jars/url-connection-client-2.24.8.jar,/opt/spark-jars/iceberg-spark-runtime-3.5_2.12-1.5.0.jar,/opt/spark-jars/nessie-spark-extensions-3.5_2.12-0.77.1.jar --conf spark.driver.host=airflow-worker --conf spark.driver.bindAddress=0.0.0.0 --conf spark.driver.memory=1g --conf spark.executor.instances=1 --conf spark.dynamicAllocation.enabled=false --conf spark.executor.cores=1 --conf spark.executor.memory=2g --conf spark.executor.memoryOverhead=512m --conf spark.sql.shuffle.partitions=2 --conf spark.sql.codegen.wholeStage=false --conf spark.sql.codegen.factoryMode=NO_CODEGEN --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions --conf spark.sql.defaultCatalog=nessie --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 --conf spark.sql.catalog.nessie.ref=main --conf spark.sql.catalog.nessie.authentication.type=NONE --conf spark.sql.catalog.nessie.warehouse=s3a://warehouse/ --conf spark.sql.catalog.nessie.io-impl=org.apache.iceberg.aws.s3.S3FileIO --conf spark.sql.catalog.nessie.s3.endpoint=http://minio:9000 --conf spark.sql.catalog.nessie.s3.path-style-access=true --conf spark.sql.catalog.nessie.s3.region=us-east-1 --conf spark.sql.catalog.nessie.s3.access-key-id=admin --conf spark.sql.catalog.nessie.s3.secret-access-key=password --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 --conf spark.hadoop.fs.s3a.access.key=admin --conf spark.hadoop.fs.s3a.secret.key=password --conf spark.hadoop.fs.s3a.path.style.access=true /opt/airflow/scripts/silver_manual_posts_light.py 2>&1 | tail -15"
```

### Paso 3: Ejecutar el DAG en Airflow
1. Ir a http://localhost:8080 → login: `admin` / `admin`
2. Activar el DAG: `stackoverflow_lakehouse_pipeline`
3. Click en **"Play" → Trigger DAG**
4. El DAG ejecuta en secuencia:
   - **bronze_ingest_users_2021**: descarga users 2021 desde S3 público → MinIO bronze/
   - **silver_transform_users_hist**: consolida users_hist en Iceberg (merge manual)
   - **gold_aggregation_post_counts**: genera post_counts_by_user cruzando posts_hist + users_hist

## Consulta desde Dremio (Gold)

1. Ir a http://localhost:9047
2. En el primer uso, crear cuenta de administrador
3. Agregar fuente **Nessie**:
   - Nessie Endpoint URL: `http://nessie:19120/api/v2`
   - Authentication: None
   - Storage: AWS S3
     - Root Path: `warehouse`
     - Access Key: `admin`
     - Secret Key: `password`
     - Encrypt connection: **desactivar**
     - Connection Properties:
       - `fs.s3a.endpoint` = `minio:9000`
       - `fs.s3a.path.style.access` = `true`
       - `dremio.s3.compat` = `true`
4. Ejecutar:
```sql
SELECT * FROM Nessie.gold.post_counts_by_user LIMIT 20
```

## Consulta desde Trino (Silver → replicar Gold)

1. Conectar via CLI:
```bash
docker compose exec trino trino
```
2. Verificar tablas:
```sql
SHOW SCHEMAS FROM iceberg;
SELECT * FROM iceberg.silver.users_hist LIMIT 10;
SELECT * FROM iceberg.silver.posts_hist LIMIT 10;
SELECT * FROM iceberg.gold.post_counts_by_user LIMIT 10;
```
3. Replicar la lógica Gold desde Silver:
```sql
SELECT
    p.OwnerUserId,
    COUNT(*) AS total_posts,
    SUM(CASE WHEN CAST(p.PostTypeId AS INTEGER) = 1 THEN 1 ELSE 0 END) AS total_preguntas,
    SUM(CASE WHEN CAST(p.PostTypeId AS INTEGER) = 2 THEN 1 ELSE 0 END) AS total_respuestas,
    SUM(COALESCE(CAST(p.Score AS INTEGER), 0)) AS total_score,
    SUM(COALESCE(CAST(p.ViewCount AS INTEGER), 0)) AS total_views,
    SUM(COALESCE(CAST(p.CommentCount AS INTEGER), 0)) AS total_comments,
    u.DisplayName,
    u.Location
FROM iceberg.silver.posts_hist p
LEFT JOIN iceberg.silver.users_hist u
    ON p.OwnerUserId = u.Id
WHERE p.OwnerUserId IS NOT NULL
GROUP BY p.OwnerUserId, u.DisplayName, u.Location
ORDER BY total_posts DESC
LIMIT 20;
```

## Notebooks de respaldo

En caso de fallo en Airflow, ejecutar manualmente desde Jupyter (http://localhost:8888):

| Notebook | Capa | Descripción |
|----------|------|-------------|
| `bronze_ingest.ipynb` | Bronze | Carga manual de posts/users + ingesta users_2021 |
| `silver_transform.ipynb` | Silver | Crea users_hist (merge) y posts_hist (sin Body) |
| `gold_agg.ipynb` | Gold | Genera post_counts_by_user con métricas |

## Estructura de archivos

```
Proyecto2/
├── docker-compose.yml
├── Dockerfile.airflow
├── Dockerfile.spark
├── spark-defaults.conf
├── .env
├── README.md
├── consultas_trino.sql
├── trino/catalog/
│   └── iceberg.properties
├── dags/
│   └── stackoverflow_lakehouse_dag.py
├── scripts/
│   ├── bronze_ingest_users.py
│   ├── bronze_manual_load.py
│   ├── silver_transform_users.py
│   ├── silver_manual_posts.py
│   ├── silver_manual_posts_light.py
│   └── gold_aggregation.py
│    
├── notebooks/
│   ├── bronze_ingest.ipynb
│   ├── silver_transform.ipynb
│   └── gold_agg.ipynb
├── data/
├── config/
├── plugins/
└── logs/
```

## Notas técnicas
- Spark ejecuta en modo **client** contra el cluster (spark-master:7077)
- El executor usa 2GB de memoria con 1 core para caber en Docker con 12GB RAM
- La tabla `posts_hist` en Silver no incluye la columna `Body` (HTML crudo) para optimizar memoria
- El merge en Silver y Gold usa merge manual (left_anti + overwritePartitions) por compatibilidad con Iceberg 1.5 + Nessie 0.77
- En Trino el catálogo se llama `iceberg` (no `nessie`)
- En Dremio el catálogo se llama `Nessie`

## Detener servicios
```bash
docker compose down
```
Para eliminar volúmenes (datos):
```bash
docker compose down -v
```