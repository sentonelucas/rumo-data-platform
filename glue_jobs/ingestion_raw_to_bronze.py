"""
Glue Job: raw → bronze
Rumo Logística | Plataforma Moderna de Dados

Responsabilidades:
  - Lê arquivos Parquet/CSV/JSON do S3 raw zone
  - Aplica deduplicação com base em primary key + timestamp
  - Valida schema mínimo esperado
  - Escreve no S3 bronze zone em formato Apache Iceberg (ACID, time travel)
  - Registra métricas de qualidade no CloudWatch

Execução: Diária (via MWAA Airflow), 01h30 BRT
Trigger: s3://rumo-raw/ evento ou Airflow DAG
"""

import sys
import boto3
from datetime import datetime, timezone

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Parâmetros do Job ───────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_path",       # ex: s3://rumo-raw/sql_server/eventos_ferroviarios/
    "target_path",       # ex: s3://rumo-bronze/eventos_ferroviarios/
    "domain",            # ex: eventos_ferroviarios
    "primary_key",       # ex: evento_id
    "watermark_col",     # ex: updated_at
    "database_name",     # ex: rumo_bronze (Glue Catalog)
    "table_name",        # ex: eventos_ferroviarios
])

# ── Inicialização ────────────────────────────────────────────────────────────
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
cw = boto3.client("cloudwatch", region_name="us-east-1")

DOMAIN = args["domain"]
PK = args["primary_key"]
WATERMARK = args["watermark_col"]
DATABASE = args["database_name"]
TABLE = args["table_name"]

# Iceberg catalog config
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.warehouse", args["target_path"])
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
spark.conf.set("spark.sql.extensions",
               "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")


def publish_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    """Publica métrica customizada no CloudWatch para monitoramento de pipeline."""
    try:
        cw.put_metric_data(
            Namespace="Rumo/DataPlatform",
            MetricData=[{
                "MetricName": metric_name,
                "Dimensions": [{"Name": "Domain", "Value": DOMAIN}],
                "Value": value,
                "Unit": unit,
                "Timestamp": datetime.now(timezone.utc),
            }]
        )
    except Exception as e:
        logger.warn(f"[CW] Falha ao publicar métrica {metric_name}: {e}")


def read_raw_data(source_path: str):
    """
    Lê dados do raw zone.
    Suporta Parquet, CSV e JSON automaticamente via inferência de formato pelo path.
    """
    if "csv" in source_path.lower():
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
    elif "json" in source_path.lower():
        df = spark.read.option("multiline", "true").json(source_path)
    else:
        # Default: Parquet (gerado pelo DMS)
        df = spark.read.parquet(source_path)

    record_count = df.count()
    logger.info(f"[RAW] Registros lidos de {source_path}: {record_count:,}")
    publish_metric("RawRecordsRead", record_count)
    return df


def validate_schema(df, required_cols: list) -> None:
    """Valida que as colunas obrigatórias existem no DataFrame."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        msg = f"[SCHEMA] Colunas obrigatórias ausentes: {missing}"
        logger.error(msg)
        publish_metric("SchemaValidationFailure", 1)
        raise ValueError(msg)
    logger.info("[SCHEMA] Validação de schema OK.")


def deduplicate(df, pk: str, watermark: str):
    """
    Remove duplicatas mantendo o registro mais recente por chave primária.
    Usa Window Function para ranquear por watermark desc.
    """
    window = Window.partitionBy(pk).orderBy(F.col(watermark).desc())
    df_dedup = (
        df
        .withColumn("_rank", F.row_number().over(window))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )

    original = df.count()
    deduped = df_dedup.count()
    duplicates_removed = original - deduped

    logger.info(f"[DEDUP] Original: {original:,} | Após dedup: {deduped:,} | Removidos: {duplicates_removed:,}")
    publish_metric("DuplicatesRemoved", duplicates_removed)
    return df_dedup


def enrich_metadata(df):
    """Adiciona colunas de controle de pipeline (auditoria e particionamento)."""
    return df.withColumn("_ingested_at", F.current_timestamp()) \
             .withColumn("_source_domain", F.lit(DOMAIN)) \
             .withColumn("_partition_date", F.to_date(F.col(WATERMARK))) \
             .withColumn("_job_run_id", F.lit(args["JOB_NAME"]))


def write_iceberg(df, database: str, table: str) -> None:
    """
    Escreve no S3 em formato Apache Iceberg via MERGE INTO (upsert).
    Garante ACID, time travel e schema evolution.
    """
    full_table = f"glue_catalog.{database}.{table}"

    # Cria tabela Iceberg se não existir
    schema_ddl = ", ".join([f"`{f.name}` {f.dataType.simpleString()}" for f in df.schema.fields])
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {full_table} ({schema_ddl})
        USING iceberg
        PARTITIONED BY (_partition_date)
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'snappy',
            'history.expire.max-snapshot-age-ms' = '604800000'
        )
    """)

    # Registra view temporária para o MERGE
    df.createOrReplaceTempView("source_data")

    # MERGE INTO: upsert por primary key
    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING source_data AS source
        ON target.`{PK}` = source.`{PK}`
        WHEN MATCHED AND source.`{WATERMARK}` > target.`{WATERMARK}` THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    logger.info(f"[ICEBERG] Upsert concluído em {full_table}")
    publish_metric("BronzeRecordsWritten", df.count())


# ── Main Pipeline ────────────────────────────────────────────────────────────
try:
    logger.info(f"[START] Job {DOMAIN} iniciado — {datetime.now(timezone.utc).isoformat()}")

    # 1. Leitura
    df_raw = read_raw_data(args["source_path"])

    # 2. Validação de schema mínimo
    required_cols = [PK, WATERMARK]
    validate_schema(df_raw, required_cols)

    # 3. Deduplicação
    df_dedup = deduplicate(df_raw, PK, WATERMARK)

    # 4. Enriquecimento de metadados de controle
    df_enriched = enrich_metadata(df_dedup)

    # 5. Escrita em Iceberg (ACID upsert)
    write_iceberg(df_enriched, DATABASE, TABLE)

    publish_metric("JobSuccess", 1)
    logger.info(f"[END] Job {DOMAIN} concluído com sucesso.")

except Exception as e:
    publish_metric("JobFailure", 1)
    logger.error(f"[ERROR] Job {DOMAIN} falhou: {str(e)}")
    raise

finally:
    job.commit()
