"""
Glue Job: bronze → silver
Rumo Logística | Plataforma Moderna de Dados

Responsabilidades:
  - Lê dados validados da camada bronze (Iceberg)
  - Aplica transformações de negócio (limpeza, enriquecimento, joins)
  - Escreve na camada silver em Apache Iceberg
  - Exemplo concreto: eventos ferroviários enriquecidos com dados de vagão e contrato

Execução: Diária (via MWAA Airflow), 02h30 BRT (após bronze)
"""

import sys
from datetime import datetime, timedelta, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

# ── Parâmetros ───────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "bronze_database",   # rumo_bronze
    "silver_database",   # rumo_silver
    "processing_date",   # YYYY-MM-DD (data a processar, injetada pelo Airflow)
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

BRONZE_DB = args["bronze_database"]
SILVER_DB = args["silver_database"]
PROCESSING_DATE = args["processing_date"]

# Iceberg config
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.extensions",
               "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")


def read_bronze(table: str, date_filter: str = None):
    """Lê tabela da camada bronze com filtro de data para leitura incremental eficiente."""
    full_table = f"glue_catalog.{BRONZE_DB}.{table}"
    df = spark.read.format("iceberg").load(full_table)

    if date_filter:
        df = df.filter(F.col("_partition_date") == date_filter)

    count = df.count()
    logger.info(f"[READ] {full_table} — {count:,} registros para {date_filter}")
    return df


def transform_eventos_ferroviarios(df_eventos, df_vagoes, df_contratos):
    """
    Transformação central:
    Enriquece eventos ferroviários com informações do vagão e do contrato associado.

    Regras de negócio aplicadas:
      - Normalização de status (maiúsculas, trim)
      - Cálculo de duração do evento em minutos
      - Join com tabela de vagões para adicionar tipo e capacidade
      - Join com contratos para adicionar cliente e SLA esperado
      - Flag de violação de SLA
    """
    # Normaliza campos de status
    df = df_eventos \
        .withColumn("status_norm", F.upper(F.trim(F.col("status")))) \
        .withColumn("status_norm", F.regexp_replace(F.col("status_norm"), r"\s+", "_"))

    # Calcula duração do evento (em minutos)
    df = df.withColumn(
        "duracao_minutos",
        (F.unix_timestamp("data_fim") - F.unix_timestamp("data_inicio")) / 60
    )

    # Join com vagões (LEFT JOIN para não perder eventos sem vagão cadastrado)
    df = df.join(
        df_vagoes.select("vagao_id", "tipo_vagao", "capacidade_ton", "ano_fabricacao"),
        on="vagao_id",
        how="left"
    )

    # Join com contratos para obter SLA e cliente
    df = df.join(
        df_contratos.select("contrato_id", "cliente_nome", "sla_horas", "segmento"),
        on="contrato_id",
        how="left"
    )

    # Flag de violação de SLA (duração real > SLA contratado)
    df = df.withColumn(
        "sla_violado",
        F.when(
            F.col("duracao_minutos") > (F.col("sla_horas") * 60),
            F.lit(True)
        ).otherwise(F.lit(False))
    )

    # Remove registros com evento_id nulo (dados inválidos)
    df = df.filter(F.col("evento_id").isNotNull())

    # Adiciona coluna de partição e metadados silver
    df = df \
        .withColumn("_silver_processed_at", F.current_timestamp()) \
        .withColumn("_partition_date", F.to_date(F.col("data_inicio")))

    return df


def write_silver_iceberg(df, table: str) -> None:
    """Escreve na camada silver em Iceberg com MERGE INTO (upsert)."""
    full_table = f"glue_catalog.{SILVER_DB}.{table}"

    df.createOrReplaceTempView("silver_source")

    spark.sql(f"""
        MERGE INTO {full_table} AS target
        USING silver_source AS source
        ON target.evento_id = source.evento_id
        WHEN MATCHED AND source._silver_processed_at > target._silver_processed_at THEN
            UPDATE SET *
        WHEN NOT MATCHED THEN
            INSERT *
    """)

    logger.info(f"[SILVER] Escrita concluída em {full_table}: {df.count():,} registros")


# ── Main ─────────────────────────────────────────────────────────────────────
try:
    logger.info(f"[START] Transformação silver para {PROCESSING_DATE}")

    # Carrega tabelas bronze
    df_eventos = read_bronze("eventos_ferroviarios", PROCESSING_DATE)
    df_vagoes  = read_bronze("vagoes")      # dimensão, sem filtro de data
    df_contratos = read_bronze("contratos") # dimensão, sem filtro de data

    # Aplica transformações de negócio
    df_silver = transform_eventos_ferroviarios(df_eventos, df_vagoes, df_contratos)

    # Escreve na camada silver
    write_silver_iceberg(df_silver, "fato_eventos_ferroviarios")

    logger.info("[END] Transformação silver concluída com sucesso.")

except Exception as e:
    logger.error(f"[ERROR] Transformação silver falhou: {str(e)}")
    raise

finally:
    job.commit()
