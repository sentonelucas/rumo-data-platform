"""
Glue Data Quality — Regras de Validação
Rumo Logística | Plataforma Moderna de Dados

Valida dados antes de promover de silver → gold.
Em caso de falha, o pipeline é interrompido e alerta SNS é disparado.

Execução: Após job transformation_silver, antes de aggregation_gold.
"""

import sys
import json
import boto3
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "database",
    "table",
    "sns_topic_arn",
    "processing_date",
    "fail_on_error",     # "true" | "false" — se falso, só alerta sem parar o pipeline
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()
sns = boto3.client("sns", region_name="us-east-1")

# Iceberg config
spark.conf.set("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
spark.conf.set("spark.sql.extensions",
               "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

FAIL_ON_ERROR = args.get("fail_on_error", "true").lower() == "true"
PROCESSING_DATE = args["processing_date"]
TABLE = f"glue_catalog.{args['database']}.{args['table']}"


def send_alert(failures: list) -> None:
    """Envia alerta SNS com detalhes das falhas de qualidade."""
    message = {
        "alert_type": "DATA_QUALITY_FAILURE",
        "table": TABLE,
        "processing_date": PROCESSING_DATE,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "failures": failures,
    }
    sns.publish(
        TopicArn=args["sns_topic_arn"],
        Subject=f"[ALERTA] Falha de Qualidade — {args['table']} — {PROCESSING_DATE}",
        Message=json.dumps(message, ensure_ascii=False, indent=2),
    )
    logger.warn(f"[SNS] Alerta enviado: {len(failures)} falha(s) detectada(s)")


def run_quality_checks(df) -> list:
    """
    Executa todas as regras de qualidade.
    Retorna lista de falhas encontradas.
    """
    failures = []
    total = df.count()

    if total == 0:
        failures.append({
            "rule": "NonEmptyTable",
            "severity": "CRITICAL",
            "message": f"Tabela {TABLE} não possui registros para {PROCESSING_DATE}",
            "threshold": "> 0",
            "actual": 0,
        })
        return failures

    # ── Regra 1: Completude de colunas críticas ──────────────────────────────
    critical_cols = ["evento_id", "vagao_id", "status_norm", "data_inicio"]
    for col in critical_cols:
        null_count = df.filter(F.col(col).isNull()).count()
        null_pct = (null_count / total) * 100
        if null_pct > 1.0:  # Tolera até 1% de nulos
            failures.append({
                "rule": f"Completeness_{col}",
                "severity": "ERROR",
                "message": f"Coluna '{col}': {null_pct:.2f}% nulos (threshold: 1%)",
                "threshold": "< 1%",
                "actual": f"{null_pct:.2f}%",
            })

    # ── Regra 2: Unicidade da chave primária ─────────────────────────────────
    pk_count = df.select("evento_id").count()
    distinct_pk = df.select("evento_id").distinct().count()
    if pk_count != distinct_pk:
        duplicates = pk_count - distinct_pk
        failures.append({
            "rule": "Uniqueness_evento_id",
            "severity": "ERROR",
            "message": f"Chave primária 'evento_id' possui {duplicates:,} duplicatas",
            "threshold": "0 duplicatas",
            "actual": duplicates,
        })

    # ── Regra 3: Validade de status ──────────────────────────────────────────
    valid_statuses = ["EM_TRANSITO", "PARADO", "CHEGOU", "SAIU", "MANUTENCAO", "CARREGANDO", "DESCARREGANDO"]
    invalid_status_count = df.filter(~F.col("status_norm").isin(valid_statuses)).count()
    invalid_pct = (invalid_status_count / total) * 100
    if invalid_pct > 0.5:
        failures.append({
            "rule": "ValidValues_status_norm",
            "severity": "WARNING",
            "message": f"{invalid_pct:.2f}% de registros com status inválido",
            "threshold": "< 0.5%",
            "actual": f"{invalid_pct:.2f}%",
        })

    # ── Regra 4: Consistência temporal (data_inicio <= data_fim) ─────────────
    temporal_violations = df.filter(
        F.col("data_inicio") > F.col("data_fim")
    ).count()
    if temporal_violations > 0:
        failures.append({
            "rule": "TemporalConsistency",
            "severity": "ERROR",
            "message": f"{temporal_violations:,} registros com data_inicio > data_fim",
            "threshold": "0",
            "actual": temporal_violations,
        })

    # ── Regra 5: Volume mínimo esperado ─────────────────────────────────────
    # Alerta se volume < 70% do histórico médio (anti-regression de pipeline)
    EXPECTED_MIN_RECORDS = 500_000  # Ajustar conforme baseline histórico
    if total < EXPECTED_MIN_RECORDS:
        failures.append({
            "rule": "MinimumVolumeCheck",
            "severity": "WARNING",
            "message": f"Volume {total:,} abaixo do mínimo esperado de {EXPECTED_MIN_RECORDS:,}",
            "threshold": f">= {EXPECTED_MIN_RECORDS:,}",
            "actual": total,
        })

    critical_failures = [f for f in failures if f["severity"] in ("ERROR", "CRITICAL")]
    logger.info(f"[DQ] Total: {len(failures)} issue(s) — Críticas: {len(critical_failures)}")
    return failures


# ── Main ─────────────────────────────────────────────────────────────────────
try:
    df = spark.read.format("iceberg").load(TABLE) \
               .filter(F.col("_partition_date") == PROCESSING_DATE)

    failures = run_quality_checks(df)

    if failures:
        send_alert(failures)
        critical = [f for f in failures if f["severity"] in ("ERROR", "CRITICAL")]
        if critical and FAIL_ON_ERROR:
            raise RuntimeError(
                f"[DQ] {len(critical)} falha(s) crítica(s) detectada(s). "
                f"Pipeline interrompido para proteger a camada gold."
            )
        else:
            logger.warn(f"[DQ] {len(failures)} issue(s) detectada(s) — pipeline continua (fail_on_error=false)")
    else:
        logger.info("[DQ] Todas as verificações de qualidade passaram. ✓")

except RuntimeError:
    raise
except Exception as e:
    logger.error(f"[DQ] Erro inesperado: {str(e)}")
    raise
finally:
    job.commit()
