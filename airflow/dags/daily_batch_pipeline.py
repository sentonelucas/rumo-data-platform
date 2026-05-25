"""
DAG: daily_batch_pipeline
Rumo Logística | Plataforma Moderna de Dados

Pipeline batch diário D-1.
Orquestra: ingestão raw→bronze, transformação bronze→silver,
qualidade, agregação silver→gold e carga no Redshift.

Schedule: 01:00 BRT (04:00 UTC) — diariamente
SLA: concluir até 07:00 BRT (D-1 garantido para início do expediente)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.dates import days_ago

# ── Configuração da DAG ──────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@rumo.com.br"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=1),
}

with DAG(
    dag_id="rumo_daily_batch_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline batch diário D-1 — Rumo Plataforma de Dados",
    schedule_interval="0 4 * * *",         # 04:00 UTC = 01:00 BRT
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["rumo", "batch", "d-1", "production"],
    # SLA: alerta se a DAG não completar em 6 horas
    sla_miss_callback=lambda dag, task_list, blocking_task_list, slas, blocking_tis:
        print(f"[SLA MISS] DAG {dag.dag_id} — tasks: {task_list}"),
) as dag:

    # ── Variáveis de contexto ────────────────────────────────────────────────
    processing_date = "{{ ds }}"  # Data de execução (D-1 automático)
    glue_iam_role = "arn:aws:iam::{{ var.value.aws_account_id }}:role/GlueServiceRole"

    # ── 1. Sensor: aguarda DMS depositar arquivos no S3 raw ──────────────────
    wait_sql_server_raw = S3KeySensor(
        task_id="wait_sql_server_raw",
        bucket_name="rumo-raw",
        bucket_key=f"sql_server/eventos_ferroviarios/date={processing_date}/_SUCCESS",
        aws_conn_id="aws_default",
        timeout=7200,          # Aguarda até 2h
        poke_interval=300,     # Checa a cada 5 min
        mode="reschedule",     # Libera worker enquanto aguarda
        soft_fail=False,
    )

    wait_oracle_raw = S3KeySensor(
        task_id="wait_oracle_raw",
        bucket_name="rumo-raw",
        bucket_key=f"oracle/contratos/date={processing_date}/_SUCCESS",
        aws_conn_id="aws_default",
        timeout=7200,
        poke_interval=300,
        mode="reschedule",
    )

    # ── 2. Raw → Bronze ──────────────────────────────────────────────────────
    bronze_eventos = GlueJobOperator(
        task_id="bronze_eventos_ferroviarios",
        job_name="rumo-ingestion-raw-to-bronze",
        script_args={
            "--source_path":    f"s3://rumo-raw/sql_server/eventos_ferroviarios/date={processing_date}/",
            "--target_path":    "s3://rumo-bronze/eventos_ferroviarios/",
            "--domain":         "eventos_ferroviarios",
            "--primary_key":    "evento_id",
            "--watermark_col":  "updated_at",
            "--database_name":  "rumo_bronze",
            "--table_name":     "eventos_ferroviarios",
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        iam_role_name="GlueServiceRole",
        create_job_kwargs={
            "GlueVersion": "4.0",
            "NumberOfWorkers": 10,
            "WorkerType": "G.1X",
        },
        sla=timedelta(hours=1),
    )

    bronze_contratos = GlueJobOperator(
        task_id="bronze_contratos",
        job_name="rumo-ingestion-raw-to-bronze",
        script_args={
            "--source_path":   f"s3://rumo-raw/oracle/contratos/date={processing_date}/",
            "--target_path":   "s3://rumo-bronze/contratos/",
            "--domain":        "contratos",
            "--primary_key":   "contrato_id",
            "--watermark_col": "updated_at",
            "--database_name": "rumo_bronze",
            "--table_name":    "contratos",
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        iam_role_name="GlueServiceRole",
        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 5, "WorkerType": "G.1X"},
        sla=timedelta(hours=1),
    )

    # ── 3. Bronze → Silver ───────────────────────────────────────────────────
    silver_transform = GlueJobOperator(
        task_id="silver_transformation",
        job_name="rumo-transformation-silver",
        script_args={
            "--bronze_database":  "rumo_bronze",
            "--silver_database":  "rumo_silver",
            "--processing_date":  processing_date,
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        iam_role_name="GlueServiceRole",
        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 20, "WorkerType": "G.2X"},
        sla=timedelta(hours=2),
    )

    # ── 4. Data Quality ──────────────────────────────────────────────────────
    data_quality = GlueJobOperator(
        task_id="data_quality_checks",
        job_name="rumo-data-quality-checks",
        script_args={
            "--database":         "rumo_silver",
            "--table":            "fato_eventos_ferroviarios",
            "--sns_topic_arn":    "arn:aws:sns:us-east-1:{{ var.value.aws_account_id }}:rumo-data-alerts",
            "--processing_date":  processing_date,
            "--fail_on_error":    "true",
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        iam_role_name="GlueServiceRole",
        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 5, "WorkerType": "G.1X"},
        sla=timedelta(minutes=30),
    )

    # ── 5. Silver → Gold ─────────────────────────────────────────────────────
    gold_aggregation = GlueJobOperator(
        task_id="gold_aggregation",
        job_name="rumo-aggregation-gold",
        script_args={
            "--silver_database": "rumo_silver",
            "--gold_database":   "rumo_gold",
            "--processing_date": processing_date,
        },
        aws_conn_id="aws_default",
        region_name="us-east-1",
        iam_role_name="GlueServiceRole",
        create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 15, "WorkerType": "G.1X"},
        sla=timedelta(hours=1),
    )

    # ── 6. Carga no Redshift (COPY via Spectrum) ─────────────────────────────
    load_redshift = RedshiftSQLOperator(
        task_id="load_redshift_gold",
        sql=f"""
            -- Carga incremental no Redshift via Redshift Spectrum (S3 → Redshift)
            INSERT INTO rumo_dw.fato_eventos_ferroviarios_daily
            SELECT *
            FROM rumo_gold_spectrum.fato_eventos_ferroviarios
            WHERE _partition_date = '{processing_date}';

            -- Atualiza tabela de controle de carga
            INSERT INTO rumo_dw.pipeline_control (domain, processing_date, loaded_at, status)
            VALUES ('eventos_ferroviarios', '{processing_date}', GETDATE(), 'SUCCESS');
        """,
        redshift_conn_id="redshift_default",
    )

    # ── 7. Alerta de sucesso ─────────────────────────────────────────────────
    notify_success = SnsPublishOperator(
        task_id="notify_pipeline_success",
        target_arn="arn:aws:sns:us-east-1:{{ var.value.aws_account_id }}:rumo-pipeline-status",
        message=f"✅ Pipeline D-1 concluído com sucesso para {processing_date}",
        aws_conn_id="aws_default",
    )

    # ── Grafo de dependências ────────────────────────────────────────────────
    [wait_sql_server_raw, wait_oracle_raw] >> bronze_eventos
    wait_oracle_raw >> bronze_contratos

    [bronze_eventos, bronze_contratos] >> silver_transform
    silver_transform >> data_quality
    data_quality >> gold_aggregation
    gold_aggregation >> load_redshift
    load_redshift >> notify_success
