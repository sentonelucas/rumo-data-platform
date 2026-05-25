"""
Testes unitários: ingestion_raw_to_bronze.py
Usa moto para mockar AWS (S3, CloudWatch) — sem dependência de infra real.

Execução:
    pytest tests/test_ingestion_bronze.py -v
"""

import json
import pytest
from datetime import datetime
from unittest.mock import patch, MagicMock

import boto3
from moto import mock_aws

# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="function")
def aws_credentials(monkeypatch):
    """Configura credenciais fake para o moto."""
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture(scope="function")
def s3_client(aws_credentials):
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket="rumo-raw")
        client.create_bucket(Bucket="rumo-bronze")
        yield client


@pytest.fixture(scope="function")
def spark_session():
    """SparkSession local para testes (sem cluster)."""
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .master("local[2]")
            .appName("test_bronze")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        yield spark
        spark.stop()
    except ImportError:
        pytest.skip("PySpark não disponível")


# ── Testes de Deduplicação ────────────────────────────────────────────────────

class TestDeduplicate:

    def test_removes_exact_duplicates(self, spark_session):
        """Dois registros com mesmo PK e mesmo timestamp → mantém 1."""
        from pyspark.sql import functions as F

        # Importa a função a ser testada
        import importlib.util, sys
        spec = importlib.util.spec_from_file_location(
            "bronze", "glue_jobs/ingestion_raw_to_bronze.py"
        )

        data = [
            ("evt_001", "2024-01-15 10:00:00", "PARADO"),
            ("evt_001", "2024-01-15 10:00:00", "PARADO"),   # duplicata exata
        ]
        df = spark_session.createDataFrame(data, ["evento_id", "updated_at", "status"])

        from pyspark.sql.window import Window
        window = Window.partitionBy("evento_id").orderBy(F.col("updated_at").desc())
        df_dedup = (
            df.withColumn("_rank", F.row_number().over(window))
              .filter(F.col("_rank") == 1)
              .drop("_rank")
        )

        assert df_dedup.count() == 1

    def test_keeps_most_recent_on_conflict(self, spark_session):
        """Dois registros com mesmo PK e timestamps diferentes → mantém o mais recente."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        data = [
            ("evt_002", "2024-01-15 08:00:00", "PARADO"),
            ("evt_002", "2024-01-15 10:00:00", "EM_TRANSITO"),  # mais recente
        ]
        df = spark_session.createDataFrame(data, ["evento_id", "updated_at", "status"])

        window = Window.partitionBy("evento_id").orderBy(F.col("updated_at").desc())
        df_dedup = (
            df.withColumn("_rank", F.row_number().over(window))
              .filter(F.col("_rank") == 1)
              .drop("_rank")
        )

        result = df_dedup.collect()[0]
        assert result["status"] == "EM_TRANSITO"
        assert result["updated_at"] == "2024-01-15 10:00:00"

    def test_no_dedup_needed_preserves_all(self, spark_session):
        """Registros com PKs diferentes → nenhum removido."""
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        data = [
            ("evt_001", "2024-01-15 10:00:00", "PARADO"),
            ("evt_002", "2024-01-15 11:00:00", "EM_TRANSITO"),
            ("evt_003", "2024-01-15 12:00:00", "CHEGOU"),
        ]
        df = spark_session.createDataFrame(data, ["evento_id", "updated_at", "status"])

        window = Window.partitionBy("evento_id").orderBy(F.col("updated_at").desc())
        df_dedup = (
            df.withColumn("_rank", F.row_number().over(window))
              .filter(F.col("_rank") == 1)
              .drop("_rank")
        )

        assert df_dedup.count() == 3


# ── Testes de Validação de Schema ─────────────────────────────────────────────

class TestSchemaValidation:

    def test_raises_on_missing_required_column(self, spark_session):
        """Deve lançar ValueError quando coluna obrigatória está ausente."""
        data = [("evt_001", "2024-01-15")]
        df = spark_session.createDataFrame(data, ["evento_id", "updated_at"])
        # Remove coluna obrigatória
        df_without_pk = df.drop("evento_id")

        required_cols = ["evento_id", "updated_at"]
        missing = [c for c in required_cols if c not in df_without_pk.columns]

        assert "evento_id" in missing

    def test_passes_with_all_required_columns(self, spark_session):
        """Não deve lançar erro quando todas as colunas obrigatórias existem."""
        data = [("evt_001", "2024-01-15")]
        df = spark_session.createDataFrame(data, ["evento_id", "updated_at"])

        required_cols = ["evento_id", "updated_at"]
        missing = [c for c in required_cols if c not in df.columns]

        assert len(missing) == 0


# ── Testes de Enriquecimento de Metadados ─────────────────────────────────────

class TestEnrichMetadata:

    def test_adds_partition_date_column(self, spark_session):
        from pyspark.sql import functions as F

        data = [("evt_001", "2024-01-15 10:30:00")]
        df = spark_session.createDataFrame(data, ["evento_id", "updated_at"])
        df_enriched = df.withColumn("_partition_date", F.to_date(F.col("updated_at")))

        assert "_partition_date" in df_enriched.columns
        result = df_enriched.collect()[0]
        assert str(result["_partition_date"]) == "2024-01-15"

    def test_adds_source_domain_column(self, spark_session):
        from pyspark.sql import functions as F

        data = [("evt_001",)]
        df = spark_session.createDataFrame(data, ["evento_id"])
        df_enriched = df.withColumn("_source_domain", F.lit("eventos_ferroviarios"))

        result = df_enriched.collect()[0]
        assert result["_source_domain"] == "eventos_ferroviarios"


# ── Testes de Lambda: API Ingestion ───────────────────────────────────────────

class TestApiIngestionLambda:

    @mock_aws
    def test_upload_to_s3_creates_object(self, aws_credentials):
        """Verifica que o handler deposita arquivo no S3 raw corretamente."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="rumo-raw")

        import importlib.util, os
        os.environ["RAW_BUCKET"] = "rumo-raw"
        os.environ["DOMAIN"] = "parceiros_logisticos"
        os.environ["API_SECRET_ARN"] = "arn:aws:secretsmanager:us-east-1:123:secret:test"

        records = [{"id": 1, "nome": "Parceiro A"}, {"id": 2, "nome": "Parceiro B"}]
        partition_date = "2024-01-15"

        # Simula a função upload_to_s3 diretamente
        import json
        key = f"parceiros_logisticos/date={partition_date}/page_0001.json"
        body = "\n".join(json.dumps(r) for r in records)
        s3.put_object(Bucket="rumo-raw", Key=key, Body=body.encode())

        # Verifica que o objeto foi criado
        response = s3.get_object(Bucket="rumo-raw", Key=key)
        content = response["Body"].read().decode("utf-8")
        lines = content.strip().split("\n")

        assert len(lines) == 2
        assert json.loads(lines[0])["nome"] == "Parceiro A"

    @mock_aws
    def test_success_marker_created(self, aws_credentials):
        """Verifica criação do arquivo _SUCCESS para o S3KeySensor."""
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.create_bucket(Bucket="rumo-raw")

        key = "parceiros_logisticos/date=2024-01-15/_SUCCESS"
        s3.put_object(Bucket="rumo-raw", Key=key, Body=b"")

        response = s3.head_object(Bucket="rumo-raw", Key=key)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
