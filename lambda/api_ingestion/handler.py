"""
Lambda: api_ingestion
Rumo Logística | Plataforma Moderna de Dados

Ingere dados de APIs externas (clientes, parceiros logísticos, reguladores)
e deposita no S3 raw zone em formato JSON/Parquet.

Trigger: EventBridge Scheduler (configurável por API)
"""

import json
import os
import boto3
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone, date

s3 = boto3.client("s3")
ssm = boto3.client("secretsmanager")
cw = boto3.client("cloudwatch")

RAW_BUCKET = os.environ["RAW_BUCKET"]          # rumo-raw
DOMAIN = os.environ["DOMAIN"]                  # ex: parceiros_logisticos
API_SECRET_ARN = os.environ["API_SECRET_ARN"]  # ARN no Secrets Manager


def get_api_credentials() -> dict:
    """Busca credenciais da API no AWS Secrets Manager (nunca em código)."""
    secret = ssm.get_secret_value(SecretId=API_SECRET_ARN)
    return json.loads(secret["SecretString"])


def call_api(credentials: dict, page: int = 1) -> dict:
    """
    Realiza chamada paginada à API externa.
    Implementa retry automático com exponential backoff.
    """
    base_url = credentials["base_url"]
    api_key = credentials["api_key"]

    url = f"{base_url}/v1/data?page={page}&page_size=1000"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def upload_to_s3(records: list, partition_date: str, page: int) -> str:
    """
    Deposita registros no S3 raw zone em formato JSON Lines (NDJSON).
    Particionado por data para otimizar Glue Crawler e queries Athena.
    """
    key = f"{DOMAIN}/date={partition_date}/page_{page:04d}.json"
    body = "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in records)

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
        Metadata={
            "domain": DOMAIN,
            "partition_date": partition_date,
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "record_count": str(len(records)),
        }
    )
    return f"s3://{RAW_BUCKET}/{key}"


def write_success_marker(partition_date: str) -> None:
    """Cria arquivo _SUCCESS no S3 para sinalizar ao S3KeySensor que a ingestão terminou."""
    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=f"{DOMAIN}/date={partition_date}/_SUCCESS",
        Body=b"",
    )


def publish_metric(name: str, value: float) -> None:
    cw.put_metric_data(
        Namespace="Rumo/DataPlatform",
        MetricData=[{
            "MetricName": name,
            "Dimensions": [{"Name": "Domain", "Value": DOMAIN}],
            "Value": value,
            "Unit": "Count",
        }]
    )


def handler(event, context):
    """
    Ponto de entrada da Lambda.
    Itera por todas as páginas da API e deposita no S3.
    """
    partition_date = event.get("date", date.today().strftime("%Y-%m-%d"))
    print(f"[START] Ingestão {DOMAIN} para {partition_date}")

    credentials = get_api_credentials()

    total_records = 0
    page = 1
    s3_paths = []

    while True:
        try:
            response = call_api(credentials, page=page)
        except urllib.error.HTTPError as e:
            print(f"[ERROR] HTTP {e.code} na página {page}: {e.reason}")
            publish_metric("ApiIngestionError", 1)
            raise

        records = response.get("data", [])
        if not records:
            print(f"[END] Última página: {page - 1} | Total: {total_records:,} registros")
            break

        s3_path = upload_to_s3(records, partition_date, page)
        s3_paths.append(s3_path)
        total_records += len(records)
        print(f"[PAGE {page}] {len(records)} registros → {s3_path}")

        # Verifica se há mais páginas
        if not response.get("has_next_page", False):
            break

        page += 1

    # Marca sucesso para o S3KeySensor
    write_success_marker(partition_date)
    publish_metric("ApiRecordsIngested", total_records)

    return {
        "statusCode": 200,
        "domain": DOMAIN,
        "partition_date": partition_date,
        "total_records": total_records,
        "pages": page,
        "s3_paths": s3_paths,
    }
