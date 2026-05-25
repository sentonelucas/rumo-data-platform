# 🚂 Rumo Logística — Plataforma Moderna de Dados na AWS

**Case Técnico | Engenheiro de Dados**

---

## Sumário

- [Contexto](#contexto)
- [Arquitetura](#arquitetura)
- [Serviços AWS Utilizados](#serviços-aws-utilizados)
- [Fluxo dos Dados](#fluxo-dos-dados)
- [Pontos de Falha e Mitigações](#pontos-de-falha-e-mitigações)
- [Comportamento sob Carga](#comportamento-sob-carga)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Como Executar Localmente](#como-executar-localmente)

---

## Contexto

A Rumo Logística opera uma das maiores malhas ferroviárias do Brasil. A operação gera **20–30 milhões de registros/dia** vindos de:

| Fonte | Tipo | Latência Esperada |
|-------|------|-------------------|
| SQL Server (on-prem) | CDC — eventos ferroviários, vagões | ≤ 5 min (near real-time) |
| Oracle (on-prem) | CDC — contratos, faturamento, financeiro | D-1 |
| APIs externas | REST — clientes, parceiros, reguladores | D-1 / near real-time |
| SFTP (CSV/JSON) | Arquivos — unidades de campo e sensores | D-1 |

**Problemas atuais:** cargas full lentas, ausência de CDC, reprocessamentos manuais, falhas silenciosas, e falta de rastreabilidade.

---

## Arquitetura

A plataforma é organizada em **7 camadas** (Medallion Architecture + Streaming):

```
┌─────────────────────────────────────────────────────────────────────┐
│                          FONTES DE DADOS                            │
│  SQL Server │ Oracle │ APIs Externas │ SFTP (CSV/JSON)              │
└──────┬──────┴───┬────┴──────┬────────┴──────┬──────────────────────┘
       │          │           │               │
       ▼          ▼           ▼               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         CAMADA DE INGESTÃO                          │
│                                                                     │
│  ┌─────────────┐  ┌──────────────┐  ┌──────────────┐               │
│  │  AWS DMS    │  │   Lambda     │  │AWS Transfer  │               │
│  │  (CDC)      │  │ (API Caller) │  │Family (SFTP) │               │
│  └──────┬──────┘  └──────┬───────┘  └──────┬───────┘               │
│         │                │                 │                        │
│   ┌─────▼──────┐         │                 │                        │
│   │  Kinesis   │─────────┘─────────────────┘                       │
│   │  Streams   │  (dados near real-time)                            │
│   └─────┬──────┘                                                    │
└─────────┼───────────────────────────────────────────────────────────┘
          │
          ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     DATA LAKE — AMAZON S3                           │
│                                                                     │
│  raw/          bronze/         silver/          gold/               │
│  (landing)  →  (validated)  →  (transformed) →  (aggregated)        │
│  Parquet        Iceberg         Iceberg           Iceberg           │
│                                                                     │
│               AWS Glue Data Catalog (metadados centralizados)       │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE PROCESSAMENTO                        │
│                                                                     │
│  ┌──────────────────┐    ┌───────────────────────────┐             │
│  │  AWS Glue        │    │ Kinesis Data Analytics    │             │
│  │  (PySpark Batch) │    │ (Apache Flink — Streaming)│             │
│  └──────────────────┘    └───────────────────────────┘             │
│                                                                     │
│  ┌──────────────────┐    ┌───────────────────────────┐             │
│  │  AWS MWAA        │    │  Glue Data Quality        │             │
│  │  (Airflow)       │    │  (validação automática)   │             │
│  └──────────────────┘    └───────────────────────────┘             │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE SERVING                              │
│                                                                     │
│  ┌────────────────┐  ┌────────────┐  ┌────────────────────────┐    │
│  │Amazon Redshift │  │  Athena    │  │  DynamoDB              │    │
│  │(Data Warehouse)│  │(Ad-hoc SQL)│  │  (operacional ≤10ms)   │    │
│  └────────────────┘  └────────────┘  └────────────────────────┘    │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      CAMADA DE CONSUMO                              │
│                                                                     │
│  Amazon QuickSight  │  Power BI / Tableau  │  API Gateway + Lambda │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Serviços AWS Utilizados

### Ingestão
| Serviço | Uso | Justificativa |
|---------|-----|---------------|
| **AWS DMS** | CDC de SQL Server e Oracle | Captura alterações em tempo real sem impacto no sistema de origem; suporta ongoing replication |
| **Amazon Kinesis Data Streams** | Buffer de eventos near real-time | Desacopla produtores e consumidores; retém dados por até 7 dias; escala por shards |
| **AWS Transfer Family** | Ingestão SFTP | Managed SFTP nativo AWS sem infra adicional; deposita diretamente no S3 |
| **AWS Lambda** | Chamadas às APIs externas | Serverless, pago por execução, fácil de escalar e versionar |
| **Amazon EventBridge** | Gatilho de eventos (ex: arquivo chegou no S3) | Orquestração event-driven sem polling |

### Armazenamento
| Serviço | Uso | Justificativa |
|---------|-----|---------------|
| **Amazon S3** | Data Lake (raw → bronze → silver → gold) | Custo baixo, durabilidade 99.999999999%, escala ilimitada |
| **Apache Iceberg** | Formato de tabela no S3 | ACID transactions, time travel, schema evolution, upserts eficientes (essencial para CDC) |
| **AWS Glue Data Catalog** | Metadados centralizados | Único catálogo para Glue, Athena e Redshift Spectrum |

### Processamento
| Serviço | Uso | Justificativa |
|---------|-----|---------------|
| **AWS Glue (PySpark)** | Transformações batch | Serverless Spark gerenciado; integração nativa com S3 e Catalog |
| **Kinesis Data Analytics (Flink)** | Processamento streaming | Flink gerenciado; janelas temporais, joins de streams, exatamente-uma-vez (EOS) |
| **AWS MWAA (Airflow)** | Orquestração | Airflow gerenciado; DAGs versionados em Git; retry, SLA monitoring, alertas |
| **Glue Data Quality** | Validação automática | Regras declarativas; falha o pipeline antes de dados ruins chegarem ao gold |

### Serving
| Serviço | Uso | Justificativa |
|---------|-----|---------------|
| **Amazon Redshift** | Analytics e DW | Columnar, MPP; Concurrency Scaling para picos; Spectrum para queries no S3 |
| **Amazon Athena** | Queries ad-hoc | Serverless SQL sobre S3/Iceberg; pago por dado escaneado; ideal para exploração |
| **Amazon DynamoDB** | Dados operacionais em tempo real | Latência <10ms; escala automática; ideal para consultas de status de vagão/trem |

### Segurança & Governança
| Serviço | Uso | Justificativa |
|---------|-----|---------------|
| **AWS Lake Formation** | Controle de acesso ao Data Lake | Permissões por tabela/coluna/linha; integra com Glue Catalog e Athena |
| **AWS IAM** | Identidade e permissões | Roles por time (engenharia, analistas, BI); princípio de menor privilégio |
| **AWS KMS** | Criptografia em repouso | Chaves gerenciadas por domínio; conformidade com LGPD |
| **AWS Secrets Manager** | Credenciais de banco | Rotação automática; sem credencial em código |

### Observabilidade
| Serviço | Uso | Justificativa |
|---------|-----|---------------|
| **Amazon CloudWatch** | Métricas, logs, alarmes | Centraliza observabilidade; integra com todos os serviços AWS |
| **AWS CloudTrail** | Auditoria de ações | Rastreabilidade de quem acessou o quê e quando |
| **Amazon SNS** | Notificações | Alertas em tempo real para falhas de pipeline via email/Slack/PagerDuty |

---

## Fluxo dos Dados

### Fluxo 1 — Near Real-Time (SLA ≤ 5 min)
```
SQL Server (eventos ferroviários)
    │
    ▼  CDC via AWS DMS
Amazon Kinesis Data Streams  ──►  Kinesis Data Analytics (Flink)
    │                                        │
    │  (raw events)                          │  (agregações, enriquecimento)
    ▼                                        ▼
S3 / raw zone                    S3 / bronze zone (Iceberg)
                                             │
                                             ▼
                                  Amazon DynamoDB  ──►  API Gateway  ──►  Clientes
                                  (status de vagões)
```

### Fluxo 2 — Batch D-1 (Oracle / SFTP / APIs)
```
Oracle (financeiro)  ──►  AWS DMS  ──►  S3 / raw
SFTP (CSV/JSON)      ──►  Transfer Family  ──►  S3 / raw
APIs externas        ──►  Lambda  ──►  S3 / raw
        │
        ▼  (MWAA Airflow — DAG diário às 01:00)
    Glue Job: raw → bronze  (Parquet + Iceberg, dedup, validação de schema)
        │
        ▼
    Glue Job: bronze → silver  (transformações de negócio, joins, enriquecimento)
        │
        ▼
    Glue Data Quality  (regras de qualidade: completude, unicidade, integridade)
        │
        ▼
    dbt run  (silver → gold: staging → marts via SQL versionado)
        │  dbt test  (not_null, unique, accepted_values, expectations)
        │
        ├──►  Amazon Redshift (marts)  ──►  QuickSight / BI Tools
        └──►  Athena (queries ad-hoc no S3/Iceberg)
```

---

## Pontos de Falha e Mitigações

| # | Componente | Falha Potencial | Mitigação |
|---|-----------|-----------------|-----------|
| 1 | **AWS DMS** | Replicação CDC interrompida (rede, schema change) | CloudWatch alarm no `CDCLatency`; replay automático; Glue Schema Registry para detectar drift |
| 2 | **Kinesis Data Streams** | Throttling de shards em pico sazonal | Auto-scaling via Lambda + CW alarm; Enhanced Fan-Out para consumidores críticos |
| 3 | **Kinesis Data Analytics (Flink)** | Job crash em schema inesperado | Dead Letter Queue no Kinesis; checkpointing a cada 1 min; restart automático com estado salvo no S3 |
| 4 | **AWS Glue Job** | Falha durante transformação batch | Jobs idempotentes (Iceberg upsert); retries com backoff exponencial; alertas SNS; dados raw sempre preservados |
| 5 | **S3 Raw Zone** | Arquivo corrompido ou formato inválido | Lambda de validação no `s3:ObjectCreated`; arquivo movido para `s3://quarantine/` com metadado de erro |
| 6 | **Amazon Redshift** | Lentidão em horário de pico analítico | Concurrency Scaling automático; WLM com filas por prioridade; Materialized Views pré-computadas |
| 7 | **AWS MWAA (Airflow)** | DAG falha silenciosamente | SLA Miss callbacks; CloudWatch alarm no `TaskFailed`; notificação SNS → Slack/email |
| 8 | **AWS DMS + Schema Change** | Mudança de schema no Oracle/SQL Server quebra pipeline | Schema Registry + Glue Crawler; pipeline com `PERMISSIVE` mode + dead letter para campos novos |

---

## Comportamento sob Carga

### Pico Sazonal (Safra — Jun/Jul/Ago)

| Componente | Comportamento Normal | Comportamento no Pico | Mecanismo |
|-----------|---------------------|----------------------|-----------|
| Kinesis Streams | 4 shards (40 MB/s) | 12 shards (120 MB/s) | Auto-scaling via Lambda + CW |
| Glue Workers | 10 DPUs | 50 DPUs | `--number-of-workers` dinâmico via Airflow |
| Glue Spot Instances | Habilitado | Habilitado | Redução de custo de 70% |
| Redshift | 2 nós ra3.4xlarge | + Concurrency Scaling (auto) | Elastic resize ou concurrency scaling |
| DynamoDB | On-demand mode | On-demand mode | Escala automática sem configuração |
| Lambda | Auto-scaling | Auto-scaling | Concorrência reservada para funções críticas |

### Estimativa de Capacidade

```
Volume diário: 30M registros × 500 bytes avg = ~15 GB/dia
Histórico 5 anos: 15 GB × 365 × 5 = ~27 TB (raw)
Com Parquet (compressão 10x): ~2.7 TB (bronze/silver)
Custo estimado S3 (~3 TB): ~$70/mês
Custo Glue batch diário: ~$15/dia (10 DPUs × 1h × G.1X)
```

---

## Estrutura do Repositório

```
rumo-data-platform/
├── README.md
├── docs/
│   ├── architecture.md               # Detalhamento técnico + decisões de design
│   ├── data_contracts/               # Schemas e contratos de dados
│   └── runbooks/                     # Procedimentos operacionais
├── glue_jobs/
│   ├── ingestion_raw_to_bronze.py    # Job: raw → bronze (dedup, Iceberg MERGE)
│   ├── transformation_silver.py      # Job: bronze → silver (regras de negócio)
│   └── data_quality_checks.py        # Validações com Glue Data Quality + SNS
├── dbt/                              # silver → gold via SQL versionado
│   ├── dbt_project.yml               # Configuração do projeto
│   ├── profiles.yml                  # Conexões por ambiente (dev/ci/prod)
│   ├── packages.yml                  # dbt_utils, dbt_expectations, audit_helper
│   ├── macros/
│   │   ├── generate_schema_name.sql  # Schema isolado por ambiente
│   │   └── get_custom_alias.sql
│   └── models/
│       ├── staging/                  # Views sobre o silver (renomeia, cast, filtra)
│       │   ├── stg_eventos_ferroviarios.sql
│       │   ├── stg_vagoes.sql
│       │   └── schema.yml            # Testes + documentação + freshness
│       └── marts/                    # Tabelas gold para consumo BI
│           ├── fct_eventos_ferroviarios.sql   # Fato incremental (merge)
│           ├── mart_sla_violations.sql        # Mart analítico de SLA
│           └── schema.yml
├── airflow/
│   └── dags/
│       ├── daily_batch_pipeline.py   # DAG D-1: Glue + dbt run + dbt test
│       └── pipeline_monitor.py       # DAG de monitoramento e alertas
├── lambda/
│   ├── api_ingestion/handler.py      # Ingestão paginada de APIs externas
│   └── sftp_trigger/handler.py       # Trigger pós-upload SFTP → S3
├── terraform/
│   ├── main.tf                       # Infra: S3, KMS, Kinesis, Redshift, DynamoDB
│   ├── variables.tf
│   └── modules/
│       ├── s3/  kinesis/  glue/  redshift/  dms/
├── tests/
│   └── test_ingestion_bronze.py      # pytest + moto: testa jobs sem infra AWS real
└── .github/workflows/
    ├── ci.yml     # PR: lint, unit tests, DAG validation, dbt parse, tf validate
    └── deploy.yml # merge/main: deploy Glue, DAGs MWAA, dbt run+test, notify Slack
```

---

## Como Executar Localmente

### Pré-requisitos
```bash
# Python 3.10+
pip install awscli boto3 apache-airflow pyspark

# Terraform 1.5+
brew install terraform

# AWS CLI configurado
aws configure
```

### Deploy da Infra (Terraform)
```bash
cd terraform/
terraform init
terraform plan -var-file="envs/dev.tfvars"
terraform apply -var-file="envs/dev.tfvars"
```

### Executar Glue Job localmente (com Glue Local)
```bash
cd glue_jobs/
docker run -it \
  -v ~/.aws:/root/.aws \
  -v $(pwd):/home/glue_user/workspace \
  amazon/aws-glue-libs:glue_libs_4.0.0_image_01 \
  python3 ingestion_raw_to_bronze.py
```

### Testar DAGs Airflow
```bash
cd airflow/
airflow standalone
# Acesse http://localhost:8080 (user: admin)
```

---

## Autor

**Lucas** — Case Técnico para Engenheiro de Dados | Rumo Logística
