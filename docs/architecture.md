# Arquitetura Técnica Detalhada
## Rumo Logística — Plataforma Moderna de Dados na AWS

---

## 1. Princípios de Design

| Princípio | Aplicação |
|-----------|-----------|
| **Dados imutáveis no raw** | Nenhum dado é deletado ou modificado na zona raw; apenas appended |
| **Idempotência** | Todos os jobs podem ser reexecutados sem efeitos colaterais (Iceberg MERGE) |
| **Falha rápida** | Dados ruins são barrados antes de chegar ao gold (Data Quality gate) |
| **Observabilidade** | Cada etapa publica métricas no CloudWatch e logs estruturados |
| **Menor privilégio** | IAM Roles por função; Lake Formation por tabela/coluna |
| **Custo controlado** | Spot instances no Glue; Athena para exploração; S3 lifecycle para arquivo |

---

## 2. Decisões de Arquitetura

### Por que Apache Iceberg no S3?

O formato Iceberg foi escolhido em vez de Delta Lake ou Hudi pelos seguintes motivos:

- **Integração nativa AWS**: Suportado nativamente por Athena, Glue e EMR sem configuração extra
- **ACID transactions**: Permite MERGE INTO para CDC eficiente sem full rewrites
- **Time travel**: `SELECT * FROM tabela FOR SYSTEM_TIME AS OF '2024-01-01'` — essencial para auditoria e reprocessamento
- **Schema evolution**: Adicionar/remover colunas sem quebrar queries existentes
- **Partition evolution**: Mudar estratégia de particionamento sem reescrever dados

### Por que Kinesis em vez de Kafka?

Para o contexto da Rumo (AWS-native, equipe de dados sem SRE dedicado):

- **Zero operação**: Kinesis é serverless, Kafka requer cluster gerenciado (MSK) ou self-managed
- **Integração AWS**: Conector nativo com DMS, Lambda, Firehose, KDA
- **On-demand scaling**: Shards escalam automaticamente sem reconfiguração manual
- **Custo**: Para 20-30M registros/dia, Kinesis On-Demand é mais econômico que MSK

### Por que Redshift + Athena juntos?

Cada ferramenta tem um papel distinto:

| Critério | Redshift | Athena |
|----------|----------|--------|
| Tipo de query | Dashboards, relatórios regulares | Exploração ad-hoc, investigação |
| Performance | Consistente (tabelas materializadas) | Variável (depende do volume) |
| Custo | Fixo (nós ra3) | Por dado escaneado ($5/TB) |
| Usuários | BI tools (Power BI, Tableau) | Analistas, engenheiros |
| Dados | Gold zone apenas | Todas as zonas |

---

## 3. Decisões de Modelagem

### Medallion Architecture (Bronze/Silver/Gold)

```
RAW         → dados tal como chegam (CSV, JSON, Parquet do DMS)
BRONZE      → dados validados, tipados, deduplicados, em Iceberg
SILVER      → dados transformados com regras de negócio, enriquecidos
GOLD        → tabelas dimensionais e fatos, prontas para consumo analítico
```

### Particionamento

Todas as tabelas são particionadas por `_partition_date` (DATE). Isso garante:
- Queries filtradas por data escaneiam apenas as partições relevantes (cost efficiency)
- Glue jobs processam apenas a partição do dia (incremental, não full scan)
- Reprocessamento de um dia específico é cirúrgico

### Tabelas Fato no Gold

```sql
-- Exemplo: fato_eventos_ferroviarios (modelagem estrela simplificada)
CREATE TABLE rumo_gold.fato_eventos_ferroviarios (
    evento_sk        BIGINT IDENTITY,    -- surrogate key
    evento_id        VARCHAR(50),        -- natural key (source)
    vagao_sk         BIGINT,             -- FK → dim_vagao
    contrato_sk      BIGINT,             -- FK → dim_contrato
    cliente_sk       BIGINT,             -- FK → dim_cliente
    data_sk          INTEGER,            -- FK → dim_data
    status_norm      VARCHAR(50),
    duracao_minutos  DECIMAL(10,2),
    sla_violado      BOOLEAN,
    capacidade_ton   DECIMAL(10,2),
    _partition_date  DATE
)
DISTSTYLE KEY DISTKEY(vagao_sk)
SORTKEY(_partition_date, status_norm);
```

---

## 4. SLA e Monitoramento

### SLA Tracking

| Pipeline | SLA | Monitoramento |
|----------|-----|---------------|
| Batch D-1 (corporate) | Dados disponíveis até 07:00 BRT | MWAA SLA Miss callback + CloudWatch alarm |
| Near real-time (operacional) | Latência ≤ 5 min | Kinesis `GetRecords.IteratorAgeMilliseconds` alarm |
| Data Quality gate | 0 registros ruins no gold | Glue DQ + SNS alert |

### CloudWatch Alarms Críticos

```
Rumo/DataPlatform/JobFailure > 0              → SNS alert imediato
Rumo/DataPlatform/DuplicatesRemoved > 50000   → investigar CDC
Rumo/DataPlatform/SchemaValidationFailure > 0  → alert crítico
Kinesis IteratorAge > 300000ms (5min)          → SLA near real-time em risco
Redshift CPUUtilization > 80%                  → considerar resize
```

---

## 5. Segurança e Compliance (LGPD)

- **Dados PII (CPF, nome de clientes)**: armazenados em colunas com `column-level encryption` via AWS KMS
- **Lake Formation column masking**: Analistas veem apenas dados mascarados
- **CloudTrail**: Toda query ao data lake é auditada (who, what, when)
- **VPC Endpoints**: Comunicação S3 ↔ Glue sem tráfego pela internet pública
- **Secrets Manager**: Zero credenciais em código — rotação automática a cada 30 dias

---

## 6. Estimativa de Custo Mensal (prod, us-east-1)

| Serviço | Uso Estimado | Custo/mês |
|---------|-------------|-----------|
| S3 (3 TB úteis após compressão) | Standard + Glacier | ~$80 |
| AWS DMS (2 instâncias r5.large) | 24/7 | ~$280 |
| Kinesis On-Demand | 30M events/day | ~$150 |
| Glue Jobs (batch diário) | 20 DPUs × 2h | ~$200 |
| MWAA (mw1.small) | 24/7 | ~$250 |
| Redshift (2× ra3.4xlarge) | 24/7 | ~$1.500 |
| Athena | 5 TB escaneado/mês | ~$25 |
| DynamoDB (On-Demand) | 30M writes/day | ~$150 |
| Lambda + EventBridge | Ingestão APIs | ~$10 |
| Transfer Family (SFTP) | 100 GB/mês | ~$30 |
| CloudWatch + SNS | Logs e alertas | ~$50 |
| **Total estimado** | | **~$2.725/mês** |

> **Otimização com Spot Instances**: Jobs Glue batch podem usar G.1X Spot, reduzindo ~70% o custo de processamento.
