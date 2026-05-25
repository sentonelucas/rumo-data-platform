##############################################################################
# Rumo Logística — Plataforma Moderna de Dados na AWS
# Terraform — Infraestrutura Principal
##############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  # State remoto no S3 + DynamoDB locking
  backend "s3" {
    bucket         = "rumo-terraform-state"
    key            = "data-platform/terraform.tfstate"
    region         = "us-east-1"
    dynamodb_table = "rumo-terraform-lock"
    encrypt        = true
  }
}

provider "aws" {
  region = var.aws_region
  default_tags {
    tags = {
      Project     = "rumo-data-platform"
      Environment = var.environment
      ManagedBy   = "terraform"
      Team        = "data-engineering"
    }
  }
}

##############################################################################
# DATA LAKE — S3 BUCKETS (4 zonas: raw, bronze, silver, gold)
##############################################################################

resource "aws_s3_bucket" "data_lake" {
  for_each = toset(["raw", "bronze", "silver", "gold"])

  bucket = "rumo-${each.key}-${var.environment}"

  lifecycle {
    prevent_destroy = true
  }
}

resource "aws_s3_bucket_versioning" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  bucket   = each.value.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "data_lake" {
  for_each = aws_s3_bucket.data_lake
  bucket   = each.value.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm     = "aws:kms"
      kms_master_key_id = aws_kms_key.data_lake.arn
    }
  }
}

# Lifecycle rules: move dados antigos para Glacier
resource "aws_s3_bucket_lifecycle_configuration" "raw_lifecycle" {
  bucket = aws_s3_bucket.data_lake["raw"].id

  rule {
    id     = "archive-old-data"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 365
      storage_class = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 30
    }
  }
}

##############################################################################
# KMS — Chave de criptografia do Data Lake
##############################################################################

resource "aws_kms_key" "data_lake" {
  description             = "Rumo Data Lake encryption key"
  deletion_window_in_days = 30
  enable_key_rotation     = true

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "Enable IAM User Permissions"
        Effect = "Allow"
        Principal = { AWS = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root" }
        Action    = "kms:*"
        Resource  = "*"
      }
    ]
  })
}

resource "aws_kms_alias" "data_lake" {
  name          = "alias/rumo-data-lake-${var.environment}"
  target_key_id = aws_kms_key.data_lake.key_id
}

##############################################################################
# GLUE DATA CATALOG
##############################################################################

resource "aws_glue_catalog_database" "zones" {
  for_each = toset(["raw", "bronze", "silver", "gold"])
  name     = "rumo_${each.key}"
}

# IAM Role para jobs Glue
resource "aws_iam_role" "glue_service_role" {
  name = "rumo-glue-service-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_service_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "glue_s3_access" {
  name = "rumo-glue-s3-policy"
  role = aws_iam_role.glue_service_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"]
        Resource = [for b in aws_s3_bucket.data_lake : "${b.arn}/*"]
      },
      {
        Effect   = "Allow"
        Action   = ["kms:Decrypt", "kms:GenerateDataKey"]
        Resource = aws_kms_key.data_lake.arn
      },
      {
        Effect   = "Allow"
        Action   = ["cloudwatch:PutMetricData", "logs:CreateLogGroup",
                    "logs:CreateLogStream", "logs:PutLogEvents"]
        Resource = "*"
      }
    ]
  })
}

##############################################################################
# KINESIS DATA STREAMS — Eventos near real-time
##############################################################################

resource "aws_kinesis_stream" "operational_events" {
  name             = "rumo-operational-events-${var.environment}"
  retention_period = 168  # 7 dias

  stream_mode_details {
    stream_mode = "ON_DEMAND"  # Auto-scaling de shards
  }

  encryption_type = "KMS"
  kms_key_id      = aws_kms_key.data_lake.arn
}

# Firehose: Kinesis → S3 (buffer automático)
resource "aws_kinesis_firehose_delivery_stream" "events_to_s3" {
  name        = "rumo-events-to-s3-${var.environment}"
  destination = "extended_s3"

  kinesis_source_configuration {
    kinesis_stream_arn = aws_kinesis_stream.operational_events.arn
    role_arn           = aws_iam_role.firehose_role.arn
  }

  extended_s3_configuration {
    role_arn           = aws_iam_role.firehose_role.arn
    bucket_arn         = aws_s3_bucket.data_lake["raw"].arn
    prefix             = "kinesis/events/date=!{timestamp:yyyy-MM-dd}/"
    error_output_prefix = "kinesis/errors/date=!{timestamp:yyyy-MM-dd}/!{firehose:error-output-type}/"
    buffering_interval = 300   # 5 minutos (SLA near real-time)
    buffering_size     = 128   # 128 MB

    data_format_conversion_configuration {
      enabled = true
      input_format_configuration {
        deserializer { open_x_json_ser_de {} }
      }
      output_format_configuration {
        serializer { parquet_ser_de {} }
      }
      schema_configuration {
        database_name = "rumo_raw"
        table_name    = "eventos_ferroviarios_stream"
        role_arn      = aws_iam_role.firehose_role.arn
      }
    }
  }
}

##############################################################################
# AMAZON REDSHIFT — Data Warehouse
##############################################################################

resource "aws_redshift_cluster" "rumo_dw" {
  cluster_identifier     = "rumo-dw-${var.environment}"
  database_name          = "rumo_dw"
  master_username        = "admin"
  master_password        = data.aws_secretsmanager_secret_version.redshift_password.secret_string
  node_type              = "ra3.4xlarge"
  cluster_type           = "multi-node"
  number_of_nodes        = 2

  encrypted              = true
  kms_key_id             = aws_kms_key.data_lake.arn

  vpc_security_group_ids = [aws_security_group.redshift.id]
  cluster_subnet_group_name = aws_redshift_subnet_group.main.name

  # Habilita Concurrency Scaling automático
  aqua_configuration_status = "auto"

  skip_final_snapshot = false
  final_snapshot_identifier = "rumo-dw-final-${var.environment}"

  automated_snapshot_retention_period = 7  # dias
}

##############################################################################
# AMAZON DYNAMODB — Dados operacionais de baixa latência
##############################################################################

resource "aws_dynamodb_table" "vagoes_status" {
  name         = "rumo-vagoes-status-${var.environment}"
  billing_mode = "PAY_PER_REQUEST"  # Auto-scaling, ideal para picos de safra
  hash_key     = "vagao_id"
  range_key    = "updated_at"

  attribute {
    name = "vagao_id"
    type = "S"
  }
  attribute {
    name = "updated_at"
    type = "S"
  }

  # TTL para registros antigos (mantém apenas últimas 24h no hot storage)
  ttl {
    attribute_name = "expires_at"
    enabled        = true
  }

  server_side_encryption {
    enabled     = true
    kms_key_arn = aws_kms_key.data_lake.arn
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = { Name = "rumo-vagoes-status" }
}

##############################################################################
# SNS — Alertas de pipeline
##############################################################################

resource "aws_sns_topic" "data_alerts" {
  name              = "rumo-data-alerts-${var.environment}"
  kms_master_key_id = aws_kms_key.data_lake.arn
}

resource "aws_sns_topic_subscription" "data_alerts_email" {
  topic_arn = aws_sns_topic.data_alerts.arn
  protocol  = "email"
  endpoint  = var.alert_email
}

##############################################################################
# DATA SOURCES
##############################################################################

data "aws_caller_identity" "current" {}

data "aws_secretsmanager_secret_version" "redshift_password" {
  secret_id = "rumo/redshift/master-password"
}
