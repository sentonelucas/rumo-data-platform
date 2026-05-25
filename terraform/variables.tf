variable "aws_region" {
  description = "Região AWS principal"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente de deployment (dev, staging, prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Ambiente deve ser dev, staging ou prod."
  }
}

variable "alert_email" {
  description = "Email para alertas SNS de pipeline"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID onde os recursos serão provisionados"
  type        = string
}

variable "private_subnet_ids" {
  description = "IDs das subnets privadas para Redshift e MWAA"
  type        = list(string)
}
