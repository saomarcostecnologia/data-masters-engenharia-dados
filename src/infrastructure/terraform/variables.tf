# infrastructure/terraform/variables.tf
variable "aws_region" {
  description = "Região AWS onde os recursos serão provisionados"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente de deploy (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Nome do projeto"
  type        = string
  default     = "economic-indicators-etl"
}

variable "data_lake_bucket" {
  description = "Nome do bucket S3 para o Data Lake"
  type        = string
  default     = "economic-indicators-data-lake"
}

variable "alert_emails" {
  description = "Lista de emails para receber alertas"
  type        = list(string)
  default     = []
}