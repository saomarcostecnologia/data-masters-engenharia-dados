# infrastructure/terraform/main.tf
provider "aws" {
  region = var.aws_region
}

# ---------------------------------------------------------------------------------------------------------------------
# Módulo para bucket S3 (Data Lake)
# ---------------------------------------------------------------------------------------------------------------------
module "s3_data_lake" {
  source = "./modules/s3"
  
  bucket_name              = var.data_lake_bucket
  environment              = var.environment
  versioning_enabled       = true
  lifecycle_rules_enabled  = true
  
  # Configuração de camadas (bronze, silver, gold)
  storage_tiers = {
    bronze = {
      transition_days = 90
      storage_class   = "STANDARD_IA"
    },
    silver = {
      transition_days = 90
      storage_class   = "STANDARD_IA"
    },
    gold = {
      transition_days = 90
      storage_class   = "STANDARD_IA"
    }
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS Glue Catalog Database
# ---------------------------------------------------------------------------------------------------------------------
module "glue_catalog" {
  source = "./modules/glue"
  
  database_names          = ["economic_indicators_bronze", "economic_indicators_silver", "economic_indicators_gold"]
  crawler_role_name       = "economic-indicators-glue-crawler-role"
  crawler_schedule        = "cron(0 1 * * ? *)"  # Executa diariamente à 1h UTC
  
  s3_targets = {
    bronze = {
      path = "s3://${var.data_lake_bucket}/bronze/"
    },
    silver = {
      path = "s3://${var.data_lake_bucket}/silver/"
    },
    gold = {
      path = "s3://${var.data_lake_bucket}/gold/"
    }
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS Step Functions para orquestração do pipeline
# ---------------------------------------------------------------------------------------------------------------------
module "step_functions" {
  source = "./modules/step_functions"
  
  state_machine_name    = "economic-indicators-etl-pipeline"
  state_machine_role    = "economic-indicators-step-functions-role"
  execution_log_group   = "/aws/stepfunctions/economic-indicators-etl"
  definition_file       = "${path.module}/templates/etl_state_machine.json"
  
  pipeline_parameters = {
    GlueJobNameBronzeToSilver = module.glue_jobs.job_names["bronze_to_silver"]
    GlueJobNameSilverToGold   = module.glue_jobs.job_names["silver_to_gold"]
    S3BucketName              = var.data_lake_bucket
    LambdaCollectDataArn      = module.lambda_functions.function_arns["collect_data"]
    SNSTopicArn               = module.monitoring.sns_topic_arn
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS Lambda Functions para coleta de dados
# ---------------------------------------------------------------------------------------------------------------------
module "lambda_functions" {
  source = "./modules/lambda"
  
  functions = {
    collect_data = {
      name        = "economic-indicators-collect-data"
      description = "Coleta dados econômicos de diversas fontes"
      runtime     = "python3.11"
      handler     = "lambda_handler.handler"
      timeout     = 300  # 5 minutos
      memory_size = 512  # 512 MB
      source_dir  = "${path.module}/../lambda/collect_data"
      
      environment_variables = {
        DATA_LAKE_BUCKET = var.data_lake_bucket
        ENVIRONMENT      = var.environment
      }
    },
    
    validate_data = {
      name        = "economic-indicators-validate-data"
      description = "Valida dados econômicos usando Great Expectations"
      runtime     = "python3.11"
      handler     = "lambda_handler.handler"
      timeout     = 300
      memory_size = 512
      source_dir  = "${path.module}/../lambda/validate_data"
      
      environment_variables = {
        DATA_LAKE_BUCKET = var.data_lake_bucket
        ENVIRONMENT      = var.environment
      }
    }
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# AWS Glue Jobs para transformações ETL com Spark
# ---------------------------------------------------------------------------------------------------------------------
module "glue_jobs" {
  source = "./modules/glue_jobs"
  
  jobs = {
    bronze_to_silver = {
      name              = "economic-indicators-bronze-to-silver"
      description       = "Transforma dados da camada bronze para silver"
      glue_version      = "4.0"
      worker_type       = "G.1X"
      number_of_workers = 2
      max_retries       = 1
      timeout           = 60  # minutos
      script_location   = "s3://${var.data_lake_bucket}/scripts/bronze_to_silver.py"
      
      default_arguments = {
        "--job-language"         = "python"
        "--job-bookmark-option"  = "job-bookmark-enable"
        "--TempDir"              = "s3://${var.data_lake_bucket}/temp/"
        "--enable-metrics"       = "true"
        "--enable-continuous-cloudwatch-log" = "true"
        "--enable-spark-ui"      = "true"
        "--DATA_LAKE_BUCKET"     = var.data_lake_bucket
        "--ENVIRONMENT"          = var.environment
      }
    },
    
    silver_to_gold = {
      name              = "economic-indicators-silver-to-gold"
      description       = "Transforma dados da camada silver para gold"
      glue_version      = "4.0"
      worker_type       = "G.1X"
      number_of_workers = 2
      max_retries       = 1
      timeout           = 60
      script_location   = "s3://${var.data_lake_bucket}/scripts/silver_to_gold.py"
      
      default_arguments = {
        "--job-language"         = "python"
        "--job-bookmark-option"  = "job-bookmark-enable"
        "--TempDir"              = "s3://${var.data_lake_bucket}/temp/"
        "--enable-metrics"       = "true"
        "--enable-continuous-cloudwatch-log" = "true"
        "--enable-spark-ui"      = "true"
        "--DATA_LAKE_BUCKET"     = var.data_lake_bucket
        "--ENVIRONMENT"          = var.environment
      }
    }
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# EventBridge para agendamento e automação
# ---------------------------------------------------------------------------------------------------------------------
module "event_bridge" {
  source = "./modules/event_bridge"
  
  rules = {
    daily_collection = {
      name        = "economic-indicators-daily-collection"
      description = "Inicia o pipeline de coleta diariamente"
      schedule    = "cron(0 0 * * ? *)"  # Meia-noite UTC
      
      targets = [
        {
          arn  = module.step_functions.state_machine_arn
          role = "economic-indicators-eventbridge-role"
          
          input = jsonencode({
            sources     = ["bcb", "ibge"],
            indicators  = "all",
            environment = var.environment
          })
        }
      ]
    }
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}

# ---------------------------------------------------------------------------------------------------------------------
# Monitoramento e alertas
# ---------------------------------------------------------------------------------------------------------------------
module "monitoring" {
  source = "./modules/monitoring"
  
  project_name   = var.project_name
  environment    = var.environment
  
  sns_topic_name = "economic-indicators-alerts"
  sns_emails     = var.alert_emails
  
  cloudwatch_alarms = {
    step_functions_failure = {
      name          = "economic-indicators-step-functions-failure"
      description   = "Alerta para falhas no pipeline de ETL"
      comparison_operator = "GreaterThanOrEqualToThreshold"
      evaluation_periods  = 1
      metric_name   = "ExecutionsFailed"
      namespace     = "AWS/States"
      period        = 60
      statistic     = "Sum"
      threshold     = 1
      
      dimensions = {
        StateMachineArn = module.step_functions.state_machine_arn
      }
    },
    
    glue_job_failure = {
      name          = "economic-indicators-glue-job-failure"
      description   = "Alerta para falhas em jobs Glue"
      comparison_operator = "GreaterThanOrEqualToThreshold"
      evaluation_periods  = 1
      metric_name   = "JobFailure"
      namespace     = "AWS/Glue"
      period        = 60
      statistic     = "Sum"
      threshold     = 1
    }
  }
  
  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}