# infrastructure/glue/bronze_to_silver.py
"""
Script AWS Glue para transformar dados da camada Bronze para Silver.
Este script utiliza PySpark para processamento distribuído.

Parâmetros:
--S3_BUCKET: Nome do bucket S3 para entrada/saída de dados
--SOURCES: Lista de fontes de dados a processar (opcional, padrão todas)
--INDICATORS: Lista de indicadores a processar (opcional, padrão todos)
"""

import sys
import time
from datetime import datetime
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import boto3

# Inicialização do Glue e Spark
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# Obter parâmetros do job
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'S3_BUCKET',
    'SOURCES',  # Opcional, valor padrão definido abaixo
    'INDICATORS'  # Opcional, valor padrão definido abaixo
])

job.init(args['JOB_NAME'], args)

# Parâmetros do job
s3_bucket = args['S3_BUCKET']
sources = args.get('SOURCES', 'bcb,ibge').split(',')
indicators = args.get('INDICATORS', 'all').split(',') if args.get('INDICATORS', 'all') != 'all' else None

# Configurações
bronze_path = f"s3://{s3_bucket}/bronze"
silver_path = f"s3://{s3_bucket}/silver"
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

# Configurações de logging
logger = glueContext.get_logger()
logger.info(f"Iniciando job Bronze para Silver. Bucket: {s3_bucket}, Fontes: {sources}")

class BronzeToSilverTransformer:
    """
    Classe responsável pela transformação de dados Bronze para Silver.
    Implementa transformações específicas para cada tipo de indicador.
    """
    
    def __init__(self, spark_session):
        """
        Inicializa o transformador.
        
        Args:
            spark_session: Sessão Spark/Glue ativa
        """
        self.spark = spark_session
        self.logger = logger
    
    def list_bronze_files(self, source, indicator=None):
        """
        Lista arquivos na camada Bronze para uma fonte e indicador específicos.
        
        Args:
            source: Nome da fonte de dados (bcb, ibge)
            indicator: Nome do indicador (opcional)
            
        Returns:
            Lista de caminhos S3 dos arquivos encontrados
        """
        try:
            # Padrão de caminho
            prefix = f"{source}_indicators/"
            if indicator:
                prefix = f"{prefix}{indicator}"
                
            # Caminho completo
            path = f"{bronze_path}/{prefix}"
            
            # Usa boto3 para listar os objetos
            s3_client = boto3.client('s3')
            response = s3_client.list_objects_v2(
                Bucket=s3_bucket,
                Prefix=prefix
            )
            
            # Filtra apenas arquivos parquet
            if 'Contents' in response:
                files = [f"s3://{s3_bucket}/{obj['Key']}" for obj in response['Contents'] 
                        if obj['Key'].endswith('.parquet')]
                self.logger.info(f"Encontrados {len(files)} arquivos bronze para {source}/{indicator}")
                return files
            else:
                self.logger.warning(f"Nenhum arquivo encontrado para {source}/{indicator}")
                return []
                
        except Exception as e:
            self.logger.error(f"Erro ao listar arquivos bronze: {str(e)}")
            return []
    
    def read_bronze_data(self, source, indicator):
        """
        Lê dados da camada bronze para um DataFrame Spark.
        
        Args:
            source: Nome da fonte de dados
            indicator: Nome do indicador
            
        Returns:
            DataFrame Spark com os dados brutos ou None se não encontrado
        """
        try:
            # Lista arquivos
            files = self.list_bronze_files(source, indicator)
            
            if not files:
                return None
                
            # Lê os arquivos mais recentes apenas (para este exemplo, o último)
            latest_file = sorted(files)[-1]
            
            # Lê o arquivo parquet
            df = self.spark.read.parquet(latest_file)
            
            # Log de informações
            count = df.count()
            self.logger.info(f"Leitura de {source}/{indicator} concluída. Registros: {count}")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao ler dados bronze de {source}/{indicator}: {str(e)}")
            return None
    
    def write_silver_data(self, df, source, indicator, partition_cols=None):
        """
        Escreve DataFrame para a camada silver.
        
        Args:
            df: DataFrame a ser escrito
            source: Nome da fonte de dados
            indicator: Nome do indicador
            partition_cols: Colunas para particionamento (opcional)
            
        Returns:
            bool: True se sucesso, False se falha
        """
        try:
            # Define o caminho de saída
            output_path = f"{silver_path}/{source}_indicators/{indicator}_{timestamp}"
            
            # Configura writer com particionamento se especificado
            writer = df.write.mode("overwrite")
            
            if partition_cols and len(partition_cols) > 0:
                writer = writer.partitionBy(*partition_cols)
                
            # Escreve no formato parquet
            writer.parquet(output_path)
            
            self.logger.info(f"Dados silver de {source}/{indicator} escritos com sucesso")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao escrever dados silver de {source}/{indicator}: {str(e)}")
            return False
    
    def transform_ipca(self, df, source="bcb"):
        """
        Transforma dados do IPCA.
        
        Args:
            df: DataFrame com dados brutos
            source: Fonte dos dados (bcb ou ibge)
            
        Returns:
            DataFrame transformado
        """
        try:
            # Identifica a coluna de valor baseada na fonte
            value_col = "ipca" if source == "bcb" else "ipca15" if source == "ibge" else df.columns[1]
            
            # Padronizando nomes das colunas
            df = df.withColumnRenamed("data", "date") if "data" in df.columns else df
            
            # Converte data para formato padrão
            df = df.withColumn("date", F.to_date(F.col("date")))
            
            # Extrai componentes de data para agregações e joins futuros
            df = df.withColumn("year", F.year(F.col("date")))
            df = df.withColumn("month", F.month(F.col("date")))
            df = df.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))
            
            # Renomeia para padronizar
            df = df.withColumnRenamed(value_col, "value")
            
            # Janela para cálculos de variação
            windowSpec = Window.orderBy("date")
            
            # Calcula variação mensal
            df = df.withColumn(
                "monthly_change_pct",
                F.round(
                    (F.col("value") / F.lag("value", 1).over(windowSpec) - 1) * 100,
                    2
                )
            )
            
            # Calcula variação anual (12 meses)
            df = df.withColumn(
                "year_over_year_pct",
                F.round(
                    (F.col("value") / F.lag("value", 12).over(windowSpec) - 1) * 100,
                    2
                )
            )
            
            # Calcula média móvel de 3 meses
            df = df.withColumn(
                "moving_avg_3m",
                F.round(
                    F.avg("value").over(Window.orderBy("date").rowsBetween(-2, 0)),
                    2
                )
            )
            
            # Adiciona cálculo de YTD (Year To Date)
            df = df.withColumn(
                "ytd_accumulated",
                F.sum("value").over(Window.partitionBy(F.year("date")).orderBy("date").rangeBetween(
                    Window.unboundedPreceding, Window.currentRow
                ))
            )
            
            # Adiciona/Mantém metadados
            if "indicator" not in df.columns:
                df = df.withColumn("indicator", 
                                  F.lit(value_col if value_col != "value" else "ipca"))
            
            if "indicator_name" not in df.columns:
                indicator_name = "IPCA - Índice Nacional de Preços ao Consumidor Amplo" if source == "bcb" else \
                               "IPCA-15 - Índice Nacional de Preços ao Consumidor Amplo-15" if source == "ibge" else \
                               "Índice de Preços ao Consumidor"
                df = df.withColumn("indicator_name", F.lit(indicator_name))
            
            if "unit" not in df.columns:
                df = df.withColumn("unit", F.lit("%"))
                
            if "frequency" not in df.columns:
                df = df.withColumn("frequency", F.lit("monthly"))
                
            if "source" not in df.columns:
                df = df.withColumn("source", F.lit(source))
                
            df = df.withColumn("processed_at", F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao transformar dados do IPCA/{source}: {str(e)}")
            return None
    
    def transform_selic(self, df):
        """
        Transforma dados da SELIC.
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame transformado
        """
        try:
            # Padronizando nomes das colunas
            df = df.withColumnRenamed("data", "date") if "data" in df.columns else df
            
            # Identifica a coluna de valor
            value_col = "selic" if "selic" in df.columns else df.columns[1]
            
            # Converte data para formato padrão
            df = df.withColumn("date", F.to_date(F.col("date")))
            
            # Extrai componentes de data para agregações e joins futuros
            df = df.withColumn("year", F.year(F.col("date")))
            df = df.withColumn("month", F.month(F.col("date")))
            df = df.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))
            
            # Renomeia para padronizar
            df = df.withColumnRenamed(value_col, "value")
            
            # Para a SELIC, agrega valores diários para mensais
            monthly_df = df.groupBy("year", "month", "year_month") \
                           .agg(
                               F.last("date").alias("date"),
                               F.avg("value").alias("value")
                           )
            
            # Ordena por data
            monthly_df = monthly_df.orderBy("date")
            
            # Janela para cálculos de variação
            windowSpec = Window.orderBy("date")
            
            # Calcula variação mensal em pontos base
            monthly_df = monthly_df.withColumn(
                "change_bps",
                F.round(
                    (F.col("value") - F.lag("value", 1).over(windowSpec)) * 100,
                    0
                )
            )
            
            # Calcula média móvel de 3 meses
            monthly_df = monthly_df.withColumn(
                "moving_avg_3m",
                F.round(
                    F.avg("value").over(Window.orderBy("date").rowsBetween(-2, 0)),
                    2
                )
            )
            
            # Adiciona/Mantém metadados
            if "indicator" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("indicator", F.lit("selic"))
            
            if "indicator_name" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("indicator_name", F.lit("Taxa SELIC"))
            
            if "unit" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("unit", F.lit("%"))
                
            if "frequency" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("frequency", F.lit("monthly"))
                
            if "source" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("source", F.lit("bcb"))
                
            monthly_df = monthly_df.withColumn("processed_at", 
                                            F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            return monthly_df
            
        except Exception as e:
            self.logger.error(f"Erro ao transformar dados da SELIC: {str(e)}")
            return None
    
    def transform_pnad(self, df):
        """
        Transforma dados da PNAD (desemprego).
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame transformado
        """
        try:
            # Padronizando nomes das colunas
            df = df.withColumnRenamed("data", "date") if "data" in df.columns else df
            
            # Identifica a coluna de valor
            value_col = "pnad" if "pnad" in df.columns else df.columns[1]
            
            # Converte data para formato padrão
            df = df.withColumn("date", F.to_date(F.col("date")))
            
            # Extrai componentes de data
            df = df.withColumn("year", F.year(F.col("date")))
            df = df.withColumn("quarter", F.quarter(F.col("date")))
            df = df.withColumn("year_quarter", 
                             F.concat(F.year(F.col("date")), F.lit("Q"), F.quarter(F.col("date"))))
            
            # Renomeia para padronizar
            df = df.withColumnRenamed(value_col, "value")
            
            # Janela para cálculos de variação
            windowSpec = Window.orderBy("date")
            
            # Calcula variação em pontos percentuais
            df = df.withColumn(
                "quarterly_change_pp",
                F.round(
                    F.col("value") - F.lag("value", 1).over(windowSpec),
                    2
                )
            )
            
            # Calcula variação anual (4 trimestres)
            df = df.withColumn(
                "annual_change_pp",
                F.round(
                    F.col("value") - F.lag("value", 4).over(windowSpec),
                    2
                )
            )
            
            # Calcula média móvel de 3 trimestres
            df = df.withColumn(
                "moving_avg_3q",
                F.round(
                    F.avg("value").over(Window.orderBy("date").rowsBetween(-2, 0)),
                    2
                )
            )
            
            # Adiciona/Mantém metadados
            if "indicator" not in df.columns:
                df = df.withColumn("indicator", F.lit("desemprego"))
            
            if "indicator_name" not in df.columns:
                df = df.withColumn("indicator_name", F.lit("Taxa de Desemprego - PNAD"))
            
            if "unit" not in df.columns:
                df = df.withColumn("unit", F.lit("%"))
                
            if "frequency" not in df.columns:
                df = df.withColumn("frequency", F.lit("quarterly"))
                
            if "source" not in df.columns:
                df = df.withColumn("source", F.lit("ibge"))
                
            df = df.withColumn("processed_at", F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            return df
            
        except Exception as e:
            self.logger.error(f"Erro ao transformar dados da PNAD: {str(e)}")
            return None
    
    def transform_cambio(self, df):
        """
        Transforma dados da taxa de câmbio.
        
        Args:
            df: DataFrame com dados brutos
            
        Returns:
            DataFrame transformado
        """
        try:
            # Padronizando nomes das colunas
            df = df.withColumnRenamed("data", "date") if "data" in df.columns else df
            
            # Identifica a coluna de valor
            value_col = "cambio" if "cambio" in df.columns else df.columns[1]
            
            # Converte data para formato padrão
            df = df.withColumn("date", F.to_date(F.col("date")))
            
            # Extrai componentes de data 
            df = df.withColumn("year", F.year(F.col("date")))
            df = df.withColumn("month", F.month(F.col("date")))
            df = df.withColumn("year_month", F.date_format(F.col("date"), "yyyy-MM"))
            
            # Renomeia para padronizar
            df = df.withColumnRenamed(value_col, "value")
            
            # Agrega por mês para análise financeira OHLC (Open, High, Low, Close)
            monthly_df = df.groupBy("year", "month", "year_month") \
                           .agg(
                               F.last("date").alias("date"),
                               F.first("value").alias("open"),
                               F.max("value").alias("high"),
                               F.min("value").alias("low"),
                               F.last("value").alias("close"),
                               F.avg("value").alias("avg"),
                               F.stddev("value").alias("volatility")
                           )
            
            # Valor padrão é o fechamento
            monthly_df = monthly_df.withColumn("value", F.col("close"))
            
            # Ordena por data
            monthly_df = monthly_df.orderBy("date")
            
            # Janela para cálculos de variação
            windowSpec = Window.orderBy("date")
            
            # Calcula variações percentuais
            monthly_df = monthly_df.withColumn(
                "monthly_change_pct",
                F.round(
                    (F.col("close") / F.lag("close", 1).over(windowSpec) - 1) * 100,
                    2
                )
            )
            
            # Calcula amplitude mensal
            monthly_df = monthly_df.withColumn(
                "monthly_amplitude_pct",
                F.round(
                    (F.col("high") - F.col("low")) / F.col("low") * 100,
                    2
                )
            )
            
            # Calcula médias móveis
            monthly_df = monthly_df.withColumn(
                "ma_3m",
                F.round(
                    F.avg("close").over(Window.orderBy("date").rowsBetween(-2, 0)),
                    4
                )
            )
            
            monthly_df = monthly_df.withColumn(
                "ma_6m",
                F.round(
                    F.avg("close").over(Window.orderBy("date").rowsBetween(-5, 0)),
                    4
                )
            )
            
            # Adiciona/Mantém metadados
            if "indicator" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("indicator", F.lit("cambio"))
            
            if "indicator_name" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("indicator_name", 
                                                 F.lit("Taxa de Câmbio (USD/BRL)"))
            
            if "unit" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("unit", F.lit("BRL"))
                
            if "frequency" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("frequency", F.lit("monthly"))
                
            if "source" not in monthly_df.columns:
                monthly_df = monthly_df.withColumn("source", F.lit("bcb"))
                
            monthly_df = monthly_df.withColumn("processed_at", 
                                             F.lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            return monthly_df
            
        except Exception as e:
            self.logger.error(f"Erro ao transformar dados do Câmbio: {str(e)}")
            return None
    
    def process_indicator(self, source, indicator):
        """
        Processa um indicador específico da camada bronze para silver.
        
        Args:
            source: Nome da fonte de dados
            indicator: Nome do indicador
            
        Returns:
            bool: True se processado com sucesso
        """
        try:
            self.logger.info(f"Processando indicador: {source}/{indicator}")
            
            # Lê dados da camada bronze
            df = self.read_bronze_data(source, indicator)
            
            if df is None:
                self.logger.error(f"Dados não disponíveis para {source}/{indicator}")
                return False
            
            # Mapeia indicadores para suas funções de transformação
            transform_funcs = {
                'ipca': self.transform_ipca,
                'ipca15': self.transform_ipca,
                'selic': self.transform_selic,
                'pnad': self.transform_pnad,
                'cambio': self.transform_cambio
            }
            
            # Seleciona a função de transformação
            if indicator in transform_funcs:
                transform_func = transform_funcs[indicator]
                
                # Para IPCA e IPCA15, passa o parâmetro source
                if indicator in ['ipca', 'ipca15']:
                    silver_df = transform_func(df, source)
                else:
                    silver_df = transform_func(df)
            else:
                self.logger.error(f"Transformação não definida para {source}/{indicator}")
                return False
            
            if silver_df is None:
                self.logger.error(f"Erro na transformação dos dados para {source}/{indicator}")
                return False
            
            # Determina colunas de particionamento baseado no indicador
            partition_cols = ["year", "month"] if indicator in ['ipca', 'ipca15', 'selic', 'cambio'] \
                            else ["year", "quarter"] if indicator in ['pnad'] \
                            else ["year"]
            
            # Salva na camada silver
            success = self.write_silver_data(silver_df, source, indicator, partition_cols)
            
            if not success:
                self.logger.error(f"Erro ao salvar dados silver para {source}/{indicator}")
                return False
            
            self.logger.info(f"Transformação para camada silver concluída: {source}/{indicator}")
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao processar indicador {source}/{indicator}: {str(e)}")
            return False
    
    def process_all_sources(self, sources, indicators=None):
        """
        Processa todos os indicadores especificados para todas as fontes.
        
        Args:
            sources: Lista de fontes a processar
            indicators: Lista de indicadores a processar (se None, processa todos disponíveis)
            
        Returns:
            Dict: Mapeamento de indicadores para status de processamento
        """
        results = {}
        
        for source in sources:
            # Lista arquivos na camada bronze
            if indicators:
                source_indicators = indicators
            else:
                # Lista todos indicadores disponíveis para a fonte
                all_files = self.list_bronze_files(source)
                source_indicators = set()
                
                for file_path in all_files:
                    # Extrai o nome do indicador do caminho
                    parts = file_path.split('/')
                    for part in parts:
                        if '_' in part and '.parquet' in part:
                            indicator = part.split('_')[0]
                            source_indicators.add(indicator)
                
                source_indicators = list(source_indicators)
            
            self.logger.info(f"Indicadores para processar em {source}: {source_indicators}")
            
            # Processa cada indicador
            for indicator in source_indicators:
                key = f"{source}/{indicator}"
                success = self.process_indicator(source, indicator)
                results[key] = success
                
                status_txt = "✅ Sucesso" if success else "❌ Falha"
                self.logger.info(f"Resultado para {key}: {status_txt}")
        
        return results

# Execução principal
def main():
    """Função principal do job."""
    try:
        # Cria o transformador
        transformer = BronzeToSilverTransformer(spark)
        
        # Processa todas as fontes e indicadores
        results = transformer.process_all_sources(sources, indicators)
        
        # Contagem de sucessos e falhas
        success_count = sum(1 for success in results.values() if success)
        failed_count = sum(1 for success in results.values() if not success)
        
        logger.info(f"Processamento concluído. Sucesso: {success_count}, Falhas: {failed_count}")
        
        if failed_count > 0:
            logger.warning("Alguns indicadores falharam. Verifique os logs para detalhes.")
            
        # Logs detalhados dos resultados
        for key, success in results.items():
            status = "Sucesso" if success else "Falha"
            logger.info(f"{key}: {status}")
        
    except Exception as e:
        logger.error(f"Erro durante a execução do job: {str(e)}")
        raise

# Executa o job
main()
job.commit()