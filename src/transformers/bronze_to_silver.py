# src/transformers/bronze_to_silver.py
import pandas as pd
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union
import os
from dotenv import load_dotenv

# Import da classe S3Handler
from ..utils.aws_utils import S3Handler

s3_handler = S3Handler()

latest_file = s3_handler.get_latest_file(prefix)
df = s3_handler.read_parquet(key)
success = s3_handler.write_parquet(df, output_key)
output_path = s3_handler.get_path_with_timestamp(base_path)

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logger = get_logger("bronze_to_silver")

class EconomicIndicatorTransformer:
    """
    Classe responsável por transformar dados brutos de indicadores econômicos
    da camada bronze para a camada silver.
    """
    
    def __init__(self):
        """Inicializa o transformador."""
        self.s3_handler = S3Handler()
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
        
    @log_execution_time(logger=logger, operation_name="Transformação IPCA")
    def transform_ipca(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma dados do IPCA.
        
        Args:
            df: DataFrame com dados brutos do IPCA
            
        Returns:
            DataFrame transformado
        """
        # Inspeção inicial
        inspect_dataframe(df, "IPCA Bronze")
        
        # Identificação e limpeza
        value_column = identify_value_column(df)
        df = safe_rename_columns(df, {'data': 'date', value_column: 'value'})
        df = standardize_date_column(df)
        df = ensure_numeric(df, ['value'])
        
        # Criação de features de data
        df = create_date_features(df)
        
        # Cálculo de variações
        variations = {
            'pct_change': {'periods': 1, 'column': 'monthly_change_pct', 'multiply': 100},
            'year_over_year': {'periods': 12, 'column': 'year_over_year_pct', 'multiply': 100}
        }
        df = calculate_variations(df, variations=variations)
        
        # Cálculo de YTD
        df = calculate_year_to_date(df)
        
        # Média móvel
        df = calculate_moving_average(df, window=3, result_col='moving_avg_3m')
        
        # Adiciona metadados
        df['indicator'] = 'ipca'
        df['indicator_name'] = 'IPCA - Índice Nacional de Preços ao Consumidor Amplo'
        df['unit'] = '%'
        df['frequency'] = 'monthly'
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Limpa colunas temporárias
        if 'year' in df.columns:
            df = df.drop('year', axis=1)
            
        log_dataframe_stats(df, logger, "IPCA Silver")
        return df
    
    @log_execution_time(logger=logger, operation_name="Transformação SELIC")
    def transform_selic(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma dados da SELIC.
        
        Args:
            df: DataFrame com dados brutos da SELIC
            
        Returns:
            DataFrame transformado
        """
        # Inspeção inicial
        inspect_dataframe(df, "SELIC Bronze")
        
        # Identificação e limpeza
        value_column = identify_value_column(df)
        df = safe_rename_columns(df, {'data': 'date', value_column: 'value'})
        df = standardize_date_column(df)
        df = ensure_numeric(df, ['value'])
        
        # Criação de features de data para agregação mensal
        df = create_date_features(df)
        
        # Agrega mensalmente
        monthly_data = []
        for ym, group in df.groupby('year_month'):
            monthly_data.append({
                'date': group['date'].max(),
                'value': group['value'].mean(),
                'year': group['year'].iloc[0],
                'month': group['month'].iloc[0]
            })
        
        monthly_df = pd.DataFrame(monthly_data)
        
        # Calcula variação em pontos base
        monthly_df = calculate_variations(
            monthly_df, 
            variations={
                'diff': {'periods': 1, 'column': 'change_bps', 'multiply': 100}
            }
        )
        
        # Média móvel
        monthly_df = calculate_moving_average(
            monthly_df, window=3, result_col='moving_avg_3m'
        )
        
        # Placeholder para taxa real de juros
        monthly_df['real_interest_rate'] = 0
        
        # Metadados
        monthly_df['indicator'] = 'selic'
        monthly_df['indicator_name'] = 'Taxa SELIC'
        monthly_df['unit'] = '%'
        monthly_df['frequency'] = 'monthly'
        monthly_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Limpa colunas temporárias
        if 'year' in monthly_df.columns:
            monthly_df = monthly_df.drop(['year', 'month'], axis=1)
            
        log_dataframe_stats(monthly_df, logger, "SELIC Silver")
        return monthly_df
    
    @log_execution_time(logger=logger, operation_name="Transformação PIB")
    def transform_pib(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma dados do PIB.
        
        Args:
            df: DataFrame com dados brutos do PIB
            
        Returns:
            DataFrame transformado
        """
        # Inspeção inicial
        inspect_dataframe(df, "PIB Bronze")
        
        # Identificação e limpeza
        value_column = identify_value_column(df)
        df = safe_rename_columns(df, {'data': 'date', value_column: 'value'})
        df = standardize_date_column(df)
        df = ensure_numeric(df, ['value'])
        
        # Cálculo de variações
        df = calculate_variations(
            df, 
            variations={
                'pct_change': {'periods': 1, 'column': 'quarterly_change_pct', 'multiply': 100},
                'year_over_year': {'periods': 4, 'column': 'annual_change_pct', 'multiply': 100}
            }
        )
        
        # Acumulado em 12 meses (4 trimestres)
        df['accumulated_value'] = df['value'].rolling(window=4).sum()
        
        # Metadados
        df['indicator'] = 'pib'
        df['indicator_name'] = 'Produto Interno Bruto'
        df['unit'] = 'R$ milhões'
        df['frequency'] = 'quarterly'
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        log_dataframe_stats(df, logger, "PIB Silver")
        return df
    
    @log_execution_time(logger=logger, operation_name="Transformação Câmbio")
    def transform_cambio(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma dados da taxa de câmbio.
        
        Args:
            df: DataFrame com dados brutos da taxa de câmbio
            
        Returns:
            DataFrame transformado
        """
        # Inspeção inicial
        inspect_dataframe(df, "Câmbio Bronze")
        
        # Identificação e limpeza
        value_column = identify_value_column(df)
        df = safe_rename_columns(df, {'data': 'date', value_column: 'value'})
        df = standardize_date_column(df)
        df = ensure_numeric(df, ['value'])
        
        # Criação de features de data para agregação mensal
        df = create_date_features(df)
        
        # Cálculos financeiros para OHLC e outros
        monthly_data = []
        for ym, group in df.groupby('year_month'):
            monthly_data.append({
                'date': group['date'].max(),
                'open': group['value'].iloc[0],
                'close': group['value'].iloc[-1],
                'high': group['value'].max(),
                'low': group['value'].min(),
                'avg': group['value'].mean(),
                'volatility': group['value'].std() if len(group) > 1 else 0
            })
        
        monthly_df = pd.DataFrame(monthly_data)
        
        # Assegura que 'value' existe para manter padrão
        monthly_df['value'] = monthly_df['close']
        
        # Calcula métricas financeiras adicionais
        monthly_df = calculate_financial_metrics(monthly_df, price_col='close')
        
        # Calcula amplitude
        monthly_df['monthly_amplitude_pct'] = (monthly_df['high'] - monthly_df['low']) / monthly_df['low'] * 100
        
        # Metadados
        monthly_df['indicator'] = 'cambio'
        monthly_df['indicator_name'] = 'Taxa de Câmbio (USD/BRL)'
        monthly_df['unit'] = 'BRL'
        monthly_df['frequency'] = 'monthly'
        monthly_df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        log_dataframe_stats(monthly_df, logger, "Câmbio Silver")
        return monthly_df
    
    @log_execution_time(logger=logger, operation_name="Transformação Desemprego")
    def transform_desemprego(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma dados da taxa de desemprego.
        
        Args:
            df: DataFrame com dados brutos da taxa de desemprego
            
        Returns:
            DataFrame transformado
        """
        # Inspeção inicial
        inspect_dataframe(df, "Desemprego Bronze")
        
        # Identificação e limpeza
        value_column = identify_value_column(df)
        df = safe_rename_columns(df, {'data': 'date', value_column: 'value'})
        df = standardize_date_column(df)
        df = ensure_numeric(df, ['value'])
        
        # Variações em pontos percentuais
        df = calculate_variations(
            df, 
            variations={
                'diff': {'periods': 1, 'column': 'quarterly_change_pp', 'multiply': 1},
                'diff': {'periods': 4, 'column': 'annual_change_pp', 'multiply': 1}
            }
        )
        
        # Média móvel
        df = calculate_moving_average(df, window=3, result_col='moving_avg_3q')
        
        # Metadados
        df['indicator'] = 'desemprego'
        df['indicator_name'] = 'Taxa de Desemprego'
        df['unit'] = '%'
        df['frequency'] = 'quarterly'
        df['processed_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        log_dataframe_stats(df, logger, "Desemprego Silver")
        return df
    
    @log_execution_time(logger=logger, operation_name="Processamento de Indicador")
    def process_indicator(self, indicator: str) -> bool:
        """
        Processa um indicador específico da camada bronze para silver.
        
        Args:
            indicator: Nome do indicador a ser processado
            
        Returns:
            bool: True se processado com sucesso
        """
        try:
            # Mapeia indicadores para suas funções de transformação
            transformers = {
                'ipca': self.transform_ipca,
                'selic': self.transform_selic,
                'pib': self.transform_pib,
                'cambio': self.transform_cambio,
                'desemprego': self.transform_desemprego
            }
            
            if indicator not in transformers:
                logger.error(f"Indicador não suportado: {indicator}")
                return False
            
            # Lista arquivos na camada bronze
            bronze_files = self.s3_handler.list_files(prefix=f"bronze/economic_indicators/{indicator}")
            
            if not bronze_files:
                logger.warning(f"Nenhum arquivo bronze encontrado para {indicator}")
                return False
            
            # Pega o arquivo mais recente
            latest_file = sorted(bronze_files)[-1]
            logger.info(f"Processando arquivo: {latest_file}")
            
            # Carrega dados da camada bronze
            bronze_df = self.s3_handler.download_file(latest_file)
            
            if bronze_df is None or bronze_df.empty:
                logger.error(f"Erro ao carregar dados bronze para {indicator}")
                return False
            
            # Transforma dados
            transform_func = transformers[indicator]
            silver_df = transform_func(bronze_df)
            
            if silver_df is None or silver_df.empty:
                logger.error(f"Erro na transformação dos dados para {indicator}")
                return False
            
            # Salva na camada silver
            file_path = get_s3_path_with_timestamp(f"silver/economic_indicators/{indicator}")
            success = write_parquet_to_s3(silver_df, self.bucket_name, file_path)
            
            if not success:
                logger.error(f"Erro ao salvar dados silver para {indicator}")
                return False
            
            logger.info(f"Transformação para camada silver concluída: {indicator}")
            return True
            
        except Exception as e:
            logger.error(f"Erro ao processar indicador {indicator}: {str(e)}")
            return False
    
    @log_execution_time(logger=logger, operation_name="Processamento de Todos Indicadores")
    def process_all_indicators(self) -> Dict[str, bool]:
        """
        Processa todos os indicadores da camada bronze para silver.
        
        Returns:
            Dict[str, bool]: Status de processamento para cada indicador
        """
        indicators = ['ipca', 'selic', 'pib', 'cambio', 'desemprego']
        results = {}
        
        for indicator in indicators:
            logger.info(f"Iniciando processamento do indicador: {indicator}")
            success = self.process_indicator(indicator)
            results[indicator] = success
            
            status_txt = "✅ Sucesso" if success else "❌ Falha"
            logger.info(f"Resultado para {indicator}: {status_txt}")
            
        return results

# Função para executar o transformador
if __name__ == "__main__":
    # Executa transformações
    transformer = EconomicIndicatorTransformer()
    results = transformer.process_all_indicators()
    
    # Imprime resultados
    print("\nResultados do processamento:")
    for indicator, success in results.items():
        status = "✅ Sucesso" if success else "❌ Falha"
        print(f"{indicator}: {status}")