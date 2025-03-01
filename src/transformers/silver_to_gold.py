# src/transformers/silver_to_gold.py
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple
import os
from dotenv import load_dotenv

from ..utils.aws_utils import S3Handler
from ..utils.helpers.logging_utils import get_logger, log_execution_time, log_dataframe_stats
from ..utils.helpers.aws_helpers import (
    get_latest_s3_file, read_parquet_from_s3, write_parquet_to_s3,
    get_s3_path_with_timestamp, list_s3_files
)
from ..utils.helpers.date_utils import standardize_date_column, create_date_features
from ..utils.helpers.math_utils import calculate_moving_average

# Carrega variáveis de ambiente
load_dotenv()

# Configuração de logging
logger = get_logger("silver_to_gold")

class EconomicIndicatorsGoldTransformer:
    """
    Classe responsável por transformar dados da camada silver para gold,
    criando indicadores compostos, agregações e análises de alto nível.
    """
    
    def __init__(self):
        """Inicializa o transformador da camada gold."""
        self.s3_handler = S3Handler()
        self.bucket_name = os.getenv('AWS_BUCKET_NAME')
    
    def normalize_date_column(self, df: pd.DataFrame, date_col: str = 'last_date') -> pd.DataFrame:
        """
        Normaliza coluna de data para garantir compatibilidade com Parquet.
        
        Args:
            df: DataFrame a ser processado
            date_col: Nome da coluna de data
            
        Returns:
            DataFrame com coluna de data normalizada
        """
        if date_col not in df.columns:
            return df
            
        # Converte para datetime se for string com formato de data
        if pd.api.types.is_object_dtype(df[date_col]):
            try:
                df[date_col] = pd.to_datetime(df[date_col])
            except:
                # Se não conseguir converter, transforma em string
                df[date_col] = df[date_col].astype(str)
                
        return df
        
    @log_execution_time(logger=logger, operation_name="Carregamento dos Indicadores Silver")
    def load_latest_indicators(self) -> Dict[str, pd.DataFrame]:
        """
        Carrega os indicadores mais recentes da camada silver.
        
        Returns:
            Dict[str, pd.DataFrame]: Dicionário com DataFrames para cada indicador
        """
        indicators = {
            'ipca': None,
            'selic': None,
            'pib': None,
            'cambio': None,
            'desemprego': None
        }
        
        for indicator in indicators.keys():
            # Lista arquivos na camada silver para este indicador
            prefix = f"silver/economic_indicators/{indicator}"
            silver_files = self.s3_handler.list_files(prefix=prefix)
            
            if not silver_files:
                logger.warning(f"Nenhum arquivo encontrado para {indicator} na camada silver")
                continue
                
            # Pega o arquivo mais recente
            latest_file = sorted(silver_files)[-1]
            logger.info(f"Carregando {indicator} da camada silver: {latest_file}")
            
            # Carrega o DataFrame
            df = self.s3_handler.download_file(latest_file)
            
            if df is None or df.empty:
                logger.error(f"Erro ao carregar dados de {indicator}")
                continue
                
            # Armazena o DataFrame no dicionário
            indicators[indicator] = df
            logger.info(f"Indicador {indicator} carregado com sucesso. Shape: {df.shape}")
            
        return indicators
    
    @log_execution_time(logger=logger, operation_name="Indicadores Econômicos Mensais")
    def create_monthly_indicators(self, indicators: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Cria um painel integrado de indicadores econômicos mensais.
        
        Args:
            indicators: Dicionário com DataFrames para cada indicador
            
        Returns:
            DataFrame com painel mensal de indicadores
        """
        # Verifica se há dados suficientes
        if not all(k in indicators and indicators[k] is not None for k in ['ipca', 'selic', 'cambio']):
            logger.error("Dados insuficientes para criar painel mensal")
            return pd.DataFrame()
            
        # Prepara DataFrames individuais para join
        ipca_df = indicators['ipca'].copy()
        selic_df = indicators['selic'].copy()
        cambio_df = indicators['cambio'].copy()
        
        # Log das colunas disponíveis para debugging
        logger.info(f"Colunas disponíveis em IPCA: {ipca_df.columns.tolist()}")
        logger.info(f"Colunas disponíveis em SELIC: {selic_df.columns.tolist()}")
        logger.info(f"Colunas disponíveis em Câmbio: {cambio_df.columns.tolist()}")
        
        # Ajusta as colunas dinamicamente baseado no que está disponível
        # IPCA
        ipca_cols = ['date', 'value']
        if 'monthly_change_pct' in ipca_df.columns:
            ipca_cols.append('monthly_change_pct')
        if 'year_over_year_pct' in ipca_df.columns:
            ipca_cols.append('year_over_year_pct')
            
        # SELIC
        selic_cols = ['date', 'value']
        if 'moving_avg_3m' in selic_df.columns:
            selic_cols.append('moving_avg_3m')
            
        # Câmbio
        cambio_cols = ['date', 'value']
        # Verifica qual coluna de variação mensal está disponível
        if 'monthly_change_pct' in cambio_df.columns:
            cambio_cols.append('monthly_change_pct')
        elif 'return_pct' in cambio_df.columns:
            cambio_cols.append('return_pct')
        # Verifica qual coluna de volatilidade está disponível
        if 'volatility' in cambio_df.columns:
            cambio_cols.append('volatility')
        elif 'volatility_20' in cambio_df.columns:
            cambio_cols.append('volatility_20')
        
        # Seleciona colunas disponíveis
        ipca_selected = ipca_df[ipca_cols].copy()
        selic_selected = selic_df[selic_cols].copy()
        cambio_selected = cambio_df[cambio_cols].copy()
        
        # Renomeia colunas para evitar conflitos no merge
        ipca_rename = {'value': 'ipca'}
        if 'monthly_change_pct' in ipca_selected:
            ipca_rename['monthly_change_pct'] = 'ipca_monthly_change'
        if 'year_over_year_pct' in ipca_selected:
            ipca_rename['year_over_year_pct'] = 'ipca_annual_change'
            
        selic_rename = {'value': 'selic'}
        if 'moving_avg_3m' in selic_selected:
            selic_rename['moving_avg_3m'] = 'selic_moving_avg'
            
        cambio_rename = {'value': 'cambio'}
        if 'monthly_change_pct' in cambio_selected:
            cambio_rename['monthly_change_pct'] = 'cambio_monthly_change'
        elif 'return_pct' in cambio_selected:
            cambio_rename['return_pct'] = 'cambio_monthly_change'
        if 'volatility' in cambio_selected:
            cambio_rename['volatility'] = 'cambio_volatility'
        elif 'volatility_20' in cambio_selected:
            cambio_rename['volatility_20'] = 'cambio_volatility'
            
        ipca_selected = ipca_selected.rename(columns=ipca_rename)
        selic_selected = selic_selected.rename(columns=selic_rename)
        cambio_selected = cambio_selected.rename(columns=cambio_rename)
        
        # Certifica que a data está no formato datetime
        ipca_selected['date'] = pd.to_datetime(ipca_selected['date'])
        selic_selected['date'] = pd.to_datetime(selic_selected['date'])
        cambio_selected['date'] = pd.to_datetime(cambio_selected['date'])
        
        # Extrai ano e mês para agrupamento uniforme
        ipca_selected['year_month'] = ipca_selected['date'].dt.strftime('%Y-%m')
        selic_selected['year_month'] = selic_selected['date'].dt.strftime('%Y-%m')
        cambio_selected['year_month'] = cambio_selected['date'].dt.strftime('%Y-%m')
        
        # Faz merge dos DataFrames por ano-mês
        monthly_panel = pd.merge(
            ipca_selected, 
            selic_selected, 
            on='year_month', 
            how='outer', 
            suffixes=('', '_selic')
        )
        
        monthly_panel = pd.merge(
            monthly_panel, 
            cambio_selected, 
            on='year_month', 
            how='outer', 
            suffixes=('', '_cambio')
        )
        
        # Limpa colunas duplicadas de data
        if 'date_selic' in monthly_panel.columns:
            monthly_panel = monthly_panel.drop('date_selic', axis=1)
        if 'date_cambio' in monthly_panel.columns:
            monthly_panel = monthly_panel.drop('date_cambio', axis=1)
            
        # Calcula taxa de juros real (SELIC - IPCA)
        if all(col in monthly_panel.columns for col in ['selic', 'ipca']):
            monthly_panel['real_interest_rate'] = monthly_panel['selic'] - monthly_panel['ipca']
        
        # Cria Índice de Pressão Econômica - verifica se temos as colunas necessárias
        try:
            if ('ipca_annual_change' in monthly_panel.columns and 
                'selic' in monthly_panel.columns and
                'cambio_volatility' in monthly_panel.columns):
                
                monthly_panel['economic_pressure_index'] = (
                    0.4 * monthly_panel['ipca_annual_change'] + 
                    0.3 * monthly_panel['selic'] + 
                    0.3 * monthly_panel['cambio_volatility']
                )
                
                # Normaliza o índice para fácil interpretação (0-100)
                min_val = monthly_panel['economic_pressure_index'].min()
                max_val = monthly_panel['economic_pressure_index'].max()
                
                if max_val > min_val:
                    monthly_panel['economic_pressure_index'] = (
                        (monthly_panel['economic_pressure_index'] - min_val) / (max_val - min_val) * 100
                    )
            else:
                logger.warning("Não foi possível calcular o índice de pressão econômica: colunas necessárias ausentes")
                # Cria um índice simplificado baseado apenas no que temos disponível
                if 'selic' in monthly_panel.columns and 'ipca' in monthly_panel.columns:
                    monthly_panel['economic_pressure_index'] = monthly_panel['selic'] + monthly_panel['ipca']
                
        except Exception as e:
            logger.error(f"Erro ao calcular índice de pressão econômica: {str(e)}")
        
        # Ordena pelo ano/mês
        monthly_panel = monthly_panel.sort_values('year_month')
        
        # Metadados
        monthly_panel['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return monthly_panel
    
    @log_execution_time(logger=logger, operation_name="Indicadores de Mercado de Trabalho")
    def create_labor_market_indicators(self, indicators: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Cria painel de indicadores do mercado de trabalho.
        
        Args:
            indicators: Dicionário com DataFrames para cada indicador
            
        Returns:
            DataFrame com indicadores do mercado de trabalho
        """
        # Verifica se há dados de desemprego
        if 'desemprego' not in indicators or indicators['desemprego'] is None:
            logger.error("Dados de desemprego não disponíveis")
            return pd.DataFrame()
            
        # Log para debugging
        desemprego_df = indicators['desemprego'].copy()
        logger.info(f"Colunas disponíveis em Desemprego: {desemprego_df.columns.tolist()}")
        
        # Verifica quais colunas estão disponíveis
        available_cols = ['date', 'value']
        rename_dict = {'value': 'unemployment_rate'}
        
        # Determina colunas de variação
        quarterly_change_col = None
        annual_change_col = None
        
        if 'quarterly_change_pp' in desemprego_df.columns:
            quarterly_change_col = 'quarterly_change_pp'
            available_cols.append(quarterly_change_col)
            rename_dict[quarterly_change_col] = 'unemployment_qtr_change'
        
        if 'annual_change_pp' in desemprego_df.columns:
            annual_change_col = 'annual_change_pp'
            available_cols.append(annual_change_col)
            rename_dict[annual_change_col] = 'unemployment_annual_change'
            
        # Cria indicadores trimestrais com o que está disponível
        labor_df = desemprego_df[available_cols].copy()
        
        # Renomeia colunas
        labor_df = labor_df.rename(columns=rename_dict)
        
        # Se tiver dados de PIB, adiciona correlação com desemprego
        if 'pib' in indicators and indicators['pib'] is not None:
            pib_df = indicators['pib'].copy()
            logger.info(f"Colunas disponíveis em PIB: {pib_df.columns.tolist()}")
            
            # Verifica se temos a coluna de variação necessária
            if 'quarterly_change_pct' in pib_df.columns or 'value' in pib_df.columns:
                # Ajusta datas para formato trimestral
                labor_df['quarter'] = pd.PeriodIndex(labor_df['date'], freq='Q')
                pib_df['quarter'] = pd.PeriodIndex(pib_df['date'], freq='Q')
                
                # Determina quais colunas juntar
                pib_cols = ['quarter']
                if 'value' in pib_df.columns:
                    pib_cols.append('value')
                if 'quarterly_change_pct' in pib_df.columns:
                    pib_cols.append('quarterly_change_pct')
                
                # Prepara renomeação
                pib_rename = {}
                if 'value' in pib_cols:
                    pib_rename['value'] = 'gdp'
                if 'quarterly_change_pct' in pib_cols:
                    pib_rename['quarterly_change_pct'] = 'gdp_growth'
                
                # Join com dados do PIB
                labor_df = pd.merge(
                    labor_df,
                    pib_df[pib_cols].rename(columns=pib_rename),
                    on='quarter',
                    how='left'
                )
                
                # Calcula elasticidade apenas se tivermos as colunas necessárias
                if ('unemployment_qtr_change' in labor_df.columns and 
                    'gdp_growth' in labor_df.columns and
                    'unemployment_rate' in labor_df.columns):
                    try:
                        # Converte variação em pontos percentuais para variação percentual
                        labor_df['unemployment_pct_change'] = (
                            labor_df['unemployment_qtr_change'] / labor_df['unemployment_rate'].shift(1) * 100
                        )
                        
                        # Calcula elasticidade onde há dados disponíveis
                        # Evita divisão por zero
                        labor_df['employment_gdp_elasticity'] = labor_df.apply(
                            lambda row: -row['unemployment_pct_change'] / row['gdp_growth'] 
                            if row['gdp_growth'] != 0 else None,
                            axis=1
                        )
                        
                        # Calcula média móvel da elasticidade (suavização)
                        labor_df['employment_gdp_elasticity_ma'] = labor_df['employment_gdp_elasticity'].rolling(
                            window=4, min_periods=1
                        ).mean()
                        
                    except Exception as e:
                        logger.error(f"Erro ao calcular elasticidade desemprego-PIB: {str(e)}")
                else:
                    logger.warning("Colunas necessárias para cálculo de elasticidade não disponíveis")
                
                # Remove coluna auxiliar
                if 'quarter' in labor_df.columns:
                    labor_df = labor_df.drop(['quarter'], axis=1)
                if 'unemployment_pct_change' in labor_df.columns:
                    labor_df = labor_df.drop(['unemployment_pct_change'], axis=1)
        
        # Metadados
        labor_df['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        return labor_df
    
    @log_execution_time(logger=logger, operation_name="Painel Macroeconômico")
    def create_macro_dashboard(self, indicators: Dict[str, pd.DataFrame]) -> pd.DataFrame:
        """
        Cria um dashboard macroeconômico com indicadores-chave.
        
        Args:
            indicators: Dicionário com DataFrames para cada indicador
            
        Returns:
            DataFrame com painel macroeconômico
        """
        # Lista de indicadores necessários
        required = ['ipca', 'selic', 'pib', 'cambio', 'desemprego']
        
        # Verifica disponibilidade dos indicadores
        available = [ind for ind in required if ind in indicators and indicators[ind] is not None]
        missing = [ind for ind in required if ind not in indicators or indicators[ind] is None]
        
        if missing:
            logger.warning(f"Indicadores ausentes para o painel macro: {missing}")
            
        if len(available) < 2:  # Exigimos pelo menos 2 indicadores
            logger.error("Dados insuficientes para criar painel macroeconômico")
            return pd.DataFrame()
            
        # Inicializa DataFrame vazio com as colunas esperadas
        dashboard = pd.DataFrame(columns=[
            'indicator', 'indicator_name', 'last_value', 'last_date',
            'unit', 'annual_change', 'trend', 'updated_at'
        ])
        
        # Para cada indicador, pega o valor mais recente
        for ind in available:
            df = indicators[ind]
            
            # Ordena por data
            df = df.sort_values('date')
            
            # Pega o registro mais recente
            latest = df.iloc[-1].copy()
            
            # Cria linha para o dashboard
            row = {
                'indicator': ind,
                'indicator_name': latest.get('indicator_name', ind.upper()),
                'last_value': latest.get('value', None),
                'last_date': latest.get('date', None),
                'unit': latest.get('unit', '%'),
                'annual_change': None,
                'trend': None
            }
            
            # Certifica que a data está em formato datetime
            # Isto é crucial para evitar erro ao salvar no formato Parquet
            if 'last_date' in row and row['last_date'] is not None:
                try:
                    row['last_date'] = pd.to_datetime(row['last_date'])
                except:
                    # Se não conseguir converter, usa data atual
                    row['last_date'] = pd.to_datetime(datetime.now().date())
            
            # Adiciona variação anual se disponível
            annual_changes = {
                'ipca': 'year_over_year_pct',
                'pib': 'annual_change_pct',
                'desemprego': 'annual_change_pp'
            }
            
            if ind in annual_changes and annual_changes[ind] in df.columns:
                row['annual_change'] = latest.get(annual_changes[ind], None)
            elif ind == 'cambio' and len(df) > 12:
                # Calcula variação anual para câmbio
                try:
                    annual_change = (
                        (latest.get('value', 0) / df.iloc[-13].get('value', 0) - 1) * 100
                        if df.iloc[-13].get('value', 0) > 0 else None
                    )
                    row['annual_change'] = annual_change
                except:
                    pass
                
            # Determina tendência baseada nos últimos 3 registros
            if len(df) >= 3:
                try:
                    last_values = df.iloc[-3:]['value'].values
                    if last_values[2] > last_values[0]:
                        row['trend'] = 'rising'
                    elif last_values[2] < last_values[0]:
                        row['trend'] = 'falling'
                    else:
                        row['trend'] = 'stable'
                except:
                    pass
            
            # Adiciona a linha ao dashboard
            new_row = pd.DataFrame([row])
            dashboard = pd.concat([dashboard, new_row], ignore_index=True)
            
        # Adiciona índice econômico
        try:
            # Verifica quais indicadores temos disponíveis para o índice
            has_ipca = 'ipca' in available
            has_selic = 'selic' in available
            has_desemprego = 'desemprego' in available
            
            # Se temos pelo menos dois indicadores, cria um índice simples
            if (has_ipca and has_selic) or (has_ipca and has_desemprego) or (has_selic and has_desemprego):
                # Valores mais recentes dos indicadores disponíveis
                weights = {}
                values = {}
                
                if has_ipca:
                    weights['ipca'] = 0.35
                    values['ipca'] = indicators['ipca'].iloc[-1]['value']
                    ipca_avg = indicators['ipca']['value'].mean()
                    values['ipca_norm'] = values['ipca'] / ipca_avg if ipca_avg > 0 else 1
                
                if has_selic:
                    weights['selic'] = 0.3
                    values['selic'] = indicators['selic'].iloc[-1]['value']
                    selic_avg = indicators['selic']['value'].mean()
                    values['selic_norm'] = values['selic'] / selic_avg if selic_avg > 0 else 1
                
                if has_desemprego:
                    weights['desemprego'] = 0.35
                    values['desemprego'] = indicators['desemprego'].iloc[-1]['value']
                    unemp_avg = indicators['desemprego']['value'].mean()
                    values['desemprego_norm'] = values['desemprego'] / unemp_avg if unemp_avg > 0 else 1
                
                # Normaliza pesos para somarem 1
                weight_sum = sum(weights.values())
                weights = {k: v / weight_sum for k, v in weights.items()}
                
                # Calcula índice composto
                econ_index = 0
                for ind in weights:
                    econ_index += weights[ind] * values[f'{ind}_norm']
                
                # Converte para escala 0-100 onde 50 é a média histórica
                econ_score = min(max(50 * econ_index, 0), 100)
                
                # Determina a situação econômica
                if econ_score < 35:
                    situation = 'excelente'
                elif econ_score < 45:
                    situation = 'boa'
                elif econ_score < 55:
                    situation = 'estável'
                elif econ_score < 70:
                    situation = 'desafiadora'
                else:
                    situation = 'crítica'
                    
                # Adiciona ao dashboard
                summary_row = {
                    'indicator': 'economic_health',
                    'indicator_name': 'Índice de Saúde Econômica',
                    'last_value': round(econ_score, 1),
                    'last_date': pd.to_datetime(datetime.now().date()),  # Usa objeto datetime
                    'unit': 'pontos',
                    'annual_change': None,
                    'trend': None,
                    'situation': situation
                }
                
                # Adiciona a linha ao dashboard
                new_row = pd.DataFrame([summary_row])
                dashboard = pd.concat([dashboard, new_row], ignore_index=True)
            else:
                logger.warning("Indicadores insuficientes para calcular índice de saúde econômica")
                
        except Exception as e:
            logger.error(f"Erro ao calcular índice de saúde econômica: {str(e)}")
            
        # Metadados
        dashboard['updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # Certifica que todas as datas estão em formato adequado para o Parquet
        dashboard = self.normalize_date_column(dashboard, 'last_date')
        
        # Log informativo dos tipos de dados antes de salvar
        logger.info(f"Tipos de dados do dashboard macro: {dashboard.dtypes.to_dict()}")
        
        return dashboard
    
    def save_to_gold_layer(self, df: pd.DataFrame, dashboard_name: str) -> bool:
        """
        Salva um dashboard na camada gold com tratamento de erros melhorado.
        
        Args:
            df: DataFrame a ser salvo
            dashboard_name: Nome do dashboard
            
        Returns:
            bool: True se salvo com sucesso
        """
        try:
            # Verifica se o DataFrame está vazio
            if df is None or df.empty:
                logger.error(f"DataFrame vazio para {dashboard_name}")
                return False
                
            # Gera caminho com timestamp
            file_path = get_s3_path_with_timestamp(f"gold/dashboards/{dashboard_name}")
            
            # Log dos tipos de dados para debug
            logger.info(f"Tipos de dados em {dashboard_name}: {df.dtypes}")
            
            # Converte tipos problemáticos
            # Datas para datetime
            date_columns = [col for col in df.columns if 'date' in col.lower()]
            for col in date_columns:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col])
                    except:
                        logger.warning(f"Não foi possível converter coluna {col} para datetime")
                        # Converte para string como fallback
                        df[col] = df[col].astype(str)
            
            # Salva no S3
            success = write_parquet_to_s3(df, self.bucket_name, file_path)
            
            if success:
                logger.info(f"{dashboard_name} salvo com sucesso: {file_path}")
            else:
                logger.error(f"Erro ao salvar {dashboard_name}")
                
            return success
            
        except Exception as e:
            logger.error(f"Erro ao salvar {dashboard_name}: {str(e)}")
            return False
    
    @log_execution_time(logger=logger, operation_name="Processamento Gold")
    def process_gold_layer(self) -> bool:
        """
        Processa toda a camada gold a partir da silver.
        
        Returns:
            bool: True se processamento foi bem-sucedido
        """
        try:
            # Carrega os indicadores da camada silver
            indicators = self.load_latest_indicators()
            
            if not indicators or all(v is None for v in indicators.values()):
                logger.error("Nenhum indicador disponível na camada silver")
                return False
                
            success_count = 0
            
            # 1. Painel mensal de indicadores
            try:
                monthly_panel = self.create_monthly_indicators(indicators)
                if not monthly_panel.empty:
                    logger.info(f"Painel mensal criado com sucesso. Shape: {monthly_panel.shape}")
                    
                    # Salva na camada gold
                    if self.save_to_gold_layer(monthly_panel, "monthly_indicators"):
                        success_count += 1
            except Exception as e:
                logger.error(f"Erro ao criar painel mensal: {str(e)}")
            
            # 2. Painel do mercado de trabalho
            try:
                labor_panel = self.create_labor_market_indicators(indicators)
                if not labor_panel.empty:
                    logger.info(f"Painel do mercado de trabalho criado com sucesso. Shape: {labor_panel.shape}")
                    
                    # Salva na camada gold
                    if self.save_to_gold_layer(labor_panel, "labor_market"):
                        success_count += 1
            except Exception as e:
                logger.error(f"Erro ao criar painel do mercado de trabalho: {str(e)}")
                
            # 3. Dashboard macroeconômico
            try:
                macro_dashboard = self.create_macro_dashboard(indicators)
                if not macro_dashboard.empty:
                    logger.info(f"Dashboard macroeconômico criado com sucesso. Shape: {macro_dashboard.shape}")
                    
                    # Salva na camada gold
                    if self.save_to_gold_layer(macro_dashboard, "macro_dashboard"):
                        success_count += 1
            except Exception as e:
                logger.error(f"Erro ao criar dashboard macroeconômico: {str(e)}")
            
            # Considera sucesso se pelo menos um dashboard foi criado
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Erro no processamento da camada gold: {str(e)}")
            return False