# src/collectors/ibge_collector.py
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
from typing import Dict, List, Optional, Union, Any
import time

from .base_collector import BaseCollector

class IBGECollector(BaseCollector):
    """
    Coletor de dados do IBGE (Instituto Brasileiro de Geografia e Estatística).
    Implementa a coleta de indicadores socioeconômicos via APIs do IBGE.
    """
    
    def __init__(self):
        """Inicializa o coletor do IBGE."""
        super().__init__()
        
        # URLs base para diferentes APIs do IBGE
        self.sidra_url = "https://servicodados.ibge.gov.br/api/v3/agregados"
        self.pnad_url = "https://servicodados.ibge.gov.br/api/v1/pesquisas/5457/periodos"
    
    def get_source_name(self) -> str:
        """
        Retorna o nome da fonte de dados.
        
        Returns:
            str: 'ibge'
        """
        return 'ibge'
    
    def get_available_indicators(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna os indicadores disponíveis para coleta do IBGE.
        
        Returns:
            Dict: Mapeamento de indicadores para suas configurações
        """
        return {
            'ipca15': {  # IPCA-15
                'code': '7062',
                'name': 'IPCA-15 - Índice Nacional de Preços ao Consumidor Amplo-15',
                'unit': '%',
                'frequency': 'monthly',
                'variables': ['all', '63', '69', '2265'],  # Geral, Alimentação, Transportes, Educação
                'classifications': {
                    'month': {'code': '315', 'category': 'all'}
                }
            },
            'inpc': {  # Índice Nacional de Preços ao Consumidor
                'code': '7063',
                'name': 'INPC - Índice Nacional de Preços ao Consumidor',
                'unit': '%',
                'frequency': 'monthly',
                'variables': ['all', '63', '69', '2265'],
                'classifications': {
                    'month': {'code': '315', 'category': 'all'}
                }
            },
            'pnad': {  # PNAD Contínua - Taxa de Desocupação
                'code': 'pnad_taxa_desocupacao',
                'name': 'Taxa de Desocupação - PNAD Contínua',
                'unit': '%',
                'frequency': 'quarterly',
                'type': 'special',  # Indica uso de endpoint específico
            },
            'pib_ibge': {  # PIB Trimestral
                'code': '1621',
                'name': 'PIB Trimestral - IBGE',
                'unit': 'R$ milhões',
                'frequency': 'quarterly',
                'variables': ['all'],
                'classifications': {
                    'quarter': {'code': '7', 'category': 'all'}
                }
            }
        }
    
    def get_series_data(self, 
                       indicator: str,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       **kwargs) -> Optional[pd.DataFrame]:
        """
        Coleta dados de uma série específica do IBGE.
        
        Args:
            indicator: Nome do indicador (ipca15, inpc, pnad, pib_ibge)
            start_date: Data inicial para coleta
            end_date: Data final para coleta
            
        Returns:
            DataFrame com os dados coletados ou None em caso de erro
        """
        try:
            # Verifica se o indicador está disponível
            indicators = self.get_available_indicators()
            if indicator not in indicators:
                self._log_error(f"Indicador {indicator} não disponível no IBGE.")
                return None
            
            # Obtém configuração do indicador
            indicator_config = indicators[indicator]
            
            # Seleciona o método de coleta apropriado com base no tipo de indicador
            if indicator == 'pnad':
                # Endpoint específico para PNAD
                return self._get_pnad_data(start_date, end_date)
            else:
                # Usa API SIDRA para outros indicadores
                return self._get_sidra_data(indicator, indicator_config, start_date, end_date)
                
        except Exception as e:
            self._log_error(f"Erro ao coletar dados do indicador {indicator}: {str(e)}")
            return None

    def _get_sidra_data(self, indicator: str, config: Dict[str, Any], 
                      start_date: Optional[datetime] = None, 
                      end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Coleta dados da API SIDRA do IBGE.
        
        Args:
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        try:
            # Configura datas
            end_date = end_date or datetime.now()
            start_date = start_date or (end_date - timedelta(days=365 * 2))  # 2 anos por padrão
            
            # Formata períodos para o SIDRA (depende da frequência)
            if config['frequency'] == 'monthly':
                period_start = start_date.strftime('%Y%m')
                period_end = end_date.strftime('%Y%m')
            elif config['frequency'] == 'quarterly':
                # Trimestres são formatados como YYYYQN (ex: 2023Q1)
                quarter_start = (start_date.month - 1) // 3 + 1
                quarter_end = (end_date.month - 1) // 3 + 1
                period_start = f"{start_date.year}Q{quarter_start}"
                period_end = f"{end_date.year}Q{quarter_end}"
            else:
                # Anual
                period_start = str(start_date.year)
                period_end = str(end_date.year)
                
            # Constrói a URL da consulta
            table_code = config['code']
            variable = config['variables'][0] if 'variables' in config else 'all'
            
            # Monta parâmetros de classificação
            classifications = ""
            if 'classifications' in config:
                for name, cls_config in config['classifications'].items():
                    classifications += f"/{cls_config['code']}/{cls_config['category']}"
            
            # URL final
            url = f"{self.sidra_url}/{table_code}/periodos/{period_start}-{period_end}/variaveis/{variable}{classifications}"
            
            self._log_info(f"Consultando API SIDRA: {url}")
            
            # Faz a requisição
            response = requests.get(url)
            response.raise_for_status()
            
            # Processa a resposta
            data = response.json()
            
            if not data or len(data) == 0 or 'resultados' not in data[0]:
                self._log_error(f"Dados não encontrados para {indicator}")
                return None
                
            # Extrai os resultados
            results = data[0]['resultados'][0]['series']
            
            # Converte para DataFrame
            rows = []
            for series in results:
                # Extrai os valores da série
                series_data = []
                
                # Nome da série/localidade
                loc_name = series['localidade']['nome'] if 'localidade' in series else 'Brasil'
                
                for item in series['serie']:
                    # Período e valor
                    period = item['periodo']
                    value = float(item['valor'].replace(',', '.')) if isinstance(item['valor'], str) else item['valor']
                    
                    # Converte o período para data
                    if config['frequency'] == 'monthly':
                        # Formato YYYYMM
                        year = int(period[:4])
                        month = int(period[4:6])
                        date = datetime(year, month, 1)
                    elif config['frequency'] == 'quarterly':
                        # Formato YYYYQN
                        year = int(period[:4])
                        quarter = int(period[5])
                        month = (quarter - 1) * 3 + 1
                        date = datetime(year, month, 1)
                    else:
                        # Anual - YYYY
                        date = datetime(int(period), 1, 1)
                    
                    # Adiciona à lista
                    series_data.append({
                        'data': date,
                        'valor': value,
                        'localidade': loc_name
                    })
                
                rows.extend(series_data)
            
            # Cria DataFrame final
            if not rows:
                self._log_error(f"Nenhum dado encontrado para {indicator}")
                return None
                
            df = pd.DataFrame(rows)
            
            # Renomeia a coluna 'valor' para o nome do indicador
            df = df.rename(columns={'valor': indicator})
            
            self._log_info(f"Dados coletados com sucesso para {indicator}. Shape: {df.shape}")
            return df
            
        except requests.exceptions.RequestException as e:
            self._log_error(f"Erro na requisição HTTP: {str(e)}")
            return None
        except Exception as e:
            self._log_error(f"Erro ao processar dados SIDRA: {str(e)}")
            return None
            
    def _get_pnad_data(self, 
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Coleta dados da PNAD Contínua através da API específica do IBGE.
        
        Args:
            start_date: Data inicial 
            end_date: Data final
            
        Returns:
            DataFrame com dados da PNAD ou None em caso de erro
        """
        try:
            # Endpoint para listar os períodos disponíveis
            self._log_info("Consultando períodos disponíveis da PNAD")
            periods_url = f"{self.pnad_url}"
            periods_response = requests.get(periods_url)
            periods_response.raise_for_status()
            
            available_periods = periods_response.json()
            
            # Determina os períodos a coletar com base nas datas
            end_date = end_date or datetime.now()
            start_date = start_date or (end_date - timedelta(days=365 * 2))
            
            # Filtra períodos dentro do intervalo
            # Períodos são no formato YYYYQN (ex: 202301 para 1º trimestre de 2023)
            filtered_periods = []
            for period in available_periods:
                period_id = period['id']
                
                # Extrai ano e trimestre
                if len(period_id) == 6:
                    year = int(period_id[:4])
                    quarter = int(period_id[4:6]) // 3 + 1 if int(period_id[4:6]) % 3 == 0 else int(period_id[4:6]) // 3
                    
                    # Converte para data (primeiro dia do trimestre)
                    month = (quarter - 1) * 3 + 1
                    period_date = datetime(year, month, 1)
                    
                    # Verifica se está no intervalo
                    if start_date <= period_date <= end_date:
                        filtered_periods.append(period_id)
            
            if not filtered_periods:
                self._log_error("Nenhum período encontrado para PNAD no intervalo especificado")
                return None
                
            self._log_info(f"Períodos selecionados para PNAD: {filtered_periods}")
            
            # Coleta dados para cada período
            all_data = []
            
            for period in filtered_periods:
                # Taxa de desocupação - indicador 4099
                url = f"{self.pnad_url}/{period}/indicadores/4099"
                
                self._log_info(f"Consultando PNAD para período {period}: {url}")
                response = requests.get(url)
                
                # Verifica se houve erro
                if response.status_code != 200:
                    self._log_error(f"Erro ao consultar período {period}: {response.status_code}")
                    continue
                    
                data = response.json()
                
                if not data or 'resultados' not in data[0]:
                    self._log_error(f"Dados não encontrados para período {period}")
                    continue
                
                # Extrai resultado nacional (Brasil)
                results = data[0]['resultados']
                
                for result in results:
                    if 'series' in result:
                        for series in result['series']:
                            if series['localidade']['nome'] == 'Brasil':
                                # Valor da taxa de desocupação nacional
                                value = series['serie'][0]['valor']
                                
                                # Converte para data
                                year = int(period[:4])
                                month_code = int(period[4:6])
                                quarter = month_code // 3 + 1 if month_code % 3 == 0 else month_code // 3
                                month = (quarter - 1) * 3 + 1
                                
                                # Cria registro
                                record = {
                                    'data': datetime(year, month, 1),
                                    'pnad': float(value.replace(',', '.')) if isinstance(value, str) else float(value),
                                    'localidade': 'Brasil',
                                    'trimestre': quarter
                                }
                                
                                all_data.append(record)
                
                # Pausa para não sobrecarregar a API
                time.sleep(0.5)
            
            # Cria DataFrame final
            if not all_data:
                self._log_error("Nenhum dado encontrado para PNAD")
                return None
                
            df = pd.DataFrame(all_data)
            df = df.sort_values('data')
            
            self._log_info(f"Dados PNAD coletados com sucesso. Shape: {df.shape}")
            return df
            
        except requests.exceptions.RequestException as e:
            self._log_error(f"Erro na requisição HTTP: {str(e)}")
            return None
        except Exception as e:
            self._log_error(f"Erro ao processar dados PNAD: {str(e)}")
            return None
    
    def _post_collect_hook(self, df: pd.DataFrame, indicator: str, **kwargs) -> pd.DataFrame:
        """
        Hook para processar dados após coleta.
        Adiciona metadados do indicador.
        
        Args:
            df: DataFrame coletado
            indicator: Nome do indicador
            
        Returns:
            DataFrame processado
        """
        indicators = self.get_available_indicators()
        if indicator in indicators:
            metadata = indicators[indicator]
            
            # Adiciona metadados
            df['indicator'] = indicator
            df['indicator_name'] = metadata['name']
            df['unit'] = metadata['unit']
            df['frequency'] = metadata['frequency']
            df['source'] = self.get_source_name()
            df['collected_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
        return df