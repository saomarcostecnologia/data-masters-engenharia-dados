# src/collectors/bcb_collector.py
import pandas as pd
from datetime import datetime, timedelta
import requests
from typing import Dict, List, Optional, Union, Any

from .base_collector import BaseCollector

class BCBCollector(BaseCollector):
    """
    Coletor de dados do Banco Central do Brasil.
    Implementa a coleta de indicadores econômicos via API do BCB.
    """
    
    def __init__(self):
        """Inicializa o coletor do BCB."""
        super().__init__()
        self.base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"
    
    def get_source_name(self) -> str:
        """
        Retorna o nome da fonte de dados.
        
        Returns:
            str: 'bcb'
        """
        return 'bcb'
    
    def get_available_indicators(self) -> Dict[str, Dict[str, Any]]:
        """
        Retorna os indicadores disponíveis para coleta do BCB.
        
        Returns:
            Dict: Mapeamento de indicadores para códigos de série
        """
        return {
            'ipca': {
                'code': 433,
                'name': 'IPCA - Índice Nacional de Preços ao Consumidor Amplo',
                'unit': '%',
                'frequency': 'monthly'
            },
            'selic': {
                'code': 11,
                'name': 'Taxa SELIC',
                'unit': '%',
                'frequency': 'daily'
            },
            'pib': {
                'code': 4380,
                'name': 'PIB Mensal',
                'unit': 'R$ milhões',
                'frequency': 'monthly'
            },
            'cambio': {
                'code': 1,
                'name': 'Taxa de Câmbio - Livre - Dólar americano (compra) - Média de período',
                'unit': 'BRL',
                'frequency': 'daily'
            },
            'desemprego': {
                'code': 24369,
                'name': 'Taxa de Desemprego',
                'unit': '%',
                'frequency': 'quarterly'
            }
        }
    
    def get_series_data(self, 
                       indicator: str,
                       start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None,
                       **kwargs) -> Optional[pd.DataFrame]:
        """
        Coleta dados de uma série específica do BCB.
        
        Args:
            indicator: Nome do indicador (ipca, selic, pib, cambio, desemprego)
            start_date: Data inicial para coleta
            end_date: Data final para coleta
            
        Returns:
            DataFrame com os dados coletados ou None em caso de erro
        """
        try:
            # Obtém o código da série
            indicators = self.get_available_indicators()
            
            if indicator not in indicators:
                self._log_error(f"Indicador {indicator} não encontrado.")
                return None
                
            series_code = indicators[indicator]['code']
                
            # Configura as datas
            end_date = end_date or datetime.now()
            start_date = start_date or (end_date - timedelta(days=365))
            
            # Formata as datas
            start_date_str = start_date.strftime('%d/%m/%Y')
            end_date_str = end_date.strftime('%d/%m/%Y')
            
            # Monta a URL
            url = self.base_url.format(series_code)
            params = {
                'formato': 'json',
                'dataInicial': start_date_str,
                'dataFinal': end_date_str
            }
            
            self._log_info(f"Coletando dados da série {indicator}")
            self._log_info(f"URL: {url}")
            self._log_info(f"Parâmetros: {params}")
            
            response = requests.get(url, params=params)
            response.raise_for_status()  # Levanta exceção para status codes de erro
            
            data = response.json()
            self._log_info(f"Dados recebidos para {indicator}. Tamanho: {len(data)}")
            
            if not data:
                self._log_error(f"Nenhum dado retornado para {indicator}")
                return None
                
            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
            df = df.rename(columns={'valor': indicator})
            
            self._log_info(f"DataFrame criado com sucesso. Shape: {df.shape}")
            return df
            
        except requests.exceptions.RequestException as e:
            self._log_error(f"Erro na requisição HTTP para {indicator}: {str(e)}")
            return None
        except Exception as e:
            self._log_error(f"Erro ao coletar dados da série {indicator}: {str(e)}")
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