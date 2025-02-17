# src/collectors/bcb_collector.py
import pandas as pd
from datetime import datetime, timedelta
import logging
import requests
from ..utils.aws_utils import S3Handler

class BCBCollector:
    def __init__(self):
        self.base_url = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.{}/dados"
        self.series_ids = {
            'ipca': 433,        # IPCA
            'selic': 11,        # Taxa SELIC
            'pib': 4380,        # PIB Mensal
            'cambio': 1,        # Taxa de Câmbio
            'desemprego': 24369 # Taxa de Desemprego
        }
        self.s3_handler = S3Handler()
        
    def get_series_data(self, series_name: str, 
                       start_date: datetime = None,
                       end_date: datetime = None) -> pd.DataFrame:
        """Coleta dados de uma série específica do BCB."""
        try:
            if series_name not in self.series_ids:
                raise ValueError(f"Série {series_name} não encontrada.")
                
            # Configura as datas
            end_date = end_date or datetime.now()
            start_date = start_date or (end_date - timedelta(days=365))
            
            # Formata as datas
            start_date_str = start_date.strftime('%d/%m/%Y')
            end_date_str = end_date.strftime('%d/%m/%Y')
            
            # Monta a URL
            url = self.base_url.format(self.series_ids[series_name])
            params = {
                'formato': 'json',
                'dataInicial': start_date_str,
                'dataFinal': end_date_str
            }
            
            logging.info(f"Coletando dados da série {series_name}")
            logging.info(f"URL: {url}")
            logging.info(f"Parâmetros: {params}")
            
            response = requests.get(url, params=params)
            response.raise_for_status()  # Levanta exceção para status codes de erro
            
            data = response.json()
            logging.info(f"Dados recebidos para {series_name}. Tamanho: {len(data)}")
            
            if not data:
                logging.error(f"Nenhum dado retornado para {series_name}")
                return None
                
            df = pd.DataFrame(data)
            df['data'] = pd.to_datetime(df['data'], format='%d/%m/%Y')
            df = df.rename(columns={'valor': series_name})
            
            logging.info(f"DataFrame criado com sucesso. Shape: {df.shape}")
            return df
            
        except requests.exceptions.RequestException as e:
            logging.error(f"Erro na requisição HTTP para {series_name}: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Erro ao coletar dados da série {series_name}: {str(e)}")
            return None

    def collect_and_store(self, start_date=None, end_date=None) -> bool:
        """Coleta dados de todos os indicadores e armazena no S3."""
        try:
            for series_name in self.series_ids:
                logging.info(f"Iniciando coleta para {series_name}")
                df = self.get_series_data(series_name, start_date, end_date)
                
                if df is None or df.empty:
                    logging.error(f"Dados não disponíveis para {series_name}")
                    continue
                    
                # Upload para S3
                success = self.s3_handler.upload_dataframe(
                    df=df,
                    file_path=f"economic_indicators/{series_name}",
                    layer='bronze',
                    format='parquet'
                )
                
                if not success:
                    logging.error(f"Falha no upload dos dados de {series_name}")
                    continue
                    
                logging.info(f"Dados de {series_name} processados com sucesso")
                
            return True
            
        except Exception as e:
            logging.error(f"Erro no processo de coleta e armazenamento: {str(e)}")
            return False

if __name__ == "__main__":
    # Configura logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Cria instância do coletor
    collector = BCBCollector()
    
    # Coleta e armazena dados do último ano
    start_date = datetime.now() - timedelta(days=365)
    success = collector.collect_and_store(start_date=start_date)
    
    if success:
        print("Processo completado com sucesso!")
    else:
        print("Houve erros no processo. Verifique os logs.")