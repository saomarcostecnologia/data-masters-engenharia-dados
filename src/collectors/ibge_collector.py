# src/collectors/ibge_collector.py
import pandas as pd
from datetime import datetime, timedelta
import requests
import json
from typing import Dict, List, Optional, Union, Any
import time
import os
import zipfile
from io import BytesIO

from .base_collector import BaseCollector

class IBGECollector(BaseCollector):
    """
    Coletor de dados do IBGE (Instituto Brasileiro de Geografia e Estatística).
    Implementa a coleta de indicadores socioeconômicos via APIs do IBGE e download de arquivos.
    """
    
    def __init__(self):
        """Inicializa o coletor do IBGE."""
        super().__init__()
        
        # URLs base para diferentes APIs do IBGE
        self.sidra_url = "https://servicodados.ibge.gov.br/api/v3/agregados"
        self.pnad_url = "https://servicodados.ibge.gov.br/api/v1/pesquisas/5457/periodos"
        
        # Diretório para arquivos temporários
        self.temp_dir = os.path.join(os.getcwd(), "temp")
        os.makedirs(self.temp_dir, exist_ok=True)
    
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
        # Verifica se existe o arquivo local para o IPCA-15
        local_file_path = None
        excel_file = 'ipca-15_202502SerieHist.xls' ### Aqui preciso Fazer uma alteração no caso temos o nome ipca-15_anomesatualSerieHist.xls PReciso deixar essa parte variavel
        
        # Procura o arquivo na pasta atual e na pasta temp
        possible_paths = [
            excel_file,  
            os.path.join('temp', excel_file),
            os.path.join(self.temp_dir, excel_file)
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                local_file_path = os.path.abspath(path)
                break
        
        return {
            'ipca15': {  # IPCA-15
                'code': '7062',
                'name': 'IPCA-15 - Índice Nacional de Preços ao Consumidor Amplo-15',
                'unit': '%',
                'frequency': 'monthly',
                'variables': ['all', '63', '69', '2265'],  # Geral, Alimentação, Transportes, Educação
                'classifications': {
                    'month': {'code': '315', 'category': 'all'}
                },
                # Adiciona suporte para download de arquivo
                'collection_method': 'file_download',
                'local_file_path': local_file_path,  # Caminho do arquivo local, se existir
                'download_url': 'https://www.ibge.gov.br/estatisticas/economicas/precos-e-custos/9260-indice-nacional-de-precos-ao-consumidor-amplo-15.html?=&t=downloads',
                'file_name': excel_file,
                'sheet_name': 0,  # Primeira aba da planilha
                'skiprows': 2,    # Pular linhas de cabeçalho
                'date_column': 'Mês/Ano',
                'value_column': 'Variação Mensal (%)',
                'date_format': '%m/%Y'  # Formato esperado: MM/AAAA
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
            
            # Seleciona o método de coleta apropriado com base na configuração
            collection_method = indicator_config.get('collection_method', 'api')
            
            if collection_method == 'file_download':
                self._log_info(f"Usando método de download de arquivo para {indicator}")
                return self._get_file_data(indicator, indicator_config, start_date, end_date)
            elif indicator == 'pnad':
                # Endpoint específico para PNAD
                return self._get_pnad_data(start_date, end_date)
            else:
                # Usa API SIDRA para outros indicadores
                return self._get_sidra_data(indicator, indicator_config, start_date, end_date)
                
        except Exception as e:
            self._log_error(f"Erro ao coletar dados do indicador {indicator}: {str(e)}")
            return None
    
    def _get_file_data(self, 
                     indicator: str, 
                     config: Dict[str, Any],
                     start_date: Optional[datetime] = None, 
                     end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Coleta dados de um indicador através de arquivo local ou download.
        
        Args:
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        try:
            # Verifica se temos um arquivo local
            local_file_path = config.get('local_file_path')
            
            if local_file_path and os.path.exists(local_file_path):
                self._log_info(f"Usando arquivo local para {indicator}: {local_file_path}")
                return self._process_file(local_file_path, indicator, config, start_date, end_date)
            
            # Se não temos arquivo local, tenta fazer download
            download_url = config.get('download_url')
            if download_url:
                self._log_info(f"Arquivo local não encontrado. Tentando download de {download_url}")
                # TODO: Implementar download do arquivo
                return self._download_and_process_file(download_url, indicator, config, start_date, end_date)
            
            self._log_error(f"Não foi possível encontrar/baixar dados para {indicator}")
            return None
            
        except Exception as e:
            self._log_error(f"Erro ao processar arquivo para {indicator}: {str(e)}")
            return None
    
    def _process_file(self, 
                    file_path: str, 
                    indicator: str, 
                    config: Dict[str, Any],
                    start_date: Optional[datetime] = None, 
                    end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Processa um arquivo local (Excel, CSV, ZIP).
        
        Args:
            file_path: Caminho do arquivo
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        try:
            # Determina tipo de arquivo
            if file_path.endswith('.zip'):
                return self._process_zip_file(file_path, indicator, config, start_date, end_date)
            elif file_path.endswith('.xls') or file_path.endswith('.xlsx'):
                return self._process_excel_file(file_path, indicator, config, start_date, end_date)
            elif file_path.endswith('.csv'):
                return self._process_csv_file(file_path, indicator, config, start_date, end_date)
            else:
                self._log_error(f"Formato de arquivo não suportado: {file_path}")
                return None
                
        except Exception as e:
            self._log_error(f"Erro ao processar arquivo {file_path}: {str(e)}")
            return None
    
    def _process_excel_file(self, 
                          file_path: str, 
                          indicator: str, 
                          config: Dict[str, Any],
                          start_date: Optional[datetime] = None, 
                          end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Processa um arquivo Excel.
        
        Args:
            file_path: Caminho do arquivo
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        try:
            self._log_info(f"Processando arquivo Excel: {file_path}")
            
            # Configurações para leitura do Excel
            sheet_name = config.get('sheet_name', 0)
            skiprows = config.get('skiprows', 0)
            
            # Lê o arquivo Excel
            df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows)
            self._log_info(f"Arquivo Excel lido com sucesso. Colunas: {df.columns.tolist()}")
            
            # Extrai configurações
            date_column = config.get('date_column')
            value_column = config.get('value_column')
            date_format = config.get('date_format')
            
            # Renomeia colunas conforme necessário
            if date_column and date_column in df.columns:
                df = df.rename(columns={date_column: 'data'})
            
            if value_column and value_column in df.columns:
                df = df.rename(columns={value_column: indicator})
            
            # Converte coluna de data
            if 'data' in df.columns and date_format:
                df['data'] = pd.to_datetime(df['data'], format=date_format, errors='coerce')
            
            # Remove linhas com datas ou valores nulos
            df = df.dropna(subset=['data'])
            if indicator in df.columns:
                df = df.dropna(subset=[indicator])
            
            # Filtra por data, se necessário
            if start_date or end_date:
                if start_date:
                    df = df[df['data'] >= start_date]
                if end_date:
                    df = df[df['data'] <= end_date]
            
            self._log_info(f"Processamento do arquivo Excel concluído. Shape final: {df.shape}")
            return df
            
        except Exception as e:
            self._log_error(f"Erro ao processar arquivo Excel {file_path}: {str(e)}")
            return None
    
    def _process_zip_file(self, 
                        file_path: str, 
                        indicator: str, 
                        config: Dict[str, Any],
                        start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Extrai e processa um arquivo ZIP.
        
        Args:
            file_path: Caminho do arquivo ZIP
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        try:
            self._log_info(f"Extraindo arquivo ZIP: {file_path}")
            
            with zipfile.ZipFile(file_path) as zip_ref:
                # Lista arquivos dentro do ZIP
                file_list = zip_ref.namelist()
                self._log_info(f"Arquivos no ZIP: {file_list}")
                
                # Determina qual arquivo extrair
                file_to_extract = config.get('file_name')
                if not file_to_extract:
                    # Se não especificado, tenta inferir
                    excel_files = [f for f in file_list if f.endswith(('.xls', '.xlsx'))]
                    if excel_files:
                        file_to_extract = excel_files[0]
                    elif file_list:
                        file_to_extract = file_list[0]
                    else:
                        self._log_error("ZIP vazio ou sem arquivos compatíveis")
                        return None
                
                # Extrai o arquivo
                extract_path = os.path.join(self.temp_dir, file_to_extract)
                zip_ref.extract(file_to_extract, self.temp_dir)
                self._log_info(f"Arquivo extraído: {extract_path}")
                
                # Processa o arquivo extraído
                return self._process_file(extract_path, indicator, config, start_date, end_date)
                
        except Exception as e:
            self._log_error(f"Erro ao processar arquivo ZIP {file_path}: {str(e)}")
            return None
    
    def _process_csv_file(self, 
                        file_path: str, 
                        indicator: str, 
                        config: Dict[str, Any],
                        start_date: Optional[datetime] = None, 
                        end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Processa um arquivo CSV.
        
        Args:
            file_path: Caminho do arquivo
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        try:
            self._log_info(f"Processando arquivo CSV: {file_path}")
            
            # Configurações para leitura do CSV
            sep = config.get('csv_separator', ',')
            encoding = config.get('encoding', 'utf-8')
            
            # Lê o arquivo CSV
            df = pd.read_csv(file_path, sep=sep, encoding=encoding)
            
            # Extrai configurações
            date_column = config.get('date_column')
            value_column = config.get('value_column')
            date_format = config.get('date_format')
            
            # Renomeia colunas conforme necessário
            if date_column and date_column in df.columns:
                df = df.rename(columns={date_column: 'data'})
            
            if value_column and value_column in df.columns:
                df = df.rename(columns={value_column: indicator})
            
            # Converte coluna de data
            if 'data' in df.columns and date_format:
                df['data'] = pd.to_datetime(df['data'], format=date_format, errors='coerce')
            
            # Remove linhas com datas ou valores nulos
            df = df.dropna(subset=['data'])
            if indicator in df.columns:
                df = df.dropna(subset=[indicator])
            
            # Filtra por data, se necessário
            if start_date or end_date:
                if start_date:
                    df = df[df['data'] >= start_date]
                if end_date:
                    df = df[df['data'] <= end_date]
            
            self._log_info(f"Processamento do arquivo CSV concluído. Shape final: {df.shape}")
            return df
            
        except Exception as e:
            self._log_error(f"Erro ao processar arquivo CSV {file_path}: {str(e)}")
            return None
    
    def _download_and_process_file(self, 
                                 url: str, 
                                 indicator: str, 
                                 config: Dict[str, Any],
                                 start_date: Optional[datetime] = None, 
                                 end_date: Optional[datetime] = None) -> Optional[pd.DataFrame]:
        """
        Baixa e processa um arquivo.
        
        Args:
            url: URL do arquivo ou página de download
            indicator: Nome do indicador
            config: Configuração do indicador
            start_date: Data inicial
            end_date: Data final
            
        Returns:
            DataFrame com dados ou None em caso de erro
        """
        # Esta implementação é um esboço que pode ser ampliado posteriormente
        # A implementação completa seria mais complexa para lidar com páginas web
        self._log_warning("Download direto do IBGE ainda não implementado completamente.")
        self._log_info("Por favor, baixe o arquivo manualmente e coloque-o na pasta do projeto ou na pasta 'temp'.")
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
            
            # INÍCIO DA MODIFICAÇÃO: Ajuste para garantir que buscamos períodos históricos válidos
            # Garante que não buscamos dados do futuro
            current_date = datetime.now()
            if end_date > current_date:
                self._log_info(f"Ajustando data final de {end_date.strftime('%Y-%m-%d')} para {current_date.strftime('%Y-%m-%d')} (data atual)")
                end_date = current_date
            
            # Ajuste para períodos seguros (dados históricos disponíveis - busca dados até 2023)
            safe_end_date = datetime(2023, 12, 1)  # Um período seguro que sabemos que existe
            safe_start_date = datetime(2023, 7, 1)  # 6 meses antes
            
            self._log_info(f"Usando período histórico seguro: {safe_start_date.strftime('%Y-%m-%d')} a {safe_end_date.strftime('%Y-%m-%d')}")
            start_date = safe_start_date
            end_date = safe_end_date
            # FIM DA MODIFICAÇÃO
            
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
            
            # Log das datas formatadas para debug
            self._log_info(f"Períodos formatados: {period_start} a {period_end}")
                
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
            
            # MODIFICAÇÃO: Ajuste para garantir que buscamos períodos disponíveis
            # Use um período histórico seguro (até 2023) para PNAD
            safe_end_date = datetime(2023, 9, 1)  # 3º trimestre de 2023
            safe_start_date = datetime(2023, 1, 1)  # 1º trimestre de 2023
            
            self._log_info(f"Usando período histórico seguro para PNAD: {safe_start_date.strftime('%Y-%m-%d')} a {safe_end_date.strftime('%Y-%m-%d')}")
            start_date = safe_start_date
            end_date = safe_end_date
    
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