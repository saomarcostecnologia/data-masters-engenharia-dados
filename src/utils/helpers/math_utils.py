# src/utils/helpers/math_utils.py
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Union, Tuple, Any

def calculate_variations(
    df: pd.DataFrame, 
    value_col: str = 'value', 
    date_col: str = 'date',
    variations: Dict[str, Dict] = None
) -> pd.DataFrame:
    """
    Calcula variações temporais para uma série.
    
    Args:
        df: DataFrame com dados da série
        value_col: Nome da coluna de valor
        date_col: Nome da coluna de data
        variations: Dicionário com configurações de variações a calcular
        
    Returns:
        DataFrame com variações calculadas
    """
    result_df = df.copy()
    
    # Verifica se as colunas existem
    if value_col not in result_df.columns or date_col not in result_df.columns:
        logging.error(f"Colunas necessárias não encontradas: {value_col}, {date_col}")
        return result_df
        
    # Variações padrão se não especificadas
    if variations is None:
        variations = {
            'pct_change': {'periods': 1, 'column': 'monthly_change_pct', 'multiply': 100},
            'year_over_year': {'periods': 12, 'column': 'year_over_year_pct', 'multiply': 100}
        }
        
    # Ordena por data para garantir cálculos corretos
    result_df = result_df.sort_values(date_col)
    
    # Calcula cada variação
    for var_type, config in variations.items():
        periods = config.get('periods', 1)
        col_name = config.get('column', f'{var_type}_{periods}')
        multiply = config.get('multiply', 1)
        
        try:
            # Calcula variação
            if var_type == 'pct_change':
                result_df[col_name] = result_df[value_col].pct_change(periods=periods) * multiply
            elif var_type == 'diff':
                result_df[col_name] = result_df[value_col].diff(periods=periods) * multiply
            elif var_type == 'year_over_year':
                result_df[col_name] = result_df[value_col].pct_change(periods=periods) * multiply
                
        except Exception as e:
            logging.error(f"Erro ao calcular variação {var_type}: {str(e)}")
            
    return result_df

def calculate_moving_average(
    df: pd.DataFrame,
    value_col: str = 'value',
    window: int = 3,
    result_col: str = None
) -> pd.DataFrame:
    """
    Calcula média móvel para uma série.
    
    Args:
        df: DataFrame com dados da série
        value_col: Nome da coluna de valor
        window: Tamanho da janela para média móvel
        result_col: Nome da coluna para o resultado
        
    Returns:
        DataFrame com média móvel calculada
    """
    result_df = df.copy()
    
    # Verifica se a coluna existe
    if value_col not in result_df.columns:
        logging.error(f"Coluna {value_col} não encontrada para cálculo de média móvel")
        return result_df
        
    # Nome da coluna resultante
    if result_col is None:
        result_col = f'moving_avg_{window}'
        
    try:
        # Calcula média móvel
        result_df[result_col] = result_df[value_col].rolling(window=window).mean()
        
    except Exception as e:
        logging.error(f"Erro ao calcular média móvel: {str(e)}")
        
    return result_df

def calculate_cumulative_values(
    df: pd.DataFrame,
    value_col: str = 'value',
    group_col: str = None,
    result_col: str = 'cumulative_value'
) -> pd.DataFrame:
    """
    Calcula valores cumulativos, opcionalmente por grupo.
    
    Args:
        df: DataFrame com dados
        value_col: Nome da coluna de valor
        group_col: Nome da coluna para agrupar (opcional)
        result_col: Nome da coluna para o resultado
        
    Returns:
        DataFrame com valores cumulativos
    """
    result_df = df.copy()
    
    # Verifica se a coluna existe
    if value_col not in result_df.columns:
        logging.error(f"Coluna {value_col} não encontrada")
        return result_df
        
    try:
        # Calcula cumulativo (com ou sem agrupamento)
        if group_col and group_col in result_df.columns:
            result_df[result_col] = result_df.groupby(group_col)[value_col].cumsum()
        else:
            result_df[result_col] = result_df[value_col].cumsum()
        
    except Exception as e:
        logging.error(f"Erro ao calcular valores cumulativos: {str(e)}")
        
    return result_df

def calculate_year_to_date(
    df: pd.DataFrame,
    value_col: str = 'value',
    date_col: str = 'date',
    year_col: str = 'year',
    result_col: str = 'year_to_date_pct'
) -> pd.DataFrame:
    """
    Calcula variação percentual acumulada no ano (year-to-date).
    
    Args:
        df: DataFrame com dados
        value_col: Nome da coluna de valor
        date_col: Nome da coluna de data
        year_col: Nome da coluna de ano (ou criará se não existir)
        result_col: Nome da coluna para o resultado
        
    Returns:
        DataFrame com YTD calculado
    """
    result_df = df.copy()
    
    # Verifica se as colunas necessárias existem
    if value_col not in result_df.columns:
        logging.error(f"Coluna {value_col} não encontrada")
        return result_df
        
    # Cria coluna de ano se não existir
    if year_col not in result_df.columns:
        if date_col in result_df.columns:
            # Certifica que é datetime
            if not pd.api.types.is_datetime64_any_dtype(result_df[date_col]):
                result_df[date_col] = pd.to_datetime(result_df[date_col], errors='coerce')
            
            result_df[year_col] = result_df[date_col].dt.year
        else:
            logging.error(f"Colunas {date_col} ou {year_col} não encontradas")
            return result_df
            
    try:
        # Calcula YTD
        result_df[result_col] = result_df.groupby(year_col)[value_col].transform(
            lambda x: (x / x.iloc[0] - 1) * 100 if len(x) > 0 else 0
        )
        
    except Exception as e:
        logging.error(f"Erro ao calcular YTD: {str(e)}")
        
    return result_df

def calculate_volatility(
    df: pd.DataFrame,
    value_col: str = 'value',
    group_col: str = None,
    window: int = None,
    result_col: str = 'volatility'
) -> pd.DataFrame:
    """
    Calcula volatilidade (desvio padrão) de uma série.
    
    Args:
        df: DataFrame com dados
        value_col: Nome da coluna de valor
        group_col: Nome da coluna para agrupar (opcional)
        window: Tamanho da janela móvel (opcional)
        result_col: Nome da coluna para o resultado
        
    Returns:
        DataFrame com volatilidade calculada
    """
    result_df = df.copy()
    
    # Verifica se a coluna existe
    if value_col not in result_df.columns:
        logging.error(f"Coluna {value_col} não encontrada")
        return result_df
        
    try:
        # Calcula volatilidade por grupo
        if group_col and group_col in result_df.columns:
            # Usando método transform para manter o tamanho do DataFrame
            result_df[result_col] = result_df.groupby(group_col)[value_col].transform('std')
            
        # Calcula volatilidade com janela móvel
        elif window:
            result_df[result_col] = result_df[value_col].rolling(window=window).std()
            
        # Calcula volatilidade global
        else:
            result_df[result_col] = result_df[value_col].std()
        
    except Exception as e:
        logging.error(f"Erro ao calcular volatilidade: {str(e)}")
        
    return result_df

def calculate_financial_metrics(
    df: pd.DataFrame,
    price_col: str = 'close',
    high_col: str = 'high',
    low_col: str = 'low',
    date_col: str = 'date'
) -> pd.DataFrame:
    """
    Calcula métricas financeiras comuns para dados de preços.
    
    Args:
        df: DataFrame com dados
        price_col: Nome da coluna de preço/fechamento
        high_col: Nome da coluna de máxima
        low_col: Nome da coluna de mínima
        date_col: Nome da coluna de data
        
    Returns:
        DataFrame com métricas financeiras calculadas
    """
    result_df = df.copy()
    
    # Verifica se as colunas principais existem
    if price_col not in result_df.columns:
        logging.error(f"Coluna {price_col} não encontrada")
        return result_df
        
    try:
        # Retorno diário/período
        result_df['return_pct'] = result_df[price_col].pct_change() * 100
        
        # Volatilidade (janela de 20 períodos)
        result_df['volatility_20'] = result_df[price_col].rolling(window=20).std()
        
        # Amplitude, se houver high e low
        if high_col in result_df.columns and low_col in result_df.columns:
            result_df['amplitude_pct'] = (result_df[high_col] - result_df[low_col]) / result_df[low_col] * 100
            
        # Média móvel de 20 períodos
        result_df['ma_20'] = result_df[price_col].rolling(window=20).mean()
        
        # Média móvel de 50 períodos
        result_df['ma_50'] = result_df[price_col].rolling(window=50).mean()
        
        # Sinal de cruzamento de médias móveis
        result_df['ma_cross_signal'] = np.where(result_df['ma_20'] > result_df['ma_50'], 1, -1)
        
        logging.info(f"Métricas financeiras calculadas com sucesso")
        
    except Exception as e:
        logging.error(f"Erro ao calcular métricas financeiras: {str(e)}")
        
    return result_df