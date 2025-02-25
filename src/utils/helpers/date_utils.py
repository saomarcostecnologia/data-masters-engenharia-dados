# src/utils/helpers/date_utils.py
import pandas as pd
import numpy as np
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union, Tuple

def standardize_date_column(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
    """
    Padroniza coluna de data para datetime.
    
    Args:
        df: DataFrame original
        date_col: Nome da coluna de data
        
    Returns:
        DataFrame com data padronizada
    """
    result_df = df.copy()
    
    # Verifica se a coluna existe
    if date_col not in result_df.columns:
        if 'data' in result_df.columns:
            # Renomeia 'data' para 'date'
            result_df = result_df.rename(columns={'data': 'date'})
            date_col = 'date'
        else:
            logging.error(f"Coluna de data '{date_col}' não encontrada")
            return result_df
    
    # Converte para datetime
    try:
        result_df[date_col] = pd.to_datetime(result_df[date_col], errors='coerce')
        
        # Verifica se há datas nulas
        null_dates = result_df[date_col].isnull().sum()
        if null_dates > 0:
            logging.warning(f"{null_dates} datas não puderam ser convertidas para datetime")
            
    except Exception as e:
        logging.error(f"Erro ao converter coluna de data: {str(e)}")
        
    return result_df

def create_date_features(df: pd.DataFrame, date_col: str = 'date') -> pd.DataFrame:
    """
    Cria features baseadas na data (ano, mês, trimestre, etc).
    
    Args:
        df: DataFrame original
        date_col: Nome da coluna de data
        
    Returns:
        DataFrame com features de data adicionadas
    """
    result_df = df.copy()
    
    # Verifica se a coluna existe e é datetime
    if date_col not in result_df.columns:
        logging.error(f"Coluna de data '{date_col}' não encontrada")
        return result_df
        
    try:
        # Certifica que é datetime
        if not pd.api.types.is_datetime64_any_dtype(result_df[date_col]):
            result_df[date_col] = pd.to_datetime(result_df[date_col], errors='coerce')
            
        # Cria features
        result_df['year'] = result_df[date_col].dt.year
        result_df['month'] = result_df[date_col].dt.month
        result_df['quarter'] = result_df[date_col].dt.quarter
        result_df['year_month'] = result_df[date_col].dt.strftime('%Y-%m')
        result_df['year_quarter'] = result_df[date_col].dt.to_period('Q').astype(str)
        
        logging.info(f"Features de data criadas com sucesso")
            
    except Exception as e:
        logging.error(f"Erro ao criar features de data: {str(e)}")
        
    return result_df

def create_time_windows(
    df: pd.DataFrame, 
    date_col: str = 'date', 
    windows: List[Tuple[datetime, datetime]] = None
) -> pd.DataFrame:
    """
    Adiciona coluna de janela temporal baseada em períodos específicos.
    
    Args:
        df: DataFrame original
        date_col: Nome da coluna de data
        windows: Lista de tuplas (data_inicio, data_fim, nome_janela)
        
    Returns:
        DataFrame com coluna de janela temporal
    """
    result_df = df.copy()
    
    # Verifica se a coluna existe e é datetime
    if date_col not in result_df.columns:
        logging.error(f"Coluna de data '{date_col}' não encontrada")
        return result_df
        
    if not windows:
        logging.warning("Nenhuma janela temporal especificada")
        return result_df
        
    try:
        # Certifica que é datetime
        if not pd.api.types.is_datetime64_any_dtype(result_df[date_col]):
            result_df[date_col] = pd.to_datetime(result_df[date_col], errors='coerce')
            
        # Cria coluna de janela
        result_df['time_window'] = 'other'
        
        for start_date, end_date, window_name in windows:
            mask = (result_df[date_col] >= start_date) & (result_df[date_col] <= end_date)
            result_df.loc[mask, 'time_window'] = window_name
            
        logging.info(f"Janelas temporais criadas com sucesso")
            
    except Exception as e:
        logging.error(f"Erro ao criar janelas temporais: {str(e)}")
        
    return result_df

def resample_time_series(
    df: pd.DataFrame, 
    date_col: str = 'date', 
    value_col: str = 'value',
    freq: str = 'M',
    agg_func: str = 'mean'
) -> pd.DataFrame:
    """
    Reamostra série temporal para frequência especificada.
    
    Args:
        df: DataFrame original
        date_col: Nome da coluna de data
        value_col: Nome da coluna de valor
        freq: Frequência de amostragem ('D', 'W', 'M', 'Q', 'Y')
        agg_func: Função de agregação ('mean', 'sum', 'min', 'max', 'last')
        
    Returns:
        DataFrame com dados reamostrados
    """
    result_df = df.copy()
    
    # Verifica se as colunas existem
    if date_col not in result_df.columns or value_col not in result_df.columns:
        logging.error(f"Colunas necessárias não encontradas: {date_col}, {value_col}")
        return result_df
        
    try:
        # Certifica que é datetime
        if not pd.api.types.is_datetime64_any_dtype(result_df[date_col]):
            result_df[date_col] = pd.to_datetime(result_df[date_col], errors='coerce')
            
        # Configura índice como data
        result_df = result_df.set_index(date_col)
        
        # Mapeia função de agregação
        if agg_func == 'mean':
            agg_method = np.mean
        elif agg_func == 'sum':
            agg_method = np.sum
        elif agg_func == 'min':
            agg_method = np.min
        elif agg_func == 'max':
            agg_method = np.max
        elif agg_func == 'last':
            agg_method = 'last'
        else:
            agg_method = np.mean
            
        # Reamostra
        resampled = result_df[value_col].resample(freq).agg(agg_method)
        
        # Converte de volta para DataFrame
        resampled_df = resampled.reset_index()
        resampled_df.columns = [date_col, value_col]
        
        logging.info(f"Série temporal reamostrada para frequência '{freq}'")
        return resampled_df
            
    except Exception as e:
        logging.error(f"Erro ao reamostrar série temporal: {str(e)}")
        
    # Em caso de erro, retorna DataFrame original
    return df