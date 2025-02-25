# src/utils/helpers/data_cleaning.py
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Union, Any

def inspect_dataframe(df: pd.DataFrame, label: str = "DataFrame") -> None:
    """
    Inspeciona um DataFrame e loga metadados úteis para debugging.
    
    Args:
        df: DataFrame a ser inspecionado
        label: Rótulo para identificar o DataFrame nos logs
    """
    logging.info(f"=== Inspeção do {label} ===")
    logging.info(f"Colunas: {df.columns.tolist()}")
    logging.info(f"Tipos de dados: {df.dtypes}")
    logging.info(f"Formato: {df.shape}")
    logging.info(f"Valores nulos: {df.isnull().sum().to_dict()}")
    
    # Amostra de dados
    if not df.empty:
        logging.info(f"Primeiras linhas:\n{df.head(2).to_dict()}")

def safe_rename_columns(df: pd.DataFrame, rename_dict: Dict[str, str]) -> pd.DataFrame:
    """
    Renomeia colunas de forma segura, verificando existência.
    
    Args:
        df: DataFrame original
        rename_dict: Dicionário de mapeamento {nome_antigo: nome_novo}
        
    Returns:
        DataFrame com colunas renomeadas
    """
    # Filtra apenas colunas que existem no DataFrame
    valid_renames = {k: v for k, v in rename_dict.items() if k in df.columns}
    return df.rename(columns=valid_renames)

def identify_value_column(df: pd.DataFrame, exclude_cols: List[str] = None) -> str:
    """
    Identifica automaticamente a coluna de valor em um DataFrame.
    
    Args:
        df: DataFrame a ser analisado
        exclude_cols: Lista de colunas a serem excluídas da busca
        
    Returns:
        Nome da coluna identificada como valor
    """
    if exclude_cols is None:
        exclude_cols = ['date', 'data']
        
    # Tenta identificar coluna de valor excluindo as colunas de data
    value_cols = [col for col in df.columns if col not in exclude_cols]
    
    if not value_cols:
        raise ValueError("Não foi possível identificar coluna de valor no DataFrame")
        
    return value_cols[0]

def ensure_numeric(df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    """
    Garante que as colunas especificadas são numéricas.
    
    Args:
        df: DataFrame original
        columns: Lista de colunas para converter para numérico
        
    Returns:
        DataFrame com colunas convertidas
    """
    result_df = df.copy()
    
    for col in columns:
        if col in result_df.columns:
            # Tenta converter para numérico
            try:
                result_df[col] = pd.to_numeric(result_df[col], errors='coerce')
                
                # Log de estatísticas após conversão
                non_null_count = result_df[col].count()
                total_count = len(result_df)
                if non_null_count < total_count:
                    null_pct = (1 - non_null_count/total_count) * 100
                    logging.warning(f"Coluna {col}: {null_pct:.2f}% dos valores são nulos após conversão numérica")
                    
            except Exception as e:
                logging.error(f"Erro ao converter coluna {col} para numérico: {str(e)}")
    
    return result_df

def remove_duplicates(df: pd.DataFrame, subset: List[str] = None, keep: str = 'first') -> pd.DataFrame:
    """
    Remove registros duplicados de um DataFrame.
    
    Args:
        df: DataFrame original
        subset: Colunas para verificar duplicação (None = todas)
        keep: Estratégia para manter duplicados ('first', 'last', False)
        
    Returns:
        DataFrame sem duplicações
    """
    orig_count = len(df)
    result_df = df.drop_duplicates(subset=subset, keep=keep)
    new_count = len(result_df)
    
    if orig_count > new_count:
        logging.info(f"Removidas {orig_count - new_count} linhas duplicadas")
        
    return result_df

def fill_missing_values(df: pd.DataFrame, fill_dict: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Preenche valores ausentes em um DataFrame.
    
    Args:
        df: DataFrame original
        fill_dict: Dicionário com estratégia de preenchimento por coluna
        
    Returns:
        DataFrame com valores preenchidos
    """
    if fill_dict is None:
        return df
        
    result_df = df.copy()
    
    for col, fill_value in fill_dict.items():
        if col in result_df.columns:
            # Verifica se há valores nulos para preencher
            null_count = result_df[col].isnull().sum()
            
            if null_count > 0:
                # Preenche valores ausentes
                result_df[col] = result_df[col].fillna(fill_value)
                logging.info(f"Preenchidos {null_count} valores ausentes na coluna {col}")
                
    return result_df