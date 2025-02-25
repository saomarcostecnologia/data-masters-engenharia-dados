# src/utils/helpers/data_validation.py
import pandas as pd
import numpy as np
import logging
from typing import Dict, List, Optional, Union, Tuple, Any

def validate_column_presence(df: pd.DataFrame, required_columns: List[str]) -> Tuple[bool, List[str]]:
    """
    Valida se as colunas necessárias estão presentes no DataFrame.
    
    Args:
        df: DataFrame a ser validado
        required_columns: Lista de colunas que devem estar presentes
        
    Returns:
        Tupla (is_valid, missing_columns)
    """
    if df is None or df.empty:
        return False, required_columns
        
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    is_valid = len(missing_columns) == 0
    
    if not is_valid:
        logging.error(f"Colunas obrigatórias ausentes: {missing_columns}")
        
    return is_valid, missing_columns

def validate_data_types(df: pd.DataFrame, type_dict: Dict[str, str]) -> Tuple[bool, Dict[str, str]]:
    """
    Valida se as colunas têm os tipos de dados esperados.
    
    Args:
        df: DataFrame a ser validado
        type_dict: Dicionário {coluna: tipo_esperado}
        
    Returns:
        Tupla (is_valid, invalid_columns)
    """
    if df is None or df.empty:
        return False, type_dict
        
    invalid_columns = {}
    
    for col, expected_type in type_dict.items():
        if col not in df.columns:
            continue
            
        # Mapeia strings de tipo para tipos Python/Pandas
        if expected_type == 'numeric':
            is_valid = pd.api.types.is_numeric_dtype(df[col])
        elif expected_type == 'datetime':
            is_valid = pd.api.types.is_datetime64_any_dtype(df[col])
        elif expected_type == 'string':
            is_valid = pd.api.types.is_string_dtype(df[col])
        elif expected_type == 'boolean':
            is_valid = pd.api.types.is_bool_dtype(df[col])
        else:
            is_valid = df[col].dtype.name == expected_type
            
        if not is_valid:
            invalid_columns[col] = f"Esperado: {expected_type}, Atual: {df[col].dtype.name}"
            
    is_valid = len(invalid_columns) == 0
    
    if not is_valid:
        logging.error(f"Colunas com tipos inválidos: {invalid_columns}")
        
    return is_valid, invalid_columns

def validate_value_ranges(
    df: pd.DataFrame, 
    range_dict: Dict[str, Tuple[float, float]]
) -> Tuple[bool, Dict[str, int]]:
    """
    Valida se os valores estão dentro dos intervalos esperados.
    
    Args:
        df: DataFrame a ser validado
        range_dict: Dicionário {coluna: (min, max)}
        
    Returns:
        Tupla (is_valid, out_of_range_count)
    """
    if df is None or df.empty:
        return False, {}
        
    out_of_range_count = {}
    
    for col, (min_val, max_val) in range_dict.items():
        if col not in df.columns:
            continue
            
        # Conta valores fora do intervalo
        if pd.api.types.is_numeric_dtype(df[col]):
            count = ((df[col] < min_val) | (df[col] > max_val)).sum()
            
            if count > 0:
                out_of_range_count[col] = count
        else:
            logging.warning(f"Coluna {col} não é numérica, validação de intervalo ignorada")
            
    is_valid = len(out_of_range_count) == 0
    
    if not is_valid:
        logging.error(f"Colunas com valores fora do intervalo: {out_of_range_count}")
        
    return is_valid, out_of_range_count

def validate_missing_values(
    df: pd.DataFrame, 
    threshold_pct: float = 5.0
) -> Tuple[bool, Dict[str, float]]:
    """
    Valida se o percentual de valores ausentes está abaixo do limite.
    
    Args:
        df: DataFrame a ser validado
        threshold_pct: Percentual máximo permitido de valores ausentes
        
    Returns:
        Tupla (is_valid, columns_above_threshold)
    """
    if df is None or df.empty:
        return False, {}
        
    # Calcula percentual de nulos por coluna
    null_pct = df.isnull().mean() * 100
    
    # Filtra colunas acima do limite
    columns_above_threshold = null_pct[null_pct > threshold_pct].to_dict()
    
    is_valid = len(columns_above_threshold) == 0
    
    if not is_valid:
        logging.error(f"Colunas com mais de {threshold_pct}% de valores ausentes: {columns_above_threshold}")
        
    return is_valid, columns_above_threshold

def validate_duplicates(
    df: pd.DataFrame, 
    subset: List[str] = None
) -> Tuple[bool, int]:
    """
    Valida se há registros duplicados no DataFrame.
    
    Args:
        df: DataFrame a ser validado
        subset: Colunas para verificar duplicação (None = todas)
        
    Returns:
        Tupla (is_valid, duplicate_count)
    """
    if df is None or df.empty:
        return True, 0
        
    # Conta duplicados
    duplicate_count = df.duplicated(subset=subset).sum()
    
    is_valid = duplicate_count == 0
    
    if not is_valid:
        logging.error(f"Encontrados {duplicate_count} registros duplicados")
        
    return is_valid, duplicate_count

def validate_dataset(
    df: pd.DataFrame,
    required_columns: List[str] = None,
    type_dict: Dict[str, str] = None,
    range_dict: Dict[str, Tuple[float, float]] = None,
    null_threshold_pct: float = 5.0,
    check_duplicates: bool = True,
    duplicate_subset: List[str] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Executa todas as validações no DataFrame.
    
    Args:
        df: DataFrame a ser validado
        required_columns: Lista de colunas obrigatórias
        type_dict: Dicionário de tipos esperados
        range_dict: Dicionário de intervalos válidos
        null_threshold_pct: Limite de valores ausentes
        check_duplicates: Se deve verificar duplicados
        duplicate_subset: Colunas para checar duplicação
        
    Returns:
        Tupla (is_valid, validation_results)
    """
    if df is None or df.empty:
        return False, {"error": "DataFrame vazio ou nulo"}
        
    results = {}
    is_valid = True
    
    # Valida presença de colunas
    if required_columns:
        cols_valid, missing_cols = validate_column_presence(df, required_columns)
        results["missing_columns"] = missing_cols
        is_valid = is_valid and cols_valid
        
    # Valida tipos de dados
    if type_dict:
        types_valid, invalid_types = validate_data_types(df, type_dict)
        results["invalid_types"] = invalid_types
        is_valid = is_valid and types_valid
        
    # Valida intervalos
    if range_dict:
        ranges_valid, out_of_range = validate_value_ranges(df, range_dict)
        results["out_of_range"] = out_of_range
        is_valid = is_valid and ranges_valid
        
    # Valida valores ausentes
    nulls_valid, null_cols = validate_missing_values(df, null_threshold_pct)
    results["null_columns"] = null_cols
    is_valid = is_valid and nulls_valid
    
    # Valida duplicados
    if check_duplicates:
        dups_valid, dup_count = validate_duplicates(df, duplicate_subset)
        results["duplicate_count"] = dup_count
        is_valid = is_valid and dups_valid
        
    return is_valid, results