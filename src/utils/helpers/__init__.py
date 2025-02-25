# src/utils/helpers/__init__.py
from .data_cleaning import (
    inspect_dataframe,
    safe_rename_columns,
    identify_value_column,
    ensure_numeric,
    remove_duplicates,
    fill_missing_values
)

from .data_validation import (
    validate_column_presence,
    validate_data_types,
    validate_value_ranges,
    validate_missing_values,
    validate_duplicates,
    validate_dataset
)

from .date_utils import (
    standardize_date_column,
    create_date_features,
    create_time_windows,
    resample_time_series
)

from .math_utils import (
    calculate_variations,
    calculate_moving_average,
    calculate_cumulative_values,
    calculate_year_to_date,
    calculate_volatility,
    calculate_financial_metrics
)

from .aws_helpers import (
    get_s3_client,
    list_s3_files,
    get_latest_s3_file,
    read_parquet_from_s3,
    read_csv_from_s3,
    write_parquet_to_s3,
    write_csv_to_s3,
    get_s3_path_with_timestamp,
    s3_move_file,
    test_s3_connection
)

from .logging_utils import (
    setup_logging,
    get_logger,
    log_execution_time,
    log_dataframe_stats,
    log_process_result
)

# Facilita importação de todos os módulos de uma vez
__all__ = [
    # Data Cleaning
    'inspect_dataframe',
    'safe_rename_columns',
    'identify_value_column',
    'ensure_numeric',
    'remove_duplicates',
    'fill_missing_values',
    
    # Data Validation
    'validate_column_presence',
    'validate_data_types',
    'validate_value_ranges',
    'validate_missing_values',
    'validate_duplicates',
    'validate_dataset',
    
    # Date Utils
    'standardize_date_column',
    'create_date_features',
    'create_time_windows',
    'resample_time_series',
    
    # Math Utils
    'calculate_variations',
    'calculate_moving_average',
    'calculate_cumulative_values',
    'calculate_year_to_date',
    'calculate_volatility',
    'calculate_financial_metrics',
    
    # AWS Helpers
    'get_s3_client',
    'list_s3_files',
    'get_latest_s3_file',
    'read_parquet_from_s3',
    'read_csv_from_s3',
    'write_parquet_to_s3',
    'write_csv_to_s3',
    'get_s3_path_with_timestamp',
    's3_move_file',
    'test_s3_connection',
    
    # Logging Utils
    'setup_logging',
    'get_logger',
    'log_execution_time',
    'log_dataframe_stats',
    'log_process_result'
]