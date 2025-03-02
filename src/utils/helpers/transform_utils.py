# src/utils/helpers/transform_utils.py
"""Helpers para transformações de dados que funcionam tanto em Pandas quanto Spark."""

def dataframe_info(df, is_spark=False):
    """
    Imprime informações sobre um DataFrame (Pandas ou Spark).
    
    Args:
        df: DataFrame (Pandas ou Spark)
        is_spark: True se for um DataFrame Spark
    """
    if is_spark:
        print(f"Schema: {df.schema}")
        print(f"Contagem: {df.count()}")
        print("Amostra:")
        df.show(5, truncate=False)
    else:
        print(df.info())
        print("\nAmostra:")
        print(df.head())

# Mais funções de transformação que abstraem Pandas/Spark...