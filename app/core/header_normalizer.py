"""
Header Normalizer Module
Normalizes DataFrame column headers by cleaning special characters, spaces, etc.
"""

import pandas as pd
import re
from typing import Optional


def normalize_dataframe_columns(df: pd.DataFrame, inplace: bool = False) -> Optional[pd.DataFrame]:
    """
    Normalize column headers by cleaning special characters, spaces, and converting to lowercase.
    
    Args:
        df: The pandas DataFrame to normalize
        inplace: If True, modify the DataFrame in place. If False, return a new DataFrame.
    
    Returns:
        DataFrame with normalized column names (if inplace=False), or None (if inplace=True)
    """
    if df is None or df.empty:
        return df if not inplace else None
    
    # Create a copy if not inplace
    result_df = df if inplace else df.copy()
    
    # Normalize column names
    normalized_columns = []
    for col in result_df.columns:
        if col is None:
            normalized_columns.append("unnamed_column")
            continue
        
        # Convert to string if not already
        col_str = str(col).strip()
        
        # If empty after stripping, use a default name
        if not col_str:
            normalized_columns.append("unnamed_column")
            continue
        
        # Replace spaces with underscores
        col_str = col_str.replace(" ", "_")
        
        # Remove special characters except underscores and alphanumeric
        col_str = re.sub(r'[^a-zA-Z0-9_]', '', col_str)
        
        # Remove leading/trailing underscores
        col_str = col_str.strip('_')
        
        # Convert to lowercase
        col_str = col_str.lower()
        
        # If empty after all processing, use default name
        if not col_str:
            col_str = "unnamed_column"
        
        # Handle duplicate column names by appending index
        if col_str in normalized_columns:
            index = 1
            original_col_str = col_str
            while col_str in normalized_columns:
                col_str = f"{original_col_str}_{index}"
                index += 1
        
        normalized_columns.append(col_str)
    
    # Assign normalized column names
    result_df.columns = normalized_columns
    
    return None if inplace else result_df

