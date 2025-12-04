"""
Header Normalization Utility
Normalizes Excel/CSV column headers to valid, clean names
"""

import re
from typing import List


def normalize_header(header: str) -> str:
    """
    Normalize a single column header to a clean, valid name.
    
    Rules:
    - Convert to lowercase
    - Replace spaces with underscores
    - Replace special characters with meaningful text:
      - % → percentage
      - & → and
      - @ → at
      - # → number
      - $ → dollar
      - + → plus
      - - → underscore (already handled)
      - / → or
      - \ → removed
      - * → removed
      - ( → removed
      - ) → removed
      - [ → removed
      - ] → removed
      - { → removed
      - } → removed
      - | → or
      - = → equals
      - < → less_than
      - > → greater_than
      - ? → removed
      - ! → removed
      - : → removed
      - ; → removed
      - , → removed
      - . → removed (unless it's a decimal point in a number)
      - ' → removed
      - " → removed
      - ~ → removed
      - ` → removed
      - ^ → removed
    - Remove multiple consecutive underscores
    - Remove leading/trailing underscores
    - Remove leading/trailing whitespace
    
    Examples:
        "Charged by Zomato agt rejected orders" → "charged_by_zomato_agt_rejected_orders"
        "%" → "percentage"
        "PG Chgs %" → "pg_chgs_percentage"
        "Order ID (Primary)" → "order_id_primary"
        "Amount $USD" → "amount_dollar_usd"
    
    Args:
        header: Original column header string
        
    Returns:
        Normalized header string
    """
    if not header or not isinstance(header, str):
        return ""
    
    # Convert to string and strip whitespace
    normalized = str(header).strip()
    
    # If empty after stripping, return empty string
    if not normalized:
        return ""
    
    # Convert to lowercase
    normalized = normalized.lower()
    
    # Replace special characters with meaningful text or remove them
    replacements = {
        '%': 'percentage',
        '&': 'and',
        '@': 'at',
        '#': 'number',
        '$': 'dollar',
        '+': 'plus',
        '/': 'or',
        '\\': '',
        '*': '',
        '(': '',
        ')': '',
        '[': '',
        ']': '',
        '{': '',
        '}': '',
        '|': 'or',
        '=': 'equals',
        '<': 'less_than',
        '>': 'greater_than',
        '?': '',
        '!': '',
        ':': '',
        ';': '',
        ',': '',
        "'": '',
        '"': '',
        '~': '',
        '`': '',
        '^': '',
    }
    
    # Apply replacements
    for char, replacement in replacements.items():
        normalized = normalized.replace(char, replacement)
    
    # Replace spaces and hyphens with underscores
    normalized = re.sub(r'[\s\-]+', '_', normalized)
    
    # Remove dots (periods) - they can cause issues in MongoDB field names
    normalized = normalized.replace('.', '')
    
    # Remove multiple consecutive underscores
    normalized = re.sub(r'_+', '_', normalized)
    
    # Remove leading and trailing underscores
    normalized = normalized.strip('_')
    
    # If result is empty or only underscores, use a default name
    if not normalized or normalized == '_':
        normalized = 'unnamed_column'
    
    return normalized


def normalize_headers(headers: List[str]) -> List[str]:
    """
    Normalize a list of column headers.
    
    Handles duplicate names by appending numbers:
    - If "amount" appears twice, becomes "amount" and "amount_2"
    
    Args:
        headers: List of original column header strings
        
    Returns:
        List of normalized header strings
    """
    if not headers:
        return []
    
    normalized = []
    seen = {}
    
    for header in headers:
        norm_header = normalize_header(header)
        
        # Handle duplicates
        if norm_header in seen:
            seen[norm_header] += 1
            norm_header = f"{norm_header}_{seen[norm_header]}"
        else:
            seen[norm_header] = 0
        
        # Track this normalized header
        if norm_header in normalized:
            # If somehow we still have a duplicate, append number
            counter = 2
            while f"{norm_header}_{counter}" in normalized:
                counter += 1
            norm_header = f"{norm_header}_{counter}"
        
        normalized.append(norm_header)
    
    return normalized


def normalize_dataframe_columns(df, inplace: bool = False):
    """
    Normalize column names of a pandas DataFrame.
    
    Args:
        df: pandas DataFrame
        inplace: If True, modify DataFrame in place; otherwise return a copy
        
    Returns:
        DataFrame with normalized column names (if inplace=False)
    """
    import pandas as pd
    
    normalized_columns = normalize_headers(list(df.columns))
    
    if inplace:
        df.columns = normalized_columns
        return df
    else:
        df_copy = df.copy()
        df_copy.columns = normalized_columns
        return df_copy

