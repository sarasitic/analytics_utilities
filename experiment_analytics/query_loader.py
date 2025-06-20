"""
Query Loader Utilities

Functions for loading and executing SQL queries from files, particularly useful for BigQuery operations.
"""

import pandas as pd
import glob
import os
from typing import Dict, Optional, Callable
from google.cloud import bigquery


def load_queries_from_directory(query_directory: str, pattern: str = "*.sql") -> Dict[str, str]:
    """
    Read all SQL query files from a directory and return as a dictionary.
    
    Parameters:
    -----------
    query_directory : str
        Path to directory containing SQL files
    pattern : str, optional
        File pattern to match (default: "*.sql")
        
    Returns:
    --------
    Dict[str, str]
        Dictionary mapping query names (filename without extension) to query text
    """
    query_files = glob.glob(os.path.join(query_directory, pattern))
    queries = {}
    
    for file_path in query_files:
        query_name = os.path.splitext(os.path.basename(file_path))[0]
        with open(file_path, 'r') as f:
            queries[query_name] = f.read()
    
    return queries


def execute_queries_batch(
    queries: Dict[str, str], 
    executor_func: Callable[[str], pd.DataFrame],
    verbose: bool = True
) -> Dict[str, pd.DataFrame]:
    """
    Execute a batch of queries and return results as DataFrames.
    
    Parameters:
    -----------
    queries : Dict[str, str]
        Dictionary mapping query names to query text
    executor_func : Callable[[str], pd.DataFrame]
        Function that takes a query string and returns a DataFrame
        (e.g., your refresh_bq function)
    verbose : bool, optional
        Whether to print progress messages (default: True)
        
    Returns:
    --------
    Dict[str, pd.DataFrame]
        Dictionary mapping query names to resulting DataFrames
    """
    dataframes = {}
    
    if verbose:
        print("Loading data from all queries...")
    
    for query_name, query in queries.items():
        if verbose:
            print(f"\nLoading {query_name} data...")
        dataframes[query_name] = executor_func(query)
    
    if verbose:
        print("\nDataframe shapes:")
        for name, df in dataframes.items():
            print(f"{name}: {df.shape}")
            
        print("\nUnique shops per dataframe:")
        for name, df in dataframes.items():
            if 'shop_id' in df.columns:
                print(f"{name}: {df['shop_id'].nunique()}")
    
    return dataframes


def prepare_milestone_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process milestone data for analysis."""
    df = df.copy()
    
    # Convert timestamps
    df['event_at'] = pd.to_datetime(df['event_at'])
    if 'experiment_assigned_at' in df.columns:
        df['experiment_assigned_at'] = pd.to_datetime(df['experiment_assigned_at'])
    
    return df


def prepare_csr_data(df: pd.DataFrame) -> pd.DataFrame:
    """Process CSR data for analysis."""
    df = df.copy()
    
    # Convert timestamps
    if 'ticket_created_at' in df.columns:
        df['ticket_created_at'] = pd.to_datetime(df['ticket_created_at'])
    if 'experiment_assigned_at' in df.columns:
        df['experiment_assigned_at'] = pd.to_datetime(df['experiment_assigned_at'])
    
    return df


def prepare_metric_data(df: pd.DataFrame, id_col: str = '_id') -> pd.DataFrame:
    """Process metric data for analysis."""
    df = df.copy()
    
    # Convert timestamps if they exist
    timestamp_cols = df.select_dtypes(include=['object']).columns
    for col in timestamp_cols:
        if 'time' in col.lower() or 'at' in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass
    
    return df


def auto_process_dataframes(dataframes: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Automatically process DataFrames based on their names using appropriate preparation functions.
    
    Parameters:
    -----------
    dataframes : Dict[str, pd.DataFrame]
        Dictionary of DataFrames to process
        
    Returns:
    --------
    Dict[str, pd.DataFrame]
        Dictionary of processed DataFrames
    """
    processed = {}
    
    for name, df in dataframes.items():
        if 'milestones' in name.lower():
            processed[name] = prepare_milestone_data(df)
        elif 'csr' in name.lower():
            processed[name] = prepare_csr_data(df)
        else:
            processed[name] = prepare_metric_data(df)
    
    return processed