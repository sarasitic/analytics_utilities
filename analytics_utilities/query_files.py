
import pandas as pd
import glob
import os



def prepare_metric_data(df, id_col='_id'):

    """Process metric data for analysis."""
    df = df.copy()
    timestamp_cols = df.select_dtypes(include=['object']).columns
    for col in timestamp_cols:
        if 'time' in col.lower() or 'at' in col.lower():
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass
    return df

def load_and_prepare_data(query_dir, refresh_bq_func, verbose=True, id_col='_id'):
    """
    Loads all SQL queries from a directory, runs them using the provided function,
    and processes the resulting DataFrames.
    
    Args:
        query_dir (str): Path to directory containing .sql files.
        refresh_bq_func (callable): Function to execute SQL and return a DataFrame.
        verbose (bool): Whether to print progress and summary info.
        id_col (str): Name of the unique identifier column (default '_id').
    
    Returns:
        dict: Mapping of query names to processed DataFrames.
    """
    # Read all SQL queries from files
    query_files = glob.glob(os.path.join(query_dir, '*.sql'))
    queries = {}
    for file_path in query_files:
        query_name = os.path.splitext(os.path.basename(file_path))[0]
        with open(file_path, 'r') as f:
            queries[query_name] = f.read()
    
    if verbose:
        print("Loading data from all queries...")
    dataframes = {}
    for query_name, query in queries.items():
        if verbose:
            print(f"\nLoading {query_name} data...")
        dataframes[query_name] = refresh_bq_func(query)
    
    if verbose:
        print("\nDataframe shapes:")
        for name, df in dataframes.items():
            print(f"{name}: {df.shape}")
        print("\nUnique IDs per dataframe:")
        for name, df in dataframes.items():
            if id_col in df.columns:
                print(f"{name}: {df[id_col].nunique()}")
    
    if verbose:
        print("\nProcessing data...")
    for name, df in dataframes.items():
        dataframes[name] = prepare_metric_data(df, id_col=id_col)
    
    return dataframes