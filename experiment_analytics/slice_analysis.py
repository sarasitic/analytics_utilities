"""
Slice Distribution Analysis

Functions for analyzing conversion distributions within different data slices for A/B testing.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Union, Any
from .statistical_analysis import calculate_significance, safe_calculate_significance


def analyze_slice_distribution(
    df: pd.DataFrame, 
    slice_column: str, 
    conversion_timestamp_col: str = 'conversion_timestamp',
    assignment_timestamp_col: str = 'first_assigned_at',
    variant_col: str = 'variant',
    time_bounds: Optional[List[Union[int, None]]] = None
) -> Dict[Union[int, str], Dict[str, Any]]:
    """
    Analyze the distribution of conversions within a slice based on timestamp, split by variant,
    for multiple time bounds.

    Parameters:
    -----------
    df : DataFrame
        Input dataframe with experiment data
    slice_column : str
        Column to slice by
    conversion_timestamp_col : str
        Column containing the conversion timestamp
    assignment_timestamp_col : str
        Column containing the assignment timestamp
    variant_col : str
        Column containing the variant assignment
    time_bounds : list of int or None
        List of days to bound the conversion window, None for unbounded

    Returns:
    --------
    Dict[Union[int, str], Dict[str, Any]]
        Results for each time bound containing overall stats, by variant, and by slice/variant
    """
    if time_bounds is None:
        time_bounds = [None]
        
    results = {}

    # Find the latest date in the data (either assignment or conversion)
    max_date = max(
        df[assignment_timestamp_col].max(),
        df[conversion_timestamp_col].max()
    )

    for time_bound in time_bounds:
        print(f"\nAnalyzing distribution for {slice_column}")
        if time_bound is not None:
            print(f"Time bound: {time_bound} days")
            # Only include rows where at least time_bound days have passed since assignment
            eligible_df = df[
                (max_date - df[assignment_timestamp_col]) >= pd.Timedelta(days=time_bound)
            ].copy()
        else:
            print("Time bound: Unbounded")
            eligible_df = df.copy()
        print("="*80)

        # Define conversion calculation function to ensure consistency
        def get_conversions(group):
            if time_bound is not None:
                return (
                    ~group[conversion_timestamp_col].isna() &
                    (group[conversion_timestamp_col] < (group[assignment_timestamp_col] + pd.Timedelta(days=time_bound)))
                ).sum()
            return group[conversion_timestamp_col].notna().sum()

        def get_conversion_rate(group):
            conversions = get_conversions(group)
            total = len(group)
            return (conversions / total * 100) if total > 0 else 0

        # Overall statistics
        total_records = len(eligible_df)
        total_conversions = get_conversions(eligible_df)
        overall_rate = (total_conversions / total_records * 100) if total_records > 0 else 0

        print(f"Overall: {total_conversions}/{total_records} ({overall_rate:.2f}%)")

        # By variant
        variant_stats = eligible_df.groupby(variant_col).agg({
            conversion_timestamp_col: [
                ('Total', 'size'),
                ('Conversions', lambda x: get_conversions(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                }))),
                ('Conversion Rate %', lambda x: get_conversion_rate(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                })))
            ]
        }).round(2)

        variant_stats.columns = variant_stats.columns.droplevel()  # Flatten MultiIndex
        print("\nBy variant:")
        print(variant_stats)
        print("-"*80)

        # By slice value and variant
        slice_variant_stats = eligible_df.groupby([slice_column, variant_col]).agg({
            conversion_timestamp_col: [
                ('Total', 'size'),
                ('Conversions', lambda x: get_conversions(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                }))),
                ('Conversion Rate %', lambda x: get_conversion_rate(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                })))
            ]
        }).round(2)

        slice_variant_stats.columns = slice_variant_stats.columns.droplevel()  # Flatten MultiIndex
        print(f"\nBy {slice_column} and variant:")
        print(slice_variant_stats)

        # Check for perfect conversion combinations
        perfect_conv = slice_variant_stats[slice_variant_stats['Conversion Rate %'].isin([0, 100])]
        if not perfect_conv.empty:
            print("\nWarning: Perfect conversion rates detected:")
            print(perfect_conv)

        results[time_bound] = {
            'overall': {'total': total_records, 'conversions': total_conversions, 'rate': overall_rate},
            'by_variant': variant_stats,
            'by_slice_and_variant': slice_variant_stats
        }

    return results


def run_time_bound_analysis(
    df: pd.DataFrame, 
    slice_column: Optional[Union[str, List[str]]] = None, 
    time_bounds: Optional[List[Union[int, None]]] = None,
    slice_values: Optional[List] = None, 
    conversion_timestamp_col: str = 'conversion_timestamp',
    assignment_timestamp_col: str = 'first_assigned_at',
    conversion_col: str = 'conversion',
    variant_col: str = 'variant',
    id_col: str = '_id'
) -> Dict[str, Any]:
    """
    Run analysis across different time bounds and slices of data, handling nulls and edge cases.
    Calls calculate_significance for each group.
    
    Parameters:
    -----------
    df : DataFrame
        Input dataframe with experiment data
    slice_column : str or List[str], optional
        Column(s) to slice by. If None, runs for whole dataset
    time_bounds : list of int or None, optional
        List of days to bound the conversion window
    slice_values : list, optional
        Specific slice values to analyze
    conversion_timestamp_col : str
        Column containing conversion timestamp
    assignment_timestamp_col : str
        Column containing assignment timestamp
    conversion_col : str
        Binary conversion column
    variant_col : str
        Column containing variant assignment
    id_col : str
        Unique identifier column
        
    Returns:
    --------
    Dict[str, Any]
        Results for each slice and time bound
    """
    if time_bounds is None:
        time_bounds = [None]
        
    results = {}

    # Defensive: check required columns
    required_cols = {id_col, assignment_timestamp_col, variant_col, conversion_timestamp_col}
    missing = required_cols - set(df.columns)
    if missing:
        print(f"Missing required columns: {missing}")
        return {}

    max_date = max(df[assignment_timestamp_col].max(), df[conversion_timestamp_col].max())

    # Handle multiple slice columns (optional)
    if isinstance(slice_column, list):
        for col in slice_column:
            if col in df.columns:
                print(f"\n--- Analyzing by slice: {col} ---")
                results[col] = run_time_bound_analysis(
                    df, slice_column=col, time_bounds=time_bounds, 
                    slice_values=None, conversion_timestamp_col=conversion_timestamp_col,
                    assignment_timestamp_col=assignment_timestamp_col,
                    conversion_col=conversion_col,
                    variant_col=variant_col,
                    id_col=id_col
                )
            else:
                print(f"Column '{col}' not found in DataFrame. Skipping.")
        return results

    # If no slice column specified, run for whole dataset
    if slice_column is None:
        for bound in time_bounds:
            bound_name = 'unbound' if bound is None else f'{bound}_days'
            if bound is not None:
                eligible_df = df[(max_date - df[assignment_timestamp_col]) >= pd.Timedelta(days=bound)].copy()
            else:
                eligible_df = df.copy()
            if len(eligible_df) == 0:
                print(f"\nResults for {bound_name}: No data available.")
                results[bound_name] = None
                continue
            # Add conversion column if needed
            if conversion_col not in eligible_df.columns and conversion_timestamp_col in eligible_df.columns:
                if bound is not None:
                    eligible_df[conversion_col] = ~eligible_df[conversion_timestamp_col].isna() & \
                        (eligible_df[conversion_timestamp_col] < (eligible_df[assignment_timestamp_col] + pd.Timedelta(days=bound)))
                else:
                    eligible_df[conversion_col] = ~eligible_df[conversion_timestamp_col].isna()
            # Call calculate_significance
            print(f"\nResults for {bound_name}:")
            result = calculate_significance(
                eligible_df,
                variant_col=variant_col,
                conversion_col=conversion_col,
                conversion_timestamp_col=conversion_timestamp_col if conversion_col not in eligible_df.columns else None
            )
            print(result)
            results[bound_name] = result
            print('-'*100)
        return results

    # If slice_column is provided but not in df, skip
    if slice_column not in df.columns:
        print(f"Column '{slice_column}' not found in DataFrame. Skipping slice analysis.")
        return results

    # Get slice values if not specified, including nulls
    if slice_values is None:
        slice_values = list(df[slice_column].unique())
        if df[slice_column].isnull().any():
            slice_values.append(None)

    for slice_value in slice_values:
        if slice_value is None:
            slice_df = df[df[slice_column].isnull()]
            display_value = "NULL"
        else:
            slice_df = df[df[slice_column] == slice_value]
            display_value = str(slice_value)

        if len(slice_df) == 0:
            print(f"\nNo data for {slice_column}: {display_value}")
            continue

        results[display_value] = {}

        print(f"\nResults for {slice_column}: {display_value}")
        print(f"Number of records: {len(slice_df)}")
        conversion_rate = (~slice_df[conversion_timestamp_col].isna()).mean()
        print(f"Overall conversion rate: {100 * conversion_rate:.2f}%")
        print('='*100)

        for bound in time_bounds:
            bound_name = 'unbound' if bound is None else f'{bound}_days'
            if bound is not None:
                eligible_slice_df = slice_df[(max_date - slice_df[assignment_timestamp_col]) >= pd.Timedelta(days=bound)].copy()
            else:
                eligible_slice_df = slice_df.copy()

            if len(eligible_slice_df) > 0:
                print(f"\n{bound_name} - Data Summary:")
                # Add conversion column if needed
                if conversion_col not in eligible_slice_df.columns and conversion_timestamp_col in eligible_slice_df.columns:
                    if bound is not None:
                        eligible_slice_df[conversion_col] = ~eligible_slice_df[conversion_timestamp_col].isna() & \
                            (eligible_slice_df[conversion_timestamp_col] < (eligible_slice_df[assignment_timestamp_col] + pd.Timedelta(days=bound)))
                    else:
                        eligible_slice_df[conversion_col] = ~eligible_slice_df[conversion_timestamp_col].isna()
                # Call calculate_significance
                result = calculate_significance(
                    eligible_slice_df,
                    variant_col=variant_col,
                    conversion_col=conversion_col,
                    conversion_timestamp_col=conversion_timestamp_col if conversion_col not in eligible_slice_df.columns else None
                )
                print(result)
                results[display_value][bound_name] = result
            else:
                print(f"\n{bound_name}: No data available")
                results[display_value][bound_name] = None
            print('-'*100)

    return results


def analyze_conversion_timing(
    df: pd.DataFrame,
    conversion_timestamp_col: str = 'conversion_timestamp',
    assignment_col: str = 'first_assigned_at',
    variant_col: str = 'variant',
    call_timestamp_col: str = 'call_date',
    variants: Optional[List[str]] = None
) -> Dict[str, Dict[str, float]]:
    """
    Analyze the timing relationship between support calls and conversions.
    
    Parameters:
    -----------
    df : DataFrame
        Input dataframe with call and conversion data
    conversion_timestamp_col : str
        Column containing conversion timestamp
    assignment_col : str
        Column containing assignment timestamp  
    variant_col : str
        Column containing variant assignment
    call_timestamp_col : str
        Column containing call timestamp
    variants : List[str], optional
        List of variants to analyze
        
    Returns:
    --------
    Dict[str, Dict[str, float]]
        Timing analysis results by variant
    """
    if variants is None:
        variants = df[variant_col].unique()
    
    results = {}
    
    for variant in variants:
        if variant == 'control':
            continue
            
        variant_data = df[df[variant_col] == variant].copy()
        total_shops = len(variant_data)
        
        # Calculate days difference for timing classification
        variant_data['days_diff'] = (variant_data[conversion_timestamp_col] - variant_data[call_timestamp_col]).dt.total_seconds() / (24 * 60 * 60)
        
        # Classify timing
        converted_before = len(variant_data[
            (variant_data[conversion_timestamp_col].notna()) & 
            (variant_data['days_diff'] < -1)  # More than 1 day before
        ])
        
        converted_same_day = len(variant_data[
            (variant_data[conversion_timestamp_col].notna()) & 
            (variant_data['days_diff'].between(-1, 1))  # Within same day (-1 to +1 day)
        ])
        
        converted_after = len(variant_data[
            (variant_data[conversion_timestamp_col].notna()) & 
            (variant_data['days_diff'] > 1)  # More than 1 day after
        ])
        
        no_conversion = total_shops - converted_before - converted_same_day - converted_after
        
        results[variant] = {
            'total': total_shops,
            'before': (converted_before / total_shops) * 100,
            'same_day': (converted_same_day / total_shops) * 100,
            'after': (converted_after / total_shops) * 100,
            'none': (no_conversion / total_shops) * 100
        }
    
    return results