"""
Statistical Analysis Core

Core statistical functions for A/B testing and experiment analysis.
"""

import pandas as pd
import numpy as np
from statsmodels.stats.proportion import proportion_confint, proportions_ztest
from typing import Optional, Union


def calculate_significance(
    df: pd.DataFrame, 
    variant_col: str = 'variant', 
    conversion_col: str = 'conversion', 
    control_label: str = 'control',
    alpha: float = 0.05,
    conversion_timestamp_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate statistical significance for A/B test variants vs control.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame containing experiment data
    variant_col : str
        Column containing variant assignments
    conversion_col : str
        Column containing binary conversion indicator (0/1)
    control_label : str
        Label for the control group
    alpha : float
        Significance level (default: 0.05)
    conversion_timestamp_col : str, optional
        If provided, creates binary conversion column from timestamp
        
    Returns:
    --------
    pd.DataFrame
        Statistical results with uplift, confidence intervals, and p-values
    """
    temp_conversion_col = None
    if conversion_timestamp_col is not None:
        temp_conversion_col = '__temp_conversion__'
        df = df.copy()
        df[temp_conversion_col] = df[conversion_timestamp_col].notna().astype(int)
        use_conversion_col = temp_conversion_col
    else:
        use_conversion_col = conversion_col

    # Aggregate stats
    stats = (
        df.groupby(variant_col)[use_conversion_col]
        .agg(['sum', 'count'])
        .rename(columns={'sum': 'conversions', 'count': 'total'})
        .reset_index()
    )
    stats['cvr'] = stats['conversions'] / stats['total']

    # Find control row
    if control_label not in stats[variant_col].values:
        raise ValueError(f"No '{control_label}' group found in variant column.")
    control_row = stats[stats[variant_col] == control_label].iloc[0]

    # Confidence intervals
    ci_low, ci_upp = proportion_confint(stats['conversions'], stats['total'], alpha=alpha, method='wilson')
    stats['ci_lower'] = ci_low
    stats['ci_upper'] = ci_upp

    # Uplift, p-value, significance
    stats['uplift'] = stats['cvr'] / control_row['cvr'] - 1    
    stats['p_value'] = np.nan
    stats['is_significant'] = False

    for i, row in stats.iterrows():
        if row[variant_col] == control_label:
            stats.at[i, 'uplift'] = 0.0
            stats.at[i, 'p_value'] = 1.0
            stats.at[i, 'is_significant'] = False
        else:
            count = np.array([row['conversions'], control_row['conversions']])
            nobs = np.array([row['total'], control_row['total']])
            stat, pval = proportions_ztest(count, nobs)
            stats.at[i, 'p_value'] = pval
            stats.at[i, 'is_significant'] = pval < alpha

    # Formatting for display
    stats['cvr'] = (stats['cvr'] * 100).map('{:.2f}%'.format)
    stats['uplift'] = stats['uplift'].map(lambda x: '{:+.2f}%'.format(x * 100))
    stats['ci_lower'] = (stats['ci_lower'] * 100).map('{:.2f}%'.format)
    stats['ci_upper'] = (stats['ci_upper'] * 100).map('{:.2f}%'.format)
    stats['p_value'] = stats['p_value'].map('{:.4f}'.format)

    # Reorder columns
    stats = stats[[variant_col, 'total', 'conversions', 'cvr', 'uplift', 'ci_lower', 'ci_upper', 'p_value', 'is_significant']]
    return stats


def calculate_significance_raw(
    df: pd.DataFrame, 
    variant_col: str = 'variant', 
    conversion_col: str = 'conversion', 
    control_label: str = 'control',
    alpha: float = 0.05,
    conversion_timestamp_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate statistical significance with raw numerical output (no formatting).
    
    Same as calculate_significance but returns raw numbers for further processing.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame containing experiment data
    variant_col : str
        Column containing variant assignments
    conversion_col : str
        Column containing binary conversion indicator (0/1)
    control_label : str
        Label for the control group
    alpha : float
        Significance level (default: 0.05)
    conversion_timestamp_col : str, optional
        If provided, creates binary conversion column from timestamp
        
    Returns:
    --------
    pd.DataFrame
        Statistical results with raw numerical values
    """
    temp_conversion_col = None
    if conversion_timestamp_col is not None:
        temp_conversion_col = '__temp_conversion__'
        df = df.copy()
        df[temp_conversion_col] = df[conversion_timestamp_col].notna().astype(int)
        use_conversion_col = temp_conversion_col
    else:
        use_conversion_col = conversion_col

    # Aggregate stats
    stats = (
        df.groupby(variant_col)[use_conversion_col]
        .agg(['sum', 'count'])
        .rename(columns={'sum': 'conversions', 'count': 'total'})
        .reset_index()
    )
    stats['cvr'] = stats['conversions'] / stats['total']

    # Find control row
    if control_label not in stats[variant_col].values:
        raise ValueError(f"No '{control_label}' group found in variant column.")
    control_row = stats[stats[variant_col] == control_label].iloc[0]

    # Confidence intervals
    ci_low, ci_upp = proportion_confint(stats['conversions'], stats['total'], alpha=alpha, method='wilson')
    stats['ci_lower'] = ci_low
    stats['ci_upper'] = ci_upp

    # Uplift, p-value, significance
    stats['uplift'] = stats['cvr'] / control_row['cvr'] - 1    
    stats['p_value'] = np.nan
    stats['is_significant'] = False

    for i, row in stats.iterrows():
        if row[variant_col] == control_label:
            stats.at[i, 'uplift'] = 0.0
            stats.at[i, 'p_value'] = 1.0
            stats.at[i, 'is_significant'] = False
        else:
            count = np.array([row['conversions'], control_row['conversions']])
            nobs = np.array([row['total'], control_row['total']])
            stat, pval = proportions_ztest(count, nobs)
            stats.at[i, 'p_value'] = pval
            stats.at[i, 'is_significant'] = pval < alpha

    return stats


def safe_calculate_significance(
    df: pd.DataFrame, 
    time_bound: Optional[int] = None,
    conversion_timestamp_col: str = 'conversion_timestamp',
    assignment_col: str = 'first_assigned_at',
    **kwargs
) -> Union[pd.DataFrame, str]:
    """
    Safely calculate significance with edge case handling.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame containing experiment data
    time_bound : int, optional
        Time bound in days for conversion window
    conversion_timestamp_col : str
        Column containing conversion timestamp
    assignment_col : str
        Column containing assignment timestamp
    **kwargs
        Additional arguments passed to calculate_significance
        
    Returns:
    --------
    Union[pd.DataFrame, str]
        Statistical results or error message string
    """
    try:
        # Create temporary DataFrame with conversion indicator
        temp_df = df.copy()
        if time_bound is not None:
            temp_df['conversion'] = ~temp_df[conversion_timestamp_col].isna() & \
                                   (temp_df[conversion_timestamp_col] < (temp_df[assignment_col] + pd.Timedelta(days=time_bound)))
        else:
            temp_df['conversion'] = ~temp_df[conversion_timestamp_col].isna()
        
        return calculate_significance(temp_df, **kwargs)
        
    except ValueError as e:
        if len(df) > 0:
            conversion_rate = (~df[conversion_timestamp_col].isna()).mean()
            if conversion_rate in [0.0, 1.0]:
                return f"{100 * conversion_rate:.1f}% conversion rate - no statistical test possible"
            else:
                return f"Error in statistical calculation: {str(e)}"
        return "No data available"