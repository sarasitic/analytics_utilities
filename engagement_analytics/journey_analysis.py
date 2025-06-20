"""
User Journey Analysis Functions

This module provides functions for analyzing user journey patterns, segment migration,
and position distribution analysis in A/B tests.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chi2_contingency, chi2
from scipy.stats.contingency import expected_freq
from typing import List, Optional, Dict, Any, Tuple, Union


def segment_users_for_log_analysis(
    df: pd.DataFrame,
    metric_column: str,
    method: str = 'log_quantiles',
    n_segments: int = 3,
    business_thresholds: Optional[Dict[str, float]] = None
) -> pd.DataFrame:
    """
    Segment users into usage groups optimized for log-scale analysis.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame containing user data
    metric_column : str
        Column containing the metric to segment on
    method : str, default='log_quantiles'
        Segmentation method ('log_quantiles', 'raw_quantiles', 'log_bins', 'business')
    n_segments : int, default=3
        Number of segments to create
    business_thresholds : dict, optional
        Custom thresholds for business segmentation (e.g., {'Low': 2, 'Medium': 6})
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with added 'usage_segment' column
    """
    
    df_segmented = df.copy()
    
    # Remove zero values for log-based methods
    if method in ['log_quantiles', 'log_bins']:
        non_zero_mask = df[metric_column] > 0
        non_zero_data = df[non_zero_mask][metric_column]
        
        if len(non_zero_data) == 0:
            raise ValueError(f"No non-zero values found in {metric_column}")
    
    if method == 'log_quantiles':
        # Use log-scale quantiles for better distribution with skewed data
        log_values = np.log(non_zero_data)
        quantiles = np.linspace(0, 1, n_segments + 1)
        log_thresholds = np.quantile(log_values, quantiles)
        thresholds = np.exp(log_thresholds)
        
        # Create segments
        segment_labels = [f'Segment_{i+1}' for i in range(n_segments)]
        df_segmented['usage_segment'] = pd.cut(
            df_segmented[metric_column], 
            bins=thresholds, 
            labels=segment_labels, 
            include_lowest=True
        )
        
    elif method == 'raw_quantiles':
        # Use raw quantiles
        quantiles = np.linspace(0, 1, n_segments + 1)
        thresholds = np.quantile(df[metric_column], quantiles)
        
        segment_labels = [f'Segment_{i+1}' for i in range(n_segments)]
        df_segmented['usage_segment'] = pd.cut(
            df_segmented[metric_column], 
            bins=thresholds, 
            labels=segment_labels, 
            include_lowest=True
        )
        
    elif method == 'log_bins':
        # Use logarithmic bins
        log_min = np.log(non_zero_data.min())
        log_max = np.log(non_zero_data.max())
        log_bins = np.linspace(log_min, log_max, n_segments + 1)
        thresholds = np.exp(log_bins)
        
        segment_labels = [f'Segment_{i+1}' for i in range(n_segments)]
        df_segmented['usage_segment'] = pd.cut(
            df_segmented[metric_column], 
            bins=thresholds, 
            labels=segment_labels, 
            include_lowest=True
        )
        
    elif method == 'business':
        # Use business-defined thresholds
        if business_thresholds is None:
            raise ValueError("business_thresholds must be provided for business method")
        
        # Create bins from business thresholds
        threshold_values = [0] + list(business_thresholds.values()) + [float('inf')]
        segment_labels = list(business_thresholds.keys()) + ['High']
        
        df_segmented['usage_segment'] = pd.cut(
            df_segmented[metric_column],
            bins=threshold_values,
            labels=segment_labels,
            include_lowest=True
        )
        
    else:
        raise ValueError(f"Unknown method: {method}. Choose from 'log_quantiles', 'raw_quantiles', 'log_bins', 'business'")
    
    # Fill any NaN segments (for zero values in log methods)
    if method in ['log_quantiles', 'log_bins']:
        df_segmented['usage_segment'] = df_segmented['usage_segment'].fillna('Zero_Usage')
    
    print(f"Segmented {len(df_segmented)} users using {method} method:")
    segment_counts = df_segmented['usage_segment'].value_counts().sort_index()
    for segment, count in segment_counts.items():
        print(f"  {segment}: {count:,} users ({count/len(df_segmented)*100:.1f}%)")
    
    return df_segmented


def analyze_segment_migration(
    df_segmented: pd.DataFrame,
    variant_column: str = 'variant',
    control_label: str = 'control',
    treatment_label: str = 'treatment',
    segment_column: str = 'usage_segment'
) -> Dict[str, Any]:
    """
    Analyze if treatment moves users to higher usage segments.
    
    This tests: "Are treated users more likely to be in higher usage segments?"
    
    Parameters:
    -----------
    df_segmented : pandas.DataFrame
        DataFrame with user segments
    variant_column : str, default='variant'
        Column containing variant assignment
    control_label : str, default='control'
        Label for control variant
    treatment_label : str, default='treatment'
        Label for treatment variant
    segment_column : str, default='usage_segment'
        Column containing usage segments
        
    Returns:
    --------
    dict : Dictionary containing migration analysis results
    """
    
    print("=== SEGMENT MIGRATION ANALYSIS ===")
    
    # Create contingency table
    contingency_table = pd.crosstab(
        df_segmented[variant_column], 
        df_segmented[segment_column], 
        margins=True
    )
    
    print("Segment Distribution by Variant:")
    print(contingency_table)
    
    # Calculate percentages
    percentage_table = pd.crosstab(
        df_segmented[variant_column], 
        df_segmented[segment_column], 
        normalize='index'
    ) * 100
    
    print("\nPercentage Distribution by Variant:")
    print(percentage_table.round(1))
    
    # Statistical test (Chi-square)
    contingency_data = contingency_table.iloc[:-1, :-1]  # Remove margins
    chi2_stat, p_value, dof, expected = chi2_contingency(contingency_data)
    
    # Calculate odds ratios for higher segments
    odds_ratios = {}
    segment_order = contingency_data.columns.tolist()
    
    for i, segment in enumerate(segment_order):
        if i > 0:  # Skip the lowest segment
            # For each segment, calculate odds of being in this segment or higher
            control_higher = contingency_data.loc[control_label, segment_order[i:]].sum()
            control_lower = contingency_data.loc[control_label, segment_order[:i]].sum()
            treatment_higher = contingency_data.loc[treatment_label, segment_order[i:]].sum()
            treatment_lower = contingency_data.loc[treatment_label, segment_order[:i]].sum()
            
            # Calculate odds ratio
            if control_lower > 0 and treatment_lower > 0:
                odds_ratio = (treatment_higher / treatment_lower) / (control_higher / control_lower)
                odds_ratios[f'{segment}_or_higher'] = odds_ratio
    
    results = {
        'contingency_table': contingency_table,
        'percentage_table': percentage_table,
        'chi2_statistic': chi2_stat,
        'p_value': p_value,
        'degrees_of_freedom': dof,
        'is_significant': p_value < 0.05,
        'odds_ratios': odds_ratios,
        'expected_frequencies': expected
    }
    
    # Print summary
    print(f"\nChi-square test results:")
    print(f"Chi-square statistic: {chi2_stat:.3f}")
    print(f"p-value: {p_value:.6f}")
    print(f"Degrees of freedom: {dof}")
    
    if p_value < 0.05:
        print("✅ Significant difference in segment distribution between variants")
    else:
        print("❌ No significant difference in segment distribution")
    
    print(f"\nOdds ratios (treatment vs control):")
    for segment, odds_ratio in odds_ratios.items():
        print(f"  {segment}: {odds_ratio:.2f}")
    
    return results


def test_ordinal_segment_trend(
    df_segmented: pd.DataFrame,
    variant_column: str = 'variant',
    control_label: str = 'control',
    treatment_label: str = 'treatment',
    segment_column: str = 'usage_segment'
) -> Dict[str, Any]:
    """
    Test if treatment has an ordinal trend effect (more users in higher segments).
    Uses Cochran-Armitage test for trend in proportions.
    
    Parameters:
    -----------
    df_segmented : pandas.DataFrame
        DataFrame with user segments
    variant_column : str, default='variant'
        Column containing variant assignment
    control_label : str, default='control'
        Label for control variant
    treatment_label : str, default='treatment'
        Label for treatment variant
    segment_column : str, default='usage_segment'
        Column containing usage segments
        
    Returns:
    --------
    dict : Dictionary containing trend test results
    """
    
    print("=== ORDINAL SEGMENT TREND ANALYSIS ===")
    
    # Create contingency table
    contingency_table = pd.crosstab(
        df_segmented[variant_column], 
        df_segmented[segment_column]
    )
    
    # Get treatment and control rows
    treatment_counts = contingency_table.loc[treatment_label]
    control_counts = contingency_table.loc[control_label]
    
    # Assign ordinal scores to segments (assuming ordered segments)
    segments = contingency_table.columns.tolist()
    scores = np.arange(1, len(segments) + 1)
    
    # Calculate Cochran-Armitage trend statistic
    n_treatment = treatment_counts.sum()
    n_control = control_counts.sum()
    n_total = n_treatment + n_control
    
    # Calculate weighted scores
    total_counts = treatment_counts + control_counts
    
    # Mean score
    mean_score = np.sum(scores * total_counts) / n_total
    
    # Variance of scores
    var_score = np.sum(scores**2 * total_counts) / n_total - mean_score**2
    
    # Treatment proportion in each segment
    p_treatment = n_treatment / n_total
    
    # Trend statistic numerator
    numerator = np.sum(scores * treatment_counts) - n_treatment * mean_score
    
    # Trend statistic denominator
    denominator = np.sqrt(n_treatment * n_control * var_score / n_total)
    
    # Calculate trend statistic
    if denominator > 0:
        trend_statistic = numerator / denominator
        
        # Two-tailed p-value (standard normal distribution)
        from scipy.stats import norm
        p_value = 2 * (1 - norm.cdf(abs(trend_statistic)))
    else:
        trend_statistic = 0
        p_value = 1.0
    
    results = {
        'trend_statistic': trend_statistic,
        'p_value': p_value,
        'is_significant': p_value < 0.05,
        'direction': 'positive' if trend_statistic > 0 else 'negative',
        'segment_scores': dict(zip(segments, scores)),
        'treatment_counts': treatment_counts.to_dict(),
        'control_counts': control_counts.to_dict()
    }
    
    # Print summary
    print(f"Ordinal trend test results:")
    print(f"Trend statistic: {trend_statistic:.3f}")
    print(f"p-value: {p_value:.6f}")
    print(f"Direction: {results['direction']}")
    
    if p_value < 0.05:
        direction_text = "higher" if trend_statistic > 0 else "lower"
        print(f"✅ Significant trend: Treatment moves users to {direction_text} segments")
    else:
        print("❌ No significant ordinal trend detected")
    
    return results


def create_position_distribution_for_variant(
    data: pd.DataFrame,
    variant: str,
    task_position_col: str = 'task_position',
    variant_col: str = 'variant',
    task_col: str = 'task_name'
) -> pd.DataFrame:
    """
    Create sequence visualization showing task position distribution for a specific variant.
    
    Parameters:
    -----------
    data : pandas.DataFrame
        DataFrame containing task sequence data
    variant : str
        Variant to analyze ('control' or 'treatment')
    task_position_col : str, default='task_position'
        Column containing task positions
    variant_col : str, default='variant'
        Column containing variant assignment
    task_col : str, default='task_name'
        Column containing task names
        
    Returns:
    --------
    pandas.DataFrame
        DataFrame with position distribution for the variant
    """
    
    print(f"=== POSITION DISTRIBUTION FOR {variant.upper()} ===")
    
    # Filter for the specific variant
    variant_data = data[data[variant_col] == variant].copy()
    
    if len(variant_data) == 0:
        print(f"No data found for variant: {variant}")
        return pd.DataFrame()
    
    # Calculate position distribution
    position_dist = variant_data.groupby([task_position_col, task_col]).size().reset_index(name='count')
    
    # Calculate percentages within each position
    position_totals = position_dist.groupby(task_position_col)['count'].sum()
    position_dist['percentage'] = position_dist.apply(
        lambda row: (row['count'] / position_totals[row[task_position_col]]) * 100, 
        axis=1
    )
    
    # Add total count for each position
    position_dist = position_dist.merge(
        position_totals.reset_index().rename(columns={'count': 'position_total'}),
        on=task_position_col
    )
    
    print(f"Analyzed {len(variant_data)} records for {variant} variant")
    print(f"Found {len(position_dist)} task-position combinations")
    
    # Print top tasks by position
    for position in sorted(position_dist[task_position_col].unique()):
        pos_data = position_dist[position_dist[task_position_col] == position].nlargest(3, 'percentage')
        print(f"\nPosition {position} - Top tasks:")
        for _, row in pos_data.iterrows():
            print(f"  {row[task_col]}: {row['percentage']:.1f}% ({row['count']} of {row['position_total']})")
    
    return position_dist


def analyze_all_tasks_conversion(
    data: pd.DataFrame,
    conversion_col: str = 'converted',
    variant_col: str = 'variant',
    user_id_col: str = 'user_id',
    task_col: str = 'task_name'
) -> Dict[str, Any]:
    """
    Analyze conversion rates across all tasks and variants.
    
    Parameters:
    -----------
    data : pandas.DataFrame
        DataFrame containing task and conversion data
    conversion_col : str, default='converted'
        Column indicating conversion (boolean)
    variant_col : str, default='variant'
        Column containing variant assignment
    user_id_col : str, default='user_id'
        Column containing user IDs
    task_col : str, default='task_name'
        Column containing task names
        
    Returns:
    --------
    dict : Dictionary containing conversion analysis results
    """
    
    print("=== ALL TASKS CONVERSION ANALYSIS ===")
    
    # Calculate conversion rates by variant and task
    conversion_analysis = data.groupby([variant_col, task_col]).agg({
        user_id_col: 'count',
        conversion_col: ['sum', 'mean']
    }).round(3)
    
    conversion_analysis.columns = ['total_users', 'conversions', 'conversion_rate']
    conversion_analysis = conversion_analysis.reset_index()
    
    # Calculate overall conversion rates by variant
    overall_conversion = data.groupby(variant_col).agg({
        user_id_col: 'count',
        conversion_col: ['sum', 'mean']
    }).round(3)
    
    overall_conversion.columns = ['total_users', 'conversions', 'conversion_rate']
    
    # Statistical significance testing for each task
    task_significance = {}
    
    for task in data[task_col].unique():
        task_data = data[data[task_col] == task]
        
        if len(task_data[variant_col].unique()) >= 2:
            control_data = task_data[task_data[variant_col] == 'control']
            treatment_data = task_data[task_data[variant_col] == 'treatment']
            
            if len(control_data) > 0 and len(treatment_data) > 0:
                # Chi-square test
                contingency = [
                    [treatment_data[conversion_col].sum(), len(treatment_data) - treatment_data[conversion_col].sum()],
                    [control_data[conversion_col].sum(), len(control_data) - control_data[conversion_col].sum()]
                ]
                
                chi2_stat, p_value = chi2_contingency(contingency)[:2]
                
                # Calculate lift
                control_rate = control_data[conversion_col].mean()
                treatment_rate = treatment_data[conversion_col].mean()
                lift = (treatment_rate / control_rate - 1) if control_rate > 0 else np.nan
                
                task_significance[task] = {
                    'p_value': p_value,
                    'lift': lift,
                    'is_significant': p_value < 0.05,
                    'control_rate': control_rate,
                    'treatment_rate': treatment_rate
                }
    
    results = {
        'conversion_by_task': conversion_analysis,
        'overall_conversion': overall_conversion,
        'task_significance': task_significance
    }
    
    # Print summary
    print("Overall Conversion Rates:")
    print(overall_conversion)
    
    print(f"\nTask-level significance results:")
    for task, stats in task_significance.items():
        sig_indicator = "✅" if stats['is_significant'] else "❌"
        print(f"{sig_indicator} {task}: {stats['lift']:.1%} lift (p={stats['p_value']:.3f})")
    
    return results


def analyze_segments_with_log(
    df: pd.DataFrame,
    metric_column: str,
    segment_column: str = 'usage_segment',
    variant_column: str = 'variant',
    control_label: str = 'control',
    treatment_label: str = 'treatment'
) -> Dict[str, Any]:
    """
    Analyze A/B test results by user segments using log-scale analysis.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        DataFrame with segmented users
    metric_column : str
        Column containing the metric to analyze
    segment_column : str, default='usage_segment'
        Column containing usage segments
    variant_column : str, default='variant'
        Column containing variant assignment
    control_label : str, default='control'
        Label for control variant
    treatment_label : str, default='treatment'
        Label for treatment variant
        
    Returns:
    --------
    dict : Dictionary containing segment analysis results
    """
    
    print("=== SEGMENT-WISE LOG ANALYSIS ===")
    
    results = {}
    
    for segment in df[segment_column].unique():
        if pd.isna(segment):
            continue
            
        segment_data = df[df[segment_column] == segment]
        
        control_data = segment_data[segment_data[variant_column] == control_label][metric_column]
        treatment_data = segment_data[segment_data[variant_column] == treatment_label][metric_column]
        
        if len(control_data) == 0 or len(treatment_data) == 0:
            continue
        
        # Log transformation (add 1 to handle zeros)
        control_log = np.log(control_data + 1)
        treatment_log = np.log(treatment_data + 1)
        
        # Statistical testing on log-transformed data
        from scipy.stats import ttest_ind, mannwhitneyu
        
        # Mann-Whitney U test (non-parametric)
        u_stat, u_p_value = mannwhitneyu(treatment_data, control_data, alternative='two-sided')
        
        # T-test on log-transformed data
        t_stat, t_p_value = ttest_ind(treatment_log, control_log)
        
        # Calculate effect sizes
        control_mean = control_data.mean()
        treatment_mean = treatment_data.mean()
        lift = (treatment_mean / control_mean - 1) if control_mean > 0 else np.nan
        
        control_log_mean = control_log.mean()
        treatment_log_mean = treatment_log.mean()
        log_lift = treatment_log_mean - control_log_mean
        
        results[segment] = {
            'control_count': len(control_data),
            'treatment_count': len(treatment_data),
            'control_mean': control_mean,
            'treatment_mean': treatment_mean,
            'lift': lift,
            'control_log_mean': control_log_mean,
            'treatment_log_mean': treatment_log_mean,
            'log_lift': log_lift,
            'mannwhitney_u_stat': u_stat,
            'mannwhitney_p_value': u_p_value,
            't_stat': t_stat,
            't_p_value': t_p_value,
            'is_significant_mw': u_p_value < 0.05,
            'is_significant_t': t_p_value < 0.05
        }
    
    # Print summary
    print(f"Segment analysis results for {metric_column}:")
    for segment, stats in results.items():
        mw_indicator = "✅" if stats['is_significant_mw'] else "❌"
        t_indicator = "✅" if stats['is_significant_t'] else "❌"
        print(f"\n{segment}:")
        print(f"  Sample sizes: Control={stats['control_count']}, Treatment={stats['treatment_count']}")
        print(f"  Raw lift: {stats['lift']:.1%}")
        print(f"  Log lift: {stats['log_lift']:.3f}")
        print(f"  {mw_indicator} Mann-Whitney: p={stats['mannwhitney_p_value']:.3f}")
        print(f"  {t_indicator} T-test (log): p={stats['t_p_value']:.3f}")
    
    return results