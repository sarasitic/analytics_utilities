"""
Guide-specific Analysis Functions

This module provides functions for analyzing guide completion, milestone correlations,
and component-specific analysis in A/B tests.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chi2_contingency, pearsonr
from typing import List, Optional, Dict, Any, Tuple


def analyze_milestone_completion(
    milestones_df: pd.DataFrame,
    max_days: Optional[int] = None,
    variant_col: str = 'variant',
    shop_id_col: str = 'shop_id',
    assignment_col: str = 'first_assigned_at',
    event_time_col: str = 'event_at',
    event_name_col: str = 'event_name'
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Analyze milestone completion rates with optional time limit and statistical significance testing.
    
    Parameters:
    -----------
    milestones_df : pandas.DataFrame
        DataFrame containing milestone data
    max_days : int, optional
        Maximum days after assignment to count completions
    variant_col : str, default='variant'
        Column containing variant assignment
    shop_id_col : str, default='shop_id'
        Column containing shop IDs
    assignment_col : str, default='first_assigned_at'
        Column containing assignment timestamps
    event_time_col : str, default='event_at'
        Column containing event completion timestamps
    event_name_col : str, default='event_name'
        Column containing event names
        
    Returns:
    --------
    tuple : (completion_df, stats_dict) - DataFrames with completion rates and statistical results
    """
    
    print("=== MILESTONE COMPLETION ANALYSIS ===")
    
    df = milestones_df.copy()
    
    # Filter by time window if specified
    if max_days is not None:
        cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=max_days)
        df = df[df[assignment_col] >= cutoff_date]
        print(f"Analyzing milestones within {max_days} days of assignment")
    
    # Get milestone completion rates by variant
    milestone_events = df[df[event_name_col].str.startswith('milestone_', na=False)]
    
    # Calculate completion rates
    total_shops = df.groupby(variant_col)[shop_id_col].nunique()
    completed_shops = milestone_events.groupby([variant_col, event_name_col])[shop_id_col].nunique().reset_index()
    
    results = []
    for milestone in milestone_events[event_name_col].unique():
        milestone_data = completed_shops[completed_shops[event_name_col] == milestone]
        
        for variant in ['control', 'treatment']:
            completed = milestone_data[milestone_data[variant_col] == variant][shop_id_col].sum() if len(milestone_data[milestone_data[variant_col] == variant]) > 0 else 0
            total = total_shops.get(variant, 0)
            completion_rate = completed / total if total > 0 else 0
            
            results.append({
                'milestone': milestone,
                'variant': variant,
                'completed': completed,
                'total': total,
                'completion_rate': completion_rate
            })
    
    completion_df = pd.DataFrame(results)
    
    # Statistical significance testing
    stats_results = {}
    for milestone in completion_df['milestone'].unique():
        milestone_subset = completion_df[completion_df['milestone'] == milestone]
        
        if len(milestone_subset) == 2:
            control_row = milestone_subset[milestone_subset['variant'] == 'control'].iloc[0]
            treatment_row = milestone_subset[milestone_subset['variant'] == 'treatment'].iloc[0]
            
            # Chi-square test
            contingency = [
                [treatment_row['completed'], treatment_row['total'] - treatment_row['completed']],
                [control_row['completed'], control_row['total'] - control_row['completed']]
            ]
            
            chi2, p_value = chi2_contingency(contingency)[:2]
            
            # Calculate lift
            lift = (treatment_row['completion_rate'] / control_row['completion_rate'] - 1) if control_row['completion_rate'] > 0 else np.nan
            
            stats_results[milestone] = {
                'p_value': p_value,
                'lift': lift,
                'is_significant': p_value < 0.05,
                'control_rate': control_row['completion_rate'],
                'treatment_rate': treatment_row['completion_rate']
            }
    
    # Print summary
    print(f"\nTotal shops by variant: {total_shops.to_dict()}")
    print(f"Analyzed {len(completion_df['milestone'].unique())} milestones")
    
    for milestone, stats in stats_results.items():
        sig_indicator = "✅" if stats['is_significant'] else "❌"
        print(f"{sig_indicator} {milestone}: {stats['lift']:.1%} lift (p={stats['p_value']:.3f})")
    
    return completion_df, stats_results


def analyze_guidance_completion(
    guide_interactions: pd.DataFrame,
    max_days: Optional[int] = None,
    exclude_components: Optional[List[str]] = None,
    variant_col: str = 'variant',
    shop_id_col: str = 'shop_id',
    assignment_col: str = 'first_assigned_at',
    event_time_col: str = 'event_at',
    event_name_col: str = 'event_name'
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Analyze guidance completion rates with optional time limit and statistical significance testing.
    
    Parameters:
    -----------
    guide_interactions : pandas.DataFrame
        DataFrame containing guidance interaction data
    max_days : int, optional
        Maximum days after assignment to count completions
    exclude_components : list, optional
        List of component names to exclude from analysis
    variant_col : str, default='variant'
        Column containing variant assignment
    shop_id_col : str, default='shop_id'
        Column containing shop IDs
    assignment_col : str, default='first_assigned_at'
        Column containing assignment timestamps
    event_time_col : str, default='event_at'
        Column containing event completion timestamps
    event_name_col : str, default='event_name'
        Column containing event names
        
    Returns:
    --------
    tuple : (completion_df, stats_dict) - DataFrames with completion rates and statistical results
    """
    
    print("=== GUIDANCE COMPLETION ANALYSIS ===")
    
    df = guide_interactions.copy()
    
    # Filter by time window if specified
    if max_days is not None:
        cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=max_days)
        df = df[df[assignment_col] >= cutoff_date]
        print(f"Analyzing guidance within {max_days} days of assignment")
    
    # Filter out excluded components
    if exclude_components:
        df = df[~df[event_name_col].isin(exclude_components)]
        print(f"Excluding components: {exclude_components}")
    
    # Get component completion rates by variant
    component_events = df[df[event_name_col].str.startswith('component_', na=False)]
    
    # Calculate completion rates
    total_shops = df.groupby(variant_col)[shop_id_col].nunique()
    completed_shops = component_events.groupby([variant_col, event_name_col])[shop_id_col].nunique().reset_index()
    
    results = []
    for component in component_events[event_name_col].unique():
        component_data = completed_shops[completed_shops[event_name_col] == component]
        
        for variant in ['control', 'treatment']:
            completed = component_data[component_data[variant_col] == variant][shop_id_col].sum() if len(component_data[component_data[variant_col] == variant]) > 0 else 0
            total = total_shops.get(variant, 0)
            completion_rate = completed / total if total > 0 else 0
            
            results.append({
                'component': component,
                'variant': variant,
                'completed': completed,
                'total': total,
                'completion_rate': completion_rate
            })
    
    completion_df = pd.DataFrame(results)
    
    # Statistical significance testing
    stats_results = {}
    for component in completion_df['component'].unique():
        component_subset = completion_df[completion_df['component'] == component]
        
        if len(component_subset) == 2:
            control_row = component_subset[component_subset['variant'] == 'control'].iloc[0]
            treatment_row = component_subset[component_subset['variant'] == 'treatment'].iloc[0]
            
            # Chi-square test
            contingency = [
                [treatment_row['completed'], treatment_row['total'] - treatment_row['completed']],
                [control_row['completed'], control_row['total'] - control_row['completed']]
            ]
            
            chi2, p_value = chi2_contingency(contingency)[:2]
            
            # Calculate lift
            lift = (treatment_row['completion_rate'] / control_row['completion_rate'] - 1) if control_row['completion_rate'] > 0 else np.nan
            
            stats_results[component] = {
                'p_value': p_value,
                'lift': lift,
                'is_significant': p_value < 0.05,
                'control_rate': control_row['completion_rate'],
                'treatment_rate': treatment_row['completion_rate']
            }
    
    # Print summary
    print(f"\nTotal shops by variant: {total_shops.to_dict()}")
    print(f"Analyzed {len(completion_df['component'].unique())} guide components")
    
    for component, stats in stats_results.items():
        sig_indicator = "✅" if stats['is_significant'] else "❌"
        print(f"{sig_indicator} {component}: {stats['lift']:.1%} lift (p={stats['p_value']:.3f})")
    
    return completion_df, stats_results


def analyze_component_milestone_correlations_by_variant(
    guide_interactions_df: pd.DataFrame,
    milestones_df: pd.DataFrame,
    variant_col: str = 'variant',
    shop_id_col: str = 'shop_id',
    event_name_col: str = 'event_name'
) -> Dict[str, Any]:
    """
    Analyze correlations between guide components and milestones, comparing control vs treatment.
    
    Parameters:
    -----------
    guide_interactions_df : pandas.DataFrame
        DataFrame containing guide interaction data
    milestones_df : pandas.DataFrame
        DataFrame containing milestone data
    variant_col : str, default='variant'
        Column containing variant assignment
    shop_id_col : str, default='shop_id'
        Column containing shop IDs
    event_name_col : str, default='event_name'
        Column containing event names
        
    Returns:
    --------
    dict : Dictionary containing correlation analysis results by variant
    """
    
    print("=== COMPONENT-MILESTONE CORRELATION ANALYSIS ===")
    
    # Get component completions
    components = guide_interactions_df[guide_interactions_df[event_name_col].str.startswith('component_', na=False)]
    component_completions = components.groupby([shop_id_col, variant_col, event_name_col]).size().reset_index(name='completed')
    component_pivot = component_completions.pivot_table(
        index=[shop_id_col, variant_col], 
        columns=event_name_col, 
        values='completed', 
        fill_value=0
    ).reset_index()
    
    # Get milestone completions
    milestones = milestones_df[milestones_df[event_name_col].str.startswith('milestone_', na=False)]
    milestone_completions = milestones.groupby([shop_id_col, variant_col, event_name_col]).size().reset_index(name='completed')
    milestone_pivot = milestone_completions.pivot_table(
        index=[shop_id_col, variant_col], 
        columns=event_name_col, 
        values='completed', 
        fill_value=0
    ).reset_index()
    
    # Merge component and milestone data
    merged_df = component_pivot.merge(
        milestone_pivot, 
        on=[shop_id_col, variant_col], 
        how='outer'
    ).fillna(0)
    
    # Calculate correlations by variant
    correlation_results = {}
    
    for variant in ['control', 'treatment']:
        variant_data = merged_df[merged_df[variant_col] == variant]
        
        if len(variant_data) == 0:
            continue
            
        # Get component and milestone columns
        component_cols = [col for col in variant_data.columns if col.startswith('component_')]
        milestone_cols = [col for col in variant_data.columns if col.startswith('milestone_')]
        
        variant_correlations = {}
        
        for component in component_cols:
            component_correlations = {}
            
            for milestone in milestone_cols:
                if len(variant_data) > 1:  # Need at least 2 data points for correlation
                    corr, p_value = pearsonr(variant_data[component], variant_data[milestone])
                    component_correlations[milestone] = {
                        'correlation': corr,
                        'p_value': p_value,
                        'is_significant': p_value < 0.05
                    }
            
            variant_correlations[component] = component_correlations
        
        correlation_results[variant] = variant_correlations
    
    # Print summary
    print(f"\nCorrelation analysis by variant:")
    for variant, variant_corrs in correlation_results.items():
        print(f"\n{variant.upper()} variant:")
        for component, component_corrs in variant_corrs.items():
            print(f"  {component}:")
            for milestone, corr_data in component_corrs.items():
                sig_indicator = "✅" if corr_data['is_significant'] else "❌"
                print(f"    {sig_indicator} {milestone}: r={corr_data['correlation']:.3f} (p={corr_data['p_value']:.3f})")
    
    return correlation_results


def analyze_metric_completion(
    metrics_df: pd.DataFrame,
    max_days: Optional[int] = None,
    variant_col: str = 'variant',
    shop_id_col: str = 'shop_id',
    assignment_col: str = 'first_assigned_at',
    event_time_col: str = 'event_at',
    event_name_col: str = 'event_name'
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    """
    Analyze metric completion rates with optional time limit and statistical significance testing.
    
    Parameters:
    -----------
    metrics_df : pandas.DataFrame
        DataFrame containing metrics data
    max_days : int, optional
        Maximum days after assignment to count completions
    variant_col : str, default='variant'
        Column containing variant assignment
    shop_id_col : str, default='shop_id'
        Column containing shop IDs
    assignment_col : str, default='first_assigned_at'
        Column containing assignment timestamps
    event_time_col : str, default='event_at'
        Column containing event completion timestamps
    event_name_col : str, default='event_name'
        Column containing event names
        
    Returns:
    --------
    tuple : (completion_df, stats_dict) - DataFrames with completion rates and statistical results
    """
    
    print("=== METRIC COMPLETION ANALYSIS ===")
    
    df = metrics_df.copy()
    
    # Filter by time window if specified
    if max_days is not None:
        cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=max_days)
        df = df[df[assignment_col] >= cutoff_date]
        print(f"Analyzing metrics within {max_days} days of assignment")
    
    # Get metric completion rates by variant
    metric_events = df[df[event_name_col].str.startswith('metric_', na=False)]
    
    # Calculate completion rates
    total_shops = df.groupby(variant_col)[shop_id_col].nunique()
    completed_shops = metric_events.groupby([variant_col, event_name_col])[shop_id_col].nunique().reset_index()
    
    results = []
    for metric in metric_events[event_name_col].unique():
        metric_data = completed_shops[completed_shops[event_name_col] == metric]
        
        for variant in ['control', 'treatment']:
            completed = metric_data[metric_data[variant_col] == variant][shop_id_col].sum() if len(metric_data[metric_data[variant_col] == variant]) > 0 else 0
            total = total_shops.get(variant, 0)
            completion_rate = completed / total if total > 0 else 0
            
            results.append({
                'metric': metric,
                'variant': variant,
                'completed': completed,
                'total': total,
                'completion_rate': completion_rate
            })
    
    completion_df = pd.DataFrame(results)
    
    # Statistical significance testing
    stats_results = {}
    for metric in completion_df['metric'].unique():
        metric_subset = completion_df[completion_df['metric'] == metric]
        
        if len(metric_subset) == 2:
            control_row = metric_subset[metric_subset['variant'] == 'control'].iloc[0]
            treatment_row = metric_subset[metric_subset['variant'] == 'treatment'].iloc[0]
            
            # Chi-square test
            contingency = [
                [treatment_row['completed'], treatment_row['total'] - treatment_row['completed']],
                [control_row['completed'], control_row['total'] - control_row['completed']]
            ]
            
            chi2, p_value = chi2_contingency(contingency)[:2]
            
            # Calculate lift
            lift = (treatment_row['completion_rate'] / control_row['completion_rate'] - 1) if control_row['completion_rate'] > 0 else np.nan
            
            stats_results[metric] = {
                'p_value': p_value,
                'lift': lift,
                'is_significant': p_value < 0.05,
                'control_rate': control_row['completion_rate'],
                'treatment_rate': treatment_row['completion_rate']
            }
    
    # Print summary
    print(f"\nTotal shops by variant: {total_shops.to_dict()}")
    print(f"Analyzed {len(completion_df['metric'].unique())} metrics")
    
    for metric, stats in stats_results.items():
        sig_indicator = "✅" if stats['is_significant'] else "❌"
        print(f"{sig_indicator} {metric}: {stats['lift']:.1%} lift (p={stats['p_value']:.3f})")
    
    return completion_df, stats_results