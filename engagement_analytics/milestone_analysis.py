"""
Milestone and Event Completion Analysis

This module provides generic functions for analyzing completion rates of milestones,
metrics, and guide components in A/B tests.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import chi2_contingency
from matplotlib.patches import Patch

# Define default variant colors
VARIANT_COLORS = {
    'control': '#CCCCCC',  # Light gray
    'treatment': '#4FC3F7'  # Light blue teal
}


def analyze_milestone_completion_by_time_window(milestones_df, time_windows=[5, 14, 30]):
    """
    Analyze milestone completion across different maturity windows.
    
    Parameters:
    -----------
    milestones_df : pandas.DataFrame
        DataFrame containing milestone data with first_assigned_at and event_at columns
    time_windows : list, default=[5, 14, 30]
        List of days to analyze completion within
        
    Returns:
    --------
    dict : Dictionary with filtered dataframes for each time window
    """
    results = {}
    
    for days in time_windows:
        cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=days)
        filtered_df = milestones_df[milestones_df['first_assigned_at'] <= cutoff_date]
        
        # Only count milestones completed within the time window
        filtered_df = filtered_df[
            (filtered_df['event_at'].isna()) |  # No completion yet
            (filtered_df['event_at'] <= filtered_df['first_assigned_at'] + pd.Timedelta(days=days))  # Completed within window
        ]
        
        results[f'{days}_days'] = filtered_df
        
    return results


def analyze_event_completion_generic(
    df, 
    event_prefix='milestone_',
    event_name_col='event_name',
    shop_id_col='shop_id', 
    variant_col='variant',
    assignment_col='first_assigned_at',
    event_time_col='event_at',
    min_days_old=5,
    analysis_name="Event"
):
    """
    Generic analysis function for any event completion data.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Your data (milestones, guide_interactions, or metrics)
    event_prefix : str  
        What events to analyze ('milestone_', 'component_', 'metric_', etc.)
    event_name_col : str, default='event_name'
        Column containing event names
    shop_id_col : str, default='shop_id'
        Column containing shop IDs
    variant_col : str, default='variant'
        Column containing variants (control/treatment)
    assignment_col : str, default='first_assigned_at'
        Column containing assignment timestamps
    event_time_col : str, default='event_at'
        Column containing event completion timestamps
    min_days_old : int, default=5
        Minimum days since assignment to include
    analysis_name : str, default="Event"
        Name for this analysis (for titles/labels)
        
    Returns:
    --------
    tuple : (results_df, significance_df) - DataFrames with completion rates and statistical significance
    """
    
    print(f"=== {analysis_name.upper()} COMPLETION ANALYSIS (≥{min_days_old} days old) ===")
    
    # Validate required columns
    required_cols = [event_name_col, shop_id_col, variant_col, assignment_col]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")
    
    # Filter to mature assignments
    cutoff_date = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=min_days_old)
    filtered_df = df[df[assignment_col] <= cutoff_date]
    
    print(f"Filtered from {len(df)} to {len(filtered_df)} records (≥{min_days_old} days old)")
    
    # Get events matching the prefix
    event_filter = filtered_df[event_name_col].str.startswith(event_prefix, na=False)
    event_data = filtered_df[event_filter]
    
    print(f"Found {len(event_data)} {event_prefix} events")
    
    if len(event_data) == 0:
        print(f"❌ No events found with prefix '{event_prefix}'")
        return None, None
    
    results = []
    
    # Get all unique shops by variant for denominator
    total_shops = filtered_df.groupby(variant_col)[shop_id_col].nunique()
    print(f"Total shops by variant: {total_shops.to_dict()}")
    
    # Calculate completion rates for each event
    for event in event_data[event_name_col].unique():
        event_subset = event_data[event_data[event_name_col] == event]
        
        # Count completions by variant
        completions = event_subset.groupby(variant_col)[shop_id_col].nunique()
        
        for variant in ['control', 'treatment']:
            completed = completions.get(variant, 0)
            total = total_shops.get(variant, 0)
            completion_rate = completed / total if total > 0 else 0
            
            results.append({
                'event': event,
                'variant': variant,
                'completed': completed,
                'total': total,
                'completion_rate': completion_rate,
                'sample_size': total
            })
    
    results_df = pd.DataFrame(results)
    
    if len(results_df) == 0:
        print("❌ No completion data generated")
        return None, None
    
    # Calculate statistical significance and lift
    significance_results = []
    
    for event in results_df['event'].unique():
        event_subset = results_df[results_df['event'] == event]
        
        if len(event_subset) == 2:  # Both control and treatment
            control_row = event_subset[event_subset['variant'] == 'control'].iloc[0]
            treatment_row = event_subset[event_subset['variant'] == 'treatment'].iloc[0]
            
            # Chi-square test
            contingency = [
                [treatment_row['completed'], treatment_row['total'] - treatment_row['completed']],
                [control_row['completed'], control_row['total'] - control_row['completed']]
            ]
            
            chi2, p_value = chi2_contingency(contingency)[:2]
            
            # Calculate lift
            lift = (treatment_row['completion_rate'] / control_row['completion_rate'] - 1) if control_row['completion_rate'] > 0 else np.nan
            
            significance_results.append({
                'event': event,
                'control_rate': control_row['completion_rate'],
                'treatment_rate': treatment_row['completion_rate'],
                'lift': lift,
                'p_value': p_value,
                'is_significant': p_value < 0.05,
                'control_n': control_row['total'],
                'treatment_n': treatment_row['total']
            })
    
    significance_df = pd.DataFrame(significance_results)
    
    return results_df, significance_df


def plot_event_completion_generic(results_df, significance_df, analysis_name="Event", event_label="Events", variant_colors=None):
    """
    Generic plotting function for any event completion analysis.
    
    Parameters:
    -----------
    results_df : pandas.DataFrame
        Results from analyze_event_completion_generic
    significance_df : pandas.DataFrame
        Significance results from analyze_event_completion_generic
    analysis_name : str, default="Event"
        Name for analysis titles
    event_label : str, default="Events"
        Label for events axis
    variant_colors : dict, optional
        Colors for variants. If None, uses default colors.
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure
    """
    if results_df is None or significance_df is None or len(results_df) == 0:
        print("❌ No data to plot")
        return None
    
    if variant_colors is None:
        variant_colors = VARIANT_COLORS
    
    # Merge significance data
    plot_data = results_df.merge(
        significance_df[['event', 'is_significant', 'lift']], 
        on='event', 
        how='left'
    )
    
    # Create the plot
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10))
    
    # Plot 1: Completion rates with significance indicators
    if len(significance_df) > 0:
        event_order = significance_df.sort_values('lift', ascending=False)['event'].tolist()
    else:
        event_order = plot_data['event'].unique().tolist()
    
    sns.barplot(data=plot_data, 
                x='event', 
                y='completion_rate', 
                hue='variant',
                order=event_order,
                palette=variant_colors,
                ax=ax1)
    
    # Add significance indicators
    for i, event in enumerate(event_order):
        sig_row = significance_df[significance_df['event'] == event]
        if len(sig_row) > 0 and sig_row.iloc[0]['is_significant']:
            ax1.text(i, ax1.get_ylim()[1] * 0.95, '*', horizontalalignment='center', verticalalignment='center', 
                    fontsize=16, fontweight='bold', color='red')
    
    # Add sample sizes as text
    for i, event in enumerate(event_order):
        event_data = plot_data[plot_data['event'] == event]
        for j, variant in enumerate(['control', 'treatment']):
            variant_data = event_data[event_data['variant'] == variant]
            if len(variant_data) > 0:
                n = variant_data.iloc[0]['sample_size']
                rate = variant_data.iloc[0]['completion_rate']
                ax1.text(i + (j-0.5)*0.4, rate + 0.005, f'n={n:,}', 
                        horizontalalignment='center', verticalalignment='bottom', fontsize=8, color='gray')
    
    ax1.set_title(f'{analysis_name} Completion Rates by Variant (* = p < 0.05)', fontsize=14, fontweight='bold')
    ax1.set_xlabel(event_label)
    ax1.set_ylabel('Completion Rate')
    
    # Center the x-axis labels
    ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45, ha='right')    
    ax1.legend(title='Variant')
    
    # Plot 2: Lift chart
    if len(significance_df) > 0:
        lift_data = significance_df.copy()
        lift_data['color'] = lift_data['is_significant'].map({True: '#33b07a', False: '#f67088'})
        
        bars = ax2.barh(lift_data['event'], lift_data['lift'], color=lift_data['color'])
        
        # Add value labels
        for i, (bar, lift) in enumerate(zip(bars, lift_data['lift'])):
            width = bar.get_width()
            label_x = width + 0.005 if width >= 0 else width - 0.005
            ax2.text(label_x, bar.get_y() + bar.get_height()/2, f'{lift:.1%}', 
                    horizontalalignment='left' if width >= 0 else 'right', verticalalignment='center', fontweight='bold')
        
        ax2.axvline(x=0, color='black', linestyle='-', alpha=0.3)
        ax2.set_xlabel('Lift (Treatment vs Control)')
        ax2.set_ylabel(event_label)
        ax2.set_title(f'Treatment Effect on {analysis_name} Completion')
        
        # Add legend for colors
        legend_elements = [
            Patch(facecolor='#33b07a', label='Significant (p < 0.05)'),
            Patch(facecolor='#f67088', label='Not Significant')
        ]
        ax2.legend(handles=legend_elements, loc='lower right')
    
    plt.tight_layout()
    plt.show()
    
    return fig


def run_comprehensive_analysis(df, config_name, **kwargs):
    """
    Run complete analysis with predefined configurations.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe
    config_name : str
        Configuration name ('milestones', 'guides', 'metrics', 'custom')
    **kwargs : dict
        Additional parameters to override defaults
        
    Returns:
    --------
    tuple : (results_df, significance_df, figure) - Analysis results and plot
    """
    
    # Predefined configurations for different data types
    configs = {
        'milestones': {
            'event_prefix': 'milestone_',
            'analysis_name': 'Milestone',
            'event_label': 'Milestones'
        },
        'guides': {
            'event_prefix': 'component_', 
            'analysis_name': 'Guide Component',
            'event_label': 'Components'
        },
        'metrics': {
            'event_prefix': 'metric_',
            'analysis_name': 'Metric',
            'event_label': 'Metrics'
        },
        'custom': {
            'event_prefix': kwargs.get('event_prefix', 'event_'),
            'analysis_name': kwargs.get('analysis_name', 'Custom Event'),
            'event_label': kwargs.get('event_label', 'Events')
        }
    }
    
    if config_name not in configs:
        raise ValueError(f"Config '{config_name}' not found. Available: {list(configs.keys())}")
    
    config = configs[config_name]
    config.update(kwargs)  # Allow overrides
    
    # Extract event_label for plotting but remove it from config for analyze_event_completion_generic
    event_label = config.pop('event_label')
    
    # Run analysis
    results_df, significance_df = analyze_event_completion_generic(df, **config)
    
    if results_df is not None and significance_df is not None:
        # Print summary
        print(f"\n📊 {config['analysis_name'].upper()} SUMMARY:")
        total_shops = results_df.groupby('variant')['total'].first()
        print(f"Control shops: {total_shops.get('control', 0):,}")
        print(f"Treatment shops: {total_shops.get('treatment', 0):,}")
        
        if len(significance_df) > 0:
            print(f"\n🏆 TOP PERFORMING {event_label.upper()} (by treatment lift):")
            top_events = significance_df.nlargest(5, 'lift')[
                ['event', 'control_rate', 'treatment_rate', 'lift', 'p_value', 'is_significant']
            ]
            for _, row in top_events.iterrows():
                sig_indicator = "✅" if row['is_significant'] else "❌"
                print(f"{sig_indicator} {row['event']}: {row['lift']:.1%} lift (p={row['p_value']:.3f})")
        
        # Create visualization
        fig = plot_event_completion_generic(results_df, significance_df, 
                                          config['analysis_name'], event_label)
        
        return results_df, significance_df, fig
    
    return None, None, None