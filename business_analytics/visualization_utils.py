"""
Visualization Utilities for Business Impact Analysis

This module provides enhanced visualization functions for displaying 
uplift comparisons, profitability analysis, and business impact results.
"""

import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import seaborn as sns
from typing import Dict, List, Optional, Tuple, Any


def get_color_mapping(
    keys: List[str],
    palette: Optional[List[str]] = None
) -> Dict[str, str]:
    """
    Returns a dict mapping each key to a color from the palette.
    The mapping is consistent for the same set/order of keys.
    
    Parameters:
    -----------
    keys : list
        List of values to map to colors
    palette : list, optional
        Color palette to use. If None, uses Plotly qualitative colors.
        
    Returns:
    --------
    dict : Dictionary mapping each key to a color
    """
    if palette is None:
        palette = px.colors.qualitative.Set1 + px.colors.qualitative.Pastel
    
    # Sort keys for consistent mapping
    keys = sorted(keys)
    return {key: palette[i % len(palette)] for i, key in enumerate(keys)}


def plot_extracted_uplifts(
    uplift_dict: Dict[str, float],
    title: str = "Uplift by Slice",
    figsize: Tuple[int, int] = (10, 6),
    show_plot: bool = True
) -> plt.Figure:
    """
    Plot numeric uplift values with proper percentage formatting.
    
    Parameters:
    -----------
    uplift_dict : dict
        Dictionary with slice names as keys and numeric uplift values (decimals)
    title : str, default="Uplift by Slice"
        Plot title
    figsize : tuple, default=(10, 6)
        Figure size
    show_plot : bool, default=True
        Whether to display the plot
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure
    """
    
    if not uplift_dict:
        print("No uplift data to plot")
        return None
    
    # Sort by uplift value for better visualization
    sorted_items = sorted(uplift_dict.items(), key=lambda x: x[1], reverse=True)
    slice_names, uplift_values = zip(*sorted_items)
    
    # Create plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Color bars based on positive/negative uplift
    colors = ['#33b07a' if val >= 0 else '#f67088' for val in uplift_values]
    
    bars = ax.bar(slice_names, uplift_values, color=colors, alpha=0.7, edgecolor='black')
    
    # Format y-axis as percentages (the values are already in decimal format)
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.1%}'.format(y)))
    
    # Add value labels on bars
    for bar, value in zip(bars, uplift_values):
        height = bar.get_height()
        label_y = height + max(uplift_values) * 0.02 if height >= 0 else height - max(uplift_values) * 0.02
        ax.annotate(f'{value:.2%}', 
                   (bar.get_x() + bar.get_width()/2, label_y),
                   ha='center', va='bottom' if height >= 0 else 'top',
                   fontweight='bold')
    
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_ylabel('Uplift (%)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Slice', fontsize=12, fontweight='bold')
    
    # Add horizontal line at 0
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # Rotate x-axis labels if needed
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    if show_plot:
        plt.show()
    
    # Print summary
    print(f"\\nUplift Summary:")
    print(f"Best: {slice_names[0]} ({uplift_values[0]:.2%})")
    print(f"Worst: {slice_names[-1]} ({uplift_values[-1]:.2%})")
    print(f"Average: {np.mean(uplift_values):.2%}")
    
    return fig


def plot_extracted_uplifts_with_totals(
    slice_data_dict: Dict[str, Dict[str, float]],
    title: str = "Uplift by Slice",
    figsize: Tuple[int, int] = (12, 8),
    show_plot: bool = True
) -> plt.Figure:
    """
    Plot numeric uplift values with sample size labels.
    
    Parameters:
    -----------
    slice_data_dict : dict
        Dictionary with slice names as keys and dict of {uplift, total} as values
    title : str, default="Uplift by Slice"
        Plot title
    figsize : tuple, default=(12, 8)
        Figure size
    show_plot : bool, default=True
        Whether to display the plot
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure
    """
    
    if not slice_data_dict:
        print("No uplift data to plot")
        return None
    
    # Extract data and sort by uplift value
    sorted_items = sorted(slice_data_dict.items(), key=lambda x: x[1]['uplift'], reverse=True)
    slice_names = [item[0] for item in sorted_items]
    uplift_values = [item[1]['uplift'] for item in sorted_items]
    total_values = [item[1]['total'] for item in sorted_items]
    
    # Create plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Color bars based on positive/negative uplift
    colors = ['#33b07a' if val >= 0 else '#f67088' for val in uplift_values]
    
    bars = ax.bar(slice_names, uplift_values, color=colors, alpha=0.7, edgecolor='black')
    
    # Format y-axis as percentages
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: '{:.1%}'.format(y)))
    
    # Add value labels on bars with sample sizes
    for bar, uplift, total in zip(bars, uplift_values, total_values):
        height = bar.get_height()
        
        # Position for uplift label (main label)
        uplift_label_y = height + max(uplift_values) * 0.03 if height >= 0 else height - max(uplift_values) * 0.03
        
        # Uplift percentage label
        ax.annotate(f'{uplift:.2%}', 
                   (bar.get_x() + bar.get_width()/2, uplift_label_y),
                   ha='center', va='bottom' if height >= 0 else 'top',
                   fontweight='bold', fontsize=11)
        
        # Sample size label inside the bar
        if height >= 0:
            # For positive bars, place label near the bottom
            total_label_y = height * 0.2
        else:
            # For negative bars, place label near the top
            total_label_y = height * 0.8
            
        ax.annotate(f'n={total:,}', 
                   (bar.get_x() + bar.get_width()/2, total_label_y),
                   ha='center', va='center',
                   fontsize=9, style='italic', color='white')
    
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_ylabel('Uplift (%)', fontsize=12, fontweight='bold')
    ax.set_xlabel('Slice', fontsize=12, fontweight='bold')
    
    # Add horizontal line at 0
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # Rotate x-axis labels if needed
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    if show_plot:
        plt.show()
    
    # Print summary
    print(f"\\nUplift Summary:")
    print(f"Best: {slice_names[0]} ({uplift_values[0]:.2%}, n={total_values[0]:,})")
    print(f"Worst: {slice_names[-1]} ({uplift_values[-1]:.2%}, n={total_values[-1]:,})")
    print(f"Average uplift: {np.mean(uplift_values):.2%}")
    print(f"Total sample size: {sum(total_values):,}")
    
    return fig


def plot_slice_uplift_comparison(
    results: Dict[str, Any],
    time_bound: str = '5_days',
    title: Optional[str] = None,
    figsize: Tuple[int, int] = (12, 8),
    show_plot: bool = True
) -> plt.Figure:
    """
    Create bar chart comparing uplift across different slice values.
    
    Parameters:
    -----------
    results : dict
        Results dictionary from run_time_bound_analysis
    time_bound : str, default='5_days'
        Which time bound to extract from
    title : str, optional
        Plot title. If None, auto-generates title.
    figsize : tuple, default=(12, 8)
        Figure size
    show_plot : bool, default=True
        Whether to display the plot
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure
    """
    
    # Extract uplift data from results
    uplift_data = {}
    
    for slice_name, slice_data in results.items():
        if time_bound not in slice_data or slice_data[time_bound] is None:
            continue
            
        result_df = slice_data[time_bound]
        if result_df is None or len(result_df) == 0:
            continue
            
        try:
            # Find treatment rows (non-control)
            treatment_rows = result_df[result_df['variant'] != 'control']
            
            if len(treatment_rows) > 0:
                # Take the first treatment if multiple exist
                treatment_row = treatment_rows.iloc[0]
                
                # Extract uplift string (e.g., "+1.20%")
                uplift_str = treatment_row['uplift']
                # Convert percentage string to decimal
                uplift_numeric = float(uplift_str.strip('%').strip('+')) / 100
                
                uplift_data[slice_name] = uplift_numeric
                
        except Exception as e:
            print(f"Warning: Could not extract uplift for {slice_name}: {e}")
            continue
    
    if not uplift_data:
        print("No uplift data found to plot")
        return None
    
    # Use the standard uplift plotting function
    if title is None:
        title = f"Uplift Comparison by Slice ({time_bound.replace('_', ' ').title()})"
    
    return plot_extracted_uplifts(
        uplift_data, 
        title=title, 
        figsize=figsize, 
        show_plot=show_plot
    )


def plot_financial_impact_comparison(
    segment_impacts: List[Dict[str, Any]],
    metric_type: str = 'annual_impact',
    title: Optional[str] = None,
    figsize: Tuple[int, int] = (12, 6),
    show_plot: bool = True
) -> plt.Figure:
    """
    Create a focused comparison plot for financial impact metrics.
    
    Parameters:
    -----------
    segment_impacts : list
        List of segment impact dictionaries
    metric_type : str, default='annual_impact'
        Which metric to plot ('annual_impact', 'additional_gas', 'impact_per_user')
    title : str, optional
        Plot title. If None, auto-generates.
    figsize : tuple, default=(12, 6)
        Figure size
    show_plot : bool, default=True
        Whether to display the plot
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure
    """
    
    if not segment_impacts:
        print("No segment impact data to visualize")
        return None
    
    # Extract data
    segments = [r['segment'] for r in segment_impacts]
    
    if metric_type == 'annual_impact':
        values = [r['annual_impact'] for r in segment_impacts]
        ylabel = 'Annual Revenue Impact ($)'
        format_func = lambda x: f'${x:,.0f}'
        default_title = 'Annual Revenue Impact by Segment'
    elif metric_type == 'additional_gas':
        values = [r['additional_gas'] for r in segment_impacts]
        ylabel = 'Additional Gross Adds'
        format_func = lambda x: f'{x:,.0f}'
        default_title = 'Additional Gross Adds by Segment'
    elif metric_type == 'impact_per_user':
        values = [r['annual_impact'] / r['users'] if r['users'] > 0 else 0 for r in segment_impacts]
        ylabel = 'Impact per User ($)'
        format_func = lambda x: f'${x:.2f}'
        default_title = 'Revenue Impact per User by Segment'
    else:
        raise ValueError(f"Unknown metric_type: {metric_type}")
    
    if title is None:
        title = default_title
    
    # Sort by value
    sorted_data = sorted(zip(segments, values), key=lambda x: x[1], reverse=True)
    segments, values = zip(*sorted_data)
    
    # Create plot
    fig, ax = plt.subplots(figsize=figsize)
    
    # Color bars based on positive/negative values
    colors = ['#33b07a' if val >= 0 else '#f67088' for val in values]
    
    bars = ax.bar(segments, values, color=colors, alpha=0.7, edgecolor='black')
    
    # Add value labels on bars
    for bar, value in zip(bars, values):
        height = bar.get_height()
        label_y = height + max(values) * 0.02 if height >= 0 else height - max(values) * 0.02
        ax.annotate(format_func(value), 
                   (bar.get_x() + bar.get_width()/2, label_y),
                   ha='center', va='bottom' if height >= 0 else 'top',
                   fontweight='bold')
    
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_ylabel(ylabel, fontsize=12, fontweight='bold')
    ax.set_xlabel('Growth Segment', fontsize=12, fontweight='bold')
    
    # Add horizontal line at 0
    ax.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    
    # Format y-axis
    if metric_type == 'annual_impact':
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:,.0f}'))
    elif metric_type == 'additional_gas':
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'{y:,.0f}'))
    elif metric_type == 'impact_per_user':
        ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:.2f}'))
    
    # Rotate x-axis labels if needed
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()
    
    if show_plot:
        plt.show()
    
    return fig


def create_executive_summary_plot(
    segment_impacts: List[Dict[str, Any]],
    total_annual_impact: float,
    figsize: Tuple[int, int] = (14, 10),
    show_plot: bool = True
) -> plt.Figure:
    """
    Create an executive summary visualization combining key business metrics.
    
    Parameters:
    -----------
    segment_impacts : list
        List of segment impact dictionaries
    total_annual_impact : float
        Total annual financial impact
    figsize : tuple, default=(14, 10)
        Figure size
    show_plot : bool, default=True
        Whether to display the plot
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure
    """
    
    if not segment_impacts:
        print("No segment impact data to visualize")
        return None
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=figsize)
    
    # Extract data
    segments = [r['segment'] for r in segment_impacts]
    annual_impacts = [r['annual_impact'] for r in segment_impacts]
    ga_lifts = [r['ga_lift'] * 100 for r in segment_impacts]
    user_counts = [r['users'] for r in segment_impacts]
    additional_gas = [r['additional_gas'] for r in segment_impacts]
    
    # 1. Total Impact Overview (Pie chart of contribution)
    positive_impacts = [max(0, x) for x in annual_impacts]
    if sum(positive_impacts) > 0:
        ax1.pie(positive_impacts, labels=segments, autopct='%1.1f%%', startangle=90)
        ax1.set_title('Revenue Contribution by Segment\\n(Positive Impact Only)', fontweight='bold')
    else:
        ax1.text(0.5, 0.5, 'No Positive\\nImpact Segments', ha='center', va='center', transform=ax1.transAxes)
        ax1.set_title('Revenue Contribution by Segment', fontweight='bold')
    
    # 2. Uplift by Segment (Bar chart)
    colors = ['#33b07a' if x > 0 else '#f67088' for x in ga_lifts]
    bars = ax2.bar(segments, ga_lifts, color=colors, alpha=0.7)
    ax2.set_title('Conversion Uplift by Segment', fontweight='bold')
    ax2.set_ylabel('Uplift (%)')
    ax2.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax2.tick_params(axis='x', rotation=45)
    
    # Add value labels
    for bar, value in zip(bars, ga_lifts):
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2, height + 0.1 if height >= 0 else height - 0.1,
                f'{value:+.1f}%', ha='center', va='bottom' if height >= 0 else 'top', fontsize=9)
    
    # 3. Financial Impact (Waterfall-style)
    cumulative = np.cumsum([0] + annual_impacts)
    for i, (segment, impact) in enumerate(zip(segments, annual_impacts)):
        color = '#33b07a' if impact > 0 else '#f67088'
        ax3.bar(i, impact, bottom=cumulative[i], color=color, alpha=0.7, edgecolor='black')
        
        # Add impact labels
        ax3.text(i, cumulative[i] + impact/2, f'${impact:,.0f}', 
                ha='center', va='center', fontsize=9, fontweight='bold')
    
    ax3.set_title('Cumulative Financial Impact', fontweight='bold')
    ax3.set_ylabel('Annual Revenue Impact ($)')
    ax3.set_xticks(range(len(segments)))
    ax3.set_xticklabels(segments, rotation=45)
    ax3.axhline(y=0, color='black', linestyle='-', alpha=0.3)
    ax3.yaxis.set_major_formatter(plt.FuncFormatter(lambda y, _: f'${y:,.0f}'))
    
    # Add total line
    ax3.axhline(y=total_annual_impact, color='red', linestyle='--', alpha=0.7)
    ax3.text(len(segments)/2, total_annual_impact + max(annual_impacts)*0.1, 
            f'Total: ${total_annual_impact:,.0f}', ha='center', fontweight='bold', color='red')
    
    # 4. Key Metrics Summary (Text summary)
    ax4.axis('off')
    
    # Calculate summary metrics
    total_users = sum(user_counts)
    total_additional_gas = sum(additional_gas)
    avg_uplift = np.mean(ga_lifts)
    positive_segments = len([x for x in annual_impacts if x > 0])
    negative_segments = len([x for x in annual_impacts if x < 0])
    
    summary_text = f"""
    📊 EXECUTIVE SUMMARY
    
    Total Annual Impact: ${total_annual_impact:,.0f}
    
    📈 Key Metrics:
    • Total Users: {total_users:,}
    • Additional Gross Adds: {total_additional_gas:,.0f}
    • Average Uplift: {avg_uplift:+.1f}%
    
    🎯 Segment Breakdown:
    • Positive Impact: {positive_segments} segments
    • Negative Impact: {negative_segments} segments
    
    💡 Recommendation:
    {"✅ PROCEED with rollout" if total_annual_impact > 0 else "⚠️ REVIEW before rollout"}
    """
    
    ax4.text(0.05, 0.95, summary_text, transform=ax4.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.3))
    
    plt.suptitle('Business Impact Analysis - Executive Summary', fontsize=16, fontweight='bold')
    plt.tight_layout()
    
    if show_plot:
        plt.show()
    
    return fig