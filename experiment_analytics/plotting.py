"""
Plotting Utilities for Experiment Analysis

Functions for creating visualizations commonly used in A/B test analysis.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, Optional, List


# Default variant colors - can be customized
DEFAULT_VARIANT_COLORS = {
    'control': '#f67088',
    'treatment_1': '#33b07a',
    'treatment_2': '#38a8c5',
    'shop_id_not_collected': '#ad9c31',
    'shop_not_in_assignment': '#cc79f4'
}


def plot_daily_l2c_uplift(
    df: pd.DataFrame, 
    variant_colors: Optional[Dict[str, str]] = None,
    assignment_col: str = 'first_assigned_at',
    conversion_col: str = 'conversion_timestamp',
    variant_col: str = 'variant',
    shop_id_col: str = 'shop_id'
) -> go.Figure:
    """
    Create daily L2C uplift plot comparing treatments to control with Monday labels and weekend shading.
    
    Parameters:
    -----------
    df : pandas DataFrame
        DataFrame containing the experiment data
    variant_colors : Dict[str, str], optional
        Color mapping for variants
    assignment_col : str
        Column name for assignment timestamp
    conversion_col : str
        Column name for conversion timestamp
    variant_col : str
        Column name for variant
    shop_id_col : str
        Column name for shop ID
        
    Returns:
    --------
    plotly.graph_objects.Figure
        Interactive plot figure
    """
    if variant_colors is None:
        variant_colors = DEFAULT_VARIANT_COLORS
        
    # Create a copy and convert assigned_at to date
    plot_df = df.copy()
    plot_df['assignment_date'] = plot_df[assignment_col].dt.date
    
    # Calculate daily conversion rates for each variant
    daily_rates = plot_df.groupby(['assignment_date', variant_col]).agg(
        conversions=(conversion_col, lambda x: x.notna().sum()),
        total=(shop_id_col, 'nunique')
    ).reset_index()
    daily_rates['conversion_rate'] = daily_rates['conversions'] / daily_rates['total'] * 100
    
    # Get control rates for uplift calculation
    control_rates = daily_rates[daily_rates[variant_col] == 'control'].set_index('assignment_date')['conversion_rate']
    
    # Get date range for weekend shading
    min_date = pd.to_datetime(daily_rates['assignment_date'].min())
    max_date = pd.to_datetime(daily_rates['assignment_date'].max())
    
    # Create weekend shapes
    shapes = [
        # Baseline horizontal line
        dict(
            type='line',
            yref='y',
            y0=0,
            y1=0,
            xref='paper',
            x0=0,
            x1=1,
            line=dict(color='gray', dash='dash')
        )
    ]
    
    # Add weekend shading
    current_date = min_date
    while current_date <= max_date:
        if current_date.dayofweek == 5:  # If Saturday
            weekend_end = current_date + pd.Timedelta(days=2)  # Include Sunday
            shapes.append(
                dict(
                    type="rect",
                    xref="x",
                    yref="paper",
                    x0=current_date,
                    x1=weekend_end,
                    y0=0,
                    y1=1,
                    fillcolor="lightgray",
                    opacity=0.2,
                    layer="below",
                    line_width=0,
                )
            )
        current_date += pd.Timedelta(days=1)
    
    # Create plot
    fig = go.Figure()
    
    for variant in ['treatment_1', 'treatment_2']:
        if variant not in daily_rates[variant_col].values:
            continue
            
        variant_data = daily_rates[daily_rates[variant_col] == variant].copy()
        variant_data['uplift'] = variant_data.apply(
            lambda row: ((row['conversion_rate'] - control_rates[row['assignment_date']]) / 
                        control_rates[row['assignment_date']] * 100 
                        if row['assignment_date'] in control_rates.index else np.nan),
            axis=1
        )
        
        # Convert assignment_date to datetime for Monday filtering
        variant_data['assignment_datetime'] = pd.to_datetime(variant_data['assignment_date'])
        monday_data = variant_data[variant_data['assignment_datetime'].dt.dayofweek == 0].copy()
        
        # Add main line
        fig.add_trace(
            go.Scatter(
                x=variant_data['assignment_date'],
                y=variant_data['uplift'],
                name=variant,
                mode='lines',
                line=dict(color=variant_colors.get(variant, '#000000'))
            )
        )
        
        # Add Monday points with labels
        if len(monday_data) > 0:
            fig.add_trace(
                go.Scatter(
                    x=monday_data['assignment_date'],
                    y=monday_data['uplift'],
                    name=f'{variant} (Mondays)',
                    mode='markers+text',
                    marker=dict(color=variant_colors.get(variant, '#000000')),
                    text=monday_data['uplift'].round(1).astype(str) + '%',
                    textposition='top center',
                    showlegend=False
                )
            )
    
    # Update layout
    fig.update_layout(
        title='Daily L2C Uplift Rate by Shop Assignment',
        xaxis_title='Assignment Date',
        yaxis_title='Uplift compared to Control (%)',
        showlegend=True,
        hovermode='x unified',
        yaxis=dict(tickformat='.1f'),
        shapes=shapes,
        margin=dict(t=50, b=50, l=50, r=50)
    )
    
    return fig


def plot_assignment_distribution(
    df: pd.DataFrame, 
    slice_column: str, 
    show_proportion: bool = False,
    title: Optional[str] = None
) -> go.Figure:
    """
    Plot the distribution of assignments across each slice value.

    Parameters:
    -----------
    df : DataFrame
        Input data
    slice_column : str
        Column to slice by (e.g., 'country', 'industry', etc.)
    show_proportion : bool
        If True, show proportions instead of raw counts
    title : str, optional
        Custom title for the plot
        
    Returns:
    --------
    plotly.graph_objects.Figure
        Bar chart figure
    """
    # Count assignments
    assignment_counts = df[slice_column].value_counts(dropna=False).sort_index()
    
    if show_proportion:
        assignment_props = assignment_counts / assignment_counts.sum()
        plot_data = assignment_props
        ylabel = "Proportion of Assignments"
    else:
        plot_data = assignment_counts
        ylabel = "Number of Assignments"
    
    # Create plot
    fig = go.Figure(data=[
        go.Bar(
            x=plot_data.index.astype(str),
            y=plot_data.values,
            marker_color='skyblue'
        )
    ])
    
    if title is None:
        title = f"Distribution of Assignments Across {slice_column}"
    
    fig.update_layout(
        title=title,
        xaxis_title=slice_column,
        yaxis_title=ylabel,
        showlegend=False
    )
    
    return fig


def plot_cumulative_calls_by_variant(
    df: pd.DataFrame,
    variant_colors: Optional[Dict[str, str]] = None,
    reference_dates: Optional[Dict[str, str]] = None,
    variants: Optional[List[str]] = None
) -> go.Figure:
    """
    Create cumulative support call volume plot by variant over time.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with milestones data containing 'Support Call' events
    variant_colors : Dict[str, str], optional
        Color mapping for variants
    reference_dates : Dict[str, str], optional
        Dictionary of reference dates and labels
    variants : List[str], optional
        List of variants to include (default: ['treatment_1', 'treatment_2'])
        
    Returns:
    --------
    plotly.graph_objects.Figure
        Interactive plot figure
    """
    if variant_colors is None:
        variant_colors = DEFAULT_VARIANT_COLORS
    if variants is None:
        variants = ['treatment_1', 'treatment_2']
    if reference_dates is None:
        reference_dates = {
            '2025-02-19': '5% Ramp',
            '2025-02-27': '60% Ramp',
            '2025-03-03': '100% Ramp',
            '2025-03-24': 'Treatment 2 General Access'
        }
    
    # Create cumulative counts by variant over time
    cumulative_calls = pd.DataFrame()
    
    # Filter for Support Call events and treatment variants
    calls_data = df[
        (df['event_name'] == 'Support Call') & 
        (df['variant'].isin(variants))
    ].copy()
    
    # Create cumulative counts for each variant
    for variant in variants:
        variant_calls = calls_data[calls_data['variant'] == variant].copy()
        variant_calls = variant_calls.sort_values('event_at')
        variant_calls['cumulative_calls'] = np.arange(1, len(variant_calls) + 1)
        cumulative_calls = pd.concat([cumulative_calls, variant_calls])
    
    # Create the visualization
    fig = go.Figure()
    
    # Add a line for each variant
    for variant in variants:
        variant_data = cumulative_calls[cumulative_calls['variant'] == variant]
        
        if len(variant_data) > 0:
            fig.add_trace(go.Scatter(
                x=variant_data['event_at'],
                y=variant_data['cumulative_calls'],
                name=variant,
                mode='lines',
                line=dict(color=variant_colors.get(variant, '#000000'))
            ))
    
    # Add reference lines and annotations
    for date, label in reference_dates.items():
        fig.add_shape(
            type="line",
            x0=date,
            x1=date,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color="gray", dash="dash")
        )
        fig.add_annotation(
            x=date,
            y=1,
            yref="paper",
            text=label,
            showarrow=False,
            yshift=10,
            font=dict(size=10)
        )
    
    # Update layout
    fig.update_layout(
        title='Cumulative Support Call Volume by Variant',
        xaxis_title='Date',
        yaxis_title='Number of Calls',
        width=1000,
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        margin=dict(t=150)
    )
    
    return fig


def plot_daily_call_volume(
    df: pd.DataFrame,
    variant_colors: Optional[Dict[str, str]] = None,
    reference_dates: Optional[Dict[str, str]] = None,
    variants: Optional[List[str]] = None,
    include_weekends: bool = True
) -> go.Figure:
    """
    Create daily support call volume plot by variant.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with milestones data containing 'Support Call' events
    variant_colors : Dict[str, str], optional
        Color mapping for variants
    reference_dates : Dict[str, str], optional
        Dictionary of reference dates and labels
    variants : List[str], optional
        List of variants to include
    include_weekends : bool
        Whether to shade weekends
        
    Returns:
    --------
    plotly.graph_objects.Figure
        Interactive plot figure
    """
    if variant_colors is None:
        variant_colors = DEFAULT_VARIANT_COLORS
    if reference_dates is None:
        reference_dates = {
            '2025-02-19': '5% Ramp',
            '2025-02-27': '60% Ramp', 
            '2025-03-03': '100% Ramp',
            '2025-03-24': 'Treatment 2 General Access'
        }
    
    # Create daily counts by variant
    daily_calls = df[df['event_name'] == 'Support Call'].groupby([
        pd.Grouper(key='event_at', freq='D'),
        'variant'
    ]).size().reset_index(name='daily_calls')
    
    # Create the visualization
    fig = go.Figure()
    
    # Add bars for each variant
    if variants:
        plot_variants = variants
    else:
        plot_variants = daily_calls['variant'].unique()
        
    for variant in plot_variants:
        variant_data = daily_calls[daily_calls['variant'] == variant]
        
        fig.add_trace(go.Bar(
            x=variant_data['event_at'],
            y=variant_data['daily_calls'],
            name=variant,
            marker_color=variant_colors.get(variant, '#000000')
        ))
    
    # Add reference lines and annotations
    for date, label in reference_dates.items():
        fig.add_shape(
            type="line",
            x0=date,
            x1=date,
            y0=0,
            y1=1,
            yref="paper",
            line=dict(color="gray", dash="dash")
        )
        fig.add_annotation(
            x=date,
            y=1,
            yref="paper",
            text=label,
            showarrow=False,
            yshift=10,
            font=dict(size=10)
        )
    
    # Add weekend shading if requested
    if include_weekends and len(daily_calls) > 0:
        min_date = daily_calls['event_at'].min().date()
        max_date = daily_calls['event_at'].max().date()
        
        current_date = pd.Timestamp(min_date)
        while current_date.date() <= max_date:
            if current_date.dayofweek == 5:  # Saturday
                weekend_end = current_date + pd.Timedelta(days=2)
                fig.add_shape(
                    type="rect",
                    xref="x",
                    yref="paper",
                    x0=current_date,
                    x1=weekend_end,
                    y0=0,
                    y1=1,
                    fillcolor="lightgray",
                    opacity=0.2,
                    layer="below",
                    line_width=0,
                )
            current_date += pd.Timedelta(days=1)
    
    # Update layout
    fig.update_layout(
        title='Daily Support Call Volume by Variant',
        xaxis_title='Date',
        yaxis_title='Number of Calls',
        barmode='group',
        width=1000,
        height=600,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="center",
            x=0.5
        ),
        margin=dict(t=150)
    )
    
    return fig