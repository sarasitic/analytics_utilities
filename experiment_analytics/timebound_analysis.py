"""
Time-bound Analysis Utilities

Functions for analyzing timing patterns and time-bounded relationships in experiments.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
from typing import Dict, List, Optional, Union, Tuple, Any


def analyze_first_call_timing(
    df: pd.DataFrame,
    assignment_col: str = 'assignment_date',
    call_col: str = 'call_date',
    variant_col: str = 'variant',
    shop_id_col: str = 'shop_id',
    variants: Optional[List[str]] = None,
    max_days: int = 10
) -> Tuple[go.Figure, Dict[str, Any]]:
    """
    Analyze the timing of first support calls after assignment to experiment.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame with assignment and call data
    assignment_col : str
        Column containing assignment date/timestamp
    call_col : str
        Column containing call date/timestamp
    variant_col : str
        Column containing variant assignment
    shop_id_col : str
        Column containing shop/user ID
    variants : List[str], optional
        List of variants to analyze
    max_days : int
        Maximum number of days to analyze
        
    Returns:
    --------
    Tuple[go.Figure, Dict[str, Any]]
        Plotly figure and summary statistics
    """
    if variants is None:
        variants = ['treatment_1', 'treatment_2']
    
    # Calculate days between assignment and call
    df_copy = df.copy()
    df_copy['days_between'] = (df_copy[call_col] - df_copy[assignment_col]).dt.total_seconds() / (24 * 60 * 60)
    
    # Get first call per shop
    first_calls = df_copy.groupby(shop_id_col).agg({
        variant_col: 'first',
        'days_between': 'min',
        call_col: 'min'
    }).reset_index()
    
    # Create figure
    fig = go.Figure()
    
    # Variant colors
    variant_colors = {
        'treatment_1': '#33b07a',
        'treatment_2': '#38a8c5',
        'control': '#f67088'
    }
    
    summary_stats = {}
    
    # Add bars for each treatment variant
    for variant in variants:
        variant_data = first_calls[first_calls[variant_col] == variant]
        
        # Filter for calls within max_days
        filtered_variant = variant_data[
            (variant_data['days_between'] >= 0) & 
            (variant_data['days_between'] <= max_days)
        ]
        
        # Calculate percentage of first calls
        total_first_calls = len(variant_data[variant_data['days_between'] >= 0])
        if total_first_calls > 0:
            hist, bins = np.histogram(filtered_variant['days_between'], bins=np.arange(max_days + 2))
            pct = (hist / total_first_calls) * 100
            
            fig.add_trace(go.Bar(
                x=np.arange(max_days + 1),
                y=pct,
                name=variant,
                marker_color=variant_colors.get(variant, '#000000'),
                text=[f"{p:.1f}%<br>({h})" for p, h in zip(pct, hist)],
                textposition='inside',
                insidetextanchor='middle',
                textangle=0
            ))
            
            # Store summary stats
            summary_stats[variant] = {
                'total_calls': total_first_calls,
                'calls_within_window': len(filtered_variant),
                'mean_days': filtered_variant['days_between'].mean() if len(filtered_variant) > 0 else np.nan,
                'median_days': filtered_variant['days_between'].median() if len(filtered_variant) > 0 else np.nan
            }
    
    # Update layout
    fig.update_layout(
        title=f'Distribution of First Support Call Timing by Variant (First {max_days} Days)',
        xaxis_title='Days Since Assignment',
        yaxis_title='Percentage of First Calls',
        xaxis=dict(
            tickmode='linear',
            tick0=0,
            dtick=1,
            range=[-0.5, max_days + 0.5]
        ),
        yaxis=dict(
            tickformat='.1f',
            ticksuffix='%'
        ),
        barmode='group'
    )
    
    return fig, summary_stats


def calculate_days_to_event(
    df: pd.DataFrame,
    start_col: str,
    end_col: str,
    groupby_col: Optional[str] = None
) -> pd.DataFrame:
    """
    Calculate days between two timestamp columns, optionally grouped.
    
    Parameters:
    -----------
    df : DataFrame
        Input DataFrame
    start_col : str
        Column containing start timestamp
    end_col : str
        Column containing end timestamp
    groupby_col : str, optional
        Column to group by before calculating
        
    Returns:
    --------
    DataFrame
        DataFrame with days_to_event calculated
    """
    df_copy = df.copy()
    
    if groupby_col:
        # Calculate for first occurrence in each group
        df_copy = df_copy.sort_values([groupby_col, start_col])
        df_copy = df_copy.groupby(groupby_col).first().reset_index()
    
    # Calculate days difference
    df_copy['days_to_event'] = (
        df_copy[end_col] - df_copy[start_col]
    ).dt.total_seconds() / (24 * 60 * 60)
    
    return df_copy


def create_timing_heatmap(
    df: pd.DataFrame,
    x_col: str,
    y_col: str,
    value_col: str,
    title: str = "Timing Heatmap"
) -> go.Figure:
    """
    Create a heatmap showing timing relationships between events.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame with timing data
    x_col : str
        Column for x-axis
    y_col : str
        Column for y-axis
    value_col : str
        Column for heatmap values
    title : str
        Plot title
        
    Returns:
    --------
    go.Figure
        Plotly heatmap figure
    """
    # Pivot data for heatmap
    heatmap_data = df.pivot_table(
        index=y_col,
        columns=x_col,
        values=value_col,
        aggfunc='mean'
    )
    
    # Create heatmap
    fig = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=heatmap_data.columns,
        y=heatmap_data.index,
        colorscale='RdYlBu_r',
        showscale=True
    ))
    
    fig.update_layout(
        title=title,
        xaxis_title=x_col,
        yaxis_title=y_col
    )
    
    return fig


def analyze_event_timing_distribution(
    df: pd.DataFrame,
    time_col: str,
    event_col: str,
    group_col: Optional[str] = None,
    time_unit: str = 'hours'
) -> Dict[str, Any]:
    """
    Analyze the distribution of event timing.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame with event data
    time_col : str
        Column containing timestamp
    event_col : str
        Column containing event type/name
    group_col : str, optional
        Column to group by (e.g., variant)
    time_unit : str
        Time unit for analysis ('hours', 'days', 'weeks')
        
    Returns:
    --------
    Dict[str, Any]
        Timing distribution analysis results
    """
    df_copy = df.copy()
    df_copy[time_col] = pd.to_datetime(df_copy[time_col])
    
    # Extract time components based on unit
    if time_unit == 'hours':
        df_copy['time_component'] = df_copy[time_col].dt.hour
        x_label = 'Hour of Day'
    elif time_unit == 'days':
        df_copy['time_component'] = df_copy[time_col].dt.dayofweek
        x_label = 'Day of Week'
    elif time_unit == 'weeks':
        df_copy['time_component'] = df_copy[time_col].dt.isocalendar().week
        x_label = 'Week of Year'
    else:
        raise ValueError("time_unit must be 'hours', 'days', or 'weeks'")
    
    # Analyze distribution
    if group_col:
        distribution = df_copy.groupby([group_col, 'time_component']).size().reset_index(name='count')
        results = {
            'distribution': distribution,
            'summary': distribution.groupby(group_col)['count'].agg(['mean', 'std', 'min', 'max'])
        }
    else:
        distribution = df_copy['time_component'].value_counts().sort_index()
        results = {
            'distribution': distribution,
            'summary': {
                'mean': distribution.mean(),
                'std': distribution.std(),
                'min': distribution.min(),
                'max': distribution.max()
            }
        }
    
    results['time_unit'] = time_unit
    results['x_label'] = x_label
    
    return results


def create_event_sequence_analysis(
    df: pd.DataFrame,
    user_col: str,
    event_col: str,
    timestamp_col: str,
    max_events: int = 5
) -> Dict[str, Any]:
    """
    Analyze the sequence of events for users.
    
    Parameters:
    -----------
    df : DataFrame
        DataFrame with event data
    user_col : str
        Column containing user/shop ID
    event_col : str
        Column containing event names
    timestamp_col : str
        Column containing timestamps
    max_events : int
        Maximum number of events to track in sequence
        
    Returns:
    --------
    Dict[str, Any]
        Event sequence analysis results
    """
    df_copy = df.copy()
    df_copy[timestamp_col] = pd.to_datetime(df_copy[timestamp_col])
    
    # Sort by user and timestamp
    df_copy = df_copy.sort_values([user_col, timestamp_col])
    
    # Create sequence for each user
    sequences = []
    for user_id, group in df_copy.groupby(user_col):
        events = group[event_col].head(max_events).tolist()
        sequence = ' -> '.join(events)
        sequences.append({
            user_col: user_id,
            'sequence': sequence,
            'sequence_length': len(events),
            'first_event': events[0] if events else None,
            'last_event': events[-1] if events else None
        })
    
    sequences_df = pd.DataFrame(sequences)
    
    # Analyze patterns
    sequence_counts = sequences_df['sequence'].value_counts()
    first_event_counts = sequences_df['first_event'].value_counts()
    
    results = {
        'sequences': sequences_df,
        'top_sequences': sequence_counts.head(10),
        'first_event_distribution': first_event_counts,
        'avg_sequence_length': sequences_df['sequence_length'].mean(),
        'unique_sequences': len(sequence_counts)
    }
    
    return results