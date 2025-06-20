"""
Call Volume Modeling and Forecasting Utilities

Functions for modeling and forecasting support call volumes based on experiment parameters.
"""

import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from typing import Dict, List, Optional, Tuple, Union


def get_call_distribution(
    access_days: int, 
    call_dist_current: Dict[Union[int, str], float], 
    normalize_to: float = 1.0
) -> List[float]:
    """
    Returns a list of call distribution percentages for each day up to access_days.
    If access_days > current distribution, extends the tail ('5+') proportionally.
    
    Parameters:
    -----------
    access_days : int
        Number of access days to model
    call_dist_current : Dict[Union[int, str], float]
        Current call distribution by day
    normalize_to : float
        Value to normalize distribution to
        
    Returns:
    --------
    List[float]
        Call distribution for each day
    """
    dist = []
    for day in range(access_days + 1):
        if day in call_dist_current:
            dist.append(call_dist_current[day])
        elif day == access_days and '5+' in call_dist_current:
            # Assign all remaining '5+' calls to the last day
            dist.append(call_dist_current['5+'])
        else:
            dist.append(0)
    
    # Normalize to sum to normalize_to
    total = sum(dist)
    if total == 0:
        return [0] * (access_days + 1)
    return [d / total * normalize_to for d in dist]


def model_calls_scenario(
    leads_per_week: int,
    access_days: int,
    call_dist: Dict[Union[int, str], float],
    calls_per_week: Optional[int] = None
) -> pd.DataFrame:
    """
    Returns a DataFrame with expected calls per day for the given scenario.
    
    Parameters:
    -----------
    leads_per_week : int
        Number of leads per week
    access_days : int
        Number of days each lead has access
    call_dist : Dict[Union[int, str], float]
        Call distribution by day
    calls_per_week : int, optional
        Total calls per week to scale to
        
    Returns:
    --------
    pd.DataFrame
        DataFrame with Day and Calls columns
    """
    days = list(range(access_days + 1))
    call_dist_list = get_call_distribution(access_days, call_dist)
    
    if calls_per_week:
        total_calls = calls_per_week
    else:
        # Estimate based on lead volume and call rate
        total_calls = leads_per_week * sum(call_dist_list)
    
    calls_per_day = [total_calls * p for p in call_dist_list]
    
    return pd.DataFrame({'Day': days, 'Calls': calls_per_day})


def extend_call_distribution(
    current_dist: Dict[Union[int, str], float], 
    current_access_days: int, 
    new_access_days: int
) -> List[float]:
    """
    Extends the call distribution to new_access_days by redistributing the '5+' tail
    proportionally across the new days.
    
    Parameters:
    -----------
    current_dist : Dict[Union[int, str], float]
        Current call distribution
    current_access_days : int
        Current number of access days
    new_access_days : int
        New number of access days
        
    Returns:
    --------
    List[float]
        Extended call distribution
    """
    dist = get_call_distribution(current_access_days, current_dist)
    tail = current_dist.get('5+', 0)
    extra_days = new_access_days - current_access_days
    
    if extra_days > 0:
        # Distribute the '5+' tail evenly across the extra days
        tail_per_day = tail / extra_days
        extended_dist = dist + [tail_per_day] * extra_days
    else:
        # If new_access_days <= current_access_days, just truncate
        extended_dist = dist[:new_access_days + 1]
    
    # Normalize
    total = sum(extended_dist)
    return [d / total for d in extended_dist] if total > 0 else extended_dist


def model_extended_calls(
    leads_per_week: int,
    current_dist: Dict[Union[int, str], float],
    current_access_days: int,
    new_access_days: int,
    calls_per_week: int
) -> pd.DataFrame:
    """
    Models expected calls per day if all leads have access for new_access_days.
    Redistributes the '5+' tail across the new days.
    
    Parameters:
    -----------
    leads_per_week : int
        Number of leads per week
    current_dist : Dict[Union[int, str], float]
        Current call distribution
    current_access_days : int
        Current access days
    new_access_days : int
        New access days
    calls_per_week : int
        Total calls per week
        
    Returns:
    --------
    pd.DataFrame
        DataFrame with extended call projections
    """
    days = list(range(new_access_days + 1))
    extended_dist = extend_call_distribution(current_dist, current_access_days, new_access_days)
    calls_per_day = [calls_per_week * p for p in extended_dist]
    
    return pd.DataFrame({'Day': days, 'Calls': calls_per_day})


def get_cumulative_call_probability(
    call_dist: Dict[Union[int, str], float], 
    access_days: int
) -> float:
    """
    Returns the probability that a lead calls within their access window (access_days).
    
    Parameters:
    -----------
    call_dist : Dict[Union[int, str], float]
        Call distribution by day
    access_days : int
        Number of access days
        
    Returns:
    --------
    float
        Cumulative probability of calling
    """
    prob = 0
    for day in range(access_days):
        prob += call_dist.get(day, 0)
    
    # If access_days > max(call_dist), add '5+' if present
    if access_days > max([d for d in call_dist if isinstance(d, int)]):
        prob += call_dist.get('5+', 0)
    
    return prob


def estimate_overall_lead_call_probability(
    call_dist: Dict[Union[int, str], float],
    access_loss_dist: Dict[int, float]
) -> float:
    """
    Estimates the overall probability that a lead calls, given the current access loss distribution.
    
    Parameters:
    -----------
    call_dist : Dict[Union[int, str], float]
        Call distribution by day
    access_loss_dist : Dict[int, float]
        Distribution of access loss by day
        
    Returns:
    --------
    float
        Overall call probability
    """
    total_prob = 0
    for days, fraction in access_loss_dist.items():
        # days: number of days the group has access
        access_window = days + 1
        prob = get_cumulative_call_probability(call_dist, access_window)
        total_prob += prob * fraction
    
    return total_prob


def predict_calls_per_week(
    leads_per_week: int,
    call_dist: Dict[Union[int, str], float],
    access_days: int
) -> float:
    """
    Predicts the number of calls per week if all leads have access for access_days.
    
    Parameters:
    -----------
    leads_per_week : int
        Number of leads per week
    call_dist : Dict[Union[int, str], float]
        Call distribution by day
    access_days : int
        Number of access days
        
    Returns:
    --------
    float
        Predicted calls per week
    """
    prob = get_cumulative_call_probability(call_dist, access_days)
    return leads_per_week * prob


def create_call_forecast_comparison(
    leads_per_week: int,
    current_access_days: int,
    extended_access_days: int,
    call_dist_current: Dict[Union[int, str], float],
    calls_per_week_current: int,
    access_loss_dist: Optional[Dict[int, float]] = None
) -> Tuple[go.Figure, Dict[str, float]]:
    """
    Create a comparison plot of current vs extended call forecasts.
    
    Parameters:
    -----------
    leads_per_week : int
        Number of leads per week
    current_access_days : int
        Current access days
    extended_access_days : int
        Extended access days
    call_dist_current : Dict[Union[int, str], float]
        Current call distribution
    calls_per_week_current : int
        Current calls per week
    access_loss_dist : Dict[int, float], optional
        Access loss distribution
        
    Returns:
    --------
    Tuple[go.Figure, Dict[str, float]]
        Plotly figure and summary statistics
    """
    # Model current scenario
    df_current = model_calls_scenario(
        leads_per_week=leads_per_week,
        access_days=current_access_days,
        call_dist=call_dist_current,
        calls_per_week=calls_per_week_current
    )
    
    # Calculate current effective call probability if access_loss_dist provided
    if access_loss_dist:
        current_effective_prob = estimate_overall_lead_call_probability(
            call_dist_current, access_loss_dist
        )
        adjustment_factor = 1.0 / current_effective_prob if current_effective_prob > 0 else 0
        expected_calls_extended = calls_per_week_current * adjustment_factor
    else:
        expected_calls_extended = predict_calls_per_week(
            leads_per_week, call_dist_current, extended_access_days
        )
    
    # Model extended scenario
    df_extended = model_extended_calls(
        leads_per_week=leads_per_week,
        current_dist=call_dist_current,
        current_access_days=current_access_days,
        new_access_days=extended_access_days,
        calls_per_week=expected_calls_extended
    )
    
    # Create visualization
    fig = go.Figure()
    
    fig.add_trace(go.Bar(
        x=df_current['Day'].astype(str),
        y=df_current['Calls'],
        name=f'Current ({current_access_days} days)',
        marker_color='skyblue',
        opacity=0.85
    ))
    
    fig.add_trace(go.Bar(
        x=df_extended['Day'].astype(str),
        y=df_extended['Calls'],
        name=f'Extended ({extended_access_days} days)',
        marker_color='salmon',
        opacity=0.7
    ))
    
    fig.update_layout(
        barmode='group',
        title='Predicted Calls per Day: Current vs. Extended Access',
        xaxis_title='Day of Access',
        yaxis_title='Calls per Day',
        legend=dict(x=0.01, y=0.99),
        bargap=0.2
    )
    
    # Calculate summary statistics
    summary = {
        'current_total': df_current['Calls'].sum(),
        'extended_total': df_extended['Calls'].sum(),
        'increase': df_extended['Calls'].sum() - df_current['Calls'].sum(),
        'percent_increase': ((df_extended['Calls'].sum() / df_current['Calls'].sum()) - 1) * 100
    }
    
    return fig, summary


def model_cohort_based_calls(
    leads_per_day: int,
    days_in_week: int,
    access_days: int,
    call_dist: List[float]
) -> Dict[str, Union[float, List[float]]]:
    """
    Model calls per day considering overlapping cohorts of leads.
    
    Parameters:
    -----------
    leads_per_day : int
        Number of new leads per day
    days_in_week : int
        Number of days to model
    access_days : int
        Number of days each lead has access
    call_dist : List[float]
        Call distribution by day (normalized)
        
    Returns:
    --------
    Dict[str, Union[float, List[float]]]
        Dictionary with total calls and daily breakdown
    """
    # Normalize call_dist to sum to 1
    call_dist = np.array(call_dist)
    call_dist = call_dist / call_dist.sum()
    
    # For each day, sum the expected calls from all active cohorts
    calls_per_day = []
    for day in range(days_in_week):
        calls_today = 0
        for offset in range(min(day + 1, access_days)):
            # For each cohort that is still active
            cohort_leads = leads_per_day
            call_prob = call_dist[offset] if offset < len(call_dist) else 0
            calls_today += cohort_leads * call_prob
        calls_per_day.append(calls_today)
    
    return {
        'total_calls': sum(calls_per_day),
        'calls_per_day': calls_per_day,
        'average_daily': np.mean(calls_per_day)
    }