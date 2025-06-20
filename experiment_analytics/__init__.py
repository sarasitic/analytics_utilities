"""
Experiment Analysis Toolkit

A comprehensive, consolidated toolkit for A/B test analysis. 
Combines statistical analysis, slice analysis, time-bound analysis, plotting, query loading, and forecasting.
"""

# Core statistical analysis
from .statistical_analysis import (
    calculate_significance,
    calculate_significance_raw,
    safe_calculate_significance
)

# Query loading and data preparation
from .query_loader import (
    load_queries_from_directory,
    execute_queries_batch,
    prepare_milestone_data,
    prepare_csr_data,
    prepare_metric_data,
    auto_process_dataframes
)

# Interactive plotting and visualizations
from .plotting import (
    plot_daily_l2c_uplift,
    plot_assignment_distribution,
    plot_cumulative_calls_by_variant,
    plot_daily_call_volume,
    DEFAULT_VARIANT_COLORS
)

# Slice and distribution analysis
from .slice_analysis import (
    analyze_slice_distribution,
    run_time_bound_analysis,
    analyze_conversion_timing
)

# Time-bound and timing analysis
from .timebound_analysis import (
    analyze_first_call_timing,
    calculate_days_to_event,
    create_timing_heatmap,
    analyze_event_timing_distribution,
    create_event_sequence_analysis
)

# Forecasting and modeling
from .forecasting import (
    get_call_distribution,
    model_calls_scenario,
    extend_call_distribution,
    model_extended_calls,
    get_cumulative_call_probability,
    estimate_overall_lead_call_probability,
    predict_calls_per_week,
    create_call_forecast_comparison,
    model_cohort_based_calls
)

__version__ = "0.1.0"
__all__ = [
    # Statistical analysis functions
    "calculate_significance",
    "calculate_significance_raw", 
    "safe_calculate_significance",
    
    # Query loader functions
    "load_queries_from_directory",
    "execute_queries_batch", 
    "prepare_milestone_data",
    "prepare_csr_data",
    "prepare_metric_data",
    "auto_process_dataframes",
    
    # Plotting functions
    "plot_daily_l2c_uplift",
    "plot_assignment_distribution",
    "plot_cumulative_calls_by_variant",
    "plot_daily_call_volume",
    "DEFAULT_VARIANT_COLORS",
    
    # Slice analysis functions
    "analyze_slice_distribution",
    "run_time_bound_analysis",
    "analyze_conversion_timing",
    
    # Time-bound analysis functions
    "analyze_first_call_timing",
    "calculate_days_to_event",
    "create_timing_heatmap",
    "analyze_event_timing_distribution", 
    "create_event_sequence_analysis",
    
    # Forecasting functions
    "get_call_distribution",
    "model_calls_scenario",
    "extend_call_distribution",
    "model_extended_calls",
    "get_cumulative_call_probability",
    "estimate_overall_lead_call_probability",
    "predict_calls_per_week",
    "create_call_forecast_comparison",
    "model_cohort_based_calls"
]