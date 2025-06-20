"""
MCP Integration Module for Analytics Utilities

This module provides easy integration for Shopify MCP to use analytics utilities
from another directory. It handles path management and provides convenient
import functions for all analytics modules.

Usage in MCP:
    # Add this at the top of your MCP files
    import sys
    sys.path.append('/Users/saramcneilly/src/github.com/analytics_utilities')
    from mcp_integration import *
    
    # Now you can use all analytics functions directly
    results = calculate_significance(df)
    profitability = calculate_business_impact(segment_results)
"""

import sys
import os
from pathlib import Path

# Add the analytics utilities to Python path
ANALYTICS_UTILITIES_PATH = Path(__file__).parent
if str(ANALYTICS_UTILITIES_PATH) not in sys.path:
    sys.path.append(str(ANALYTICS_UTILITIES_PATH))

# Add the src directory to path
SRC_PATH = ANALYTICS_UTILITIES_PATH / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

try:
    # Import all major functions for easy access
    
    # Experiment Analysis
    from experiment_analysis.statistical_analysis import (
        calculate_significance,
        calculate_significance_raw,
        safe_calculate_significance
    )
    
    from experiment_analysis.query_loader import (
        load_queries_from_directory,
        execute_queries_batch,
        prepare_milestone_data,
        prepare_csr_data,
        prepare_metric_data,
        auto_process_dataframes
    )
    
    from experiment_analysis.plotting import (
        plot_daily_l2c_uplift,
        plot_assignment_distribution,
        plot_cumulative_calls_by_variant,
        plot_daily_call_volume
    )
    
    from experiment_analysis.slice_analysis import (
        analyze_slice_distribution,
        run_time_bound_analysis,
        analyze_conversion_timing
    )
    
    from experiment_analysis.timebound_analysis import (
        analyze_first_call_timing,
        calculate_days_to_event,
        create_timing_heatmap,
        analyze_event_timing_distribution,
        create_event_sequence_analysis
    )
    
    from experiment_analysis.forecasting import (
        get_call_distribution,
        model_calls_scenario,
        predict_calls_per_week,
        create_call_forecast_comparison
    )
    
    # Engagement Analysis
    from engagement_analysis.milestone_analysis import (
        analyze_event_completion_generic,
        plot_event_completion_generic,
        run_comprehensive_analysis,
        analyze_milestone_completion_by_time_window
    )
    
    from engagement_analysis.guide_analysis import (
        analyze_milestone_completion,
        analyze_guidance_completion,
        analyze_component_milestone_correlations_by_variant,
        analyze_metric_completion
    )
    
    from engagement_analysis.journey_analysis import (
        segment_users_for_log_analysis,
        analyze_segment_migration,
        test_ordinal_segment_trend,
        create_position_distribution_for_variant,
        analyze_all_tasks_conversion,
        analyze_segments_with_log
    )
    
    # Business Impact Analysis
    from business_impact.profitability_analysis import (
        calculate_profitability_from_timebound_results,
        visualize_segment_profitability,
        get_ltv_mapping,
        calculate_segment_financial_impact
    )
    
    from business_impact.data_quality import (
        clean_df_for_analysis,
        validate_experiment_data,
        print_validation_summary,
        suggest_cleaning_actions
    )
    
    from business_impact.visualization_utils import (
        plot_extracted_uplifts,
        plot_extracted_uplifts_with_totals,
        plot_slice_uplift_comparison,
        get_color_mapping,
        create_executive_summary_plot
    )
    
    from business_impact.data_extraction import (
        extract_uplift_and_totals,
        extract_financial_metrics,
        prepare_business_summary,
        format_business_summary,
        create_data_export_package
    )
    
    # Session Analysis (if available)
    try:
        from session_analysis.sessions_analysis_toolkit import SessionsAnalyzer
        from session_analysis.sessions_plotting_toolkit import SessionsPlotter
        SESSION_ANALYSIS_AVAILABLE = True
    except ImportError:
        SESSION_ANALYSIS_AVAILABLE = False
    
    IMPORT_SUCCESS = True
    IMPORT_ERRORS = []

except ImportError as e:
    IMPORT_SUCCESS = False
    IMPORT_ERRORS = [str(e)]
    print(f"Warning: Some analytics utilities could not be imported: {e}")


# Convenience functions for common workflows
def run_complete_experiment_analysis(
    df,
    slice_column=None,
    time_bounds=[None, 5, 15, 30],
    conversion_timestamp_col='conversion_timestamp',
    variant_col='variant',
    control_label='control'
):
    """
    Run a complete experiment analysis workflow.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Experiment data
    slice_column : str, optional
        Column to slice analysis by
    time_bounds : list
        Time bounds for analysis
    conversion_timestamp_col : str
        Column containing conversion timestamps
    variant_col : str
        Column containing variant assignments
    control_label : str
        Label for control group
        
    Returns:
    --------
    dict : Complete analysis results
    """
    
    results = {}
    
    # Data quality check
    if 'validate_experiment_data' in globals():
        validation = validate_experiment_data(df, variant_column=variant_col)
        results['data_validation'] = validation
        
        if not validation['is_valid']:
            print("⚠️ Data validation failed. Consider cleaning data first.")
            return results
    
    # Statistical significance
    if 'calculate_significance' in globals():
        significance = calculate_significance(
            df, 
            variant_col=variant_col, 
            control_label=control_label
        )
        results['significance'] = significance
    
    # Time-bound analysis
    if 'run_time_bound_analysis' in globals():
        timebound = run_time_bound_analysis(
            df,
            slice_column=slice_column,
            time_bounds=time_bounds,
            conversion_timestamp_col=conversion_timestamp_col
        )
        results['timebound_analysis'] = timebound
    
    # Daily uplift plot
    if 'plot_daily_l2c_uplift' in globals():
        uplift_fig = plot_daily_l2c_uplift(df, slice_column=slice_column)
        results['uplift_plot'] = uplift_fig
    
    return results


def run_complete_business_analysis(
    timebound_results,
    time_bound='5_days',
    ltv_mapping=None,
    create_visualizations=True
):
    """
    Run a complete business impact analysis workflow.
    
    Parameters:
    -----------
    timebound_results : dict
        Results from run_time_bound_analysis
    time_bound : str
        Time bound to use for calculations
    ltv_mapping : dict, optional
        Custom LTV mapping
    create_visualizations : bool
        Whether to create visualizations
        
    Returns:
    --------
    dict : Complete business analysis results
    """
    
    results = {}
    
    # Calculate profitability
    if 'calculate_profitability_from_timebound_results' in globals():
        segment_results, total_impact = calculate_profitability_from_timebound_results(
            timebound_results,
            time_bound=time_bound,
            ltv_mapping=ltv_mapping
        )
        results['segment_results'] = segment_results
        results['total_impact'] = total_impact
        
        # Business summary
        if 'prepare_business_summary' in globals():
            business_summary = prepare_business_summary(segment_results, total_impact)
            results['business_summary'] = business_summary
            
            # Formatted summary
            if 'format_business_summary' in globals():
                formatted_summary = format_business_summary(business_summary)
                results['executive_summary'] = formatted_summary
                print(formatted_summary)
        
        # Create visualizations
        if create_visualizations:
            if 'visualize_segment_profitability' in globals():
                profitability_fig = visualize_segment_profitability(segment_results)
                results['profitability_plot'] = profitability_fig
            
            if 'create_executive_summary_plot' in globals():
                executive_fig = create_executive_summary_plot(segment_results, total_impact)
                results['executive_plot'] = executive_fig
    
    return results


def run_complete_engagement_analysis(
    df,
    config_name='milestones',
    event_prefix=None,
    min_days_old=5,
    analysis_name=None
):
    """
    Run a complete engagement analysis workflow.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Engagement data (milestones, guides, etc.)
    config_name : str
        Configuration name ('milestones', 'guides', 'metrics', 'custom')
    event_prefix : str, optional
        Event prefix for custom analysis
    min_days_old : int
        Minimum days since assignment
    analysis_name : str, optional
        Custom analysis name
        
    Returns:
    --------
    dict : Complete engagement analysis results
    """
    
    results = {}
    
    # Run comprehensive analysis
    if 'run_comprehensive_analysis' in globals():
        kwargs = {'min_days_old': min_days_old}
        if event_prefix:
            kwargs['event_prefix'] = event_prefix
        if analysis_name:
            kwargs['analysis_name'] = analysis_name
            
        engagement_results, significance_df, fig = run_comprehensive_analysis(
            df, 
            config_name, 
            **kwargs
        )
        
        results['engagement_results'] = engagement_results
        results['significance'] = significance_df
        results['plot'] = fig
    
    # User segmentation analysis
    if 'segment_users_for_log_analysis' in globals() and 'analyze_segment_migration' in globals():
        # Assuming there's a metric column for segmentation
        metric_columns = [col for col in df.columns if 'count' in col.lower() or 'score' in col.lower()]
        if metric_columns:
            metric_col = metric_columns[0]
            
            # Segment users
            df_segmented = segment_users_for_log_analysis(df, metric_col)
            
            # Migration analysis
            migration_results = analyze_segment_migration(df_segmented)
            results['migration_analysis'] = migration_results
    
    return results


# Utility functions for MCP integration
def check_imports():
    """Check which modules were successfully imported."""
    
    print("=== ANALYTICS UTILITIES IMPORT STATUS ===")
    print(f"Overall Import Success: {'✅' if IMPORT_SUCCESS else '❌'}")
    
    if IMPORT_ERRORS:
        print(f"Import Errors: {IMPORT_ERRORS}")
    
    # Check individual modules
    modules = {
        'Experiment Analysis': 'calculate_significance' in globals(),
        'Engagement Analysis': 'run_comprehensive_analysis' in globals(),
        'Business Impact': 'calculate_profitability_from_timebound_results' in globals(),
        'Session Analysis': SESSION_ANALYSIS_AVAILABLE if 'SESSION_ANALYSIS_AVAILABLE' in globals() else False
    }
    
    print("\nModule Availability:")
    for module, available in modules.items():
        print(f"  {module}: {'✅' if available else '❌'}")
    
    return modules


def get_available_functions():
    """Get a list of all available analytics functions."""
    
    available_functions = {}
    
    # Experiment Analysis functions
    experiment_functions = [
        'calculate_significance', 'run_time_bound_analysis', 'plot_daily_l2c_uplift',
        'analyze_slice_distribution', 'load_queries_from_directory'
    ]
    available_functions['experiment_analysis'] = [f for f in experiment_functions if f in globals()]
    
    # Engagement Analysis functions  
    engagement_functions = [
        'analyze_event_completion_generic', 'run_comprehensive_analysis',
        'analyze_segment_migration', 'segment_users_for_log_analysis'
    ]
    available_functions['engagement_analysis'] = [f for f in engagement_functions if f in globals()]
    
    # Business Impact functions
    business_functions = [
        'calculate_profitability_from_timebound_results', 'visualize_segment_profitability',
        'clean_df_for_analysis', 'plot_extracted_uplifts'
    ]
    available_functions['business_impact'] = [f for f in business_functions if f in globals()]
    
    return available_functions


def print_quick_start_guide():
    """Print a quick start guide for using analytics utilities in MCP."""
    
    guide = """
=== ANALYTICS UTILITIES QUICK START GUIDE FOR MCP ===

1. BASIC SETUP:
   Add to your MCP Python file:
   ```
   import sys
   sys.path.append('/Users/saramcneilly/src/github.com/analytics_utilities')
   from mcp_integration import *
   ```

2. EXPERIMENT ANALYSIS:
   ```
   # Basic significance testing
   results = calculate_significance(df, variant_col='variant')
   
   # Time-bound analysis by segment
   segment_results = run_time_bound_analysis(
       df, 
       slice_column='growth_segment',
       time_bounds=[5, 15, 30]
   )
   
   # Complete workflow
   analysis = run_complete_experiment_analysis(df, slice_column='growth_segment')
   ```

3. BUSINESS IMPACT ANALYSIS:
   ```
   # Calculate financial impact
   segment_impacts, total_impact = calculate_profitability_from_timebound_results(
       segment_results, 
       time_bound='5_days'
   )
   
   # Complete business analysis
   business_analysis = run_complete_business_analysis(segment_results)
   ```

4. ENGAGEMENT ANALYSIS:
   ```
   # Milestone completion analysis
   results, significance, fig = run_comprehensive_analysis(df, 'milestones')
   
   # Complete engagement workflow
   engagement = run_complete_engagement_analysis(df, 'milestones')
   ```

5. DATA QUALITY:
   ```
   # Clean and validate data
   df_clean = clean_df_for_analysis(df, 'growth_segment')
   validation = validate_experiment_data(df_clean)
   print_validation_summary(validation)
   ```

6. VISUALIZATIONS:
   ```
   # Daily uplift plot
   fig = plot_daily_l2c_uplift(df, slice_column='growth_segment')
   
   # Executive profitability dashboard
   fig = visualize_segment_profitability(segment_impacts)
   
   # Uplift comparison
   fig = plot_extracted_uplifts_with_totals(uplift_data)
   ```

Check available functions: get_available_functions()
Check import status: check_imports()
"""
    
    print(guide)


# Auto-run import check when module is loaded
if __name__ == "__main__":
    check_imports()
    print_quick_start_guide()
else:
    # Just show a brief status when imported
    status = "✅ SUCCESS" if IMPORT_SUCCESS else "⚠️ PARTIAL"
    print(f"Analytics Utilities MCP Integration: {status}")
    if not IMPORT_SUCCESS:
        print("Run check_imports() for details, or print_quick_start_guide() for usage help.")