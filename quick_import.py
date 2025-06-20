"""
Quick Import for Analytics Utilities

The simplest way to use analytics utilities in another directory.
Just copy this file to your project and import it.

Usage:
1. Copy this file to your MCP/project directory
2. In your Python files: from quick_import import *
3. Use analytics functions directly: results = calculate_significance(df)
"""

import sys
from pathlib import Path

# Add analytics utilities to Python path
ANALYTICS_PATH = Path('/Users/saramcneilly/src/github.com/analytics_utilities')
SRC_PATH = ANALYTICS_PATH / 'src'

for path in [ANALYTICS_PATH, SRC_PATH]:
    if str(path) not in sys.path:
        sys.path.append(str(path))

# Import the most commonly used functions
try:
    # Experiment Analysis - Core Functions
    from experiment_analysis.statistical_analysis import calculate_significance
    from experiment_analysis.slice_analysis import run_time_bound_analysis, analyze_slice_distribution
    from experiment_analysis.plotting import plot_daily_l2c_uplift
    
    # Business Impact Analysis - Core Functions  
    from business_impact.profitability_analysis import calculate_profitability_from_timebound_results, visualize_segment_profitability
    from business_impact.data_quality import clean_df_for_analysis, validate_experiment_data
    from business_impact.data_extraction import prepare_business_summary, format_business_summary
    
    # Engagement Analysis - Core Functions
    from engagement_analysis.milestone_analysis import run_comprehensive_analysis
    from engagement_analysis.journey_analysis import segment_users_for_log_analysis, analyze_segment_migration
    
    print("✅ Analytics Utilities: Core functions imported successfully")
    
except ImportError as e:
    print(f"❌ Analytics Utilities: Import error - {e}")
    print("Make sure analytics_utilities is at: /Users/saramcneilly/src/github.com/analytics_utilities")

# Convenience workflow functions
def quick_experiment_analysis(df, segment_column=None, time_bounds=[5, 15, 30]):
    """
    Quick complete experiment analysis workflow.
    
    Returns:
    --------
    dict with keys: significance, timebound_analysis, business_impact
    """
    results = {}
    
    # Statistical significance
    results['significance'] = calculate_significance(df)
    
    # Time-bound analysis
    if segment_column:
        results['timebound_analysis'] = run_time_bound_analysis(
            df, 
            slice_column=segment_column, 
            time_bounds=time_bounds
        )
        
        # Business impact
        if results['timebound_analysis']:
            segment_impacts, total_impact = calculate_profitability_from_timebound_results(
                results['timebound_analysis']
            )
            
            business_summary = prepare_business_summary(segment_impacts, total_impact)
            
            results['business_impact'] = {
                'segment_impacts': segment_impacts,
                'total_impact': total_impact,
                'summary': business_summary,
                'recommendation': business_summary['recommendation']
            }
    
    return results

def quick_data_quality_check(df, segment_column=None):
    """Quick data quality validation and cleaning."""
    
    # Validate
    validation = validate_experiment_data(df)
    
    # Clean if needed
    if segment_column and not validation['is_valid']:
        df_clean = clean_df_for_analysis(df, segment_column)
        return df_clean, validation
    
    return df, validation

# Export the key functions for easy access
__all__ = [
    # Core analysis functions
    'calculate_significance',
    'run_time_bound_analysis', 
    'calculate_profitability_from_timebound_results',
    'clean_df_for_analysis',
    'validate_experiment_data',
    'run_comprehensive_analysis',
    
    # Visualization functions
    'plot_daily_l2c_uplift',
    'visualize_segment_profitability',
    
    # Convenience workflows
    'quick_experiment_analysis',
    'quick_data_quality_check',
    
    # Business reporting
    'prepare_business_summary',
    'format_business_summary'
]