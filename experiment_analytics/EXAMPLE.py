"""
Experiment Analytics Package - Complete Example

This example demonstrates the full capabilities of the experiment_analytics package
for A/B test analysis, from data loading to visualization and forecasting.
"""

import pandas as pd
import numpy as np
from analytics_utilities.experiment_analytics import *

# =============================================================================
# EXAMPLE 1: Basic A/B Test Analysis
# =============================================================================

def example_basic_ab_test():
    """Basic A/B test statistical analysis"""
    
    # Sample data - replace with your actual experiment data
    np.random.seed(42)
    n_users = 10000
    
    experiment_data = pd.DataFrame({
        'user_id': range(n_users),
        'variant': np.random.choice(['control', 'treatment'], n_users),
        'converted': np.random.binomial(1, 0.12, n_users),  # 12% base conversion
        'revenue': np.random.exponential(50, n_users),
        'days_to_conversion': np.random.exponential(7, n_users)
    })
    
    # Boost treatment conversion by 2%
    treatment_mask = experiment_data['variant'] == 'treatment'
    experiment_data.loc[treatment_mask, 'converted'] = np.random.binomial(
        1, 0.14, treatment_mask.sum()
    )
    
    print("=== Basic A/B Test Analysis ===")
    
    # Statistical significance test
    results = calculate_significance(
        df=experiment_data,
        treatment_col='variant',
        outcome_col='converted',
        treatment_value='treatment',
        control_value='control'
    )
    
    print(f"Conversion Rate Difference: {results['conversion_rate_difference']:.4f}")
    print(f"P-value: {results['p_value']:.6f}")
    print(f"95% Confidence Interval: {results['confidence_interval']}")
    print(f"Statistical Significance: {results['is_significant']}")
    print()

# =============================================================================
# EXAMPLE 2: Query Loading and Data Processing  
# =============================================================================

def example_query_processing():
    """Example of loading and processing queries (mock example)"""
    
    print("=== Query Loading Example ===")
    
    # In practice, you would have SQL files in a directory
    # queries = load_queries_from_directory('../queries/')
    # dataframes = execute_queries_batch(queries, refresh_bq_function)
    
    # Mock processed data for example
    l2c_data = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=30),
        'variant': ['control'] * 15 + ['treatment'] * 15,
        'conversions': np.random.poisson(100, 30),
        'leads': np.random.poisson(1000, 30)
    })
    
    l2c_data['conversion_rate'] = l2c_data['conversions'] / l2c_data['leads']
    
    print("Sample L2C data loaded:")
    print(l2c_data.head())
    print()
    
    return l2c_data

# =============================================================================
# EXAMPLE 3: Slice Analysis
# =============================================================================

def example_slice_analysis():
    """Multi-dimensional slice analysis"""
    
    print("=== Slice Analysis Example ===")
    
    # Sample segmented data
    np.random.seed(42)
    n_users = 5000
    
    slice_data = pd.DataFrame({
        'user_id': range(n_users),
        'variant': np.random.choice(['control', 'treatment'], n_users),
        'segment': np.random.choice(['premium', 'standard', 'basic'], n_users),
        'region': np.random.choice(['US', 'EU', 'APAC'], n_users),
        'converted': np.random.binomial(1, 0.15, n_users),
        'conversion_timestamp': pd.date_range('2024-01-01', periods=n_users, freq='1min')
    })
    
    # Analyze by segment
    segment_results = analyze_slice_distribution(
        df=slice_data,
        slice_column='segment',
        variant_column='variant',
        outcome_column='converted',
        time_bounds=[None, 7, 14, 30]  # Overall, 7-day, 14-day, 30-day
    )
    
    print("Slice analysis by customer segment:")
    print(segment_results)
    print()

# =============================================================================
# EXAMPLE 4: Time-bound Analysis
# =============================================================================

def example_timebound_analysis():
    """Time-bound conversion analysis"""
    
    print("=== Time-bound Analysis Example ===")
    
    # Sample timeline data
    np.random.seed(42)
    timeline_data = pd.DataFrame({
        'user_id': range(1000),
        'variant': np.random.choice(['control', 'treatment'], 1000),
        'signup_date': pd.date_range('2024-01-01', periods=1000, freq='1H'),
        'first_call_date': None
    })
    
    # Simulate some users making calls
    call_probability = 0.3
    call_users = np.random.choice(
        timeline_data.index, 
        size=int(len(timeline_data) * call_probability), 
        replace=False
    )
    
    for idx in call_users:
        days_to_call = np.random.exponential(5)  # Average 5 days to first call
        timeline_data.loc[idx, 'first_call_date'] = (
            timeline_data.loc[idx, 'signup_date'] + pd.Timedelta(days=days_to_call)
        )
    
    # Analyze first call timing
    timing_results = analyze_first_call_timing(
        df=timeline_data,
        variants=['control', 'treatment'],
        signup_date_col='signup_date',
        event_date_col='first_call_date',
        max_days=14
    )
    
    print("First call timing analysis:")
    print(f"Results type: {type(timing_results)}")
    print()

# =============================================================================
# EXAMPLE 5: Forecasting
# =============================================================================

def example_forecasting():
    """Call volume forecasting example"""
    
    print("=== Forecasting Example ===")
    
    # Create forecast comparison
    forecast_results = create_call_forecast_comparison(
        leads_per_week=10000,
        current_access_days=3,
        extended_access_days=7,
        calls_per_week_current=500,
        title="Impact of Extending Phone Support Access"
    )
    
    print("Forecast comparison created")
    print(f"Results type: {type(forecast_results)}")
    print()

# =============================================================================
# EXAMPLE 6: Complete Workflow
# =============================================================================

def example_complete_workflow():
    """Complete analysis workflow"""
    
    print("=== Complete Analysis Workflow ===")
    
    # 1. Load data (simulated)
    l2c_data = example_query_processing()
    
    # 2. Basic significance test
    example_basic_ab_test()
    
    # 3. Slice analysis
    example_slice_analysis()
    
    # 4. Time-bound analysis
    example_timebound_analysis()
    
    # 5. Forecasting
    example_forecasting()
    
    print("Complete workflow executed successfully!")

# =============================================================================
# MCP INTEGRATION EXAMPLE
# =============================================================================

def example_mcp_integration():
    """Example of how to use this package with MCP"""
    
    print("=== MCP Integration Example ===")
    
    # MCP-friendly function that returns structured results
    def analyze_experiment_for_mcp(data_dict):
        """
        MCP-compatible function for experiment analysis
        
        Args:
            data_dict: Dictionary with keys 'df', 'treatment_col', 'outcome_col'
        
        Returns:
            Dictionary with analysis results
        """
        df = pd.DataFrame(data_dict['df'])
        
        results = calculate_significance(
            df=df,
            treatment_col=data_dict['treatment_col'],
            outcome_col=data_dict['outcome_col'],
            treatment_value=data_dict.get('treatment_value', 'treatment'),
            control_value=data_dict.get('control_value', 'control')
        )
        
        return {
            'statistical_significance': results['is_significant'],
            'p_value': float(results['p_value']),
            'effect_size': float(results['conversion_rate_difference']),
            'confidence_interval': [float(x) for x in results['confidence_interval']],
            'recommendation': 'Deploy treatment' if results['is_significant'] else 'No significant difference'
        }
    
    # Sample usage
    sample_data = {
        'df': {
            'variant': ['control'] * 500 + ['treatment'] * 500,
            'converted': ([0] * 450 + [1] * 50) + ([0] * 430 + [1] * 70)
        },
        'treatment_col': 'variant',
        'outcome_col': 'converted'
    }
    
    mcp_results = analyze_experiment_for_mcp(sample_data)
    print("MCP Results:", mcp_results)
    print()

# =============================================================================
# RUN ALL EXAMPLES
# =============================================================================

if __name__ == "__main__":
    print("Running Experiment Analytics Examples...\n")
    
    try:
        example_complete_workflow()
        example_mcp_integration()
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("Make sure all dependencies are installed.")