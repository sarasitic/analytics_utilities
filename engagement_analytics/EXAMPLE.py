"""
Engagement Analytics Package - Complete Example

This example demonstrates the full capabilities of the engagement_analytics package
for analyzing user behavior, session patterns, and engagement metrics.
"""

import pandas as pd
import numpy as np
from analytics_utilities.engagement_analytics import *

# =============================================================================
# EXAMPLE 1: Basic Session Analysis
# =============================================================================

def create_sample_admin_sessions():
    """Create sample admin_sessions data for examples"""
    
    np.random.seed(42)
    n_shops = 2000
    
    # Simulate Shopify admin session data
    admin_sessions = pd.DataFrame({
        'shop_id': range(n_shops),
        'variant': np.random.choice(['control', 'treatment'], n_shops),
        'region': np.random.choice(['US', 'EU', 'APAC'], n_shops),
        'shop_type': np.random.choice(['new', 'existing'], n_shops, p=[0.3, 0.7]),
        
        # Engagement metrics (typical for new merchant onboarding)
        'pageviews_in_first_session_count': np.random.poisson(5, n_shops),
        'distinct_pages_viewed_in_first_session_count': np.random.poisson(3, n_shops),
        'admin_user_browser_session_count': np.random.poisson(2, n_shops),
        'admin_pageview_count': np.random.poisson(12, n_shops),
        'average_pageviews_per_session': np.random.exponential(4, n_shops)
    })
    
    # Make treatment group slightly more engaged
    treatment_mask = admin_sessions['variant'] == 'treatment'
    admin_sessions.loc[treatment_mask, 'pageviews_in_first_session_count'] += np.random.poisson(1, treatment_mask.sum())
    admin_sessions.loc[treatment_mask, 'distinct_pages_viewed_in_first_session_count'] += np.random.poisson(0.5, treatment_mask.sum())
    
    return admin_sessions

def example_basic_session_analysis():
    """Basic session analysis with SessionsAnalyzer"""
    
    print("=== Basic Session Analysis ===")
    
    # Create sample data
    admin_sessions = create_sample_admin_sessions()
    
    # Initialize analyzer
    analyzer = SessionsAnalyzer(
        df=admin_sessions,
        metric_columns=['pageviews_in_first_session_count', 'distinct_pages_viewed_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id',
        control_label='control',
        treatment_label='treatment'
    )
    
    # Statistical significance test for single metric
    results = analyzer.test_statistical_significance('pageviews_in_first_session_count')
    analyzer.print_results(results, 'basic')
    
    # Batch test multiple metrics
    print("\n--- Batch Analysis Across All Metrics ---")
    batch_results = analyzer.batch_significance_test()
    print(batch_results)
    print()

# =============================================================================
# EXAMPLE 2: Segmentation Analysis
# =============================================================================

def example_segmentation_analysis():
    """User segmentation analysis"""
    
    print("=== Segmentation Analysis ===")
    
    admin_sessions = create_sample_admin_sessions()
    
    analyzer = SessionsAnalyzer(
        df=admin_sessions,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    # Log-quantiles segmentation (recommended for web metrics)
    print("--- Log-Quantiles Segmentation ---")
    seg_results = analyzer.segment_analysis(
        'pageviews_in_first_session_count',
        method='log_quantiles',
        n_segments=3
    )
    analyzer.print_results(seg_results, 'segment')
    
    # Business rule segmentation
    print("\n--- Business Rule Segmentation ---")
    business_results = analyzer.segment_analysis(
        'pageviews_in_first_session_count',
        method='business',
        # Define meaningful engagement tiers for merchant onboarding
        business_thresholds={'Low Engagement': 2, 'Medium Engagement': 6}  # Low: 0-2, Medium: 3-6, High: 7+
    )
    analyzer.print_results(business_results, 'segment')
    print()

# =============================================================================
# EXAMPLE 3: Migration Analysis
# =============================================================================

def example_migration_analysis():
    """Analysis of user migration between engagement segments"""
    
    print("=== Migration Analysis ===")
    
    admin_sessions = create_sample_admin_sessions()
    
    analyzer = SessionsAnalyzer(
        df=admin_sessions,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    # Test if treatment moves users to higher engagement segments
    migration_results = analyzer.migration_analysis('pageviews_in_first_session_count')
    analyzer.print_results(migration_results, 'migration')
    
    # Test for ordinal trends (does treatment systematically increase engagement?)
    print("\n--- Ordinal Trend Test ---")
    trend_results = analyzer.ordinal_trend_test('pageviews_in_first_session_count')
    analyzer.print_results(trend_results, 'trend')
    print()

# =============================================================================
# EXAMPLE 4: Multi-Dimensional Analysis
# =============================================================================

def example_multidimensional_analysis():
    """Analysis by additional dimensions (region, shop type, etc.)"""
    
    print("=== Multi-Dimensional Analysis ===")
    
    admin_sessions = create_sample_admin_sessions()
    
    analyzer = SessionsAnalyzer(
        df=admin_sessions,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    # Analysis by region
    print("--- Analysis by Region ---")
    regional_results = analyzer.test_statistical_significance(
        'pageviews_in_first_session_count',
        groupby_dimensions=['region']
    )
    
    for region, result in regional_results.items():
        print(f"\n{region} Region:")
        analyzer.print_results(result, 'basic')
    
    # Analysis by multiple dimensions
    print("\n--- Analysis by Region and Shop Type ---")
    multi_results = analyzer.test_statistical_significance(
        'pageviews_in_first_session_count',
        groupby_dimensions=['region', 'shop_type']
    )
    
    for dimension_combo, result in multi_results.items():
        print(f"\n{dimension_combo}:")
        analyzer.print_results(result, 'basic')
    print()

# =============================================================================
# EXAMPLE 5: Custom Column Names
# =============================================================================

def example_custom_columns():
    """Example with custom column names and labels"""
    
    print("=== Custom Column Names Example ===")
    
    # Custom dataset with different column names
    custom_data = pd.DataFrame({
        'user_id': range(1000),
        'experiment_group': np.random.choice(['baseline', 'experiment'], 1000),
        'engagement_score': np.random.poisson(8, 1000),
        'session_depth': np.random.poisson(4, 1000),
        'user_segment': np.random.choice(['new', 'returning'], 1000)
    })
    
    analyzer = SessionsAnalyzer(
        df=custom_data,
        metric_columns=['engagement_score', 'session_depth'],
        variant_column='experiment_group',
        user_id_column='user_id',
        control_label='baseline',
        treatment_label='experiment'
    )
    
    # Analysis works the same way
    results = analyzer.test_statistical_significance('engagement_score')
    analyzer.print_results(results, 'basic')
    print()

# =============================================================================
# EXAMPLE 6: Minimal Version Usage
# =============================================================================

def example_minimal_version():
    """Example using the minimal version (fewer dependencies)"""
    
    print("=== Minimal Version Example ===")
    
    admin_sessions = create_sample_admin_sessions()
    
    # Use minimal analyzer (only requires pandas, numpy)
    try:
        analyzer_minimal = SessionsAnalyzerMinimal(
            df=admin_sessions,
            metric_columns=['pageviews_in_first_session_count'],
            variant_column='variant',
            user_id_column='shop_id'
        )
        
        # Basic analysis
        results = analyzer_minimal.test_statistical_significance('pageviews_in_first_session_count')
        print("Minimal version results:")
        print(f"Statistical significance: {results.get('is_significant', 'Unknown')}")
        print(f"P-value: {results.get('p_value', 'N/A')}")
        
    except ImportError:
        print("Minimal version requires only pandas and numpy")
        print("Full version requires scipy, matplotlib, seaborn for advanced features")
    print()

# =============================================================================
# EXAMPLE 7: Journey Analysis
# =============================================================================

def example_journey_analysis():
    """User journey and milestone analysis"""
    
    print("=== Journey Analysis Example ===")
    
    # Sample journey data
    np.random.seed(42)
    journey_data = pd.DataFrame({
        'user_id': range(500),
        'variant': np.random.choice(['control', 'treatment'], 500),
        'milestone_1_completed': np.random.binomial(1, 0.8, 500),
        'milestone_2_completed': np.random.binomial(1, 0.6, 500),
        'milestone_3_completed': np.random.binomial(1, 0.4, 500),
        'final_conversion': np.random.binomial(1, 0.3, 500)
    })
    
    # Analyze milestone completion by variant
    print("--- Milestone Completion Analysis ---")
    for milestone in ['milestone_1_completed', 'milestone_2_completed', 'milestone_3_completed']:
        completion_rate_control = journey_data[journey_data['variant'] == 'control'][milestone].mean()
        completion_rate_treatment = journey_data[journey_data['variant'] == 'treatment'][milestone].mean()
        
        print(f"{milestone}:")
        print(f"  Control: {completion_rate_control:.3f}")
        print(f"  Treatment: {completion_rate_treatment:.3f}")
        print(f"  Difference: {completion_rate_treatment - completion_rate_control:.3f}")
    print()

# =============================================================================
# EXAMPLE 8: MCP Integration
# =============================================================================

def example_mcp_integration():
    """Example of MCP-compatible functions"""
    
    print("=== MCP Integration Example ===")
    
    def analyze_engagement_for_mcp(data_dict):
        """
        MCP-compatible function for engagement analysis
        
        Args:
            data_dict: Dictionary with engagement data
            
        Returns:
            Dictionary with analysis results
        """
        df = pd.DataFrame(data_dict['df'])
        
        analyzer = SessionsAnalyzer(
            df=df,
            metric_columns=data_dict['metric_columns'],
            variant_column=data_dict['variant_column'],
            user_id_column=data_dict['user_id_column']
        )
        
        results = analyzer.test_statistical_significance(data_dict['metric_columns'][0])
        
        return {
            'engagement_significant': results.get('is_significant', False),
            'p_value': float(results.get('p_value', 1.0)),
            'effect_size': float(results.get('effect_size', 0.0)),
            'control_mean': float(results.get('control_mean', 0.0)),
            'treatment_mean': float(results.get('treatment_mean', 0.0)),
            'recommendation': 'Treatment increases engagement' if results.get('is_significant') else 'No significant difference'
        }
    
    # Sample data for MCP
    admin_sessions = create_sample_admin_sessions()
    mcp_data = {
        'df': admin_sessions.to_dict(),
        'metric_columns': ['pageviews_in_first_session_count'],
        'variant_column': 'variant',
        'user_id_column': 'shop_id'
    }
    
    mcp_results = analyze_engagement_for_mcp(mcp_data)
    print("MCP Results:", mcp_results)
    print()

# =============================================================================
# COMPLETE WORKFLOW
# =============================================================================

def example_complete_workflow():
    """Complete engagement analysis workflow"""
    
    print("=== Complete Engagement Analysis Workflow ===")
    
    # 1. Basic analysis
    example_basic_session_analysis()
    
    # 2. Segmentation
    example_segmentation_analysis()
    
    # 3. Migration analysis
    example_migration_analysis()
    
    # 4. Multi-dimensional analysis
    example_multidimensional_analysis()
    
    # 5. Custom columns
    example_custom_columns()
    
    # 6. Journey analysis
    example_journey_analysis()
    
    print("Complete engagement analysis workflow executed successfully!")

# =============================================================================
# RUN ALL EXAMPLES
# =============================================================================

if __name__ == "__main__":
    print("Running Engagement Analytics Examples...\n")
    
    try:
        example_complete_workflow()
        example_mcp_integration()
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("Make sure all dependencies are installed.")
        print("For minimal functionality, only pandas and numpy are required.")