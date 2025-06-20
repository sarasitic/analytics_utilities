"""
Sessions Analysis Examples

Comprehensive examples showing how to use the Sessions Analysis Toolkit
with admin_sessions data and other session-based datasets.

This script demonstrates:
- Basic A/B testing with flexible column names
- User segmentation analysis
- Migration analysis
- Batch testing across multiple metrics
- Grouping by additional dimensions
- Custom business thresholds

Usage:
    python sessions_analysis_examples.py
    
Or import and use specific examples:
    from sessions_analysis_examples import run_basic_example, run_advanced_example
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Import our toolkits
try:
    from sessions_analysis_toolkit import SessionsAnalyzer
    from sessions_plotting_toolkit import SessionsPlotter
    print("✅ Sessions Analysis Toolkit imported successfully")
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Make sure sessions_analysis_toolkit.py and sessions_plotting_toolkit.py are in the same directory")
    exit(1)


def create_sample_admin_sessions_data(n_shops: int = 10000) -> pd.DataFrame:
    """
    Create sample admin sessions data similar to the original admin_sessions dataframe.
    
    This creates synthetic data with the same structure as the original admin_sessions
    to demonstrate the toolkit functionality.
    
    Parameters:
    -----------
    n_shops : int, default=10000
        Number of shops to generate
        
    Returns:
    --------
    pd.DataFrame : Sample admin sessions data
    """
    
    np.random.seed(42)  # For reproducible results
    
    # Generate shop IDs
    shop_ids = range(1, n_shops + 1)
    
    # Assign variants (roughly 50/50 split)
    variants = np.random.choice(['control', 'treatment'], size=n_shops, p=[0.5, 0.5])
    
    # Create treatment effect for demonstration
    # Treatment increases pageviews by ~15% on average
    treatment_multiplier = np.where(variants == 'treatment', 
                                  np.random.lognormal(mean=np.log(1.15), sigma=0.2), 
                                  1.0)
    
    # Generate pageviews with log-normal distribution (typical for web metrics)
    base_pageviews = np.random.lognormal(mean=2.5, sigma=1.2, size=n_shops)
    pageviews_in_first_session = np.maximum(1, (base_pageviews * treatment_multiplier).astype(int))
    
    # Generate other session metrics correlated with pageviews
    distinct_pages_first_session = np.maximum(1, 
        (pageviews_in_first_session * np.random.uniform(0.3, 0.8, n_shops)).astype(int))
    
    # Total session counts and pageviews
    session_multiplier = np.random.uniform(1, 5, n_shops)
    admin_user_browser_session_count = np.maximum(1, 
        (pageviews_in_first_session * session_multiplier * 0.2).astype(int))
    
    admin_pageview_count = np.maximum(pageviews_in_first_session,
        (pageviews_in_first_session * session_multiplier * np.random.uniform(0.8, 1.5, n_shops)).astype(int))
    
    distinct_pages_viewed_count = np.maximum(distinct_pages_first_session,
        (distinct_pages_first_session * session_multiplier * np.random.uniform(0.6, 1.2, n_shops)).astype(int))
    
    # Calculate derived metrics
    average_pageviews_per_session = admin_pageview_count / admin_user_browser_session_count
    average_distinct_pages_per_session = distinct_pages_viewed_count / admin_user_browser_session_count
    
    # Add some additional dimensions for grouping examples
    regions = np.random.choice(['North America', 'Europe', 'Asia', 'Other'], 
                              size=n_shops, p=[0.4, 0.3, 0.2, 0.1])
    shop_types = np.random.choice(['New', 'Existing'], size=n_shops, p=[0.6, 0.4])
    
    # Create dataframe
    df = pd.DataFrame({
        'shop_id': shop_ids,
        'variant': variants,
        'pageviews_in_first_session_count': pageviews_in_first_session,
        'distinct_pages_viewed_in_first_session_count': distinct_pages_first_session,
        'admin_user_browser_session_count': admin_user_browser_session_count,
        'admin_pageview_count': admin_pageview_count,
        'distinct_pages_viewed_count': distinct_pages_viewed_count,
        'average_pageviews_per_session': average_pageviews_per_session,
        'average_distinct_pages_viewed_per_session': average_distinct_pages_per_session,
        'region': regions,
        'shop_type': shop_types
    })
    
    return df


def run_basic_example(df: pd.DataFrame = None):
    """
    Basic example showing core functionality with admin_sessions-like data.
    
    Parameters:
    -----------
    df : pd.DataFrame, optional
        Admin sessions dataframe. If None, creates sample data.
    """
    
    print("🚀 BASIC SESSIONS ANALYSIS EXAMPLE")
    print("=" * 60)
    
    # Use provided data or create sample data
    if df is None:
        print("📊 Creating sample admin sessions data...")
        df = create_sample_admin_sessions_data(5000)
        print(f"Created {len(df)} shops with {df['variant'].value_counts().to_dict()}")
    
    # Define the metrics we want to analyze
    metrics_to_analyze = [
        'pageviews_in_first_session_count',
        'distinct_pages_viewed_in_first_session_count', 
        'admin_pageview_count',
        'average_pageviews_per_session'
    ]
    
    # Initialize the analyzer
    analyzer = SessionsAnalyzer(
        df=df,
        metric_columns=metrics_to_analyze,
        variant_column='variant',
        user_id_column='shop_id',
        control_label='control',
        treatment_label='treatment'
    )
    
    print(f"\n✅ Analyzer initialized with {len(df)} shops and {len(metrics_to_analyze)} metrics")
    
    # Example 1: Single metric analysis
    print(f"\n" + "="*50)
    print("EXAMPLE 1: Single Metric Analysis")
    print("="*50)
    
    results = analyzer.test_statistical_significance('pageviews_in_first_session_count')
    analyzer.print_results(results, 'basic')
    
    # Example 2: Batch analysis of all metrics
    print(f"\n" + "="*50)
    print("EXAMPLE 2: Batch Analysis")
    print("="*50)
    
    batch_results = analyzer.batch_significance_test()
    print(f"\n📊 Batch Results Summary:")
    print(batch_results[['metric', 'relative_lift_percent', 'p_value', 'significance']].to_string(index=False))
    
    # Example 3: Segmentation analysis
    print(f"\n" + "="*50)
    print("EXAMPLE 3: User Segmentation Analysis")
    print("="*50)
    
    seg_results = analyzer.segment_analysis('pageviews_in_first_session_count', method='log_quantiles', n_segments=3)
    analyzer.print_results(seg_results, 'segment')
    
    # Example 4: Migration analysis
    print(f"\n" + "="*50)
    print("EXAMPLE 4: Segment Migration Analysis")
    print("="*50)
    
    migration_results = analyzer.migration_analysis('pageviews_in_first_session_count')
    analyzer.print_results(migration_results, 'migration')
    
    return analyzer, batch_results, seg_results, migration_results


def run_advanced_example(df: pd.DataFrame = None):
    """
    Advanced example showing flexible usage with custom configurations.
    
    Parameters:
    -----------
    df : pd.DataFrame, optional
        Admin sessions dataframe. If None, creates sample data.
    """
    
    print("🔥 ADVANCED SESSIONS ANALYSIS EXAMPLE")
    print("=" * 60)
    
    # Use provided data or create sample data
    if df is None:
        print("📊 Creating sample admin sessions data...")
        df = create_sample_admin_sessions_data(8000)
    
    # Example 1: Analysis with custom column names and labels
    print(f"\n" + "="*50)
    print("EXAMPLE 1: Custom Column Names and Labels")
    print("="*50)
    
    # Rename columns to show flexibility
    df_custom = df.copy()
    df_custom = df_custom.rename(columns={
        'shop_id': 'user_id',
        'variant': 'experiment_group',
        'pageviews_in_first_session_count': 'first_session_views'
    })
    
    custom_analyzer = SessionsAnalyzer(
        df=df_custom,
        metric_columns=['first_session_views'],
        variant_column='experiment_group',
        user_id_column='user_id',
        control_label='control',
        treatment_label='treatment'
    )
    
    results = custom_analyzer.test_statistical_significance('first_session_views')
    print(f"✅ Analysis with custom column names successful!")
    print(f"Control mean: {results['control_mean']:.2f}, Treatment mean: {results['treatment_mean']:.2f}")
    print(f"P-value: {results['p_value']:.6f}, Significant: {results['is_significant']}")
    
    # Example 2: Grouping by additional dimensions
    print(f"\n" + "="*50)
    print("EXAMPLE 2: Analysis by Additional Dimensions")
    print("="*50)
    
    analyzer = SessionsAnalyzer(
        df=df,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id',
        additional_dimensions=['region', 'shop_type']
    )
    
    # Analyze by region
    regional_results = analyzer.test_statistical_significance(
        'pageviews_in_first_session_count',
        groupby_dimensions=['region']
    )
    
    print(f"\n📊 Results by Region:")
    print(regional_results[['region', 'control_mean', 'treatment_mean', 'relative_lift_percent', 'p_value', 'is_significant']].round(4))
    
    # Analyze by shop type
    shop_type_results = analyzer.test_statistical_significance(
        'pageviews_in_first_session_count',
        groupby_dimensions=['shop_type']
    )
    
    print(f"\n📊 Results by Shop Type:")
    print(shop_type_results[['shop_type', 'control_mean', 'treatment_mean', 'relative_lift_percent', 'p_value', 'is_significant']].round(4))
    
    # Example 3: Custom business segmentation
    print(f"\n" + "="*50)
    print("EXAMPLE 3: Custom Business Segmentation")
    print("="*50)
    
    # Define custom business thresholds for pageviews
    business_thresholds = {
        'Low': 3,      # 0-3 pageviews = Low engagement
        'Medium': 10,  # 4-10 pageviews = Medium engagement  
        'High': 25     # 11-25 pageviews = High engagement
                       # 26+ pageviews = Very High engagement
    }
    
    business_seg_results = analyzer.segment_analysis(
        'pageviews_in_first_session_count',
        method='business',
        business_thresholds=business_thresholds
    )
    
    print(f"\n📊 Custom Business Segmentation Results:")
    analyzer.print_results(business_seg_results, 'segment')
    
    # Example 4: Ordinal trend analysis
    print(f"\n" + "="*50)
    print("EXAMPLE 4: Ordinal Trend Analysis")
    print("="*50)
    
    trend_results = analyzer.ordinal_trend_test('pageviews_in_first_session_count')
    analyzer.print_results(trend_results, 'trend')
    
    return analyzer, regional_results, business_seg_results, trend_results


def run_plotting_example(df: pd.DataFrame = None):
    """
    Example showing plotting capabilities.
    
    Parameters:
    -----------
    df : pd.DataFrame, optional
        Admin sessions dataframe. If None, creates sample data.
    """
    
    print("📊 PLOTTING EXAMPLES")
    print("=" * 60)
    
    # Use provided data or create sample data
    if df is None:
        print("📊 Creating sample admin sessions data...")
        df = create_sample_admin_sessions_data(3000)
    
    # Initialize analyzer and plotter
    analyzer = SessionsAnalyzer(
        df=df,
        metric_columns=['pageviews_in_first_session_count', 'admin_pageview_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    plotter = SessionsPlotter(analyzer, figsize=(12, 8))
    
    # Example 1: Basic A/B test plot
    print(f"\n📈 Creating A/B test visualization...")
    results, fig = plotter.plot_ab_test('pageviews_in_first_session_count', show_clt=True)
    if fig:
        plt.show()
        print("✅ A/B test plot created successfully!")
    
    # Example 2: Segment analysis plot
    print(f"\n📈 Creating segment analysis visualization...")
    seg_results = analyzer.segment_analysis('pageviews_in_first_session_count')
    seg_fig = plotter.plot_segment_comparison(seg_results, 'pageviews_in_first_session_count')
    if seg_fig:
        plt.show()
        print("✅ Segment analysis plot created successfully!")
    
    # Example 3: Migration analysis plot
    print(f"\n📈 Creating migration analysis visualization...")
    migration_results = analyzer.migration_analysis('pageviews_in_first_session_count')
    migration_fig = plotter.plot_migration_analysis(migration_results)
    if migration_fig:
        plt.show()
        print("✅ Migration analysis plot created successfully!")
    
    # Example 4: Batch results plot
    print(f"\n📈 Creating batch results visualization...")
    batch_results = analyzer.batch_significance_test()
    batch_fig = plotter.plot_batch_results(batch_results)
    if batch_fig:
        plt.show()
        print("✅ Batch results plot created successfully!")
    
    # Example 5: Ordinal trend plot
    print(f"\n📈 Creating ordinal trend visualization...")
    trend_results = analyzer.ordinal_trend_test('pageviews_in_first_session_count')
    trend_fig = plotter.plot_ordinal_trend(trend_results)
    if trend_fig:
        plt.show()
        print("✅ Ordinal trend plot created successfully!")
    
    return plotter


def demonstrate_flexibility():
    """
    Demonstrate the flexibility of the toolkit with different data structures.
    """
    
    print("🔧 FLEXIBILITY DEMONSTRATION")
    print("=" * 60)
    
    # Example 1: Different variant labels
    print(f"\n📊 Example 1: Different Variant Labels")
    df1 = create_sample_admin_sessions_data(2000)
    df1['variant'] = df1['variant'].map({'control': 'baseline', 'treatment': 'experiment'})
    
    analyzer1 = SessionsAnalyzer(
        df=df1,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id',
        control_label='baseline',
        treatment_label='experiment'
    )
    
    results1 = analyzer1.test_statistical_significance('pageviews_in_first_session_count')
    print(f"✅ Custom variant labels work! P-value: {results1['p_value']:.6f}")
    
    # Example 2: Different metric names
    print(f"\n📊 Example 2: Different Metric Names")
    df2 = create_sample_admin_sessions_data(2000)
    df2 = df2.rename(columns={
        'pageviews_in_first_session_count': 'engagement_score',
        'admin_pageview_count': 'total_interactions'
    })
    
    analyzer2 = SessionsAnalyzer(
        df=df2,
        metric_columns=['engagement_score', 'total_interactions'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    batch_results2 = analyzer2.batch_significance_test()
    print(f"✅ Custom metric names work! Found {len(batch_results2)} metrics")
    print(batch_results2[['metric', 'relative_lift_percent', 'significance']].to_string(index=False))
    
    # Example 3: Multiple grouping dimensions
    print(f"\n📊 Example 3: Multiple Grouping Dimensions")
    df3 = create_sample_admin_sessions_data(3000)
    
    analyzer3 = SessionsAnalyzer(
        df=df3,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    multi_group_results = analyzer3.test_statistical_significance(
        'pageviews_in_first_session_count',
        groupby_dimensions=['region', 'shop_type']
    )
    
    print(f"✅ Multiple grouping dimensions work! Found {len(multi_group_results)} combinations")
    print(multi_group_results[['region', 'shop_type', 'relative_lift_percent', 'is_significant']].head().to_string(index=False))


def main():
    """
    Main function to run all examples.
    """
    
    print("🎯 SESSIONS ANALYSIS TOOLKIT - COMPREHENSIVE EXAMPLES")
    print("=" * 80)
    print("This script demonstrates the full capabilities of the Sessions Analysis Toolkit")
    print("using admin_sessions-like data.")
    print()
    
    try:
        # Run basic example
        print("🚀 Running Basic Example...")
        analyzer, batch_results, seg_results, migration_results = run_basic_example()
        print("\n✅ Basic example completed successfully!")
        
        # Run advanced example
        print("\n" + "="*80)
        print("🔥 Running Advanced Example...")
        analyzer_adv, regional_results, business_seg_results, trend_results = run_advanced_example()
        print("\n✅ Advanced example completed successfully!")
        
        # Run plotting example (commented out to avoid showing plots in automated runs)
        # Uncomment these lines to see the plots:
        
        # print("\n" + "="*80)
        # print("📊 Running Plotting Example...")
        # plotter = run_plotting_example()
        # print("\n✅ Plotting example completed successfully!")
        
        # Demonstrate flexibility
        print("\n" + "="*80)
        print("🔧 Demonstrating Flexibility...")
        demonstrate_flexibility()
        print("\n✅ Flexibility demonstration completed successfully!")
        
        # Summary
        print("\n" + "="*80)
        print("🎉 ALL EXAMPLES COMPLETED SUCCESSFULLY!")
        print("="*80)
        print("The Sessions Analysis Toolkit is ready for use with your admin_sessions data.")
        print("\nKey features demonstrated:")
        print("✅ Statistical significance testing with multiple test types")
        print("✅ User segmentation with log-quantiles and business rules")
        print("✅ Segment migration analysis")
        print("✅ Ordinal trend testing")
        print("✅ Batch analysis across multiple metrics")
        print("✅ Flexible column naming and grouping")
        print("✅ Comprehensive visualization capabilities")
        print("\nReady to analyze your real admin_sessions data! 🚀")
        
    except Exception as e:
        print(f"❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()