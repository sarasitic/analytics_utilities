"""
Business Analytics Package - Complete Example

This example demonstrates the full capabilities of the business_analytics package
for analyzing business impact, profitability, and ROI of experiments and product changes.
"""

import pandas as pd
import numpy as np
from analytics_utilities.business_analytics import *

# =============================================================================
# EXAMPLE 1: Sample Data Creation
# =============================================================================

def create_sample_business_data():
    """Create sample business data for examples"""
    
    np.random.seed(42)
    n_customers = 5000
    
    # Sample experiment results with business metrics
    business_data = pd.DataFrame({
        'customer_id': range(n_customers),
        'variant': np.random.choice(['control', 'treatment'], n_customers),
        'customer_segment': np.random.choice(['enterprise', 'mid_market', 'smb'], n_customers, p=[0.1, 0.3, 0.6]),
        'region': np.random.choice(['US', 'EU', 'APAC'], n_customers),
        
        # Revenue metrics
        'monthly_revenue': np.random.lognormal(4, 1, n_customers),  # Log-normal distribution
        'total_revenue': np.random.lognormal(5.5, 1.2, n_customers),
        'acquisition_cost': np.random.exponential(200, n_customers),
        
        # Engagement metrics
        'feature_usage_score': np.random.beta(2, 5, n_customers) * 100,
        'support_tickets': np.random.poisson(2, n_customers),
        'retention_months': np.random.exponential(12, n_customers),
        
        # Conversion metrics
        'converted_to_paid': np.random.binomial(1, 0.25, n_customers),
        'upgraded_tier': np.random.binomial(1, 0.15, n_customers)
    })
    
    # Make treatment group perform better
    treatment_mask = business_data['variant'] == 'treatment'  
    business_data.loc[treatment_mask, 'monthly_revenue'] *= 1.08  # 8% revenue boost
    business_data.loc[treatment_mask, 'converted_to_paid'] = np.random.binomial(
        1, 0.28, treatment_mask.sum()  # 3% conversion boost
    )
    business_data.loc[treatment_mask, 'retention_months'] *= 1.15  # 15% retention boost
    
    return business_data

def create_sample_ltv_data():
    """Create sample customer lifetime value mapping"""
    
    return {
        'enterprise': 50000,
        'mid_market': 15000, 
        'smb': 3000
    }

# =============================================================================
# EXAMPLE 2: Profitability Analysis
# =============================================================================

def example_profitability_analysis():
    """Comprehensive profitability analysis"""
    
    print("=== Profitability Analysis ===")
    
    # Create sample data
    business_data = create_sample_business_data()
    ltv_mapping = create_sample_ltv_data()
    
    # Mock timebound results (normally from experiment_analytics)
    timebound_results = {
        'overall': {
            'control': {'conversion_rate': 0.25, 'sample_size': 2500},
            'treatment': {'conversion_rate': 0.28, 'sample_size': 2500}
        },
        'enterprise': {
            'control': {'conversion_rate': 0.40, 'sample_size': 250},
            'treatment': {'conversion_rate': 0.45, 'sample_size': 250}
        },
        'mid_market': {
            'control': {'conversion_rate': 0.30, 'sample_size': 750},
            'treatment': {'conversion_rate': 0.34, 'sample_size': 750}
        },
        'smb': {
            'control': {'conversion_rate': 0.20, 'sample_size': 1500},
            'treatment': {'conversion_rate': 0.22, 'sample_size': 1500} 
        }
    }
    
    # Calculate profitability from timebound results
    profitability_results = calculate_profitability_from_timebound_results(
        timebound_results=timebound_results,
        ltv_mapping=ltv_mapping,
        segment_column='customer_segment'
    )
    
    print("Profitability Results:")
    for segment, results in profitability_results.items():
        print(f"\n{segment.upper()}:")
        print(f"  Revenue Impact: ${results['revenue_impact']:,.0f}")
        print(f"  Conversion Lift: {results['conversion_lift']:.1%}")
        print(f"  ROI: {results['roi']:.1%}")
        if 'payback_months' in results:
            print(f"  Payback Period: {results['payback_months']:.1f} months")
    
    # Visualize segment profitability
    fig = visualize_segment_profitability(
        profitability_results,
        title="Revenue Impact by Customer Segment"
    )
    print(f"\nProfitability visualization created: {type(fig)}")
    print()

# =============================================================================
# EXAMPLE 3: Data Quality Validation
# =============================================================================

def example_data_quality():
    """Data quality validation and cleaning"""
    
    print("=== Data Quality Analysis ===")
    
    # Create sample data with quality issues
    business_data = create_sample_business_data()
    
    # Introduce some data quality issues
    business_data.loc[:50, 'monthly_revenue'] = np.nan  # Missing values
    business_data.loc[100:110, 'customer_segment'] = ''  # Empty strings
    business_data.loc[200:205, 'monthly_revenue'] = -100  # Invalid negative revenue
    
    print(f"Original data shape: {business_data.shape}")
    print(f"Missing monthly_revenue: {business_data['monthly_revenue'].isna().sum()}")
    print(f"Empty customer_segment: {(business_data['customer_segment'] == '').sum()}")
    print(f"Negative revenue: {(business_data['monthly_revenue'] < 0).sum()}")
    
    # Clean the data
    cleaned_data = clean_df_for_analysis(business_data)
    
    print(f"\nCleaned data shape: {cleaned_data.shape}")
    print(f"Rows removed: {business_data.shape[0] - cleaned_data.shape[0]}")
    
    # Validate experiment data
    validation_report = validate_experiment_data(
        cleaned_data, 
        required_columns=['customer_id', 'variant', 'monthly_revenue']
    )
    
    print(f"\nData validation passed: {validation_report['is_valid']}")
    if not validation_report['is_valid']:
        print(f"Issues found: {validation_report['issues']}")
    print()

# =============================================================================
# EXAMPLE 4: Financial Metrics Extraction
# =============================================================================

def example_financial_metrics():
    """Extract and calculate financial metrics"""
    
    print("=== Financial Metrics Extraction ===")
    
    business_data = create_sample_business_data()
    
    # Extract uplift and totals by segment
    uplift_data = extract_uplift_and_totals(
        df=business_data,
        metric_columns=['monthly_revenue', 'total_revenue'],
        variant_column='variant',
        segment_column='customer_segment',
        control_label='control',
        treatment_label='treatment'
    )
    
    print("Revenue Uplift by Segment:")
    print(uplift_data)
    
    # Extract comprehensive financial metrics  
    financial_metrics = extract_financial_metrics(
        df=business_data,
        revenue_column='monthly_revenue',
        cost_column='acquisition_cost',
        variant_column='variant'
    )
    
    print(f"\nFinancial Metrics:")
    print(f"Average Revenue per Customer:")
    print(f"  Control: ${financial_metrics['control']['avg_revenue']:,.0f}")
    print(f"  Treatment: ${financial_metrics['treatment']['avg_revenue']:,.0f}")
    print(f"  Lift: {financial_metrics['revenue_lift']:.1%}")
    
    print(f"\nReturn on Investment:")
    print(f"  ROI: {financial_metrics['roi']:.1%}")
    print(f"  Payback Period: {financial_metrics.get('payback_months', 'N/A')}")
    print()

# =============================================================================
# EXAMPLE 5: Business Reporting
# =============================================================================

def example_business_reporting():
    """Generate business summary reports"""
    
    print("=== Business Summary Reporting ===")
    
    business_data = create_sample_business_data()
    ltv_mapping = create_sample_ltv_data()
    
    # Calculate key business metrics
    control_revenue = business_data[business_data['variant'] == 'control']['monthly_revenue'].mean()
    treatment_revenue = business_data[business_data['variant'] == 'treatment']['monthly_revenue'].mean()
    revenue_lift = (treatment_revenue - control_revenue) / control_revenue
    
    control_conversion = business_data[business_data['variant'] == 'control']['converted_to_paid'].mean()
    treatment_conversion = business_data[business_data['variant'] == 'treatment']['converted_to_paid'].mean()
    conversion_lift = (treatment_conversion - control_conversion) / control_conversion
    
    metrics = {
        'revenue_lift': revenue_lift,
        'conversion_lift': conversion_lift,
        'control_revenue': control_revenue,
        'treatment_revenue': treatment_revenue,
        'sample_size': len(business_data),
        'experiment_duration_days': 30
    }
    
    # Prepare business summary
    summary_report = prepare_business_summary(
        metrics=metrics,
        experiment_name="Q4 Feature Enhancement Test"
    )
    
    print("Business Summary Report:")
    print("=" * 50)
    print(summary_report)
    print()

# =============================================================================
# EXAMPLE 6: Segment Financial Impact
# =============================================================================

def example_segment_impact():
    """Calculate financial impact by customer segment"""
    
    print("=== Segment Financial Impact Analysis ===")
    
    business_data = create_sample_business_data()
    ltv_mapping = create_sample_ltv_data()
    
    # Calculate segment-level financial impact
    segment_impact = calculate_segment_financial_impact(
        df=business_data,
        ltv_mapping=ltv_mapping,
        segment_column='customer_segment',
        variant_column='variant',
        conversion_column='converted_to_paid'
    )
    
    print("Financial Impact by Customer Segment:")
    total_impact = 0
    
    for segment, impact in segment_impact.items():
        print(f"\n{segment.upper()}:")
        print(f"  Customer LTV: ${ltv_mapping[segment]:,}")
        print(f"  Conversion Lift: {impact['conversion_lift']:.1%}")
        print(f"  Revenue Impact: ${impact['revenue_impact']:,.0f}")
        print(f"  Customers Affected: {impact['sample_size']:,}")
        
        total_impact += impact['revenue_impact']
    
    print(f"\nTOTAL EXPERIMENT IMPACT: ${total_impact:,.0f}")
    print()

# =============================================================================
# EXAMPLE 7: Visualization Examples
# =============================================================================

def example_visualizations():
    """Business intelligence visualizations"""
    
    print("=== Business Intelligence Visualizations ===")
    
    business_data = create_sample_business_data()
    
    # Extract uplift data for visualization
    uplift_data = extract_uplift_and_totals(
        df=business_data,
        metric_columns=['monthly_revenue'],
        variant_column='variant',
        segment_column='customer_segment'
    )
    
    # Create uplift visualization
    fig1 = plot_extracted_uplifts(
        uplift_data,
        title="Monthly Revenue Uplift by Customer Segment"
    )
    print(f"Uplift visualization created: {type(fig1)}")
    
    # Create uplift with totals
    fig2 = plot_extracted_uplifts_with_totals(
        uplift_data,
        title="Revenue Impact with Sample Sizes"
    )
    print(f"Uplift with totals visualization created: {type(fig2)}")
    
    # Slice comparison visualization
    fig3 = plot_slice_uplift_comparison(
        uplift_data,
        title="Customer Segment Performance Comparison"
    )
    print(f"Slice comparison visualization created: {type(fig3)}")
    print()

# =============================================================================
# EXAMPLE 8: MCP Integration
# =============================================================================

def example_mcp_integration():
    """MCP-compatible business intelligence functions"""
    
    print("=== MCP Integration Example ===")
    
    def analyze_business_impact_for_mcp(data_dict):
        """
        MCP-compatible function for business impact analysis
        
        Args:
            data_dict: Dictionary with business data and parameters
            
        Returns:
            Dictionary with business analysis results
        """
        df = pd.DataFrame(data_dict['df'])
        ltv_mapping = data_dict.get('ltv_mapping', {})
        
        # Calculate key business metrics
        control_mask = df[data_dict['variant_column']] == data_dict['control_label']
        treatment_mask = df[data_dict['variant_column']] == data_dict['treatment_label']
        
        control_revenue = df[control_mask][data_dict['revenue_column']].mean()
        treatment_revenue = df[treatment_mask][data_dict['revenue_column']].mean()
        revenue_lift = (treatment_revenue - control_revenue) / control_revenue
        
        control_conversion = df[control_mask][data_dict['conversion_column']].mean()
        treatment_conversion = df[treatment_mask][data_dict['conversion_column']].mean()
        conversion_lift = (treatment_conversion - control_conversion) / control_conversion
        
        # Estimate annual impact
        annual_impact = revenue_lift * control_revenue * len(df) * 12  # Monthly to annual
        
        return {
            'revenue_lift_percent': float(revenue_lift * 100),
            'conversion_lift_percent': float(conversion_lift * 100),
            'estimated_annual_impact': float(annual_impact),
            'control_avg_revenue': float(control_revenue),
            'treatment_avg_revenue': float(treatment_revenue),
            'statistical_power': float(len(df) / 1000),  # Rough power estimate
            'recommendation': 'Deploy - positive business impact' if revenue_lift > 0.02 else 'Monitor - marginal impact'
        }
    
    # Sample data for MCP
    business_data = create_sample_business_data()
    ltv_mapping = create_sample_ltv_data()
    
    mcp_data = {
        'df': business_data.to_dict(),
        'variant_column': 'variant',
        'control_label': 'control',
        'treatment_label': 'treatment',
        'revenue_column': 'monthly_revenue',
        'conversion_column': 'converted_to_paid',
        'ltv_mapping': ltv_mapping
    }
    
    mcp_results = analyze_business_impact_for_mcp(mcp_data)
    print("MCP Business Analysis Results:")
    print(f"  Revenue Lift: {mcp_results['revenue_lift_percent']:.1f}%")
    print(f"  Conversion Lift: {mcp_results['conversion_lift_percent']:.1f}%")
    print(f"  Annual Impact: ${mcp_results['estimated_annual_impact']:,.0f}")
    print(f"  Recommendation: {mcp_results['recommendation']}")
    print()

# =============================================================================
# COMPLETE WORKFLOW
# =============================================================================

def example_complete_workflow():
    """Complete business intelligence workflow"""
    
    print("=== Complete Business Intelligence Workflow ===")
    
    # 1. Data quality validation
    example_data_quality()
    
    # 2. Profitability analysis
    example_profitability_analysis()
    
    # 3. Financial metrics extraction
    example_financial_metrics()
    
    # 4. Segment impact analysis
    example_segment_impact()
    
    # 5. Business reporting
    example_business_reporting()
    
    # 6. Visualizations
    example_visualizations()
    
    print("Complete business intelligence workflow executed successfully!")

# =============================================================================
# RUN ALL EXAMPLES
# =============================================================================

if __name__ == "__main__":
    print("Running Business Intelligence Examples...\n")
    
    try:
        example_complete_workflow()
        example_mcp_integration()
        print("\n✅ All examples completed successfully!")
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        print("Make sure all dependencies are installed.")
        print("Required: pandas, numpy, matplotlib, plotly, scipy")