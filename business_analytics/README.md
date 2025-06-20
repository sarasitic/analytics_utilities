# Business Analytics Package

Business impact analysis and data operations toolkit for measuring financial impact of A/B tests and maintaining data quality.

## Overview

This package provides tools for analyzing the business and financial impact of experiments, including profitability analysis, LTV calculations, data quality validation, and business-focused visualizations.

## Key Features

- **Profitability Analysis**: Calculate ROI and financial impact of experiments
- **Data Quality**: Validation and cleaning utilities for business data
- **Financial Metrics**: LTV, revenue, and profitability calculations
- **Business Visualizations**: Charts focused on business KPIs
- **Data Extraction**: Tools for preparing business summary reports

## Installation

```python
from analytics_utilities.business_analytics import *
```

## Core Components

### Profitability Analysis
- `calculate_profitability_from_timebound_results()` - Financial impact calculation
- `visualize_segment_profitability()` - Profitability visualization by segment
- `get_ltv_mapping()` - Customer lifetime value mapping
- `calculate_segment_financial_impact()` - Segment-level financial analysis

### Data Quality
- `clean_df_for_analysis()` - Data cleaning and preparation
- `validate_experiment_data()` - Experiment data validation
- `check_data_completeness()` - Data completeness checks

### Visualization
- `plot_extracted_uplifts()` - Uplift visualization
- `plot_slice_uplift_comparison()` - Segment comparison charts
- `create_business_dashboard()` - Business KPI dashboards

### Data Extraction
- `extract_uplift_and_totals()` - Extract key metrics
- `extract_financial_metrics()` - Financial data extraction
- `prepare_business_summary()` - Business summary reports

## Quick Start

### Profitability Analysis
```python
from analytics_utilities.business_analytics import *

# Calculate financial impact
profitability_results = calculate_profitability_from_timebound_results(
    timebound_results=experiment_results,
    ltv_mapping=customer_ltv_data,
    segment_column='customer_segment'
)

# Visualize profitability by segment
fig = visualize_segment_profitability(
    profitability_results,
    title="Revenue Impact by Customer Segment"
)
```

### Data Quality Validation
```python
# Clean and validate data
cleaned_data = clean_df_for_analysis(raw_experiment_data)
validation_report = validate_experiment_data(cleaned_data)

# Check data completeness
completeness_check = check_data_completeness(
    df=business_data,
    required_columns=['revenue', 'customer_id', 'variant']
)
```

### Business Reporting
```python
# Extract key business metrics
business_metrics = extract_financial_metrics(
    df=experiment_results,
    revenue_column='total_revenue',
    cost_column='acquisition_cost'
)

# Create business summary
summary_report = prepare_business_summary(
    metrics=business_metrics,
    experiment_name="Q4 Pricing Test"
)
```

## Business-Focused Features

### LTV Integration
```python
# Map customer segments to LTV values
ltv_mapping = get_ltv_mapping(
    segment_data=customer_segments,
    ltv_column='customer_lifetime_value'
)

# Calculate segment-level financial impact
impact_analysis = calculate_segment_financial_impact(
    experiment_results=results,
    ltv_mapping=ltv_mapping,
    time_horizon_months=12
)
```

### ROI Calculations
```python
# Calculate experiment ROI
roi_metrics = calculate_roi_metrics(
    revenue_lift=0.05,  # 5% revenue increase
    implementation_cost=50000,
    customer_base_size=100000
)
```

## Dependencies

- pandas
- numpy
- matplotlib
- plotly
- scipy (for statistical calculations)