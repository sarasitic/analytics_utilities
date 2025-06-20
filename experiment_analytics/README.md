# Experiment Analytics Package

A comprehensive toolkit for A/B testing and statistical analysis, designed for MCP integration and Jupyter notebook workflows.

## Overview

This package consolidates all experiment analysis functionality into a unified toolkit, providing statistical testing, forecasting, plotting, slice analysis, and time-bound analysis capabilities.

## Key Features

- **Statistical Analysis**: Robust significance testing with proper handling of edge cases
- **Query Management**: Batch loading and execution of SQL files
- **Interactive Visualizations**: Plotly-based charts for experiment results
- **Slice Analysis**: Multi-dimensional analysis across segments
- **Time-bound Analysis**: Conversion analysis within time windows
- **Forecasting**: Call volume modeling and impact prediction

## Installation

```python
from analytics_utilities.experiment_analytics import *
```

## Core Components

### Statistical Analysis
- `calculate_significance()` - Statistical significance testing
- `bootstrap_confidence_interval()` - Bootstrap confidence intervals
- `chi_square_test()` - Chi-square tests for independence

### Query Operations
- `load_queries_from_directory()` - Load SQL files in batch
- `execute_queries_batch()` - Execute multiple queries
- `auto_process_dataframes()` - Automatic data processing

### Plotting
- `plot_daily_l2c_uplift()` - Daily uplift visualizations
- `plot_assignment_distribution()` - Experiment assignment plots
- `plot_cumulative_calls_by_variant()` - Cumulative metric tracking

### Analysis
- `analyze_slice_distribution()` - Multi-dimensional slice analysis
- `analyze_first_call_timing()` - Event timing analysis
- `create_call_forecast_comparison()` - Forecasting comparisons

## Quick Start

```python
from analytics_utilities.experiment_analytics import *

# Load and process data
queries = load_queries_from_directory('../queries/')
dataframes = execute_queries_batch(queries, refresh_bq)
processed_data = auto_process_dataframes(dataframes)

# Statistical analysis
results = calculate_significance(df, 'treatment', 'control', 'conversion_rate')

# Visualization
fig = plot_daily_l2c_uplift(l2c_df, variants=['treatment', 'control'])

# Slice analysis
slice_results = analyze_slice_distribution(
    df=experiment_data,
    slice_column='segment',
    time_bounds=[None, 7, 14, 30]
)
```

## Dependencies

- pandas
- numpy
- statsmodels
- scipy
- matplotlib
- plotly
- google-cloud-bigquery (optional)