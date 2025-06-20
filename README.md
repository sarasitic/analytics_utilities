# Analytics Utilities 📊

A comprehensive Python toolkit for A/B testing, user engagement analysis, and business intelligence. Designed for data analysts, product managers, and researchers conducting controlled experiments.

## 🎯 Overview

This repository consolidates battle-tested analytics utilities into three focused packages, optimized for both MCP (Model Context Protocol) integration and Jupyter notebook workflows.

## 📦 Package Libraries

### 🧪 [Experiment Analytics](./experiment_analytics/)
**A/B testing and statistical analysis toolkit**
- Statistical significance testing with robust error handling
- Interactive visualizations with Plotly
- Multi-dimensional slice analysis
- Time-bound conversion analysis  
- Forecasting and impact modeling
- SQL query management and BigQuery integration

```python
from analytics_utilities.experiment_analytics import *

# Statistical analysis
results = calculate_significance(df, 'treatment', 'control', 'conversion_rate')

# Interactive visualization
fig = plot_daily_l2c_uplift(l2c_df, variants=['treatment', 'control'])

# Slice analysis
slice_results = analyze_slice_distribution(df, 'segment', time_bounds=[7, 14, 30])
```

### 👥 [Engagement Analytics](./engagement_analytics/)
**User behavior and engagement analysis toolkit**
- Session analysis with flexible data schemas
- User journey mapping and funnel analysis
- Milestone and goal completion tracking
- Guide effectiveness measurement
- Behavioral segmentation with statistical rigor

```python
from analytics_utilities.engagement_analytics import *

# Session analysis
analyzer = SessionsAnalyzer(df, metric_columns=['pageviews'], variant_column='variant')
results = analyzer.test_statistical_significance('pageviews')

# User segmentation
seg_results = analyzer.segment_analysis('pageviews', method='log_quantiles')

# Migration analysis
migration_results = analyzer.migration_analysis('pageviews')
```

### 💰 [Business Analytics](./business_analytics/)
**Business impact and financial analysis toolkit**
- ROI and profitability calculations
- Customer lifetime value (LTV) integration
- Data quality validation and cleaning
- Executive-ready reporting and visualizations
- Business case development tools

```python
from analytics_utilities.business_analytics import *

# Financial impact analysis
profitability_results = calculate_profitability_from_timebound_results(
    experiment_results, ltv_mapping={'enterprise': 50000, 'smb': 3000}
)

# Business visualization
fig = visualize_segment_profitability(profitability_results)

# Executive reporting
summary = prepare_business_summary(metrics, "Q4 Feature Test")
```

## 🚀 Quick Start

### Installation

```bash
git clone https://github.com/saramcneilly/analytics_utilities.git
cd analytics_utilities
pip install -e .
```

### Basic Usage

```python
# Import everything
from analytics_utilities.experiment_analytics import *
from analytics_utilities.engagement_analytics import *  
from analytics_utilities.business_analytics import *

# Or import specific packages
from analytics_utilities import experiment_analytics
from analytics_utilities import engagement_analytics
from analytics_utilities import business_analytics
```

### MCP Integration

These packages are designed for seamless MCP integration:

```python
def analyze_experiment_for_mcp(data_dict):
    """MCP-compatible experiment analysis"""
    results = calculate_significance(
        df=pd.DataFrame(data_dict['df']),
        treatment_col=data_dict['treatment_col'],
        outcome_col=data_dict['outcome_col']
    )
    return {
        'significant': results['is_significant'],
        'p_value': float(results['p_value']),
        'effect_size': float(results['conversion_rate_difference']),
        'recommendation': 'Deploy' if results['is_significant'] else 'No effect'
    }
```

## 📋 Data Requirements

The toolkit works with standard experiment data formats:

```python
experiment_data = {
    'user_id': 'Unique participant identifier',
    'variant': 'Experiment group (control/treatment)',  
    'assigned_at': 'Assignment timestamp',
    'conversion_at': 'Conversion timestamp (nullable)',
    'segment': 'User segment for slice analysis',
    'metric_value': 'Numeric outcome metric'
}
```

## 🔧 Key Features

### Statistical Rigor
- Proper significance testing with multiple test corrections
- Bootstrap confidence intervals
- Non-parametric tests for skewed data
- Effect size calculations

### Flexible Data Handling
- Works with any column names (configurable)
- Handles missing data gracefully
- Supports multiple outcome metrics
- Time-bound analysis capabilities

### Production Ready
- Comprehensive error handling
- Edge case management (perfect conversion rates, small samples)
- Data quality validation
- Executive-level reporting

### Visualization Excellence
- Interactive Plotly charts
- Publication-ready static plots
- Business-focused dashboards
- Statistical significance indicators

## 📊 Use Cases

### For Data Analysts
- **A/B Test Analysis**: Complete statistical evaluation toolkit
- **User Segmentation**: Behavioral analysis across dimensions
- **Conversion Funnels**: Journey mapping and drop-off analysis
- **Time-Series Analysis**: Metric evolution over time

### For Product Managers  
- **Feature Impact**: Measure engagement and adoption
- **Business Cases**: ROI calculations for product investments
- **User Journey Optimization**: Identify friction points
- **Experiment Planning**: Sample size and power calculations

### For Executives
- **Financial Impact**: Revenue and profitability analysis
- **Risk Assessment**: Confidence intervals and effect sizes
- **Strategic Decisions**: Business-focused experiment summaries
- **Performance Tracking**: KPI monitoring and alerting

## 🏗️ Architecture

```
analytics_utilities/
├── experiment_analytics/          # A/B testing & statistics
│   ├── statistical_analysis.py   # Core significance testing
│   ├── plotting.py               # Interactive visualizations  
│   ├── slice_analysis.py         # Multi-dimensional analysis
│   ├── forecasting.py            # Impact modeling
│   └── query_loader.py           # SQL/BigQuery integration
│
├── engagement_analytics/         # User behavior analysis
│   ├── sessions_analysis_toolkit.py  # Session analysis
│   ├── journey_analysis.py           # User journey mapping
│   ├── milestone_analysis.py         # Goal completion
│   └── guide_analysis.py             # Guide effectiveness
│
└── business_analytics/           # Business intelligence
    ├── profitability_analysis.py # ROI calculations
    ├── data_quality.py           # Data validation
    ├── visualization_utils.py    # Business charts
    └── data_extraction.py        # Metrics extraction
```

## 🤝 Contributing

1. **Fork the repository**
2. **Create a feature branch**: `git checkout -b feature-name`
3. **Make your changes** with comprehensive tests
4. **Follow the coding standards**: Type hints, docstrings, error handling
5. **Submit a pull request** with a clear description

### Development Guidelines
- Maintain statistical rigor in all analysis functions
- Include comprehensive docstrings and examples
- Handle edge cases gracefully
- Follow the existing code patterns
- Add tests for new functionality

## 📚 Documentation

- [Experiment Analytics Guide](./experiment_analytics/README.md)
- [Engagement Analytics Guide](./engagement_analytics/README.md)  
- [Business Analytics Guide](./business_analytics/README.md)
- [Complete Examples](./examples/) (coming soon)

## 🔬 Dependencies

- **Core**: pandas, numpy, scipy, statsmodels
- **Visualization**: matplotlib, plotly, seaborn
- **Optional**: google-cloud-bigquery (for BigQuery integration)

```bash
pip install pandas numpy scipy statsmodels matplotlib plotly seaborn
# Optional: pip install google-cloud-bigquery
```

## 📄 License

MIT License - see [LICENSE](./LICENSE) for details.

## 🙋‍♀️ Support

- **Issues**: Report bugs and request features via [GitHub Issues](https://github.com/saramcneilly/analytics_utilities/issues)
- **Discussions**: Ask questions in [GitHub Discussions](https://github.com/saramcneilly/analytics_utilities/discussions)
- **Documentation**: Check the package-specific README files

## 🏆 Why Analytics Utilities?

✅ **Battle-tested** - Proven in production A/B testing environments  
✅ **Comprehensive** - End-to-end experiment analysis workflow  
✅ **Flexible** - Works with any data schema and business model  
✅ **Statistical** - Rigorous statistical methods with proper corrections  
✅ **Business-focused** - Translates results into actionable insights  
✅ **MCP-ready** - Designed for modern AI-assisted workflows  

---

**Transform your experiment data into actionable business insights! 🚀**