# Sessions Analysis Toolkit

A comprehensive, reusable toolkit for analyzing user session data with A/B testing capabilities, user segmentation, and migration analysis. This toolkit was extracted and refactored from the engagement analysis notebook to provide flexible, production-ready functions for session data analysis.

## 🚀 Quick Start

```python
from sessions_analysis_toolkit import SessionsAnalyzer
from sessions_plotting_toolkit import SessionsPlotter

# Initialize with your admin_sessions data
analyzer = SessionsAnalyzer(
    df=admin_sessions,
    metric_columns=['pageviews_in_first_session_count', 'admin_pageview_count'],
    variant_column='variant',
    user_id_column='shop_id'
)

# Run basic A/B test
results = analyzer.test_statistical_significance('pageviews_in_first_session_count')
analyzer.print_results(results, 'basic')

# Run segmentation analysis
seg_results = analyzer.segment_analysis('pageviews_in_first_session_count')
analyzer.print_results(seg_results, 'segment')
```

## 📁 Files Overview

| File | Description |
|------|-------------|
| `sessions_analysis_toolkit.py` | Core analysis functions and SessionsAnalyzer class |
| `sessions_plotting_toolkit.py` | Visualization functions and SessionsPlotter class |
| `sessions_analysis_examples.py` | Comprehensive examples and demonstrations |
| `SESSIONS_TOOLKIT_README.md` | This documentation file |

## 🔧 Core Features

### ✅ Statistical Significance Testing
- Multiple test types: Mann-Whitney U, Student's t-test, Welch's t-test, auto-selection
- Proper handling of non-normal distributions (common in web metrics)
- Confidence intervals and effect size calculations
- Batch testing across multiple metrics

### ✅ User Segmentation
- **Log-quantiles**: Equal-sized groups in log space (recommended for skewed data)
  - Segment_1: Lowest engagement users (bottom 1/3 in log space)
  - Segment_2: Medium engagement users (middle 1/3 in log space) 
  - Segment_3: Highest engagement users (top 1/3 in log space)
- **Raw quantiles**: Traditional equal-sized groups
  - Segment_1: Lowest metric values (bottom 1/3 in raw space)
  - Segment_2: Medium metric values (middle 1/3 in raw space)
  - Segment_3: Highest metric values (top 1/3 in raw space)
- **Log bins**: Equal-width bins in log space
- **Business rules**: Custom thresholds based on business logic
  - No_Engagement: Users with 0 activity
  - Low_Engagement: Users below business threshold (e.g., ≤2 pageviews)
  - Medium_Engagement: Users between thresholds (e.g., 3-6 pageviews)
  - High_Engagement: Users above highest threshold (e.g., 7+ pageviews)

### ✅ Segment Migration Analysis
- Chi-square test for independence
- Odds ratios for higher usage segments
- Cochran-Armitage test for ordinal trends
- Comprehensive migration visualizations

### ✅ Flexible Configuration
- Custom column names for any dataframe structure
- Configurable variant labels (control/treatment, baseline/experiment, etc.)
- Additional grouping dimensions (region, user type, etc.)
- Custom business thresholds for segmentation

### ✅ Comprehensive Visualizations
- A/B test plots with Central Limit Theorem demonstrations
- Segment comparison charts
- Migration analysis visualizations
- Ordinal trend plots
- Batch results summaries

## 📊 Expected Data Format

The toolkit is designed to work with admin_sessions-like data but is flexible for any session data:

```python
# Minimum required columns:
df = pd.DataFrame({
    'shop_id': [1, 2, 3, ...],                           # User identifier (individual merchants/businesses)
    'variant': ['control', 'treatment', ...],            # Experiment group
    'pageviews_in_first_session_count': [5, 12, 3, ...], # Metric to analyze (merchant engagement)
    # ... additional metrics and dimensions
})
```

**Note**: In the Shopify context, "business" refers to individual merchants/shops that use Shopify, not Shopify the company. Each row represents a merchant's usage patterns of the Shopify admin interface.

### Required Columns
- **User ID column**: Unique identifier for each user/shop
- **Variant column**: Experiment group assignment
- **Metric columns**: Numeric metrics to analyze

### Optional Columns
- **Additional dimensions**: Any categorical columns for grouping (region, user_type, etc.)

## 📖 Usage Examples

### Basic A/B Testing

```python
from sessions_analysis_toolkit import SessionsAnalyzer

# Initialize analyzer
analyzer = SessionsAnalyzer(
    df=your_dataframe,
    metric_columns=['pageviews_in_first_session_count'],
    variant_column='variant',
    user_id_column='shop_id'
)

# Test single metric
results = analyzer.test_statistical_significance('pageviews_in_first_session_count')
analyzer.print_results(results, 'basic')

# Test multiple metrics
batch_results = analyzer.batch_significance_test()
print(batch_results)
```

### User Segmentation Analysis

```python
# Log-quantiles segmentation (recommended)
seg_results = analyzer.segment_analysis(
    'pageviews_in_first_session_count',
    method='log_quantiles',
    n_segments=3
)
analyzer.print_results(seg_results, 'segment')

# Business-driven segmentation based on meaningful merchant engagement levels
# Define thresholds that represent actual business success patterns
business_thresholds = {'Low': 2, 'Medium': 6}  # Based on merchant onboarding success data
business_results = analyzer.segment_analysis(
    'pageviews_in_first_session_count',
    method='business',
    business_thresholds=business_thresholds
)
```

### Migration Analysis

```python
# Test if treatment moves users to higher segments
migration_results = analyzer.migration_analysis('pageviews_in_first_session_count')
analyzer.print_results(migration_results, 'migration')

# Test for ordinal trends
trend_results = analyzer.ordinal_trend_test('pageviews_in_first_session_count')
analyzer.print_results(trend_results, 'trend')
```

### Analysis by Additional Dimensions

```python
# Analyze by region
regional_results = analyzer.test_statistical_significance(
    'pageviews_in_first_session_count',
    groupby_dimensions=['region']
)

# Analyze by multiple dimensions
multi_results = analyzer.test_statistical_significance(
    'pageviews_in_first_session_count',
    groupby_dimensions=['region', 'user_type']
)
```

### Custom Column Names and Labels

```python
# Flexible configuration for any dataframe structure
custom_analyzer = SessionsAnalyzer(
    df=your_dataframe,
    metric_columns=['engagement_score', 'total_interactions'],
    variant_column='experiment_group',
    user_id_column='user_id',
    control_label='baseline',
    treatment_label='experiment'
)
```

### Visualization

```python
from sessions_plotting_toolkit import SessionsPlotter

# Initialize plotter
plotter = SessionsPlotter(analyzer)

# Plot A/B test results
results, fig = plotter.plot_ab_test('pageviews_in_first_session_count')
plt.show()

# Plot segment analysis
seg_results = analyzer.segment_analysis('pageviews_in_first_session_count')
fig = plotter.plot_segment_comparison(seg_results, 'pageviews_in_first_session_count')
plt.show()

# Plot migration analysis
migration_results = analyzer.migration_analysis('pageviews_in_first_session_count')
fig = plotter.plot_migration_analysis(migration_results)
plt.show()
```

## 🧪 Testing and Examples

Run the comprehensive examples to see the toolkit in action:

```python
python sessions_analysis_examples.py
```

This will run:
- Basic A/B testing examples
- Advanced segmentation and migration analysis
- Plotting demonstrations
- Flexibility tests with custom configurations

## 🔬 Statistical Methods

### Test Selection Logic
- **Mann-Whitney U**: Default for non-normal data (common with web metrics)
- **Welch's t-test**: For normal data with unequal variances
- **Student's t-test**: For normal data with equal variances
- **Auto-selection**: Automatically chooses based on normality and variance tests

### Segmentation Methods
1. **Log-quantiles** (Recommended): Equal-sized groups in log space, optimal for skewed data
   - Creates segments with equal sample sizes after log transformation
   - Segment_1: Bottom third of users when ranked by log(metric+1)
   - Segment_2: Middle third of users when ranked by log(metric+1)
   - Segment_3: Top third of users when ranked by log(metric+1)
2. **Raw quantiles**: Traditional approach, can be problematic for highly skewed data
   - Creates segments with equal sample sizes in raw metric space
   - May result in very different engagement levels if data is highly skewed
3. **Log bins**: Equal-width bins in log space, good for understanding distribution shape
4. **Business rules**: Custom thresholds based on domain knowledge and business context
   - Allows you to define meaningful engagement levels for your specific business
   - Example: "power users" might be those with 10+ pageviews per session

### Migration Analysis
- **Chi-square test**: Tests independence of segment distribution from treatment
- **Odds ratios**: Quantifies likelihood of being in higher segments
- **Cochran-Armitage trend test**: Tests for ordinal progression toward higher segments

## ⚙️ Configuration Options

### SessionsAnalyzer Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `df` | DataFrame | Required | Input dataframe with session data |
| `metric_columns` | List[str] | Required | Column names containing metrics to analyze |
| `variant_column` | str | 'variant' | Column containing variant labels |
| `user_id_column` | str | 'user_id' | Column containing unique user identifiers |
| `control_label` | str | 'control' | Label for control group |
| `treatment_label` | str | 'treatment' | Label for treatment group |
| `additional_dimensions` | List[str] | None | Additional columns for grouping |

### Test Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `alpha` | float | 0.05 | Significance level |
| `test_type` | str | 'auto' | Statistical test type |
| `min_segment_size` | int | 100 | Minimum size for segment analysis |

### Segmentation Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `method` | str | 'log_quantiles' | Segmentation method |
| `n_segments` | int | 3 | Number of segments to create |
| `business_thresholds` | Dict | None | Custom thresholds for business segmentation |

## 🤔 When to Use Each Method

### Segmentation Methods
- **Use log_quantiles when**: You have skewed data (typical for web metrics) and want equal sample sizes
- **Use raw_quantiles when**: You have normally distributed data or need absolute value thresholds
- **Use business when**: You have specific business thresholds that matter (e.g., "engaged merchants" = 10+ admin pageviews, "at-risk merchants" = <3 pageviews)
- **Use log_bins when**: You want to understand the shape of the distribution in log space

### Statistical Tests
- **Use mannwhitney when**: Data is skewed, ordinal, or you don't want to assume normality
- **Use welch when**: Data is approximately normal but group variances differ
- **Use ttest when**: Data is normal and variances are equal
- **Use auto when**: You want automatic selection based on data properties

### Analysis Types
- **Basic A/B testing**: When you want to know if treatment affects the overall metric
- **Segmentation analysis**: When you suspect treatment effects vary by user engagement level
- **Migration analysis**: When you want to know if treatment moves users between engagement tiers
- **Trend analysis**: When you want to test for progressive shifts toward higher engagement

## 🔍 Troubleshooting

### Common Issues

1. **ImportError**: Make sure all three files are in the same directory or properly installed
2. **Column not found**: Check that your column names match the configuration
3. **No significant results**: Consider using log_quantiles segmentation for skewed data
4. **Small sample sizes**: Increase min_segment_size or reduce n_segments
5. **Plotting errors**: Ensure matplotlib and seaborn are installed

### Data Quality Checks

```python
# Check your data before analysis
print(f"Data shape: {df.shape}")
print(f"Variants: {df['variant'].value_counts()}")
print(f"Missing values: {df.isnull().sum()}")
print(f"Metric distribution: {df['your_metric'].describe()}")
```

## 📝 Best Practices

1. **Always check data quality** before running analysis
2. **Use log_quantiles segmentation** for typical web metrics (pageviews, sessions, etc.)
3. **Validate results** by checking multiple segmentation methods
4. **Consider business context** when interpreting statistical significance
5. **Use visualizations** to communicate findings effectively
6. **Test on subgroups** to understand heterogeneous treatment effects

## 🚀 Integration with Existing Code

### Backward Compatibility
The toolkit includes convenience functions for backward compatibility:

```python
from sessions_analysis_toolkit import test_statistical_significance, print_significance_results

# Works just like the original notebook functions
results = test_statistical_significance(df, 'pageviews_in_first_session_count')
print_significance_results(results)
```

### Migration from Notebook
To migrate from the original notebook:

1. Replace individual function calls with SessionsAnalyzer class
2. Update column names in the analyzer configuration
3. Use the new print_results method with result_type parameter
4. Optional: Add plotting with SessionsPlotter class

## 📊 Performance Considerations

- **Large datasets**: The toolkit is optimized for datasets up to ~1M rows
- **Memory usage**: Segmentation creates copies of data, monitor memory for very large datasets
- **Plotting**: Plotting functions may be slower for large datasets; consider sampling for visualization

## 🔄 Extensions and Customization

The toolkit is designed to be extensible:

1. **Add new statistical tests** by extending the test_statistical_significance method
2. **Create custom segmentation methods** by adding new cases to the segment_users method
3. **Add new plot types** by extending the SessionsPlotter class
4. **Custom business logic** can be added through the business_thresholds parameter
   - Define meaningful engagement tiers based on your business context
   - Examples: Merchant success tiers, churn risk segments, onboarding completion levels

## 📚 References

- Mann-Whitney U test: Non-parametric test for comparing two independent samples
- Welch's t-test: Modified t-test for unequal variances
- Cochran-Armitage trend test: Test for trend in proportions across ordered groups
- Log-normal distribution: Common model for web analytics data

---

**Ready to analyze your admin_sessions data!** 🎯

For questions or issues, refer to the examples in `sessions_analysis_examples.py` or the original notebook for context.