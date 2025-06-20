# Engagement Analytics Package

User behavior and engagement analysis toolkit for A/B testing, with specialized focus on session analysis, user journeys, and engagement metrics.

## Overview

This package provides comprehensive tools for analyzing user engagement patterns, session behavior, milestone achievement, and user journey analysis in A/B test environments.

## Key Features

- **Session Analysis**: Comprehensive analysis of user session patterns and behavior
- **User Journey Mapping**: Track user progression through conversion funnels
- **Milestone Analysis**: Analyze completion rates and timing of key events
- **Guide Analysis**: Measure effectiveness of onboarding guides and tutorials
- **Statistical Testing**: Specialized tests for engagement metrics

## Installation

```python
from analytics_utilities.engagement_analytics import *
```

## Core Components

### Session Analysis
- `SessionsAnalyzer` - Full-featured analysis toolkit
- `SessionsAnalyzerMinimal` - Lightweight version with core functionality
- `SessionsPlotter` - Visualization toolkit for session data

### Journey Analysis
- `map_user_journey()` - Track user progression through steps
- `analyze_conversion_funnel()` - Funnel analysis with drop-off points
- `analyze_segment_migration()` - Movement between engagement segments

### Milestone Analysis
- `track_milestone_achievement()` - Completion rate analysis
- `analyze_milestone_timing()` - Time-to-completion analysis
- `run_comprehensive_analysis()` - Full milestone analysis suite

### Guide Analysis
- `analyze_guide_engagement()` - Guide effectiveness measurement
- `track_guide_completion()` - Completion tracking across variants

## Quick Start

### Basic Session Analysis
```python
from analytics_utilities.engagement_analytics import SessionsAnalyzer

# Initialize analyzer
analyzer = SessionsAnalyzer(
    df=admin_sessions,
    metric_columns=['pageviews_in_first_session_count'],
    variant_column='variant',
    user_id_column='shop_id'
)

# Statistical significance testing
results = analyzer.test_statistical_significance('pageviews_in_first_session_count')
analyzer.print_results(results, 'basic')

# Segmentation analysis
seg_results = analyzer.segment_analysis(
    'pageviews_in_first_session_count',
    method='log_quantiles',
    n_segments=3
)
```

### User Journey Analysis
```python
from analytics_utilities.engagement_analytics import *

# Analyze conversion funnel
funnel_results = analyze_conversion_funnel(
    df=user_data,
    steps=['signup', 'first_action', 'conversion'],
    variant_column='treatment_group'
)

# Track milestone completion
milestone_results = track_milestone_achievement(
    df=milestone_data,
    milestone_column='milestone_reached',
    time_window_days=30
)
```

## Specialized Features

### Flexible Column Names
Works with any DataFrame structure - specify your column names:

```python
analyzer = SessionsAnalyzer(
    df=custom_df,
    metric_columns=['engagement_score'],
    variant_column='experiment_group',
    user_id_column='user_id',
    control_label='baseline',
    treatment_label='experiment'
)
```

### Multi-Dimensional Analysis
Slice analysis by additional dimensions:

```python
regional_results = analyzer.test_statistical_significance(
    'pageviews_in_first_session_count',
    groupby_dimensions=['region', 'user_segment']
)
```

## Dependencies

- pandas
- numpy
- scipy
- matplotlib
- seaborn (for full version)