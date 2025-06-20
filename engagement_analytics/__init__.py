"""
Engagement Analytics Package

Comprehensive user behavior and engagement analysis toolkit.
Consolidates session analysis, user journeys, milestones, and guide analysis.
"""

# Session analysis
from .sessions_analysis_toolkit import SessionsAnalyzer
from .sessions_analysis_minimal import SessionsAnalyzerMinimal  
from .sessions_plotting_toolkit import SessionsPlotter

# Guide analysis
from .guide_analysis import (
    analyze_milestone_completion,
    analyze_metric_completion,
    analyze_guidance_completion,
    analyze_component_milestone_correlations_by_variant
)

# Journey analysis
from .journey_analysis import (
    create_position_distribution_for_variant,
    analyze_all_tasks_conversion,
    analyze_segment_migration,
    test_ordinal_segment_trend
)

# Milestone analysis
from .milestone_analysis import (
    analyze_event_completion_generic,
    plot_event_completion_generic,
    run_comprehensive_analysis,
    analyze_milestone_completion_by_time_window
)

__version__ = "0.1.0"
__all__ = [
    # Session analysis
    'SessionsAnalyzer',
    'SessionsAnalyzerMinimal', 
    'SessionsPlotter',
    
    # Guide analysis
    'analyze_milestone_completion',
    'analyze_metric_completion',
    'analyze_guidance_completion',
    'analyze_component_milestone_correlations_by_variant',
    
    # Journey analysis  
    'create_position_distribution_for_variant',
    'analyze_all_tasks_conversion',
    'analyze_segment_migration',
    'test_ordinal_segment_trend',
    
    # Milestone analysis
    'analyze_event_completion_generic',
    'plot_event_completion_generic',
    'run_comprehensive_analysis',
    'analyze_milestone_completion_by_time_window'
]