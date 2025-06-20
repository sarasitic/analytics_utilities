"""
Business Analytics Package

Business impact analysis and data operations toolkit.
Provides functions for calculating and visualizing the business and financial 
impact of A/B test results, including profitability analysis, LTV calculations, 
and enhanced visualization tools.
"""

from .profitability_analysis import (
    calculate_profitability_from_timebound_results,
    visualize_segment_profitability,
    get_ltv_mapping,
    calculate_segment_financial_impact
)

from .data_quality import (
    clean_df_for_analysis,
    validate_experiment_data
)

from .visualization_utils import (
    plot_extracted_uplifts,
    plot_extracted_uplifts_with_totals,
    plot_slice_uplift_comparison,
    get_color_mapping
)

from .data_extraction import (
    extract_uplift_and_totals,
    extract_financial_metrics,
    prepare_business_summary
)

__version__ = "1.0.0"
__all__ = [
    # Profitability analysis
    "calculate_profitability_from_timebound_results",
    "visualize_segment_profitability", 
    "get_ltv_mapping",
    "calculate_segment_financial_impact",
    
    # Data quality
    "clean_df_for_analysis",
    "validate_experiment_data",
    
    # Visualization utilities
    "plot_extracted_uplifts",
    "plot_extracted_uplifts_with_totals",
    "plot_slice_uplift_comparison",
    "get_color_mapping",
    
    # Data extraction
    "extract_uplift_and_totals",
    "extract_financial_metrics",
    "prepare_business_summary"
]