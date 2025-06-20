"""
Data Extraction Utilities for Business Impact Analysis

This module provides functions for extracting and transforming data 
from experiment analysis results for business impact calculations.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any, Union


def extract_uplift_and_totals(
    results: Dict[str, Any],
    time_bound: str = '5_days'
) -> Dict[str, Dict[str, float]]:
    """
    Extract numeric uplift values and sample sizes from run_time_bound_analysis results.
    
    Parameters:
    -----------
    results : dict
        Results dictionary from run_time_bound_analysis
    time_bound : str, default='5_days'
        Which time bound to extract from (e.g., '5_days', 'unbound')
        
    Returns:
    --------
    dict : Dictionary with slice names as keys and dict of {uplift, total} as values
    """
    
    slice_data_dict = {}
    
    for slice_name, slice_data in results.items():
        if time_bound not in slice_data or slice_data[time_bound] is None:
            continue
            
        # Handle both tuple format (display_df, current_data) and direct DataFrame format
        result_data = slice_data[time_bound]
        
        # Check if it's a tuple or direct DataFrame
        if isinstance(result_data, tuple) and len(result_data) == 2:
            display_df, current_data = result_data
        elif hasattr(result_data, 'columns') and 'variant' in result_data.columns:
            # It's a DataFrame directly
            display_df = result_data
        else:
            print(f"Warning: Unexpected data format for {slice_name} at {time_bound}")
            continue
        
        # Find treatment variants (non-control)
        treatment_rows = display_df[display_df['variant'] != 'control']
        
        if len(treatment_rows) > 0:
            # Take the first treatment if multiple exist
            treatment_row = treatment_rows.iloc[0]
            
            try:
                # Extract uplift string (e.g., "+1.20%")
                uplift_str = treatment_row['uplift']
                # Convert percentage string (e.g. "+1.20%") to decimal
                uplift_numeric = float(str(uplift_str).strip('%').strip('+')) / 100
                
                # Extract total (sample size)
                total_value = treatment_row['total']
                
                # Store both uplift and total
                slice_data_dict[slice_name] = {
                    'uplift': uplift_numeric,
                    'total': total_value
                }
                
            except Exception as e:
                print(f"Warning: Could not extract data for {slice_name}: {e}")
                continue
    
    return slice_data_dict


def extract_financial_metrics(
    results: Dict[str, Any],
    ltv_mapping: Dict[str, float],
    time_bound: str = '5_days'
) -> Dict[str, Dict[str, float]]:
    """
    Extract financial metrics from experiment results using LTV mapping.
    
    Parameters:
    -----------
    results : dict
        Results dictionary from run_time_bound_analysis
    ltv_mapping : dict
        Mapping of segment names to LTV values
    time_bound : str, default='5_days'
        Which time bound to extract from
        
    Returns:
    --------
    dict : Dictionary with segment financial metrics
    """
    
    financial_metrics = {}
    
    for segment_name, segment_data in results.items():
        if time_bound not in segment_data or segment_data[time_bound] is None:
            continue
            
        result_df = segment_data[time_bound]
        if result_df is None or len(result_df) == 0:
            continue
        
        try:
            # Extract treatment and control data
            treatment_rows = result_df[result_df['variant'] != 'control']
            control_rows = result_df[result_df['variant'] == 'control']
            
            if len(treatment_rows) == 0 or len(control_rows) == 0:
                continue
                
            treatment_row = treatment_rows.iloc[0]
            control_row = control_rows.iloc[0]
            
            # Extract uplift
            uplift_str = treatment_row['uplift']
            uplift_value = float(str(uplift_str).strip('%').strip('+')) / 100
            
            # Extract baseline metrics
            control_users = control_row['total']
            control_conversions = control_row['conversions']
            control_rate = control_row['conversion_rate']
            
            # Get LTV for this segment
            segment_ltv = ltv_mapping.get(segment_name, ltv_mapping.get('default', 379))
            
            # Calculate financial impact
            additional_gas = control_conversions * uplift_value
            annual_impact = additional_gas * segment_ltv
            impact_per_user = annual_impact / control_users if control_users > 0 else 0
            
            financial_metrics[segment_name] = {
                'users': control_users,
                'current_gas': control_conversions,
                'current_rate': control_rate,
                'uplift': uplift_value,
                'ltv': segment_ltv,
                'additional_gas': additional_gas,
                'annual_impact': annual_impact,
                'impact_per_user': impact_per_user
            }
            
        except Exception as e:
            print(f"Warning: Could not extract financial metrics for {segment_name}: {e}")
            continue
    
    return financial_metrics


def prepare_business_summary(
    segment_impacts: List[Dict[str, Any]],
    total_annual_impact: float
) -> Dict[str, Any]:
    """
    Prepare a comprehensive business summary from segment impact data.
    
    Parameters:
    -----------
    segment_impacts : list
        List of segment impact dictionaries
    total_annual_impact : float
        Total annual financial impact
        
    Returns:
    --------
    dict : Comprehensive business summary
    """
    
    if not segment_impacts:
        return {
            'total_annual_impact': 0,
            'recommendation': 'No data available',
            'summary_metrics': {},
            'risk_assessment': {},
            'segment_performance': {}
        }
    
    # Calculate summary metrics
    total_users = sum([r['users'] for r in segment_impacts])
    total_current_gas = sum([r['current_gas'] for r in segment_impacts])
    total_additional_gas = sum([r['additional_gas'] for r in segment_impacts])
    
    # Calculate weighted average uplift
    weighted_uplift_sum = sum([r['ga_lift'] * r['current_gas'] for r in segment_impacts])
    weighted_avg_uplift = weighted_uplift_sum / total_current_gas if total_current_gas > 0 else 0
    
    # Segment performance analysis
    positive_segments = [r for r in segment_impacts if r['annual_impact'] > 0]
    negative_segments = [r for r in segment_impacts if r['annual_impact'] < 0]
    neutral_segments = [r for r in segment_impacts if r['annual_impact'] == 0]
    
    positive_impact = sum([r['annual_impact'] for r in positive_segments])
    negative_impact = sum([r['annual_impact'] for r in negative_segments])
    
    # Risk assessment
    total_positive_users = sum([r['users'] for r in positive_segments])
    total_negative_users = sum([r['users'] for r in negative_segments])
    
    risk_score = abs(negative_impact) / (positive_impact + abs(negative_impact)) if (positive_impact + abs(negative_impact)) > 0 else 0
    
    # Best and worst performing segments
    best_segment = max(segment_impacts, key=lambda x: x['annual_impact'])
    worst_segment = min(segment_impacts, key=lambda x: x['annual_impact'])
    
    # Generate recommendation
    if total_annual_impact > 0:
        if risk_score < 0.2:
            recommendation = "STRONG PROCEED - Low risk, high reward"
        elif risk_score < 0.4:
            recommendation = "PROCEED - Positive impact with manageable risk"
        else:
            recommendation = "PROCEED WITH CAUTION - High risk but positive overall impact"
    else:
        if abs(total_annual_impact) < 10000:  # Small negative impact
            recommendation = "NEUTRAL - Minimal financial impact"
        else:
            recommendation = "DO NOT PROCEED - Negative financial impact"
    
    # ROI calculation (assuming implementation cost)
    estimated_implementation_cost = 50000  # Default assumption
    roi = (total_annual_impact / estimated_implementation_cost) if estimated_implementation_cost > 0 else 0
    
    return {
        'total_annual_impact': total_annual_impact,
        'recommendation': recommendation,
        'summary_metrics': {
            'total_users': total_users,
            'total_current_gas': total_current_gas,
            'total_additional_gas': total_additional_gas,
            'weighted_avg_uplift': weighted_avg_uplift,
            'estimated_roi': roi
        },
        'risk_assessment': {
            'risk_score': risk_score,
            'positive_segments_count': len(positive_segments),
            'negative_segments_count': len(negative_segments),
            'positive_impact': positive_impact,
            'negative_impact': negative_impact,
            'positive_user_coverage': total_positive_users / total_users if total_users > 0 else 0,
            'negative_user_coverage': total_negative_users / total_users if total_users > 0 else 0
        },
        'segment_performance': {
            'best_segment': {
                'name': best_segment['segment'],
                'impact': best_segment['annual_impact'],
                'uplift': best_segment['ga_lift']
            },
            'worst_segment': {
                'name': worst_segment['segment'],
                'impact': worst_segment['annual_impact'],
                'uplift': worst_segment['ga_lift']
            },
            'segment_count': len(segment_impacts)
        }
    }


def format_business_summary(business_summary: Dict[str, Any]) -> str:
    """
    Format business summary into a readable executive summary.
    
    Parameters:
    -----------
    business_summary : dict
        Business summary from prepare_business_summary
        
    Returns:
    --------
    str : Formatted executive summary
    """
    
    summary = business_summary
    metrics = summary['summary_metrics']
    risk = summary['risk_assessment']
    performance = summary['segment_performance']
    
    formatted_summary = f"""
=== BUSINESS IMPACT EXECUTIVE SUMMARY ===

🎯 RECOMMENDATION: {summary['recommendation']}
💰 TOTAL ANNUAL IMPACT: ${summary['total_annual_impact']:,.0f}

📊 KEY METRICS:
• Total User Base: {metrics['total_users']:,}
• Current Gross Adds: {metrics['total_current_gas']:,}
• Additional Gross Adds: {metrics['total_additional_gas']:,.0f}
• Weighted Average Uplift: {metrics['weighted_avg_uplift']:+.2%}
• Estimated ROI: {metrics['estimated_roi']:.1f}x

⚠️ RISK ASSESSMENT:
• Risk Score: {risk['risk_score']:.1%} (0% = no risk, 100% = high risk)
• Positive Segments: {risk['positive_segments_count']} (covering {risk['positive_user_coverage']:.1%} of users)
• Negative Segments: {risk['negative_segments_count']} (covering {risk['negative_user_coverage']:.1%} of users)
• Positive Impact: ${risk['positive_impact']:,.0f}
• Negative Impact: ${risk['negative_impact']:,.0f}

🏆 SEGMENT PERFORMANCE:
• Best Performing: {performance['best_segment']['name']} 
  (${performance['best_segment']['impact']:,.0f} impact, {performance['best_segment']['uplift']:+.2%} uplift)
• Worst Performing: {performance['worst_segment']['name']} 
  (${performance['worst_segment']['impact']:,.0f} impact, {performance['worst_segment']['uplift']:+.2%} uplift)
• Total Segments Analyzed: {performance['segment_count']}

===================================================
"""
    
    return formatted_summary


def extract_segment_comparison(
    results: Dict[str, Any],
    time_bounds: List[str] = ['5_days', 'unbound']
) -> pd.DataFrame:
    """
    Extract segment comparison data across multiple time bounds.
    
    Parameters:
    -----------
    results : dict
        Results dictionary from run_time_bound_analysis
    time_bounds : list, default=['5_days', 'unbound']
        List of time bounds to compare
        
    Returns:
    --------
    pandas.DataFrame : Comparison table with segments and time bounds
    """
    
    comparison_data = []
    
    for segment_name, segment_data in results.items():
        row_data = {'segment': segment_name}
        
        for time_bound in time_bounds:
            if time_bound not in segment_data or segment_data[time_bound] is None:
                row_data[f'{time_bound}_uplift'] = None
                row_data[f'{time_bound}_total'] = None
                continue
                
            result_df = segment_data[time_bound]
            if result_df is None or len(result_df) == 0:
                row_data[f'{time_bound}_uplift'] = None
                row_data[f'{time_bound}_total'] = None
                continue
            
            try:
                # Extract treatment data
                treatment_rows = result_df[result_df['variant'] != 'control']
                
                if len(treatment_rows) > 0:
                    treatment_row = treatment_rows.iloc[0]
                    
                    # Extract uplift
                    uplift_str = treatment_row['uplift']
                    uplift_numeric = float(str(uplift_str).strip('%').strip('+')) / 100
                    
                    # Extract total
                    total_value = treatment_row['total']
                    
                    row_data[f'{time_bound}_uplift'] = uplift_numeric
                    row_data[f'{time_bound}_total'] = total_value
                else:
                    row_data[f'{time_bound}_uplift'] = None
                    row_data[f'{time_bound}_total'] = None
                    
            except Exception as e:
                print(f"Warning: Could not extract {time_bound} data for {segment_name}: {e}")
                row_data[f'{time_bound}_uplift'] = None
                row_data[f'{time_bound}_total'] = None
        
        comparison_data.append(row_data)
    
    comparison_df = pd.DataFrame(comparison_data)
    
    # Add uplift difference columns
    if len(time_bounds) >= 2:
        for i in range(len(time_bounds) - 1):
            tb1, tb2 = time_bounds[i], time_bounds[i+1]
            comparison_df[f'uplift_diff_{tb1}_vs_{tb2}'] = (
                comparison_df[f'{tb1}_uplift'] - comparison_df[f'{tb2}_uplift']
            )
    
    return comparison_df


def create_data_export_package(
    segment_impacts: List[Dict[str, Any]],
    business_summary: Dict[str, Any],
    export_format: str = 'dict'
) -> Union[Dict[str, Any], pd.DataFrame]:
    """
    Create a comprehensive data export package for external use.
    
    Parameters:
    -----------
    segment_impacts : list
        List of segment impact dictionaries
    business_summary : dict
        Business summary from prepare_business_summary
    export_format : str, default='dict'
        Export format ('dict' or 'dataframe')
        
    Returns:
    --------
    dict or DataFrame : Comprehensive data package
    """
    
    if export_format == 'dataframe':
        # Create a flat DataFrame for easy export
        export_data = []
        
        for segment in segment_impacts:
            export_data.append({
                'segment_name': segment['segment'],
                'user_count': segment['users'],
                'current_gross_adds': segment['current_gas'],
                'ga_lift_percent': segment['ga_lift'] * 100,
                'ltv_per_ga': segment['ltv'],
                'additional_gross_adds': segment['additional_gas'],
                'annual_revenue_impact': segment['annual_impact'],
                'impact_per_user': segment['annual_impact'] / segment['users'] if segment['users'] > 0 else 0
            })
        
        df = pd.DataFrame(export_data)
        
        # Add summary row
        summary_row = {
            'segment_name': 'TOTAL',
            'user_count': business_summary['summary_metrics']['total_users'],
            'current_gross_adds': business_summary['summary_metrics']['total_current_gas'],
            'ga_lift_percent': business_summary['summary_metrics']['weighted_avg_uplift'] * 100,
            'ltv_per_ga': np.nan,  # Not applicable for total
            'additional_gross_adds': business_summary['summary_metrics']['total_additional_gas'],
            'annual_revenue_impact': business_summary['total_annual_impact'],
            'impact_per_user': business_summary['total_annual_impact'] / business_summary['summary_metrics']['total_users']
        }
        
        df = pd.concat([df, pd.DataFrame([summary_row])], ignore_index=True)
        
        return df
    
    else:  # dict format
        return {
            'segment_impacts': segment_impacts,
            'business_summary': business_summary,
            'export_metadata': {
                'generated_at': pd.Timestamp.now().isoformat(),
                'total_segments': len(segment_impacts),
                'recommendation': business_summary['recommendation'],
                'total_annual_impact': business_summary['total_annual_impact']
            }
        }