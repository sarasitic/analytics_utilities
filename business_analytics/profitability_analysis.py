"""
Profitability Analysis Functions

This module provides functions for calculating the financial and business impact 
of A/B test results using LTV (Lifetime Value) data and segment-specific analysis.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Tuple, Optional, Any


# Default LTV mapping for different growth segments
DEFAULT_LTV_MAPPING = {
    'default': 379,
    'dropshipper_started': 138,
    'add_platformer_started': 413,
    'replatformer_started': 1005,
    'retail_started': 2051
}


def get_ltv_mapping(custom_mapping: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    """
    Get LTV mapping for growth segments.
    
    Parameters:
    -----------
    custom_mapping : dict, optional
        Custom LTV mapping to override defaults
        
    Returns:
    --------
    dict : LTV mapping by segment
    """
    if custom_mapping is not None:
        return custom_mapping
    return DEFAULT_LTV_MAPPING.copy()


def calculate_profitability_from_timebound_results(
    growth_segment_results: Dict[str, Any],
    time_bound: str = '5_days',
    ltv_mapping: Optional[Dict[str, float]] = None,
    verbose: bool = True
) -> Tuple[List[Dict], float]:
    """
    Calculate profitability impact using ACTUAL uplift values from time-bound analysis.
    
    Parameters:
    -----------
    growth_segment_results : dict
        Results dictionary from run_time_bound_analysis by growth segment
    time_bound : str, default='5_days'
        Which time bound to use for calculations ('5_days', 'unbound', etc.)
    ltv_mapping : dict, optional
        Custom LTV mapping by segment. If None, uses default mapping.
    verbose : bool, default=True
        Whether to print detailed analysis output
        
    Returns:
    --------
    tuple : (segment_results_list, total_annual_impact)
        - segment_results_list: List of dictionaries with segment analysis
        - total_annual_impact: Total annual financial impact across all segments
    """
    
    if ltv_mapping is None:
        ltv_mapping = get_ltv_mapping()
    
    if verbose:
        print("=== PROFITABILITY IMPACT FROM TIME-BOUND RESULTS ===")
        print("🎯 Using actual uplift values from your time-bound analysis\\n")
        
        print("💰 SEGMENT-LEVEL ROLLOUT IMPACT:")
        print(f"{'Segment':<25} {'Users':<10} {'GA Lift':<10} {'LTV/GA':<10} {'Addl GAs':<12} {'Annual Impact':<15}")
        print("-" * 95)
    
    total_annual_impact = 0
    segment_results = []
    
    # Extract data directly from time-bound results
    for segment_name, segment_data in growth_segment_results.items():
        if segment_data is None or time_bound not in segment_data:
            if verbose:
                print(f"⚠️  No {time_bound} data for {segment_name}")
            continue
            
        result_df = segment_data[time_bound]
        if result_df is None or len(result_df) == 0:
            if verbose:
                print(f"⚠️  Empty {time_bound} data for {segment_name}")
            continue
        
        try:
            # Extract uplift from treatment row (usually row 1)
            treatment_rows = result_df[result_df['variant'] != 'control']
            if len(treatment_rows) == 0:
                if verbose:
                    print(f"⚠️  No treatment data for {segment_name}")
                continue
                
            treatment_row = treatment_rows.iloc[0]
            uplift_str = treatment_row['uplift']
            uplift_value = float(str(uplift_str).replace('+', '').replace('%', '')) / 100
            
            # Extract control data from control row
            control_rows = result_df[result_df['variant'] == 'control']
            if len(control_rows) == 0:
                if verbose:
                    print(f"⚠️  No control data for {segment_name}")
                continue
                
            control_row = control_rows.iloc[0]
            control_users = control_row['total']
            control_conversions = control_row['conversions']
            
            # Get LTV for this segment
            segment_ltv = ltv_mapping.get(segment_name, ltv_mapping.get('default', 379))
            
            # Calculate financial impact
            additional_gas = control_conversions * uplift_value
            annual_impact = additional_gas * segment_ltv
            total_annual_impact += annual_impact
            
            segment_results.append({
                'segment': segment_name,
                'users': control_users,
                'ga_lift': uplift_value,
                'ltv': segment_ltv,
                'additional_gas': additional_gas,
                'annual_impact': annual_impact,
                'current_gas': control_conversions,
                'uplift_str': uplift_str
            })
            
            if verbose:
                print(f"{segment_name:<25} {control_users:<10,} {uplift_value:<+10.2%} ${segment_ltv:<9,.0f} {additional_gas:<12.0f} ${annual_impact:<14,.0f}")
            
        except Exception as e:
            if verbose:
                print(f"⚠️  Error processing {segment_name}: {e}")
            continue
    
    if verbose:
        print()
        print(f"🎯 TOTAL ANNUAL FINANCIAL IMPACT: ${total_annual_impact:,.0f}")
        
        # Detailed breakdown
        print(f"\\n📈 DETAILED BREAKDOWN BY SEGMENT:")
        for result in segment_results:
            impact_per_user = result['annual_impact'] / result['users'] if result['users'] > 0 else 0
            
            print(f"\\n{result['segment']}:")
            print(f"  • User base: {result['users']:,}")
            print(f"  • Current gross adds: {result['current_gas']:,}")
            print(f"  • GA lift: {result['ga_lift']:+.2%} (from time-bound analysis)")
            print(f"  • Additional gross adds: {result['additional_gas']:,.0f}")
            print(f"  • LTV per GA: ${result['ltv']:,}")
            print(f"  • Annual impact: ${result['annual_impact']:,.0f}")
            print(f"  • Impact per user: ${impact_per_user:.2f}")
        
        # Risk assessment
        negative_segments = [r for r in segment_results if r['annual_impact'] < 0]
        positive_segments = [r for r in segment_results if r['annual_impact'] > 0]
        
        print(f"\\n⚠️  RISK ASSESSMENT:")
        if negative_segments:
            negative_impact = sum([r['annual_impact'] for r in negative_segments])
            print(f"Segments with negative impact: {[r['segment'] for r in negative_segments]}")
            print(f"Total negative impact: ${negative_impact:,.0f}")
        
        if positive_segments:
            positive_impact = sum([r['annual_impact'] for r in positive_segments])
            print(f"Segments with positive impact: {[r['segment'] for r in positive_segments]}")
            print(f"Total positive impact: ${positive_impact:,.0f}")
        
        print(f"\\n🎯 RECOMMENDATION:")
        if total_annual_impact > 0:
            print(f"✅ RECOMMENDED: Full rollout has positive financial impact")
            print(f"   Expected annual revenue increase: ${total_annual_impact:,.0f}")
        else:
            print(f"⚠️  CAUTION: Full rollout has negative financial impact")
            print(f"   Expected annual revenue loss: ${abs(total_annual_impact):,.0f}")
    
    return segment_results, total_annual_impact


def calculate_segment_financial_impact(
    segment_name: str,
    control_users: int,
    control_conversions: int,
    uplift_value: float,
    segment_ltv: float
) -> Dict[str, Any]:
    """
    Calculate financial impact for a single segment.
    
    Parameters:
    -----------
    segment_name : str
        Name of the segment
    control_users : int
        Number of users in control group
    control_conversions : int
        Number of conversions in control group
    uplift_value : float
        Uplift as decimal (e.g., 0.05 for 5%)
    segment_ltv : float
        Lifetime value for this segment
        
    Returns:
    --------
    dict : Dictionary with financial impact calculations
    """
    
    # Calculate financial metrics
    additional_gas = control_conversions * uplift_value
    annual_impact = additional_gas * segment_ltv
    impact_per_user = annual_impact / control_users if control_users > 0 else 0
    
    return {
        'segment': segment_name,
        'users': control_users,
        'current_gas': control_conversions,
        'ga_lift': uplift_value,
        'ltv': segment_ltv,
        'additional_gas': additional_gas,
        'annual_impact': annual_impact,
        'impact_per_user': impact_per_user
    }


def visualize_segment_profitability(
    segment_impacts: List[Dict[str, Any]],
    figsize: Tuple[int, int] = (16, 12),
    show_plot: bool = True
) -> plt.Figure:
    """
    Create comprehensive profitability visualizations for segment analysis.
    
    Parameters:
    -----------
    segment_impacts : list
        List of segment impact dictionaries from calculate_profitability_from_timebound_results
    figsize : tuple, default=(16, 12)
        Figure size for the plot
    show_plot : bool, default=True
        Whether to display the plot
        
    Returns:
    --------
    matplotlib.figure.Figure : The created figure object
    """
    
    if not segment_impacts:
        print("No segment impact data to visualize")
        return None
    
    # Prepare data for visualization
    segments = [r['segment'] for r in segment_impacts]
    annual_impacts = [r['annual_impact'] for r in segment_impacts]
    ga_lifts = [r['ga_lift'] * 100 for r in segment_impacts]  # Convert to percentage
    user_counts = [r['users'] for r in segment_impacts]
    additional_gas = [r['additional_gas'] for r in segment_impacts]
    segment_ltvs = [r['ltv'] for r in segment_impacts]
    
    # Create figure with subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=figsize)
    
    # 1. Annual Financial Impact by Segment
    colors = ['#33b07a' if x > 0 else '#f67088' for x in annual_impacts]
    bars1 = ax1.bar(segments, annual_impacts, color=colors, alpha=0.7, edgecolor='black')
    ax1.set_title('Annual LTV Revenue Impact by Growth Segment\\n(Additional Gross Adds × Segment LTV)', 
                  fontsize=14, fontweight='bold')
    ax1.set_ylabel('Annual LTV Revenue Impact ($)')
    ax1.tick_params(axis='x', rotation=45)
    ax1.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    
    # Add value labels on bars
    for bar, value in zip(bars1, annual_impacts):
        if value != 0:
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (abs(bar.get_height()) * 0.01),
                    f'${value:,.0f}', ha='center', va='bottom' if value > 0 else 'top', fontsize=10)
    
    # 2. Gross Adds Lift by Segment
    colors2 = ['#33b07a' if x > 0 else '#f67088' for x in ga_lifts]
    bars2 = ax2.bar(segments, ga_lifts, color=colors2, alpha=0.7, edgecolor='black')
    ax2.set_title('Gross Adds (GA) Lift by Growth Segment', fontsize=14, fontweight='bold')
    ax2.set_ylabel('GA Lift (%)')
    ax2.tick_params(axis='x', rotation=45)
    ax2.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    
    # Add value labels on bars
    for bar, value in zip(bars2, ga_lifts):
        if value != 0:
            ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (abs(bar.get_height()) * 0.05),
                    f'{value:+.2f}%', ha='center', va='bottom' if value > 0 else 'top', fontsize=10)
    
    # 3. Impact vs GA Lift (bubble chart sized by LTV)
    impact_per_user = [r['annual_impact'] / r['users'] if r['users'] > 0 else 0 for r in segment_impacts]
    # Size bubbles by segment LTV (scaled for visibility)
    bubble_sizes = [ltv/10 for ltv in segment_ltvs]
    scatter = ax3.scatter(ga_lifts, impact_per_user, s=bubble_sizes, 
                         c=annual_impacts, cmap='RdYlGn', alpha=0.7, edgecolors='black')
    ax3.set_xlabel('Gross Adds Lift (%)')
    ax3.set_ylabel('Impact per User ($)')
    ax3.set_title('Impact per User vs GA Lift\\n(Bubble size = Segment LTV)', fontsize=14, fontweight='bold')
    ax3.axhline(y=0, color='black', linestyle='--', alpha=0.5)
    ax3.axvline(x=0, color='black', linestyle='--', alpha=0.5)
    
    # Add segment labels to scatter plot
    for i, segment in enumerate(segments):
        ax3.annotate(segment, (ga_lifts[i], impact_per_user[i]), 
                    xytext=(5, 5), textcoords='offset points', fontsize=9)
    
    # 4. Additional Gross Adds by Segment
    colors4 = ['#33b07a' if x > 0 else '#f67088' for x in additional_gas]
    bars4 = ax4.bar(segments, additional_gas, color=colors4, alpha=0.7, edgecolor='black')
    ax4.set_title('Additional Gross Adds by Segment', fontsize=14, fontweight='bold')
    ax4.set_ylabel('Additional Gross Adds')
    ax4.tick_params(axis='x', rotation=45)
    ax4.axhline(y=0, color='black', linestyle='-', linewidth=0.5)
    
    # Add value labels on bars
    for bar, value in zip(bars4, additional_gas):
        if value != 0:
            ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + (abs(bar.get_height()) * 0.01),
                    f'{value:+.0f}', ha='center', va='bottom' if value > 0 else 'top', fontsize=10)
    
    plt.tight_layout()
    
    if show_plot:
        plt.show()
    
    # Print summary table
    print("\\n📊 GROSS ADDS & LTV IMPACT SUMMARY:")
    print(f"{'Segment':<25} {'Users':<10} {'GA Lift':<10} {'Addl GAs':<10} {'LTV/GA':<10} {'Total Impact':<15}")
    print("-" * 90)
    
    for i, segment in enumerate(segments):
        print(f"{segment:<25} {user_counts[i]:<10,} {ga_lifts[i]:<+10.2f}% {additional_gas[i]:<10.0f} ${segment_ltvs[i]:<9,.0f} ${annual_impacts[i]:<14,.0f}")
    
    print(f"\\nTOTAL ADDITIONAL GROSS ADDS: {sum(additional_gas):,.0f}")
    print(f"TOTAL ANNUAL LTV REVENUE IMPACT: ${sum(annual_impacts):,.0f}")
    
    # Financial breakdown
    print(f"\\n💰 FINANCIAL BREAKDOWN:")
    print(f"Formula: Additional Gross Adds × LTV per GA = Revenue Impact")
    for i, segment in enumerate(segments):
        if additional_gas[i] != 0:
            print(f"{segment}: {additional_gas[i]:,.0f} × ${segment_ltvs[i]:,.0f} = ${annual_impacts[i]:,.0f}")
    
    return fig