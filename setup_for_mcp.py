"""
Setup Script for MCP Integration

This script can be copied to your MCP directory to easily set up analytics utilities.
Just run this script in your MCP environment to get started.

Usage:
1. Copy this file to your MCP directory
2. Run: python setup_for_mcp.py
3. Follow the instructions to add imports to your MCP files
"""

import os
import sys
import shutil
from pathlib import Path


def setup_analytics_utilities_for_mcp():
    """Set up analytics utilities for use in MCP directory."""
    
    print("=== ANALYTICS UTILITIES MCP SETUP ===")
    
    # Define paths
    analytics_path = Path('/Users/saramcneilly/src/github.com/analytics_utilities')
    current_dir = Path.cwd()
    
    print(f"Current directory: {current_dir}")
    print(f"Analytics utilities path: {analytics_path}")
    
    # Check if analytics utilities exists
    if not analytics_path.exists():
        print("❌ Analytics utilities not found at expected path.")
        print("Please update the path in this script or ensure analytics_utilities is cloned.")
        return False
    
    # Check if we're in the right directory (should contain MCP files)
    mcp_files = list(current_dir.glob("*.py"))
    if not mcp_files:
        print("⚠️ No Python files found in current directory.")
        print("Make sure you're running this from your MCP project directory.")
    
    # Option 1: Create a symlink (recommended)
    symlink_path = current_dir / "analytics_utilities"
    if symlink_path.exists():
        print(f"✅ Analytics utilities already linked at: {symlink_path}")
    else:
        try:
            symlink_path.symlink_to(analytics_path)
            print(f"✅ Created symlink: {symlink_path} -> {analytics_path}")
        except Exception as e:
            print(f"⚠️ Could not create symlink: {e}")
            print("Will provide alternative setup instructions.")
    
    # Create a local import helper
    import_helper_content = f'''"""
Analytics Utilities Import Helper for MCP

This file provides easy imports for analytics utilities in your MCP project.
Generated automatically by setup_for_mcp.py
"""

import sys
from pathlib import Path

# Add analytics utilities to path
ANALYTICS_PATH = Path(r"{analytics_path}")
if str(ANALYTICS_PATH) not in sys.path:
    sys.path.append(str(ANALYTICS_PATH))

# Add src directory to path  
SRC_PATH = ANALYTICS_PATH / "src"
if str(SRC_PATH) not in sys.path:
    sys.path.append(str(SRC_PATH))

# Import the MCP integration module
try:
    from mcp_integration import *
    print("✅ Analytics utilities loaded successfully")
except ImportError as e:
    print(f"❌ Failed to import analytics utilities: {{e}}")
    print("Check that the analytics_utilities directory is accessible")

# Convenience imports for common functions
try:
    # Most commonly used functions
    from experiment_analysis.statistical_analysis import calculate_significance
    from experiment_analysis.slice_analysis import run_time_bound_analysis
    from business_impact.profitability_analysis import calculate_profitability_from_timebound_results
    from business_impact.data_quality import clean_df_for_analysis
    
    CORE_FUNCTIONS_LOADED = True
except ImportError:
    CORE_FUNCTIONS_LOADED = False
    print("⚠️ Some core functions could not be imported")

def run_experiment_workflow(df, slice_column=None):
    """Run a complete experiment analysis workflow."""
    if not CORE_FUNCTIONS_LOADED:
        print("❌ Core functions not loaded. Cannot run workflow.")
        return None
        
    # Clean data
    if slice_column:
        df_clean = clean_df_for_analysis(df, slice_column)
    else:
        df_clean = df
    
    # Statistical analysis
    significance = calculate_significance(df_clean)
    
    # Time-bound analysis
    timebound = run_time_bound_analysis(df_clean, slice_column=slice_column)
    
    # Business impact (if slice_column provided)
    business_impact = None
    if slice_column and timebound:
        try:
            business_impact = calculate_profitability_from_timebound_results(timebound)
        except Exception as e:
            print(f"Business impact calculation failed: {{e}}")
    
    return {{
        'significance': significance,
        'timebound': timebound, 
        'business_impact': business_impact
    }}
'''
    
    helper_file = current_dir / "analytics_import_helper.py"
    with open(helper_file, 'w') as f:
        f.write(import_helper_content)
    
    print(f"✅ Created import helper: {helper_file}")
    
    # Create example usage file
    example_content = '''"""
Example Analytics Usage in MCP

This file shows how to use analytics utilities in your MCP functions.
Copy and adapt these examples to your MCP handlers.
"""

# Import analytics utilities
from analytics_import_helper import *

def analyze_experiment_data(df, segment_column='growth_segment'):
    """
    Example function showing complete experiment analysis.
    
    This can be called from your MCP handlers to analyze experiment data.
    """
    
    print("Starting experiment analysis...")
    
    # Step 1: Data quality check
    validation = validate_experiment_data(df)
    print_validation_summary(validation)
    
    if not validation['is_valid']:
        # Clean the data
        df = clean_df_for_analysis(df, segment_column)
    
    # Step 2: Statistical significance
    significance = calculate_significance(df)
    print("\\nSignificance Results:")
    print(significance)
    
    # Step 3: Time-bound analysis by segment
    timebound_results = run_time_bound_analysis(
        df, 
        slice_column=segment_column,
        time_bounds=[5, 15, 30]
    )
    
    # Step 4: Business impact analysis
    if timebound_results:
        segment_impacts, total_impact = calculate_profitability_from_timebound_results(
            timebound_results,
            time_bound='5_days'
        )
        
        # Generate executive summary
        business_summary = prepare_business_summary(segment_impacts, total_impact)
        executive_summary = format_business_summary(business_summary)
        
        print("\\nBusiness Impact Analysis:")
        print(executive_summary)
        
        return {
            'significance': significance,
            'timebound_results': timebound_results,
            'business_impact': {
                'segment_impacts': segment_impacts,
                'total_impact': total_impact,
                'summary': business_summary
            }
        }
    
    return {
        'significance': significance,
        'timebound_results': timebound_results
    }


def create_experiment_visualizations(df, results, segment_column='growth_segment'):
    """
    Example function for creating experiment visualizations.
    """
    
    print("Creating visualizations...")
    
    # Daily uplift plot
    uplift_fig = plot_daily_l2c_uplift(df, slice_column=segment_column)
    
    # Business impact visualizations
    if 'business_impact' in results:
        segment_impacts = results['business_impact']['segment_impacts']
        total_impact = results['business_impact']['total_impact']
        
        # Profitability dashboard
        profitability_fig = visualize_segment_profitability(segment_impacts)
        
        # Executive summary
        executive_fig = create_executive_summary_plot(segment_impacts, total_impact)
        
        return {
            'uplift_plot': uplift_fig,
            'profitability_plot': profitability_fig,
            'executive_plot': executive_fig
        }
    
    return {'uplift_plot': uplift_fig}


# Example MCP handler function
def mcp_experiment_analysis_handler(request_data):
    """
    Example MCP handler that uses analytics utilities.
    
    This is how you might structure an MCP function to analyze experiment data.
    """
    
    try:
        # Extract data from request (adapt to your MCP structure)
        df = request_data.get('dataframe')
        segment_column = request_data.get('segment_column', 'growth_segment')
        
        if df is None:
            return {'error': 'No dataframe provided'}
        
        # Run analysis
        results = analyze_experiment_data(df, segment_column)
        
        # Create visualizations (optional)
        # viz_results = create_experiment_visualizations(df, results, segment_column)
        
        # Return results in format expected by MCP
        return {
            'status': 'success',
            'analysis_results': results,
            'recommendation': results.get('business_impact', {}).get('summary', {}).get('recommendation', 'No recommendation available')
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'error_message': str(e),
            'error_type': type(e).__name__
        }


if __name__ == "__main__":
    # Test the import
    print("Testing analytics utilities import...")
    check_imports()
    print("\\nExample functions available:")
    print("- analyze_experiment_data(df, segment_column)")
    print("- create_experiment_visualizations(df, results)")
    print("- mcp_experiment_analysis_handler(request_data)")
'''
    
    example_file = current_dir / "analytics_usage_example.py"
    with open(example_file, 'w') as f:
        f.write(example_content)
    
    print(f"✅ Created usage example: {example_file}")
    
    # Print setup instructions
    print("\\n=== SETUP COMPLETE ===")
    print("\\n📋 NEXT STEPS:")
    print("1. In your MCP Python files, add this import at the top:")
    print("   from analytics_import_helper import *")
    print("\\n2. Use analytics functions directly in your MCP handlers:")
    print("   results = calculate_significance(df)")
    print("   business_impact = calculate_profitability_from_timebound_results(timebound_results)")
    print("\\n3. Check the example file for complete usage patterns:")
    print(f"   {example_file}")
    print("\\n4. Test your imports:")
    print("   python -c 'from analytics_import_helper import check_imports; check_imports()'")
    
    return True


if __name__ == "__main__":
    setup_analytics_utilities_for_mcp()