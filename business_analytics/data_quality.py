"""
Data Quality Control Functions

This module provides functions for cleaning and validating experiment data
before running business impact analysis.
"""

import pandas as pd
import numpy as np
from typing import List, Optional, Dict, Any


def clean_df_for_analysis(
    df: pd.DataFrame,
    slice_column: str,
    min_records_per_slice: int = 1,
    variant_column: str = 'variant',
    verbose: bool = True
) -> pd.DataFrame:
    """
    Remove slices that are too small or have data quality issues.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Input dataframe to clean
    slice_column : str
        Name of the column containing slice values
    min_records_per_slice : int, default=1
        Minimum number of records required per slice per variant
    variant_column : str, default='variant'
        Column containing variant assignments
    verbose : bool, default=True
        Whether to print cleaning summary
        
    Returns:
    --------
    pandas.DataFrame
        Cleaned DataFrame with problematic slices removed
    """
    
    original_count = len(df)
    
    # Remove NULL values in slice column
    df_clean = df.dropna(subset=[slice_column])
    null_removed = original_count - len(df_clean)
    
    # Check minimum records per slice per variant
    slice_variant_counts = df_clean.groupby([slice_column, variant_column]).size()
    valid_slice_variants = slice_variant_counts[slice_variant_counts >= min_records_per_slice].index
    valid_slices = valid_slice_variants.get_level_values(0).unique()
    
    # Filter to only valid slices
    df_clean = df_clean[df_clean[slice_column].isin(valid_slices)]
    
    total_removed = original_count - len(df_clean)
    small_slices_removed = total_removed - null_removed
    
    if verbose:
        print(f"Data Quality Cleaning Summary:")
        print(f"  • Original records: {original_count:,}")
        print(f"  • Removed NULL {slice_column}: {null_removed:,}")
        print(f"  • Removed small slices: {small_slices_removed:,}")
        print(f"  • Final records: {len(df_clean):,}")
        print(f"  • Remaining slices: {list(df_clean[slice_column].unique())}")
        
        if len(valid_slices) == 0:
            print("⚠️  WARNING: No slices meet minimum size requirements!")
        
        # Show slice sizes
        slice_sizes = df_clean.groupby(slice_column).size().sort_values(ascending=False)
        print(f"\\nSlice sizes after cleaning:")
        for slice_val, count in slice_sizes.items():
            print(f"  • {slice_val}: {count:,} records")
    
    return df_clean


def validate_experiment_data(
    df: pd.DataFrame,
    required_columns: List[str] = None,
    variant_column: str = 'variant',
    conversion_column: str = 'conversion',
    assignment_column: str = 'first_assigned_at',
    min_sample_size: int = 30
) -> Dict[str, Any]:
    """
    Validate experiment data for business impact analysis.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Experiment data to validate
    required_columns : list, optional
        List of required columns. If None, uses default set.
    variant_column : str, default='variant'
        Column containing variant assignments
    conversion_column : str, default='conversion'
        Column containing conversion flags
    assignment_column : str, default='first_assigned_at'
        Column containing assignment timestamps
    min_sample_size : int, default=30
        Minimum sample size per variant
        
    Returns:
    --------
    dict : Validation results with warnings and recommendations
    """
    
    if required_columns is None:
        required_columns = [variant_column, conversion_column, assignment_column]
    
    validation_results = {
        'is_valid': True,
        'warnings': [],
        'errors': [],
        'recommendations': [],
        'summary': {}
    }
    
    # Check required columns
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        validation_results['errors'].append(f"Missing required columns: {missing_columns}")
        validation_results['is_valid'] = False
    
    if not validation_results['is_valid']:
        return validation_results
    
    # Check data types and conversions
    if assignment_column in df.columns:
        try:
            pd.to_datetime(df[assignment_column])
        except:
            validation_results['warnings'].append(f"Cannot convert {assignment_column} to datetime")
    
    # Check variant distribution
    if variant_column in df.columns:
        variant_counts = df[variant_column].value_counts()
        validation_results['summary']['variant_distribution'] = variant_counts.to_dict()
        
        # Check for control group
        control_variants = [v for v in variant_counts.index if 'control' in str(v).lower()]
        if not control_variants:
            validation_results['warnings'].append("No control variant found (expected variant containing 'control')")
        
        # Check sample sizes
        small_variants = variant_counts[variant_counts < min_sample_size]
        if len(small_variants) > 0:
            validation_results['warnings'].append(f"Small sample sizes: {small_variants.to_dict()}")
            validation_results['recommendations'].append(f"Consider combining variants or increasing sample size (minimum {min_sample_size})")
    
    # Check conversion rates
    if conversion_column in df.columns:
        try:
            # Handle both boolean and numeric conversion columns
            if df[conversion_column].dtype == 'object':
                # Try to convert to numeric
                df_temp = df.copy()
                df_temp[conversion_column] = pd.to_numeric(df_temp[conversion_column], errors='coerce')
                conversion_rate = df_temp[conversion_column].mean()
            else:
                conversion_rate = df[conversion_column].mean()
            
            validation_results['summary']['overall_conversion_rate'] = conversion_rate
            
            # Check for reasonable conversion rates
            if conversion_rate == 0:
                validation_results['warnings'].append("Zero conversion rate detected")
            elif conversion_rate == 1:
                validation_results['warnings'].append("Perfect conversion rate detected (100%)")
            elif conversion_rate > 0.5:
                validation_results['warnings'].append(f"High conversion rate ({conversion_rate:.1%}) - verify data quality")
            
            # Check conversion rates by variant
            if variant_column in df.columns:
                variant_conversion_rates = df.groupby(variant_column)[conversion_column].mean()
                validation_results['summary']['conversion_rates_by_variant'] = variant_conversion_rates.to_dict()
                
                # Check for extreme differences
                max_rate = variant_conversion_rates.max()
                min_rate = variant_conversion_rates.min()
                if max_rate - min_rate > 0.5:
                    validation_results['warnings'].append(f"Large conversion rate differences between variants ({min_rate:.1%} to {max_rate:.1%})")
        
        except Exception as e:
            validation_results['errors'].append(f"Error analyzing conversion rates: {str(e)}")
    
    # Check for duplicate records
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        validation_results['warnings'].append(f"{duplicate_count:,} duplicate records found")
        validation_results['recommendations'].append("Consider removing duplicate records")
    
    # Check for missing values
    missing_summary = df.isnull().sum()
    columns_with_missing = missing_summary[missing_summary > 0]
    if len(columns_with_missing) > 0:
        validation_results['summary']['missing_values'] = columns_with_missing.to_dict()
        validation_results['recommendations'].append("Review missing value patterns")
    
    # Calculate data completeness score
    total_cells = df.shape[0] * df.shape[1]
    missing_cells = df.isnull().sum().sum()
    completeness_score = 1 - (missing_cells / total_cells)
    validation_results['summary']['data_completeness'] = completeness_score
    
    if completeness_score < 0.95:
        validation_results['warnings'].append(f"Low data completeness: {completeness_score:.1%}")
    
    # Overall validation result
    if len(validation_results['errors']) == 0 and len(validation_results['warnings']) <= 2:
        validation_results['summary']['quality_status'] = 'Good'
    elif len(validation_results['errors']) == 0:
        validation_results['summary']['quality_status'] = 'Fair'
    else:
        validation_results['summary']['quality_status'] = 'Poor'
        validation_results['is_valid'] = False
    
    return validation_results


def print_validation_summary(validation_results: Dict[str, Any]) -> None:
    """
    Print a formatted summary of data validation results.
    
    Parameters:
    -----------
    validation_results : dict
        Results from validate_experiment_data
    """
    
    print("=== DATA VALIDATION SUMMARY ===")
    print(f"Overall Status: {validation_results['summary'].get('quality_status', 'Unknown')}")
    print(f"Valid for Analysis: {'✅ Yes' if validation_results['is_valid'] else '❌ No'}")
    
    # Print errors
    if validation_results['errors']:
        print(f"\\n❌ ERRORS:")
        for error in validation_results['errors']:
            print(f"  • {error}")
    
    # Print warnings
    if validation_results['warnings']:
        print(f"\\n⚠️  WARNINGS:")
        for warning in validation_results['warnings']:
            print(f"  • {warning}")
    
    # Print recommendations
    if validation_results['recommendations']:
        print(f"\\n💡 RECOMMENDATIONS:")
        for rec in validation_results['recommendations']:
            print(f"  • {rec}")
    
    # Print summary statistics
    summary = validation_results['summary']
    if summary:
        print(f"\\n📊 SUMMARY STATISTICS:")
        
        if 'variant_distribution' in summary:
            print(f"  Variant Distribution:")
            for variant, count in summary['variant_distribution'].items():
                print(f"    • {variant}: {count:,}")
        
        if 'overall_conversion_rate' in summary:
            print(f"  Overall Conversion Rate: {summary['overall_conversion_rate']:.2%}")
        
        if 'conversion_rates_by_variant' in summary:
            print(f"  Conversion Rates by Variant:")
            for variant, rate in summary['conversion_rates_by_variant'].items():
                print(f"    • {variant}: {rate:.2%}")
        
        if 'data_completeness' in summary:
            print(f"  Data Completeness: {summary['data_completeness']:.1%}")


def suggest_cleaning_actions(
    df: pd.DataFrame,
    validation_results: Dict[str, Any]
) -> List[str]:
    """
    Suggest specific cleaning actions based on validation results.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        Original dataframe
    validation_results : dict
        Results from validate_experiment_data
        
    Returns:
    --------
    list : List of suggested cleaning actions (as code strings)
    """
    
    suggestions = []
    
    # Handle duplicate records
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        suggestions.append("df_clean = df.drop_duplicates()")
    
    # Handle missing values in critical columns
    if 'missing_values' in validation_results['summary']:
        missing_dict = validation_results['summary']['missing_values']
        for col, missing_count in missing_dict.items():
            missing_pct = missing_count / len(df)
            if missing_pct > 0.1:  # More than 10% missing
                suggestions.append(f"# Consider removing or imputing {col} (missing {missing_pct:.1%})")
                suggestions.append(f"df_clean = df.dropna(subset=['{col}'])  # or use imputation")
    
    # Handle small variant groups
    if 'variant_distribution' in validation_results['summary']:
        variant_dist = validation_results['summary']['variant_distribution']
        small_variants = [v for v, count in variant_dist.items() if count < 30]
        if small_variants:
            suggestions.append(f"# Consider excluding small variants: {small_variants}")
            suggestions.append(f"df_clean = df[~df['variant'].isin({small_variants})]")
    
    # Handle extreme conversion rates
    if 'conversion_rates_by_variant' in validation_results['summary']:
        conv_rates = validation_results['summary']['conversion_rates_by_variant']
        extreme_variants = [v for v, rate in conv_rates.items() if rate == 0 or rate == 1]
        if extreme_variants:
            suggestions.append(f"# Review variants with extreme conversion rates: {extreme_variants}")
    
    if not suggestions:
        suggestions.append("# No automatic cleaning suggestions - data looks good!")
    
    return suggestions