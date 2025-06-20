"""
Sessions Analysis Toolkit - Minimal Version

A minimal version of the sessions analysis toolkit that only requires pandas and numpy.
This version implements basic statistical tests without external dependencies like scipy.

Key Features:
- Basic A/B testing with simple statistical tests
- User segmentation (quantile-based and business rules)
- Flexible column naming and configuration
- Works with only pandas and numpy

Usage Example:
    from sessions_analysis_minimal import SessionsAnalyzer
    
    analyzer = SessionsAnalyzer(
        df=admin_sessions,
        metric_columns=['pageviews_in_first_session_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    # Run basic A/B test
    results = analyzer.test_statistical_significance('pageviews_in_first_session_count')
    analyzer.print_results(results)
"""

import pandas as pd
import numpy as np
import warnings
from typing import Dict, List, Optional, Tuple, Union, Any
warnings.filterwarnings('ignore')


class SessionsAnalyzer:
    """
    Minimal sessions analyzer that only requires pandas and numpy.
    
    This version implements basic statistical functionality without external dependencies.
    
    Parameters:
    -----------
    df : pandas.DataFrame
        The input dataframe containing session data
    metric_columns : list of str
        List of column names containing metrics to analyze
    variant_column : str, default='variant'
        Column name containing variant labels (e.g., 'control', 'treatment')
    user_id_column : str, default='user_id'
        Column name containing unique user identifiers
    control_label : str, default='control'
        Label for control group in variant column
    treatment_label : str, default='treatment'
        Label for treatment group in variant column
    additional_dimensions : list of str, optional
        Additional columns to use for slicing/grouping analysis
    """
    
    def __init__(self, 
                 df: pd.DataFrame,
                 metric_columns: List[str],
                 variant_column: str = 'variant',
                 user_id_column: str = 'user_id',
                 control_label: str = 'control',
                 treatment_label: str = 'treatment',
                 additional_dimensions: Optional[List[str]] = None):
        
        self.df = df.copy()
        self.metric_columns = metric_columns
        self.variant_column = variant_column
        self.user_id_column = user_id_column
        self.control_label = control_label
        self.treatment_label = treatment_label
        self.additional_dimensions = additional_dimensions or []
        
        # Validate inputs
        self._validate_inputs()
        
    def _validate_inputs(self):
        """Validate that required columns exist in the dataframe."""
        required_columns = [self.variant_column, self.user_id_column] + self.metric_columns
        missing_columns = [col for col in required_columns if col not in self.df.columns]
        
        if missing_columns:
            raise ValueError(f"Missing columns in dataframe: {missing_columns}")
        
        # Check if variant labels exist
        available_variants = self.df[self.variant_column].unique()
        if self.control_label not in available_variants:
            raise ValueError(f"Control label '{self.control_label}' not found in {self.variant_column}")
        if self.treatment_label not in available_variants:
            raise ValueError(f"Treatment label '{self.treatment_label}' not found in {self.variant_column}")

    def _simple_ttest(self, control_data: np.ndarray, treatment_data: np.ndarray) -> Tuple[float, float]:
        """
        Simple two-sample t-test implementation using only numpy.
        
        Returns:
        --------
        tuple : (t_statistic, p_value_approx)
        """
        n1, n2 = len(control_data), len(treatment_data)
        mean1, mean2 = np.mean(control_data), np.mean(treatment_data)
        var1, var2 = np.var(control_data, ddof=1), np.var(treatment_data, ddof=1)
        
        # Pooled standard error
        pooled_se = np.sqrt(var1/n1 + var2/n2)
        
        # T-statistic
        t_stat = (mean2 - mean1) / pooled_se
        
        # Degrees of freedom (Welch's formula)
        df = (var1/n1 + var2/n2)**2 / ((var1/n1)**2/(n1-1) + (var2/n2)**2/(n2-1))
        
        # Approximate p-value using normal distribution (good for large samples)
        # For exact p-value, would need scipy.stats.t
        p_value_approx = 2 * (1 - self._normal_cdf(abs(t_stat)))
        
        return t_stat, p_value_approx
    
    def _normal_cdf(self, x: float) -> float:
        """Approximate standard normal CDF using error function approximation."""
        # Simple approximation for standard normal CDF
        # More accurate would use scipy.stats.norm.cdf
        return 0.5 * (1 + self._erf(x / np.sqrt(2)))
    
    def _erf(self, x: float) -> float:
        """Approximate error function."""
        # Abramowitz and Stegun approximation
        a1, a2, a3, a4, a5 = 0.254829592, -0.284496736, 1.421413741, -1.453152027, 1.061405429
        p = 0.3275911
        
        sign = 1 if x >= 0 else -1
        x = abs(x)
        
        t = 1.0 / (1.0 + p * x)
        y = 1.0 - (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t * np.exp(-x * x)
        
        return sign * y
    
    def _mann_whitney_u(self, control_data: np.ndarray, treatment_data: np.ndarray) -> Tuple[float, float]:
        """
        Simple Mann-Whitney U test implementation using only numpy.
        
        Returns:
        --------
        tuple : (u_statistic, p_value_approx)
        """
        n1, n2 = len(control_data), len(treatment_data)
        
        # Combine and rank
        combined = np.concatenate([control_data, treatment_data])
        ranks = np.argsort(np.argsort(combined)) + 1  # Convert to ranks (1-based)
        
        # Sum of ranks for control group
        r1 = np.sum(ranks[:n1])
        
        # U statistics
        u1 = r1 - n1 * (n1 + 1) / 2
        u2 = n1 * n2 - u1
        
        u_stat = min(u1, u2)
        
        # Normal approximation for large samples
        mean_u = n1 * n2 / 2
        std_u = np.sqrt(n1 * n2 * (n1 + n2 + 1) / 12)
        
        z_stat = (u_stat - mean_u) / std_u
        p_value_approx = 2 * (1 - self._normal_cdf(abs(z_stat)))
        
        return u_stat, p_value_approx
    
    def test_statistical_significance(self, 
                                    metric_column: str,
                                    alpha: float = 0.05,
                                    test_type: str = 'ttest',
                                    groupby_dimensions: Optional[List[str]] = None) -> Union[Dict[str, Any], pd.DataFrame]:
        """
        Test statistical significance between control and treatment groups.
        
        Parameters:
        -----------
        metric_column : str
            Column name to test for statistical significance
        alpha : float, default=0.05
            Significance level
        test_type : str, default='ttest'
            Type of test to perform ('ttest' or 'mannwhitney')
        groupby_dimensions : list of str, optional
            Additional dimensions to group by for analysis
            
        Returns:
        --------
        dict or pd.DataFrame : Test results
        """
        
        if groupby_dimensions:
            return self._test_by_groups(metric_column, alpha, test_type, groupby_dimensions)
        
        # Filter data for control and treatment groups
        control_data = self.df[self.df[self.variant_column] == self.control_label][metric_column].dropna().values
        treatment_data = self.df[self.df[self.variant_column] == self.treatment_label][metric_column].dropna().values
        
        if len(control_data) == 0 or len(treatment_data) == 0:
            return {
                'error': f'No data found for control or treatment groups',
                'control_size': len(control_data),
                'treatment_size': len(treatment_data)
            }
        
        # Basic descriptive statistics
        control_mean = np.mean(control_data)
        treatment_mean = np.mean(treatment_data)
        control_std = np.std(control_data, ddof=1)
        treatment_std = np.std(treatment_data, ddof=1)
        control_median = np.median(control_data)
        treatment_median = np.median(treatment_data)
        
        # Calculate effect size
        relative_lift = ((treatment_mean - control_mean) / control_mean) * 100 if control_mean != 0 else np.inf
        absolute_lift = treatment_mean - control_mean
        
        # Perform statistical test
        if test_type == 'ttest':
            statistic, p_value = self._simple_ttest(control_data, treatment_data)
            test_name = "Simple t-test (approximate)"
        elif test_type == 'mannwhitney':
            statistic, p_value = self._mann_whitney_u(control_data, treatment_data)
            test_name = "Mann-Whitney U test (approximate)"
        else:
            return {'error': f'Unknown test type: {test_type}. Use "ttest" or "mannwhitney"'}
        
        return {
            'metric_column': metric_column,
            'test_name': test_name,
            'p_value': p_value,
            'is_significant': p_value < alpha,
            'alpha': alpha,
            'statistic': statistic,
            'control_mean': control_mean,
            'treatment_mean': treatment_mean,
            'absolute_lift': absolute_lift,
            'relative_lift_percent': relative_lift,
            'control_size': len(control_data),
            'treatment_size': len(treatment_data),
            'control_std': control_std,
            'treatment_std': treatment_std,
            'control_median': control_median,
            'treatment_median': treatment_median
        }
    
    def _test_by_groups(self, metric_column: str, alpha: float, test_type: str, groupby_dimensions: List[str]) -> pd.DataFrame:
        """Run statistical tests grouped by additional dimensions."""
        results = []
        
        for group_values, group_data in self.df.groupby(groupby_dimensions):
            if not isinstance(group_values, tuple):
                group_values = (group_values,)
            
            # Create temporary analyzer for this group
            temp_analyzer = SessionsAnalyzer(
                df=group_data,
                metric_columns=[metric_column],
                variant_column=self.variant_column,
                user_id_column=self.user_id_column,
                control_label=self.control_label,
                treatment_label=self.treatment_label
            )
            
            result = temp_analyzer.test_statistical_significance(metric_column, alpha, test_type)
            
            if 'error' not in result:
                # Add group information
                group_dict = dict(zip(groupby_dimensions, group_values))
                result.update(group_dict)
                results.append(result)
        
        return pd.DataFrame(results)
    
    def batch_significance_test(self, 
                              metric_columns: Optional[List[str]] = None,
                              alpha: float = 0.05,
                              test_type: str = 'ttest',
                              groupby_dimensions: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Run significance tests on multiple columns and return a summary DataFrame.
        
        Parameters:
        -----------
        metric_columns : list of str, optional
            List of column names to test. If None, uses self.metric_columns
        alpha : float, default=0.05
            Significance level
        test_type : str, default='ttest'
            Type of test to perform
        groupby_dimensions : list of str, optional
            Additional dimensions to group by for analysis
            
        Returns:
        --------
        pd.DataFrame : Summary of all test results
        """
        
        if metric_columns is None:
            metric_columns = self.metric_columns
        
        results_list = []
        
        for col in metric_columns:
            if col not in self.df.columns:
                print(f"⚠️  Warning: Column '{col}' not found in DataFrame")
                continue
                
            result = self.test_statistical_significance(col, alpha, test_type, groupby_dimensions)
            
            if isinstance(result, pd.DataFrame):
                # If grouped results, add metric column and append all rows
                result['metric'] = col
                results_list.append(result)
            elif 'error' not in result:
                # Single result
                summary_result = {
                    'metric': col,
                    'control_mean': result['control_mean'],
                    'treatment_mean': result['treatment_mean'],
                    'absolute_lift': result['absolute_lift'],
                    'relative_lift_percent': result['relative_lift_percent'],
                    'p_value': result['p_value'],
                    'is_significant': result['is_significant'],
                    'test_name': result['test_name'],
                    'control_size': result['control_size'],
                    'treatment_size': result['treatment_size']
                }
                
                if groupby_dimensions:
                    for dim in groupby_dimensions:
                        summary_result[dim] = 'All'
                        
                results_list.append(summary_result)
        
        if not results_list:
            return pd.DataFrame()
        
        if isinstance(results_list[0], pd.DataFrame):
            summary_df = pd.concat(results_list, ignore_index=True)
        else:
            summary_df = pd.DataFrame(results_list)
            
        if not summary_df.empty:
            summary_df['significance'] = summary_df['is_significant'].map({True: '✅', False: '❌'})
            numeric_columns = ['control_mean', 'treatment_mean', 'absolute_lift', 'relative_lift_percent', 'p_value']
            for col in numeric_columns:
                if col in summary_df.columns:
                    summary_df[col] = summary_df[col].round(4)
        
        return summary_df
    
    def segment_users(self, 
                     metric_column: str,
                     method: str = 'quantiles',
                     n_segments: int = 3,
                     business_thresholds: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        """
        Segment users into usage groups for analysis.
        
        Parameters:
        -----------
        metric_column : str
            Column name with the metric to segment on
        method : str, default='quantiles'
            Segmentation method: 'quantiles', 'log_quantiles', or 'business'
        n_segments : int, default=3
            Number of segments to create (ignored for 'business' method)
        business_thresholds : dict, optional
            Thresholds for business segmentation
            
        Returns:
        --------
        pd.DataFrame : DataFrame with added 'usage_segment' column
        """
        
        df = self.df.copy()
        
        # Clean the data
        df = df.dropna(subset=[metric_column])
        df[metric_column] = pd.to_numeric(df[metric_column], errors='coerce')
        df = df.dropna(subset=[metric_column])
        
        try:
            if method == 'quantiles':
                df['usage_segment'] = pd.qcut(
                    df[metric_column], 
                    q=n_segments, 
                    labels=[f'Segment_{i+1}' for i in range(n_segments)],
                    duplicates='drop'
                )
                
            elif method == 'log_quantiles':
                # Create log-transformed version
                df['log_metric'] = np.log1p(df[metric_column])
                df['usage_segment'] = pd.qcut(
                    df['log_metric'], 
                    q=n_segments, 
                    labels=[f'Segment_{i+1}' for i in range(n_segments)],
                    duplicates='drop'
                )
                
            elif method == 'business':
                if business_thresholds is None:
                    # Default pageview thresholds
                    business_thresholds = {'Low': 2, 'Medium': 6}
                
                def business_segments(value):
                    if pd.isna(value):
                        return 'Unknown'
                    elif value == 0:
                        return 'No_Engagement'
                    
                    thresholds = sorted(business_thresholds.items(), key=lambda x: x[1])
                    for i, (label, threshold) in enumerate(thresholds):
                        if value <= threshold:
                            return f'{label}_Engagement'
                    
                    return 'High_Engagement'
                
                df['usage_segment'] = df[metric_column].apply(business_segments)
            
            else:
                raise ValueError("Method must be 'quantiles', 'log_quantiles', or 'business'")
        
        except Exception as e:
            print(f"❌ Error in segmentation: {e}")
            raise
        
        # Convert to string and clean
        df['usage_segment'] = df['usage_segment'].astype(str)
        df = df.dropna(subset=['usage_segment'])
        df = df[df['usage_segment'] != 'nan']
        
        return df
    
    def segment_analysis(self,
                        metric_column: str,
                        method: str = 'log_quantiles',
                        n_segments: int = 3,
                        business_thresholds: Optional[Dict[str, float]] = None,
                        min_segment_size: int = 100,
                        test_type: str = 'ttest') -> pd.DataFrame:
        """
        Analyze A/B test results by user segments.
        
        Parameters:
        -----------
        metric_column : str
            Metric to analyze
        method : str, default='log_quantiles'
            Segmentation method
        n_segments : int, default=3
            Number of segments to create
        business_thresholds : dict, optional
            Thresholds for business segmentation
        min_segment_size : int, default=100
            Minimum size for a segment to be included in analysis
        test_type : str, default='ttest'
            Statistical test type
            
        Returns:
        --------
        pd.DataFrame : Summary of results by segment
        """
        
        # Segment users
        df_segmented = self.segment_users(metric_column, method, n_segments, business_thresholds)
        
        results = []
        valid_segments = [s for s in df_segmented['usage_segment'].unique() 
                         if pd.notna(s) and str(s) != 'nan']
        
        for segment in valid_segments:
            segment_data = df_segmented[df_segmented['usage_segment'] == segment].copy()
            
            if len(segment_data) < min_segment_size:
                continue
            
            # Ensure we have both control and treatment data
            control_data = segment_data[segment_data[self.variant_column] == self.control_label]
            treatment_data = segment_data[segment_data[self.variant_column] == self.treatment_label]
            
            if len(control_data) < 50 or len(treatment_data) < 50:
                continue
            
            # Create temporary analyzer for this segment
            temp_analyzer = SessionsAnalyzer(
                df=segment_data,
                metric_columns=[metric_column],
                variant_column=self.variant_column,
                user_id_column=self.user_id_column,
                control_label=self.control_label,
                treatment_label=self.treatment_label
            )
            
            # Statistical test
            test_result = temp_analyzer.test_statistical_significance(metric_column, test_type=test_type)
            
            if 'error' in test_result:
                continue
            
            # Segment summary statistics
            control_metric = control_data[metric_column]
            treatment_metric = treatment_data[metric_column]
            
            results.append({
                'segment': segment,
                'control_size': len(control_metric),
                'treatment_size': len(treatment_metric),
                'control_median': control_metric.median(),
                'treatment_median': treatment_metric.median(),
                'control_mean': test_result['control_mean'],
                'treatment_mean': test_result['treatment_mean'],
                'p_value': test_result['p_value'],
                'is_significant': test_result['is_significant'],
                'relative_lift_percent': test_result['relative_lift_percent'],
                'absolute_lift': test_result['absolute_lift']
            })
        
        results_df = pd.DataFrame(results)
        if not results_df.empty:
            results_df['significance'] = results_df['is_significant'].map({True: '✅', False: '❌'})
            results_df = results_df.round(4)
        
        return results_df
    
    def print_results(self, results: Union[Dict[str, Any], pd.DataFrame], result_type: str = 'basic'):
        """
        Pretty print analysis results.
        
        Parameters:
        -----------
        results : dict or pd.DataFrame
            Results from analysis functions
        result_type : str, default='basic'
            Type of results: 'basic' or 'segment'
        """
        
        if result_type == 'basic' and isinstance(results, dict):
            self._print_basic_results(results)
        elif result_type == 'segment' and isinstance(results, pd.DataFrame):
            self._print_segment_results(results)
        else:
            print(results)
    
    def _print_basic_results(self, results: Dict[str, Any]):
        """Print basic statistical test results."""
        if 'error' in results:
            print(f"❌ Error: {results['error']}")
            return
        
        print(f"📊 Statistical Test Results for '{results['metric_column']}'")
        print("=" * 60)
        print(f"Test Used: {results['test_name']}")
        print(f"Sample Sizes: Control = {results['control_size']:,}, Treatment = {results['treatment_size']:,}")
        print()
        
        print("📈 Descriptive Statistics:")
        print(f"  Control Mean:    {results['control_mean']:.4f}")
        print(f"  Treatment Mean:  {results['treatment_mean']:.4f}")
        print(f"  Absolute Lift:   {results['absolute_lift']:.4f}")
        print(f"  Relative Lift:   {results['relative_lift_percent']:.2f}%")
        print()
        
        print("🔬 Statistical Test:")
        print(f"  Test Statistic:  {results['statistic']:.4f}")
        print(f"  P-value:         {results['p_value']:.6f}")
        print(f"  Alpha:           {results['alpha']}")
        
        if results['is_significant']:
            print(f"  Result:          ✅ SIGNIFICANT (p < {results['alpha']})")
        else:
            print(f"  Result:          ❌ NOT SIGNIFICANT (p >= {results['alpha']})")
    
    def _print_segment_results(self, results: pd.DataFrame):
        """Print segment analysis results."""
        if results.empty:
            print("❌ No segment results to display")
            return
        
        print("📊 SEGMENT ANALYSIS RESULTS:")
        print("=" * 60)
        
        summary_cols = ['segment', 'control_size', 'treatment_size', 
                       'control_median', 'treatment_median', 'relative_lift_percent', 
                       'significance']
        available_cols = [col for col in summary_cols if col in results.columns]
        print(results[available_cols].to_string(index=False))
        
        significant_segments = results[results['is_significant'] == True]
        
        if len(significant_segments) > 0:
            print(f"\n🎯 SIGNIFICANT SEGMENTS:")
            for _, row in significant_segments.iterrows():
                print(f"  {row['segment']}: {row['relative_lift_percent']:.1f}% lift (p={row['p_value']:.4f})")


# Convenience function for backward compatibility
def test_statistical_significance(df: pd.DataFrame, 
                                test_column: str, 
                                variant_column: str = 'variant',
                                control_label: str = 'control', 
                                treatment_label: str = 'treatment',
                                alpha: float = 0.05, 
                                test_type: str = 'ttest') -> Dict[str, Any]:
    """
    Convenience function for single statistical significance test.
    """
    analyzer = SessionsAnalyzer(
        df=df,
        metric_columns=[test_column],
        variant_column=variant_column,
        user_id_column=df.columns[0],  # Assume first column is user ID
        control_label=control_label,
        treatment_label=treatment_label
    )
    
    return analyzer.test_statistical_significance(test_column, alpha, test_type)


if __name__ == "__main__":
    print("📚 Sessions Analysis Toolkit (Minimal Version) loaded successfully!")
    print("💡 Main class: SessionsAnalyzer")
    print("💡 Available test types: 'ttest', 'mannwhitney'")
    print("💡 Dependencies: Only pandas and numpy required")
    print("\n🔗 Example usage:")
    print("""
    from sessions_analysis_minimal import SessionsAnalyzer
    
    # Initialize analyzer
    analyzer = SessionsAnalyzer(
        df=your_dataframe,
        metric_columns=['pageviews'],
        variant_column='variant',
        user_id_column='user_id'
    )
    
    # Run basic A/B test
    results = analyzer.test_statistical_significance('pageviews')
    analyzer.print_results(results, 'basic')
    
    # Run segmentation analysis
    seg_results = analyzer.segment_analysis('pageviews')
    analyzer.print_results(seg_results, 'segment')
    """)