"""
Sessions Analysis Toolkit

A comprehensive toolkit for analyzing user session data with A/B testing capabilities,
user segmentation, and migration analysis. Extracted and refactored from engagement analysis notebook.

Key Features:
- Statistical significance testing with multiple test types
- User segmentation based on various methods (log-quantiles, business rules, etc.)
- Segment migration analysis
- Ordinal trend testing
- Batch analysis capabilities
- Flexible column naming and grouping dimensions

Usage Example:
    from sessions_analysis_toolkit import SessionsAnalyzer
    
    analyzer = SessionsAnalyzer(
        df=admin_sessions,
        metric_columns=['pageviews_in_first_session_count', 'admin_pageview_count'],
        variant_column='variant',
        user_id_column='shop_id'
    )
    
    # Run basic A/B test
    results = analyzer.test_metric('pageviews_in_first_session_count')
    
    # Run segmentation analysis
    seg_results = analyzer.segment_analysis('pageviews_in_first_session_count')
    
    # Run migration analysis
    migration_results = analyzer.migration_analysis('pageviews_in_first_session_count')
"""

import pandas as pd
import numpy as np
from scipy import stats
from scipy.stats import mannwhitneyu, ttest_ind, levene, shapiro, chi2_contingency
import warnings
from typing import Dict, List, Optional, Tuple, Union, Any
warnings.filterwarnings('ignore')

# Optional plotting dependencies
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    print("Warning: matplotlib/seaborn not available. Plotting functionality will be limited.")


class SessionsAnalyzer:
    """
    A comprehensive toolkit for analyzing user session data with A/B testing capabilities.
    
    This class provides methods for statistical testing, user segmentation, migration analysis,
    and visualization of session-based metrics.
    
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
        
        # Store results for caching
        self._results_cache = {}
        
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
            
    def test_statistical_significance(self, 
                                    metric_column: str,
                                    alpha: float = 0.05,
                                    test_type: str = 'auto',
                                    groupby_dimensions: Optional[List[str]] = None) -> Union[Dict[str, Any], pd.DataFrame]:
        """
        Test statistical significance between control and treatment groups for a given metric.
        
        Parameters:
        -----------
        metric_column : str
            Column name to test for statistical significance
        alpha : float, default=0.05
            Significance level
        test_type : str, default='auto'
            Type of test to perform ('auto', 'ttest', 'mannwhitney', 'welch')
        groupby_dimensions : list of str, optional
            Additional dimensions to group by for analysis
            
        Returns:
        --------
        dict or pd.DataFrame : Test results. Returns DataFrame if groupby_dimensions provided.
        """
        
        if groupby_dimensions:
            return self._test_by_groups(metric_column, alpha, test_type, groupby_dimensions)
        
        # Filter data for control and treatment groups
        control_data = self.df[self.df[self.variant_column] == self.control_label][metric_column].dropna()
        treatment_data = self.df[self.df[self.variant_column] == self.treatment_label][metric_column].dropna()
        
        if len(control_data) == 0 or len(treatment_data) == 0:
            return {
                'error': f'No data found for control or treatment groups',
                'control_size': len(control_data),
                'treatment_size': len(treatment_data)
            }
        
        # Basic descriptive statistics
        control_mean = control_data.mean()
        treatment_mean = treatment_data.mean()
        control_std = control_data.std()
        treatment_std = treatment_data.std()
        control_median = control_data.median()
        treatment_median = treatment_data.median()
        
        # Calculate effect size
        relative_lift = ((treatment_mean - control_mean) / control_mean) * 100 if control_mean != 0 else np.inf
        absolute_lift = treatment_mean - control_mean
        
        # Test for normality and equal variances
        normality_control = None
        normality_treatment = None
        if len(control_data) >= 8 and len(treatment_data) >= 8:
            try:
                _, p_control = shapiro(control_data.sample(min(5000, len(control_data))))
                _, p_treatment = shapiro(treatment_data.sample(min(5000, len(treatment_data))))
                normality_control = p_control > 0.05
                normality_treatment = p_treatment > 0.05
            except:
                pass
        
        equal_variances = None
        try:
            _, p_levene = levene(control_data, treatment_data)
            equal_variances = p_levene > 0.05
        except:
            pass
        
        # Determine appropriate test
        if test_type == 'auto':
            if (normality_control is not None and normality_treatment is not None and 
                normality_control and normality_treatment):
                if equal_variances:
                    test_type = 'ttest'
                else:
                    test_type = 'welch'
            else:
                test_type = 'mannwhitney'
        
        # Perform statistical test
        if test_type == 'ttest':
            statistic, p_value = ttest_ind(treatment_data, control_data, equal_var=True)
            test_name = "Student's t-test"
        elif test_type == 'welch':
            statistic, p_value = ttest_ind(treatment_data, control_data, equal_var=False)
            test_name = "Welch's t-test"
        elif test_type == 'mannwhitney':
            statistic, p_value = mannwhitneyu(treatment_data, control_data, alternative='two-sided')
            test_name = "Mann-Whitney U test"
        else:
            return {'error': f'Unknown test type: {test_type}'}
        
        # Calculate confidence interval
        pooled_se = np.sqrt((control_std**2 / len(control_data)) + (treatment_std**2 / len(treatment_data)))
        df_welch = ((control_std**2 / len(control_data)) + (treatment_std**2 / len(treatment_data)))**2 / (
            (control_std**2 / len(control_data))**2 / (len(control_data) - 1) + 
            (treatment_std**2 / len(treatment_data))**2 / (len(treatment_data) - 1)
        )
        t_critical = stats.t.ppf(1 - alpha/2, df_welch)
        ci_lower = absolute_lift - t_critical * pooled_se
        ci_upper = absolute_lift + t_critical * pooled_se
        
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
            'confidence_interval': (ci_lower, ci_upper),
            'control_size': len(control_data),
            'treatment_size': len(treatment_data),
            'control_std': control_std,
            'treatment_std': treatment_std,
            'control_median': control_median,
            'treatment_median': treatment_median,
            'normality_control': normality_control,
            'normality_treatment': normality_treatment,
            'equal_variances': equal_variances
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
                              groupby_dimensions: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Run significance tests on multiple columns and return a summary DataFrame.
        
        Parameters:
        -----------
        metric_columns : list of str, optional
            List of column names to test. If None, uses self.metric_columns
        alpha : float, default=0.05
            Significance level
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
                
            result = self.test_statistical_significance(col, alpha, groupby_dimensions=groupby_dimensions)
            
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
                     method: str = 'log_quantiles',
                     n_segments: int = 3,
                     business_thresholds: Optional[Dict[str, float]] = None) -> pd.DataFrame:
        """
        Segment users into usage groups for analysis.
        
        Parameters:
        -----------
        metric_column : str
            Column name with the metric to segment on
        method : str, default='log_quantiles'
            Segmentation method:
            - 'log_quantiles': Equal-sized groups in log space (recommended)
            - 'raw_quantiles': Equal-sized groups in raw space
            - 'log_bins': Equal-width bins in log space
            - 'business': Business-driven cutoffs using business_thresholds
        n_segments : int, default=3
            Number of segments to create (ignored for 'business' method)
        business_thresholds : dict, optional
            Thresholds for business segmentation. Format: {'Low': 2, 'Medium': 6}
            
        Returns:
        --------
        pd.DataFrame : DataFrame with added 'usage_segment' column
        """
        
        df = self.df.copy()
        
        # Clean the data
        df = df.dropna(subset=[metric_column])
        df[metric_column] = pd.to_numeric(df[metric_column], errors='coerce')
        df = df.dropna(subset=[metric_column])
        
        # Create log-transformed version
        df['log_metric'] = np.log1p(df[metric_column])
        
        try:
            if method == 'log_quantiles':
                df['usage_segment'] = pd.qcut(
                    df['log_metric'], 
                    q=n_segments, 
                    labels=[f'Segment_{i+1}' for i in range(n_segments)],
                    duplicates='drop'
                )
                
            elif method == 'raw_quantiles':
                df['usage_segment'] = pd.qcut(
                    df[metric_column], 
                    q=n_segments, 
                    labels=[f'Segment_{i+1}' for i in range(n_segments)],
                    duplicates='drop'
                )
                
            elif method == 'log_bins':
                df['usage_segment'] = pd.cut(
                    df['log_metric'], 
                    bins=n_segments, 
                    labels=[f'Segment_{i+1}' for i in range(n_segments)],
                    include_lowest=True
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
                raise ValueError("Method must be 'log_quantiles', 'raw_quantiles', 'log_bins', or 'business'")
        
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
                        min_segment_size: int = 100) -> pd.DataFrame:
        """
        Analyze A/B test results by user segments using both raw and log-scale analysis.
        
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
            
            # Raw analysis
            raw_result = temp_analyzer.test_statistical_significance(metric_column, test_type='mannwhitney')
            
            # Log analysis
            segment_data['log_metric_temp'] = np.log1p(segment_data[metric_column])
            temp_analyzer_log = SessionsAnalyzer(
                df=segment_data,
                metric_columns=['log_metric_temp'],
                variant_column=self.variant_column,
                user_id_column=self.user_id_column,
                control_label=self.control_label,
                treatment_label=self.treatment_label
            )
            log_result = temp_analyzer_log.test_statistical_significance('log_metric_temp', test_type='welch')
            
            # Convert log effect back to percentage
            pct_effect = (np.exp(log_result['absolute_lift']) - 1) * 100 if log_result['absolute_lift'] != 0 else 0
            
            # Segment summary statistics
            control_metric = control_data[metric_column]
            treatment_metric = treatment_data[metric_column]
            
            results.append({
                'segment': segment,
                'control_size': len(control_metric),
                'treatment_size': len(treatment_metric),
                'control_median': control_metric.median(),
                'treatment_median': treatment_metric.median(),
                'control_mean': control_metric.mean(),
                'treatment_mean': treatment_metric.mean(),
                'raw_p_value': raw_result['p_value'],
                'raw_significant': raw_result['is_significant'],
                'log_p_value': log_result['p_value'],
                'log_significant': log_result['is_significant'],
                'percent_effect': pct_effect,
                'absolute_effect': raw_result['absolute_lift'],
                'log_effect': log_result['absolute_lift']
            })
        
        results_df = pd.DataFrame(results)
        if not results_df.empty:
            results_df['raw_sig'] = results_df['raw_significant'].map({True: '✅', False: '❌'})
            results_df['log_sig'] = results_df['log_significant'].map({True: '✅', False: '❌'})
            results_df = results_df.round(4)
        
        return results_df
    
    def migration_analysis(self,
                          metric_column: str,
                          method: str = 'log_quantiles',
                          n_segments: int = 3,
                          business_thresholds: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        Analyze if treatment moves users to higher usage segments.
        
        Parameters:
        -----------
        metric_column : str
            Metric to analyze for migration
        method : str, default='log_quantiles'
            Segmentation method
        n_segments : int, default=3
            Number of segments to create
        business_thresholds : dict, optional
            Thresholds for business segmentation
            
        Returns:
        --------
        dict : Dictionary containing migration analysis results
        """
        
        # Segment users
        df_segmented = self.segment_users(metric_column, method, n_segments, business_thresholds)
        
        # Create contingency table
        contingency_table = pd.crosstab(
            df_segmented['usage_segment'], 
            df_segmented[self.variant_column], 
            margins=True
        )
        
        # Calculate proportions within each variant
        prop_table = pd.crosstab(
            df_segmented['usage_segment'], 
            df_segmented[self.variant_column], 
            normalize='columns'
        ) * 100
        
        # Chi-square test for independence
        chi2_table = contingency_table.iloc[:-1, :-1]  # Remove 'All' row and column
        chi2_stat, chi2_p, chi2_dof, chi2_expected = chi2_contingency(chi2_table)
        
        # Calculate odds ratios
        segments = [s for s in df_segmented['usage_segment'].unique() 
                   if pd.notna(s) and str(s) != 'nan']
        
        # Order segments logically
        segment_order = ['Segment_1', 'Segment_2', 'Segment_3', 'No_Engagement', 
                        'Low_Engagement', 'Medium_Engagement', 'High_Engagement']
        ordered_segments = [s for s in segment_order if s in segments]
        ordered_segments.extend([s for s in segments if s not in segment_order])
        
        odds_results = []
        
        for i, segment in enumerate(ordered_segments):
            higher_segments = ordered_segments[i:]
            
            control_higher = len(df_segmented[
                (df_segmented[self.variant_column] == self.control_label) & 
                (df_segmented['usage_segment'].isin(higher_segments))
            ])
            control_lower = len(df_segmented[
                (df_segmented[self.variant_column] == self.control_label) & 
                (~df_segmented['usage_segment'].isin(higher_segments))
            ])
            
            treatment_higher = len(df_segmented[
                (df_segmented[self.variant_column] == self.treatment_label) & 
                (df_segmented['usage_segment'].isin(higher_segments))
            ])
            treatment_lower = len(df_segmented[
                (df_segmented[self.variant_column] == self.treatment_label) & 
                (~df_segmented['usage_segment'].isin(higher_segments))
            ])
            
            if control_lower > 0 and treatment_lower > 0:
                odds_control = control_higher / control_lower
                odds_treatment = treatment_higher / treatment_lower
                odds_ratio = odds_treatment / odds_control
                
                se_log_or = np.sqrt(1/control_higher + 1/control_lower + 1/treatment_higher + 1/treatment_lower)
                ci_lower = np.exp(np.log(odds_ratio) - 1.96 * se_log_or)
                ci_upper = np.exp(np.log(odds_ratio) + 1.96 * se_log_or)
                
                odds_results.append({
                    'threshold_segment': segment,
                    'odds_ratio': odds_ratio,
                    'ci_lower': ci_lower,
                    'ci_upper': ci_upper,
                    'control_higher': control_higher,
                    'treatment_higher': treatment_higher,
                    'interpretation': 'Higher usage' if odds_ratio > 1 else 'Lower usage'
                })
        
        return {
            'contingency_table': contingency_table,
            'proportion_table': prop_table,
            'chi2_stat': chi2_stat,
            'chi2_p': chi2_p,
            'chi2_dof': chi2_dof,
            'odds_results': odds_results,
            'significant': chi2_p < 0.05,
            'segmented_data': df_segmented
        }
    
    def ordinal_trend_test(self,
                          metric_column: str,
                          method: str = 'log_quantiles',
                          n_segments: int = 3,
                          business_thresholds: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
        """
        Test if treatment has an ordinal trend effect (more users in higher segments).
        Uses Cochran-Armitage test for trend in proportions.
        
        Parameters:
        -----------
        metric_column : str
            Metric to analyze for ordinal trends
        method : str, default='log_quantiles'
            Segmentation method
        n_segments : int, default=3
            Number of segments to create
        business_thresholds : dict, optional
            Thresholds for business segmentation
            
        Returns:
        --------
        dict : Dictionary containing trend test results
        """
        
        # Segment users
        df_segmented = self.segment_users(metric_column, method, n_segments, business_thresholds)
        
        # Order segments
        segments = [s for s in df_segmented['usage_segment'].unique() 
                   if pd.notna(s) and str(s) != 'nan']
        segment_order = ['Segment_1', 'Segment_2', 'Segment_3', 'No_Engagement', 
                        'Low_Engagement', 'Medium_Engagement', 'High_Engagement']
        ordered_segments = [s for s in segment_order if s in segments]
        ordered_segments.extend([s for s in segments if s not in segment_order])
        
        # Create scores for segments (higher = better)
        segment_scores = {segment: i for i, segment in enumerate(ordered_segments)}
        
        # Collect data for Cochran-Armitage test
        control_data = df_segmented[df_segmented[self.variant_column] == self.control_label]
        treatment_data = df_segmented[df_segmented[self.variant_column] == self.treatment_label]
        
        # Count users in each segment for each group
        control_counts = [len(control_data[control_data['usage_segment'] == seg]) for seg in ordered_segments]
        treatment_counts = [len(treatment_data[treatment_data['usage_segment'] == seg]) for seg in ordered_segments]
        
        n_control = sum(control_counts)
        n_treatment = sum(treatment_counts)
        total_per_segment = [c + t for c, t in zip(control_counts, treatment_counts)]
        
        # Calculate test statistic
        scores = list(range(len(ordered_segments)))
        
        score_sum_treatment = sum(score * count for score, count in zip(scores, treatment_counts))
        score_sum_total = sum(score * total for score, total in zip(scores, total_per_segment))
        
        mean_score_treatment = score_sum_treatment / n_treatment if n_treatment > 0 else 0
        mean_score_overall = score_sum_total / (n_control + n_treatment)
        
        # Variance calculation
        score_variance = sum(total * (score - mean_score_overall)**2 for score, total in zip(scores, total_per_segment))
        score_variance = score_variance / (n_control + n_treatment - 1)
        
        # Test statistic
        if score_variance > 0:
            z_stat = (mean_score_treatment - mean_score_overall) * np.sqrt(n_control * n_treatment / ((n_control + n_treatment) * score_variance))
            p_value = 2 * (1 - stats.norm.cdf(abs(z_stat)))  # Two-tailed test
        else:
            z_stat = 0
            p_value = 1.0
        
        # Calculate mean segment scores
        control_mean_score = sum(score * count for score, count in zip(scores, control_counts)) / n_control if n_control > 0 else 0
        treatment_mean_score = sum(score * count for score, count in zip(scores, treatment_counts)) / n_treatment if n_treatment > 0 else 0
        
        return {
            'z_statistic': z_stat,
            'p_value': p_value,
            'significant_trend': p_value < 0.05,
            'control_mean_score': control_mean_score,
            'treatment_mean_score': treatment_mean_score,
            'score_difference': treatment_mean_score - control_mean_score,
            'segment_scores': segment_scores,
            'control_counts': control_counts,
            'treatment_counts': treatment_counts,
            'ordered_segments': ordered_segments
        }
    
    def print_results(self, results: Union[Dict[str, Any], pd.DataFrame], result_type: str = 'basic'):
        """
        Pretty print analysis results.
        
        Parameters:
        -----------
        results : dict or pd.DataFrame
            Results from analysis functions
        result_type : str, default='basic'
            Type of results: 'basic', 'segment', 'migration', 'trend'
        """
        
        if result_type == 'basic' and isinstance(results, dict):
            self._print_basic_results(results)
        elif result_type == 'segment' and isinstance(results, pd.DataFrame):
            self._print_segment_results(results)
        elif result_type == 'migration' and isinstance(results, dict):
            self._print_migration_results(results)
        elif result_type == 'trend' and isinstance(results, dict):
            self._print_trend_results(results)
        else:
            print(f"Unsupported result type '{result_type}' or wrong data format")
    
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
        
        print()
        ci_lower, ci_upper = results['confidence_interval']
        print(f"📊 95% Confidence Interval for Difference:")
        print(f"  [{ci_lower:.4f}, {ci_upper:.4f}]")
    
    def _print_segment_results(self, results: pd.DataFrame):
        """Print segment analysis results."""
        if results.empty:
            print("❌ No segment results to display")
            return
        
        print("📊 SEGMENT ANALYSIS RESULTS:")
        print("=" * 60)
        
        summary_cols = ['segment', 'control_size', 'treatment_size', 
                       'control_median', 'treatment_median', 'percent_effect', 
                       'raw_sig', 'log_sig']
        available_cols = [col for col in summary_cols if col in results.columns]
        print(results[available_cols].to_string(index=False))
        
        log_only_sig = results[(results['log_significant'] == True) & (results['raw_significant'] == False)]
        raw_only_sig = results[(results['raw_significant'] == True) & (results['log_significant'] == False)]
        both_sig = results[(results['log_significant'] == True) & (results['raw_significant'] == True)]
        
        print(f"\n🔍 PATTERN ANALYSIS:")
        print(f"✅ Significant in both raw & log: {len(both_sig)} segments")
        print(f"📈 Significant only in log (multiplicative effect): {len(log_only_sig)} segments") 
        print(f"📊 Significant only in raw (additive effect): {len(raw_only_sig)} segments")
        print(f"❌ Not significant in either: {len(results) - len(both_sig) - len(log_only_sig) - len(raw_only_sig)} segments")
        
        if len(log_only_sig) > 0:
            print(f"\n🎯 MULTIPLICATIVE EFFECTS (log significant only):")
            for _, row in log_only_sig.iterrows():
                print(f"  {row['segment']}: {row['percent_effect']:.1f}% improvement (p={row['log_p_value']:.4f})")
    
    def _print_migration_results(self, results: Dict[str, Any]):
        """Print migration analysis results."""
        print(f"🎯 SEGMENT MIGRATION ANALYSIS")
        print(f"Testing: Does treatment increase probability of higher usage?")
        print("="*60)
        
        print(f"\n📊 CONTINGENCY TABLE (Raw Counts):")
        print(results['contingency_table'])
        
        print(f"\n📈 SEGMENT DISTRIBUTION (% within variant):")
        print(results['proportion_table'].round(2))
        
        print(f"\n🔬 CHI-SQUARE TEST FOR INDEPENDENCE:")
        print(f"  H0: Segment distribution is independent of treatment")
        print(f"  H1: Treatment affects segment distribution")
        print(f"  Chi-square statistic: {results['chi2_stat']:.4f}")
        print(f"  P-value: {results['chi2_p']:.6f}")
        print(f"  Degrees of freedom: {results['chi2_dof']}")
        print(f"  Result: {'✅ SIGNIFICANT' if results['significant'] else '❌ NOT SIGNIFICANT'}")
        
        if results['odds_results']:
            print(f"\n📊 ODDS RATIOS (Treatment vs Control):")
            for odds_result in results['odds_results']:
                print(f"  {odds_result['threshold_segment']}+: OR = {odds_result['odds_ratio']:.3f} "
                      f"(95% CI: {odds_result['ci_lower']:.3f} - {odds_result['ci_upper']:.3f})")
    
    def _print_trend_results(self, results: Dict[str, Any]):
        """Print ordinal trend test results."""
        print(f"📈 ORDINAL TREND ANALYSIS")
        print(f"Testing: Does treatment show progressive shift toward higher segments?")
        print("="*50)
        
        print(f"📊 Segment ordering (scores): {results['segment_scores']}")
        
        print(f"\n📋 Raw counts:")
        for seg, ctrl, trt in zip(results['ordered_segments'], 
                                 results['control_counts'], 
                                 results['treatment_counts']):
            print(f"  {seg}: Control={ctrl}, Treatment={trt}")
        
        print(f"\n🔬 COCHRAN-ARMITAGE TREND TEST:")
        print(f"  H0: No linear trend in segment proportions")
        print(f"  H1: Treatment increases probability of higher segments")
        print(f"  Z-statistic: {results['z_statistic']:.4f}")
        print(f"  P-value: {results['p_value']:.6f}")
        print(f"  Result: {'✅ SIGNIFICANT TREND' if results['significant_trend'] else '❌ NO SIGNIFICANT TREND'}")
        
        print(f"\n📊 AVERAGE SEGMENT SCORES:")
        print(f"  Control mean: {results['control_mean_score']:.3f}")
        print(f"  Treatment mean: {results['treatment_mean_score']:.3f}")
        print(f"  Difference: {results['score_difference']:+.3f}")
        interpretation = 'Treatment users in higher segments on average' if results['score_difference'] > 0 else 'Control users in higher segments on average'
        print(f"  Interpretation: {interpretation}")


# Convenience functions for backward compatibility
def test_statistical_significance(df: pd.DataFrame, 
                                test_column: str, 
                                variant_column: str = 'variant',
                                control_label: str = 'control', 
                                treatment_label: str = 'treatment',
                                alpha: float = 0.05, 
                                test_type: str = 'auto') -> Dict[str, Any]:
    """
    Convenience function for single statistical significance test.
    
    This function maintains backward compatibility with the original notebook functions.
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


def print_significance_results(results: Dict[str, Any]):
    """
    Convenience function for printing results.
    
    This function maintains backward compatibility with the original notebook functions.
    """
    analyzer = SessionsAnalyzer(
        df=pd.DataFrame(),  # Dummy df
        metric_columns=[],
        variant_column='variant',
        user_id_column='user_id'
    )
    
    analyzer._print_basic_results(results)


if __name__ == "__main__":
    print("📚 Sessions Analysis Toolkit loaded successfully!")
    print("💡 Main class: SessionsAnalyzer")
    print("💡 Convenience functions: test_statistical_significance(), print_significance_results()")
    print("\n🔗 Example usage:")
    print("""
    from sessions_analysis_toolkit import SessionsAnalyzer
    
    # Initialize analyzer
    analyzer = SessionsAnalyzer(
        df=your_dataframe,
        metric_columns=['pageviews', 'session_count'],
        variant_column='variant',
        user_id_column='user_id'
    )
    
    # Run basic A/B test
    results = analyzer.test_statistical_significance('pageviews')
    analyzer.print_results(results, 'basic')
    
    # Run batch tests
    batch_results = analyzer.batch_significance_test()
    print(batch_results)
    
    # Run segmentation analysis
    seg_results = analyzer.segment_analysis('pageviews')
    analyzer.print_results(seg_results, 'segment')
    
    # Run migration analysis
    migration_results = analyzer.migration_analysis('pageviews')
    analyzer.print_results(migration_results, 'migration')
    """)