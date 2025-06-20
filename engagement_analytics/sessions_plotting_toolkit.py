"""
Sessions Plotting Toolkit

Visualization functions for the Sessions Analysis Toolkit.
Provides comprehensive plotting capabilities for A/B testing, segmentation analysis, and migration studies.

Key Features:
- A/B test visualization with multiple test types
- Segment comparison plots
- Migration analysis visualizations
- Ordinal trend plots
- Flexible styling and customization

Usage Example:
    from sessions_plotting_toolkit import SessionsPlotter
    from sessions_analysis_toolkit import SessionsAnalyzer
    
    analyzer = SessionsAnalyzer(df=data, metric_columns=['pageviews'], ...)
    plotter = SessionsPlotter(analyzer)
    
    # Plot A/B test results
    results, fig = plotter.plot_ab_test('pageviews')
    
    # Plot segment comparison
    seg_results = analyzer.segment_analysis('pageviews')
    fig = plotter.plot_segment_comparison(seg_results, 'pageviews')
    
    # Plot migration analysis
    migration_results = analyzer.migration_analysis('pageviews')
    fig = plotter.plot_migration_analysis(migration_results)
"""

import pandas as pd
import numpy as np
from scipy import stats
import warnings
from typing import Dict, List, Optional, Tuple, Union, Any
warnings.filterwarnings('ignore')

# Required plotting dependencies
try:
    import matplotlib.pyplot as plt
    import seaborn as sns
    PLOTTING_AVAILABLE = True
except ImportError:
    PLOTTING_AVAILABLE = False
    print("Warning: matplotlib/seaborn not available. Plotting functionality disabled.")
    
    # Create dummy classes to prevent import errors
    class MockPlotter:
        def __init__(self, *args, **kwargs):
            pass
        def __getattr__(self, name):
            def mock_method(*args, **kwargs):
                print(f"❌ Cannot use {name}() - matplotlib/seaborn not installed")
                return None
            return mock_method
    
    plt = MockPlotter()
    sns = MockPlotter()

# Try to import the main analyzer
try:
    from sessions_analysis_toolkit import SessionsAnalyzer
except ImportError:
    print("Warning: sessions_analysis_toolkit not found. Some functionality may be limited.")
    SessionsAnalyzer = None


class SessionsPlotter:
    """
    Comprehensive plotting toolkit for session analysis results.
    
    This class provides visualization methods for A/B testing, segmentation analysis,
    migration studies, and trend analysis results.
    
    Parameters:
    -----------
    analyzer : SessionsAnalyzer, optional
        SessionsAnalyzer instance to use for data and configuration
    figsize : tuple, default=(12, 8)
        Default figure size for plots
    style : str, default='whitegrid'
        Default seaborn style
    color_palette : dict, optional
        Custom color palette for variants. Default: {'control': '#3498db', 'treatment': '#e74c3c'}
    """
    
    def __init__(self, 
                 analyzer: Optional['SessionsAnalyzer'] = None,
                 figsize: Tuple[int, int] = (12, 8),
                 style: str = 'whitegrid',
                 color_palette: Optional[Dict[str, str]] = None):
        
        if not PLOTTING_AVAILABLE:
            print("❌ Warning: Plotting functionality not available. Install matplotlib and seaborn to use SessionsPlotter.")
        
        self.analyzer = analyzer
        self.figsize = figsize
        self.style = style
        self.color_palette = color_palette or {
            'control': '#3498db',   # Blue
            'treatment': '#e74c3c'  # Red
        }
        
        # Set default style if plotting is available
        if PLOTTING_AVAILABLE:
            sns.set_style(self.style)
            plt.rcParams['figure.figsize'] = self.figsize
        
    def plot_ab_test(self,
                     metric_column: str,
                     test_type: str = 'auto',
                     show_clt: bool = True,
                     figsize: Optional[Tuple[int, int]] = None) -> Tuple[Dict[str, Any], plt.Figure]:
        """
        Create comprehensive A/B test visualization with statistical analysis.
        
        Parameters:
        -----------
        metric_column : str
            Column name to plot and test
        test_type : str, default='auto'
            Type of statistical test ('auto', 'ttest', 'mannwhitney', 'welch')
        show_clt : bool, default=True
            Whether to show Central Limit Theorem plots
        figsize : tuple, optional
            Figure size override
            
        Returns:
        --------
        tuple : (test_results_dict, matplotlib_figure)
        """
        
        if self.analyzer is None:
            raise ValueError("SessionsAnalyzer instance required for this method")
        
        # Run statistical test
        test_result = self.analyzer.test_statistical_significance(metric_column, test_type=test_type)
        
        if 'error' in test_result:
            print(f"❌ Error: {test_result['error']}")
            return test_result, None
        
        # Extract data for both groups
        control_data = self.analyzer.df[
            self.analyzer.df[self.analyzer.variant_column] == self.analyzer.control_label
        ][metric_column].dropna()
        
        treatment_data = self.analyzer.df[
            self.analyzer.df[self.analyzer.variant_column] == self.analyzer.treatment_label
        ][metric_column].dropna()
        
        if len(control_data) == 0 or len(treatment_data) == 0:
            print("❌ Error: No data found for one or both groups")
            return test_result, None
        
        # Set figure size
        if figsize is None:
            figsize = (15, 10) if show_clt else (15, 5)
        
        # Set up the plot layout
        if show_clt:
            fig, axes = plt.subplots(2, 3, figsize=figsize)
            fig.suptitle(f'A/B Test Analysis: {metric_column}\nTest Used: {test_result.get("test_name", "Unknown")}', 
                         fontsize=16, fontweight='bold')
        else:
            fig, axes = plt.subplots(1, 3, figsize=figsize)
            axes = [axes]  # Make it 2D for consistent indexing
            fig.suptitle(f'A/B Test Analysis: {metric_column}\nTest Used: {test_result.get("test_name", "Unknown")}', 
                         fontsize=16, fontweight='bold')
        
        # Colors
        control_color = self.color_palette['control']
        treatment_color = self.color_palette['treatment']
        
        # Calculate statistics
        control_mean = control_data.mean()
        treatment_mean = treatment_data.mean()
        control_std = control_data.std()
        treatment_std = treatment_data.std()
        
        # Plot 1: Raw Data Distributions
        ax1 = axes[0][0] if show_clt else axes[0]
        
        combined_data = np.concatenate([control_data, treatment_data])
        n_bins = min(50, int(np.sqrt(len(combined_data))))
        
        ax1.hist(control_data, bins=n_bins, alpha=0.6, 
                label=f'{self.analyzer.control_label} (n={len(control_data):,})', 
                color=control_color, density=True)
        ax1.hist(treatment_data, bins=n_bins, alpha=0.6, 
                label=f'{self.analyzer.treatment_label} (n={len(treatment_data):,})', 
                color=treatment_color, density=True)
        
        # Add mean lines
        ax1.axvline(control_mean, color=control_color, linestyle='-', linewidth=2, alpha=0.8)
        ax1.axvline(treatment_mean, color=treatment_color, linestyle='-', linewidth=2, alpha=0.8)
        
        ax1.set_title(f'Data Distributions\n({test_result.get("test_name", "Unknown Test")})')
        ax1.set_xlabel(metric_column)
        ax1.set_ylabel('Density')
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Box Plots
        ax2 = axes[0][1] if show_clt else axes[1]
        
        box_data = [control_data, treatment_data]
        box_labels = [self.analyzer.control_label, self.analyzer.treatment_label]
        box_colors = [control_color, treatment_color]
        
        bp = ax2.boxplot(box_data, labels=box_labels, patch_artist=True, notch=True, showmeans=True)
        
        for patch, color in zip(bp['boxes'], box_colors):
            patch.set_facecolor(color)
            patch.set_alpha(0.6)
        
        # Add significance annotation
        significance_text = "✅ SIGNIFICANT" if test_result.get('is_significant') else "❌ NOT SIGNIFICANT"
        ax2.text(0.5, 0.95, f"p = {test_result.get('p_value', 'N/A'):.6f}\n{significance_text}", 
                 transform=ax2.transAxes, ha='center', va='top',
                 bbox=dict(boxstyle="round,pad=0.3", 
                          facecolor="yellow" if test_result.get('is_significant') else "lightgray", 
                          alpha=0.7))
        
        ax2.set_title('Distribution Comparison')
        ax2.set_ylabel(metric_column)
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Summary Statistics
        ax3 = axes[0][2] if show_clt else axes[2]
        ax3.axis('off')
        
        summary_text = f"""
Statistical Summary:

Test Configuration:
• Test Type: {test_result.get('test_name', 'Unknown')}
• Alpha: {test_result.get('alpha', 0.05)}

Control Group:
• Mean: {control_mean:.4f}
• Std: {control_std:.4f}
• N: {len(control_data):,}

Treatment Group:  
• Mean: {treatment_mean:.4f}
• Std: {treatment_std:.4f}
• N: {len(treatment_data):,}

Results:
• P-value: {test_result.get('p_value', 'N/A'):.6f}
• Significant: {'Yes' if test_result.get('is_significant') else 'No'}
• Absolute Lift: {test_result.get('absolute_lift', 'N/A'):.4f}
• Relative Lift: {test_result.get('relative_lift_percent', 'N/A'):.2f}%
        """
        
        ax3.text(0.05, 0.95, summary_text, transform=ax3.transAxes, fontsize=9,
                 verticalalignment='top', fontfamily='monospace',
                 bbox=dict(boxstyle="round,pad=0.5", facecolor="lightblue", alpha=0.5))
        
        if show_clt:
            # Plot 4: Central Limit Theorem Simulation
            ax4 = axes[1][0]
            
            n_simulations = 1000
            sample_size = min(100, len(control_data) // 10, len(treatment_data) // 10)
            
            if sample_size > 5:
                control_sample_means = []
                treatment_sample_means = []
                
                for _ in range(n_simulations):
                    control_sample = control_data.sample(sample_size, replace=True)
                    treatment_sample = treatment_data.sample(sample_size, replace=True)
                    control_sample_means.append(control_sample.mean())
                    treatment_sample_means.append(treatment_sample.mean())
                
                ax4.hist(control_sample_means, bins=50, alpha=0.6, 
                        label=f'{self.analyzer.control_label} Sample Means', 
                        color=control_color, density=True)
                ax4.hist(treatment_sample_means, bins=50, alpha=0.6, 
                        label=f'{self.analyzer.treatment_label} Sample Means', 
                        color=treatment_color, density=True)
                
                ax4.axvline(control_mean, color=control_color, linestyle='-', linewidth=2, alpha=0.8)
                ax4.axvline(treatment_mean, color=treatment_color, linestyle='-', linewidth=2, alpha=0.8)
                
                ax4.set_title(f'CLT: Sampling Distribution\n(Sample size = {sample_size})')
                ax4.set_xlabel('Sample Mean')
                ax4.set_ylabel('Density')
                ax4.legend()
                ax4.grid(True, alpha=0.3)
            else:
                ax4.text(0.5, 0.5, 'Insufficient data\nfor CLT simulation', 
                         ha='center', va='center', transform=ax4.transAxes, fontsize=12)
                ax4.set_title('CLT Simulation')
            
            # Plot 5: Effect Size Visualization
            ax5 = axes[1][1]
            
            if control_std > 0 and treatment_std > 0:
                pooled_std = np.sqrt((control_std**2 + treatment_std**2) / 2)
                effect_size = abs(treatment_mean - control_mean) / pooled_std
                
                x_effect = np.linspace(min(control_mean - 3*control_std, treatment_mean - 3*treatment_std),
                                       max(control_mean + 3*control_std, treatment_mean + 3*treatment_std), 1000)
                
                control_curve = stats.norm.pdf(x_effect, control_mean, control_std)
                treatment_curve = stats.norm.pdf(x_effect, treatment_mean, treatment_std)
                
                ax5.fill_between(x_effect, control_curve, alpha=0.4, color=control_color, 
                                label=self.analyzer.control_label)
                ax5.fill_between(x_effect, treatment_curve, alpha=0.4, color=treatment_color, 
                                label=self.analyzer.treatment_label)
                
                ax5.axvline(control_mean, color=control_color, linestyle='-', linewidth=2)
                ax5.axvline(treatment_mean, color=treatment_color, linestyle='-', linewidth=2)
                
                if abs(treatment_mean - control_mean) > 0:
                    mid_point = (control_mean + treatment_mean) / 2
                    ax5.annotate('', xy=(treatment_mean, max(treatment_curve)/2), 
                                xytext=(control_mean, max(treatment_curve)/2),
                                arrowprops=dict(arrowstyle='<->', color='black', lw=2))
                    
                    ax5.text(mid_point, max(treatment_curve)/2 + max(treatment_curve)*0.1, 
                             f'Effect Size (Cohen\'s d) = {effect_size:.3f}', 
                             ha='center', va='bottom', fontweight='bold')
                
                ax5.set_title('Effect Size Visualization')
                ax5.set_xlabel(metric_column)
                ax5.set_ylabel('Density')
                ax5.legend()
                ax5.grid(True, alpha=0.3)
            else:
                ax5.text(0.5, 0.5, 'Cannot compute\neffect size', 
                         ha='center', va='center', transform=ax5.transAxes, fontsize=12)
                ax5.set_title('Effect Size')
            
            # Plot 6: Q-Q Plot for Normality Check
            ax6 = axes[1][2]
            
            # Q-Q plots for both groups
            stats.probplot(control_data.sample(min(1000, len(control_data))), 
                           dist="norm", plot=ax6)
            ax6.get_lines()[0].set_markerfacecolor(control_color)
            ax6.get_lines()[0].set_markeredgecolor(control_color)
            ax6.get_lines()[0].set_alpha(0.6)
            
            stats.probplot(treatment_data.sample(min(1000, len(treatment_data))), 
                           dist="norm", plot=ax6)
            ax6.get_lines()[2].set_markerfacecolor(treatment_color)
            ax6.get_lines()[2].set_markeredgecolor(treatment_color)
            ax6.get_lines()[2].set_alpha(0.6)
            
            # Add normality test results
            normality_text = ""
            if 'normality_control' in test_result and 'normality_treatment' in test_result:
                ctrl_normal = "✅" if test_result['normality_control'] else "❌"
                trt_normal = "✅" if test_result['normality_treatment'] else "❌"
                normality_text = f"Normal: {self.analyzer.control_label} {ctrl_normal}, {self.analyzer.treatment_label} {trt_normal}"
            
            ax6.set_title(f'Q-Q Plot (Normality Check)\n{normality_text}')
            ax6.grid(True, alpha=0.3)
            
            # Add custom legend for Q-Q plot
            from matplotlib.lines import Line2D
            qq_legend = [Line2D([0], [0], marker='o', color='w', markerfacecolor=control_color, 
                                markersize=8, alpha=0.6, label=self.analyzer.control_label),
                         Line2D([0], [0], marker='o', color='w', markerfacecolor=treatment_color, 
                                markersize=8, alpha=0.6, label=self.analyzer.treatment_label)]
            ax6.legend(handles=qq_legend)
        
        plt.tight_layout()
        return test_result, fig
    
    def plot_segment_comparison(self, 
                               segment_results: pd.DataFrame,
                               metric_column: str,
                               figsize: Optional[Tuple[int, int]] = None) -> plt.Figure:
        """
        Plot segment analysis results showing treatment effects by segment.
        
        Parameters:
        -----------
        segment_results : pd.DataFrame
            Results from analyzer.segment_analysis()
        metric_column : str
            Name of the metric being analyzed
        figsize : tuple, optional
            Figure size override
            
        Returns:
        --------
        plt.Figure : Matplotlib figure object
        """
        
        if segment_results.empty:
            print("❌ No segment results to plot")
            return None
        
        if figsize is None:
            figsize = (15, 10)
        
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        fig.suptitle(f'Segment Analysis: {metric_column}', fontsize=16, fontweight='bold')
        
        # Plot 1: Control vs Treatment means by segment
        ax1 = axes[0, 0]
        
        segments = segment_results['segment']
        x = np.arange(len(segments))
        width = 0.35
        
        ax1.bar(x - width/2, segment_results['control_mean'], width, 
                label=self.analyzer.control_label if self.analyzer else 'Control', 
                color=self.color_palette['control'], alpha=0.7)
        ax1.bar(x + width/2, segment_results['treatment_mean'], width, 
                label=self.analyzer.treatment_label if self.analyzer else 'Treatment', 
                color=self.color_palette['treatment'], alpha=0.7)
        
        ax1.set_xlabel('Segment')
        ax1.set_ylabel('Mean Value')
        ax1.set_title('Mean Values by Segment')
        ax1.set_xticks(x)
        ax1.set_xticklabels(segments, rotation=45)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Percentage effect by segment
        ax2 = axes[0, 1]
        
        bars = ax2.bar(segments, segment_results['percent_effect'], 
                      color=['green' if x > 0 else 'red' for x in segment_results['percent_effect']], 
                      alpha=0.7)
        
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
        ax2.set_xlabel('Segment')
        ax2.set_ylabel('Percentage Effect (%)')
        ax2.set_title('Treatment Effect by Segment')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, effect in zip(bars, segment_results['percent_effect']):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + (0.1 if height > 0 else -0.3),
                    f'{effect:.1f}%', ha='center', va='bottom' if height > 0 else 'top', fontsize=9)
        
        # Plot 3: Sample sizes by segment
        ax3 = axes[1, 0]
        
        ax3.bar(x - width/2, segment_results['control_size'], width, 
                label=self.analyzer.control_label if self.analyzer else 'Control', 
                color=self.color_palette['control'], alpha=0.7)
        ax3.bar(x + width/2, segment_results['treatment_size'], width, 
                label=self.analyzer.treatment_label if self.analyzer else 'Treatment', 
                color=self.color_palette['treatment'], alpha=0.7)
        
        ax3.set_xlabel('Segment')
        ax3.set_ylabel('Sample Size')
        ax3.set_title('Sample Sizes by Segment')
        ax3.set_xticks(x)
        ax3.set_xticklabels(segments, rotation=45)
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Plot 4: Significance indicators
        ax4 = axes[1, 1]
        
        # Create significance matrix
        sig_data = []
        for _, row in segment_results.iterrows():
            sig_data.append([
                1 if row['raw_significant'] else 0,
                1 if row['log_significant'] else 0
            ])
        
        sig_matrix = np.array(sig_data)
        
        im = ax4.imshow(sig_matrix.T, cmap='RdYlGn', aspect='auto', vmin=0, vmax=1)
        
        ax4.set_xticks(range(len(segments)))
        ax4.set_xticklabels(segments, rotation=45)
        ax4.set_yticks([0, 1])
        ax4.set_yticklabels(['Raw Test', 'Log Test'])
        ax4.set_title('Statistical Significance\n(Green = Significant)')
        
        # Add text annotations
        for i in range(len(segments)):
            for j in range(2):
                text = '✅' if sig_matrix[i, j] == 1 else '❌'
                ax4.text(i, j, text, ha='center', va='center', fontsize=12)
        
        plt.tight_layout()
        return fig
    
    def plot_migration_analysis(self, 
                               migration_results: Dict[str, Any],
                               figsize: Optional[Tuple[int, int]] = None) -> plt.Figure:
        """
        Visualize segment migration analysis results.
        
        Parameters:
        -----------
        migration_results : dict
            Results from analyzer.migration_analysis()
        figsize : tuple, optional
            Figure size override
            
        Returns:
        --------
        plt.Figure : Matplotlib figure object
        """
        
        if figsize is None:
            figsize = (18, 6)
        
        fig, axes = plt.subplots(1, 3, figsize=figsize)
        fig.suptitle('Segment Migration Analysis: Control vs Treatment', fontsize=16, fontweight='bold')
        
        # Get the segmented data
        df_segmented = migration_results.get('segmented_data')
        if df_segmented is None:
            print("❌ No segmented data found in migration results")
            return None
        
        # Get segment info
        segments = [s for s in df_segmented['usage_segment'].unique() if pd.notna(s) and str(s) != 'nan']
        segment_order = ['Segment_1', 'Segment_2', 'Segment_3', 'Low_Usage', 'Medium_Usage', 'High_Usage']
        ordered_segments = [s for s in segment_order if s in segments]
        ordered_segments.extend([s for s in segments if s not in segment_order])
        
        # Calculate proportions
        prop_data = []
        control_label = self.analyzer.control_label if self.analyzer else 'control'
        treatment_label = self.analyzer.treatment_label if self.analyzer else 'treatment'
        variant_column = self.analyzer.variant_column if self.analyzer else 'variant'
        
        for variant in [control_label, treatment_label]:
            variant_data = df_segmented[df_segmented[variant_column] == variant]
            total_users = len(variant_data)
            
            for segment in ordered_segments:
                count = len(variant_data[variant_data['usage_segment'] == segment])
                proportion = count / total_users * 100
                prop_data.append({
                    'variant': variant,
                    'segment': segment,
                    'count': count,
                    'proportion': proportion
                })
        
        prop_df = pd.DataFrame(prop_data)
        
        # Plot 1: Side-by-side bar chart
        ax1 = axes[0]
        control_props = prop_df[prop_df['variant'] == control_label]['proportion'].values
        treatment_props = prop_df[prop_df['variant'] == treatment_label]['proportion'].values
        
        x = np.arange(len(ordered_segments))
        width = 0.35
        
        ax1.bar(x - width/2, control_props, width, label=control_label, 
                color=self.color_palette['control'], alpha=0.7)
        ax1.bar(x + width/2, treatment_props, width, label=treatment_label, 
                color=self.color_palette['treatment'], alpha=0.7)
        
        ax1.set_xlabel('Usage Segment')
        ax1.set_ylabel('Percentage of Users')
        ax1.set_title('Segment Distribution Comparison')
        ax1.set_xticks(x)
        ax1.set_xticklabels(ordered_segments, rotation=45)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Add percentage labels on bars
        for i, (ctrl, trt) in enumerate(zip(control_props, treatment_props)):
            ax1.text(i - width/2, ctrl + 0.5, f'{ctrl:.1f}%', ha='center', va='bottom', fontsize=9)
            ax1.text(i + width/2, trt + 0.5, f'{trt:.1f}%', ha='center', va='bottom', fontsize=9)
        
        # Plot 2: Difference plot (Treatment - Control)
        ax2 = axes[1]
        differences = treatment_props - control_props
        bar_colors = ['green' if d > 0 else 'red' for d in differences]
        
        bars = ax2.bar(ordered_segments, differences, color=bar_colors, alpha=0.7)
        ax2.axhline(y=0, color='black', linestyle='-', linewidth=1)
        ax2.set_xlabel('Usage Segment')
        ax2.set_ylabel('Percentage Point Difference\n(Treatment - Control)')
        ax2.set_title('Treatment Effect by Segment')
        ax2.tick_params(axis='x', rotation=45)
        ax2.grid(True, alpha=0.3)
        
        # Add value labels on bars
        for bar, diff in zip(bars, differences):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + (0.1 if height > 0 else -0.3),
                    f'{diff:+.1f}pp', ha='center', va='bottom' if height > 0 else 'top', fontsize=9)
        
        # Plot 3: Cumulative distribution
        ax3 = axes[2]
        
        # Calculate cumulative proportions from highest to lowest segment
        control_cumsum = []
        treatment_cumsum = []
        
        for i in range(len(ordered_segments)):
            segments_at_or_above = ordered_segments[i:]
            
            control_cum = prop_df[
                (prop_df['variant'] == control_label) & 
                (prop_df['segment'].isin(segments_at_or_above))
            ]['proportion'].sum()
            
            treatment_cum = prop_df[
                (prop_df['variant'] == treatment_label) & 
                (prop_df['segment'].isin(segments_at_or_above))
            ]['proportion'].sum()
            
            control_cumsum.append(control_cum)
            treatment_cumsum.append(treatment_cum)
        
        ax3.plot(range(len(ordered_segments)), control_cumsum, 'o-', 
                 color=self.color_palette['control'], linewidth=2, markersize=8, label=control_label)
        ax3.plot(range(len(ordered_segments)), treatment_cumsum, 's-', 
                 color=self.color_palette['treatment'], linewidth=2, markersize=8, label=treatment_label)
        
        ax3.set_xlabel('Minimum Usage Level')
        ax3.set_ylabel('Cumulative % of Users')
        ax3.set_title('Probability of At Least X Usage')
        ax3.set_xticks(range(len(ordered_segments)))
        ax3.set_xticklabels([f'{s}+' for s in ordered_segments], rotation=45)
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Fill area between curves
        ax3.fill_between(range(len(ordered_segments)), control_cumsum, treatment_cumsum, 
                         alpha=0.3, color='green' if treatment_cumsum[0] > control_cumsum[0] else 'red')
        
        plt.tight_layout()
        return fig
    
    def plot_ordinal_trend(self, 
                          trend_results: Dict[str, Any],
                          figsize: Optional[Tuple[int, int]] = None) -> plt.Figure:
        """
        Visualize ordinal trend analysis results.
        
        Parameters:
        -----------
        trend_results : dict
            Results from analyzer.ordinal_trend_test()
        figsize : tuple, optional
            Figure size override
            
        Returns:
        --------
        plt.Figure : Matplotlib figure object
        """
        
        if figsize is None:
            figsize = (15, 5)
        
        fig, axes = plt.subplots(1, 3, figsize=figsize)
        fig.suptitle('Ordinal Trend Analysis', fontsize=16, fontweight='bold')
        
        ordered_segments = trend_results['ordered_segments']
        control_counts = trend_results['control_counts']
        treatment_counts = trend_results['treatment_counts']
        
        control_label = self.analyzer.control_label if self.analyzer else 'control'
        treatment_label = self.analyzer.treatment_label if self.analyzer else 'treatment'
        
        # Plot 1: Raw counts by segment
        ax1 = axes[0]
        
        x = np.arange(len(ordered_segments))
        width = 0.35
        
        ax1.bar(x - width/2, control_counts, width, label=control_label, 
                color=self.color_palette['control'], alpha=0.7)
        ax1.bar(x + width/2, treatment_counts, width, label=treatment_label, 
                color=self.color_palette['treatment'], alpha=0.7)
        
        ax1.set_xlabel('Segment')
        ax1.set_ylabel('Count')
        ax1.set_title('User Counts by Segment')
        ax1.set_xticks(x)
        ax1.set_xticklabels(ordered_segments, rotation=45)
        ax1.legend()
        ax1.grid(True, alpha=0.3)
        
        # Plot 2: Proportions by segment
        ax2 = axes[1]
        
        total_control = sum(control_counts)
        total_treatment = sum(treatment_counts)
        
        control_props = [c/total_control*100 for c in control_counts]
        treatment_props = [t/total_treatment*100 for t in treatment_counts]
        
        ax2.bar(x - width/2, control_props, width, label=control_label, 
                color=self.color_palette['control'], alpha=0.7)
        ax2.bar(x + width/2, treatment_props, width, label=treatment_label, 
                color=self.color_palette['treatment'], alpha=0.7)
        
        ax2.set_xlabel('Segment')
        ax2.set_ylabel('Percentage (%)')
        ax2.set_title('Proportion by Segment')
        ax2.set_xticks(x)
        ax2.set_xticklabels(ordered_segments, rotation=45)
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        
        # Plot 3: Trend visualization
        ax3 = axes[2]
        
        segment_scores = list(range(len(ordered_segments)))
        
        # Calculate mean scores
        control_score = sum(score * count for score, count in zip(segment_scores, control_counts)) / total_control
        treatment_score = sum(score * count for score, count in zip(segment_scores, treatment_counts)) / total_treatment
        
        ax3.bar(['Control', 'Treatment'], [control_score, treatment_score], 
                color=[self.color_palette['control'], self.color_palette['treatment']], alpha=0.7)
        
        ax3.set_ylabel('Average Segment Score')
        ax3.set_title(f'Mean Segment Scores\n(Diff: {treatment_score - control_score:+.3f})')
        ax3.grid(True, alpha=0.3)
        
        # Add value labels
        ax3.text(0, control_score + 0.01, f'{control_score:.3f}', ha='center', va='bottom', fontweight='bold')
        ax3.text(1, treatment_score + 0.01, f'{treatment_score:.3f}', ha='center', va='bottom', fontweight='bold')
        
        # Add trend test results
        z_stat = trend_results['z_statistic']
        p_value = trend_results['p_value']
        significant = trend_results['significant_trend']
        
        result_text = f"Cochran-Armitage Trend Test:\nZ = {z_stat:.4f}, p = {p_value:.6f}\n"
        result_text += "✅ SIGNIFICANT TREND" if significant else "❌ NO SIGNIFICANT TREND"
        
        ax3.text(0.5, 0.7, result_text, transform=ax3.transAxes, ha='center', va='center',
                bbox=dict(boxstyle="round,pad=0.3", facecolor="yellow" if significant else "lightgray", alpha=0.7))
        
        plt.tight_layout()
        return fig
    
    def plot_batch_results(self, 
                          batch_results: pd.DataFrame,
                          figsize: Optional[Tuple[int, int]] = None) -> plt.Figure:
        """
        Plot results from batch significance testing.
        
        Parameters:
        -----------
        batch_results : pd.DataFrame
            Results from analyzer.batch_significance_test()
        figsize : tuple, optional
            Figure size override
            
        Returns:
        --------
        plt.Figure : Matplotlib figure object
        """
        
        if batch_results.empty:
            print("❌ No batch results to plot")
            return None
        
        if figsize is None:
            figsize = (15, 10)
        
        fig, axes = plt.subplots(2, 2, figsize=figsize)
        fig.suptitle('Batch A/B Test Results', fontsize=16, fontweight='bold')
        
        # Sort by absolute lift for better visualization
        batch_results = batch_results.sort_values('absolute_lift', ascending=True)
        
        # Plot 1: Absolute lift by metric
        ax1 = axes[0, 0]
        
        bars = ax1.barh(batch_results['metric'], batch_results['absolute_lift'],
                       color=['green' if x > 0 else 'red' for x in batch_results['absolute_lift']], alpha=0.7)
        
        ax1.axvline(x=0, color='black', linestyle='-', linewidth=1)
        ax1.set_xlabel('Absolute Lift')
        ax1.set_title('Absolute Treatment Effect by Metric')
        ax1.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, lift in zip(bars, batch_results['absolute_lift']):
            width = bar.get_width()
            ax1.text(width + (0.01 if width >= 0 else -0.01), bar.get_y() + bar.get_height()/2, 
                    f'{lift:.3f}', ha='left' if width >= 0 else 'right', va='center', fontweight='bold')
        
        # Plot 2: Relative lift by metric
        ax2 = axes[0, 1]
        
        bars = ax2.barh(batch_results['metric'], batch_results['relative_lift_percent'],
                       color=['green' if x > 0 else 'red' for x in batch_results['relative_lift_percent']], alpha=0.7)
        
        ax2.axvline(x=0, color='black', linestyle='-', linewidth=1)
        ax2.set_xlabel('Relative Lift (%)')
        ax2.set_title('Relative Treatment Effect by Metric')
        ax2.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, lift in zip(bars, batch_results['relative_lift_percent']):
            width = bar.get_width()
            ax2.text(width + (0.1 if width >= 0 else -0.1), bar.get_y() + bar.get_height()/2, 
                    f'{lift:.1f}%', ha='left' if width >= 0 else 'right', va='center', fontweight='bold')
        
        # Plot 3: P-values by metric
        ax3 = axes[1, 0]
        
        # Color by significance
        colors = ['green' if sig else 'red' for sig in batch_results['is_significant']]
        bars = ax3.barh(batch_results['metric'], -np.log10(batch_results['p_value']), color=colors, alpha=0.7)
        
        ax3.axvline(x=-np.log10(0.05), color='red', linestyle='--', linewidth=2, label='α = 0.05')
        ax3.set_xlabel('-log10(p-value)')
        ax3.set_title('Statistical Significance by Metric')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        
        # Add value labels
        for bar, p_val in zip(bars, batch_results['p_value']):
            width = bar.get_width()
            ax3.text(width + 0.05, bar.get_y() + bar.get_height()/2, 
                    f'{p_val:.4f}', ha='left', va='center', fontsize=9)
        
        # Plot 4: Sample sizes by metric
        ax4 = axes[1, 1]
        
        x = np.arange(len(batch_results))
        width = 0.35
        
        ax4.bar(x - width/2, batch_results['control_size'], width, label='Control', 
                color=self.color_palette['control'], alpha=0.7)
        ax4.bar(x + width/2, batch_results['treatment_size'], width, label='Treatment', 
                color=self.color_palette['treatment'], alpha=0.7)
        
        ax4.set_xlabel('Metric')
        ax4.set_ylabel('Sample Size')
        ax4.set_title('Sample Sizes by Metric')
        ax4.set_xticks(x)
        ax4.set_xticklabels(batch_results['metric'], rotation=45)
        ax4.legend()
        ax4.grid(True, alpha=0.3)
        
        plt.tight_layout()
        return fig


# Convenience functions for backward compatibility
def plot_ab_test_with_test_type(df: pd.DataFrame,
                                test_column: str,
                                variant_column: str = 'variant',
                                control_label: str = 'control',
                                treatment_label: str = 'treatment',
                                alpha: float = 0.05,
                                test_type: str = 'auto',
                                figsize: Tuple[int, int] = (15, 10),
                                show_clt: bool = True) -> Tuple[Dict[str, Any], plt.Figure]:
    """
    Convenience function for plotting A/B test results.
    
    This function maintains backward compatibility with the original notebook functions.
    """
    try:
        from sessions_analysis_toolkit import SessionsAnalyzer
        
        analyzer = SessionsAnalyzer(
            df=df,
            metric_columns=[test_column],
            variant_column=variant_column,
            user_id_column=df.columns[0],  # Assume first column is user ID
            control_label=control_label,
            treatment_label=treatment_label
        )
        
        plotter = SessionsPlotter(analyzer, figsize=figsize)
        return plotter.plot_ab_test(test_column, test_type, show_clt, figsize)
        
    except ImportError:
        print("Error: sessions_analysis_toolkit not found. Using standalone implementation.")
        # Could implement standalone version here if needed
        return {}, None


if __name__ == "__main__":
    print("📊 Sessions Plotting Toolkit loaded successfully!")
    print("💡 Main class: SessionsPlotter")
    print("💡 Convenience function: plot_ab_test_with_test_type()")
    print("\n🔗 Example usage:")
    print("""
    from sessions_plotting_toolkit import SessionsPlotter
    from sessions_analysis_toolkit import SessionsAnalyzer
    
    # Initialize analyzer and plotter
    analyzer = SessionsAnalyzer(df=data, metric_columns=['pageviews'], ...)
    plotter = SessionsPlotter(analyzer)
    
    # Plot A/B test results
    results, fig = plotter.plot_ab_test('pageviews')
    plt.show()
    
    # Plot segment analysis
    seg_results = analyzer.segment_analysis('pageviews')
    fig = plotter.plot_segment_comparison(seg_results, 'pageviews')
    plt.show()
    
    # Plot migration analysis
    migration_results = analyzer.migration_analysis('pageviews')
    fig = plotter.plot_migration_analysis(migration_results)
    plt.show()
    """)