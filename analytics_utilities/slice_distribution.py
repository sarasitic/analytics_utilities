import pandas as pd

def analyze_slice_distribution(df, slice_column, conversion_timestamp_col='conversion_timestamp', time_bounds=[None], assignment_timestamp_col='first_assigned_at'):
    """
    Analyze the distribution of conversions within a slice based on timestamp, split by variant,
    for multiple time bounds.

    Parameters:
    -----------
    df : DataFrame
    slice_column : str
        Column to slice by
    conversion_timestamp_col : str
        Column containing the conversion timestamp
    time_bounds : list of int or None
        List of days to bound the conversion window, None for unbounded
    assignment_timestamp_col : str
        Column containing the assignment timestamp (default 'first_assigned_at')
    """
    results = {}

    # Find the latest date in the data (either assignment or conversion)
    max_date = max(
        df[assignment_timestamp_col].max(),
        df[conversion_timestamp_col].max()
    )

    for time_bound in time_bounds:
        print(f"\nAnalyzing distribution for {slice_column}")
        if time_bound is not None:
            print(f"Time bound: {time_bound} days")
            # Only include rows where at least time_bound days have passed since assignment
            eligible_df = df[
                (max_date - df[assignment_timestamp_col]) >= pd.Timedelta(days=time_bound)
            ].copy()
        else:
            print("Time bound: Unbounded")
            eligible_df = df.copy()
        print("="*80)

        # Define conversion calculation function to ensure consistency
        def get_conversions(group):
            if time_bound is not None:
                return (
                    ~group[conversion_timestamp_col].isna() &
                    (group[conversion_timestamp_col] < (group[assignment_timestamp_col] + pd.Timedelta(days=time_bound)))
                ).sum()
            return group[conversion_timestamp_col].notna().sum()

        def get_conversion_rate(group):
            conversions = get_conversions(group)
            total = len(group)
            return (conversions / total * 100) if total > 0 else 0

        # Overall statistics
        total_records = len(eligible_df)
        total_conversions = get_conversions(eligible_df)
        overall_rate = (total_conversions / total_records * 100) if total_records > 0 else 0

        print(f"Overall: {total_conversions}/{total_records} ({overall_rate:.2f}%)")

        # By variant
        variant_stats = eligible_df.groupby('variant').agg({
            conversion_timestamp_col: [
                ('Total', 'size'),
                ('Conversions', lambda x: get_conversions(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                }))),
                ('Conversion Rate %', lambda x: get_conversion_rate(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                })))
            ]
        }).round(2)

        variant_stats.columns = variant_stats.columns.droplevel()  # Flatten MultiIndex
        print("\nBy variant:")
        print(variant_stats)
        print("-"*80)

        # By slice value and variant
        slice_variant_stats = eligible_df.groupby([slice_column, 'variant']).agg({
            conversion_timestamp_col: [
                ('Total', 'size'),
                ('Conversions', lambda x: get_conversions(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                }))),
                ('Conversion Rate %', lambda x: get_conversion_rate(pd.DataFrame({
                    conversion_timestamp_col: x,
                    assignment_timestamp_col: x.index.map(eligible_df[assignment_timestamp_col].get)
                })))
            ]
        }).round(2)

        slice_variant_stats.columns = slice_variant_stats.columns.droplevel()  # Flatten MultiIndex
        print(f"\nBy {slice_column} and variant:")
        print(slice_variant_stats)

        # Check for perfect conversion combinations
        perfect_conv = slice_variant_stats[slice_variant_stats['Conversion Rate %'].isin([0, 100])]
        if not perfect_conv.empty:
            print("\nWarning: Perfect conversion rates detected:")
            print(perfect_conv)

        results[time_bound] = {
            'overall': {'total': total_records, 'conversions': total_conversions, 'rate': overall_rate},
            'by_variant': variant_stats,
            'by_slice_and_variant': slice_variant_stats
        }

    return results