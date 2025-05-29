def calculateSignificance(
    df, 
    variant_col='variant', 
    conversion_col='conversion', 
    control_label='control',
    alpha=0.05,
    conversion_timestamp_col=None
):
    """
    Returns a DataFrame with stats for each variant vs control.
    If conversion_timestamp_col is provided, uses it to create a binary conversion column (1 if timestamp is not null, 0 otherwise).
    """
    temp_conversion_col = None
    if conversion_timestamp_col is not None:
        temp_conversion_col = '__temp_conversion__'
        df = df.copy()
        df[temp_conversion_col] = df[conversion_timestamp_col].notna().astype(int)
        use_conversion_col = temp_conversion_col
    else:
        use_conversion_col = conversion_col

    # Aggregate stats
    stats = (
        df.groupby(variant_col)[use_conversion_col]
        .agg(['sum', 'count'])
        .rename(columns={'sum': 'conversions', 'count': 'total'})
        .reset_index()
    )
    stats['cvr'] = stats['conversions'] / stats['total']

    # Find control row
    if control_label not in stats[variant_col].values:
        raise ValueError(f"No '{control_label}' group found in variant column.")
    control_row = stats[stats[variant_col] == control_label].iloc[0]

    # Confidence intervals
    ci_low, ci_upp = proportion_confint(stats['conversions'], stats['total'], alpha=alpha, method='wilson')
    stats['ci_lower'] = ci_low
    stats['ci_upper'] = ci_upp

    # Uplift, p-value, significance
    stats['uplift'] = stats['cvr'] / control_row['cvr'] - 1    
    stats['p_value'] = np.nan
    stats['is_significant'] = False

    for i, row in stats.iterrows():
        if row[variant_col] == control_label:
            stats.at[i, 'uplift'] = 0.0
            stats.at[i, 'p_value'] = 1.0
            stats.at[i, 'is_significant'] = False
        else:
            count = np.array([row['conversions'], control_row['conversions']])
            nobs = np.array([row['total'], control_row['total']])
            stat, pval = proportions_ztest(count, nobs)
            stats.at[i, 'p_value'] = pval
            stats.at[i, 'is_significant'] = pval < alpha

    # Formatting for display
    stats['cvr'] = (stats['cvr'] * 100).map('{:.2f}%'.format)
    stats['uplift'] = stats['uplift'].map(lambda x: '{:+.2f}%'.format(x * 100))
    stats['ci_lower'] = (stats['ci_lower'] * 100).map('{:.2f}%'.format)
    stats['ci_upper'] = (stats['ci_upper'] * 100).map('{:.2f}%'.format)
    stats['p_value'] = stats['p_value'].map('{:.4f}'.format)

    # Reorder columns
    stats = stats[[variant_col, 'total', 'conversions', 'cvr', 'uplift', 'ci_lower', 'ci_upper', 'p_value', 'is_significant']]
    return stats