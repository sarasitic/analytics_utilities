import pandas as pd

def run_time_bound_analysis(
    df, 
    slice_column=None, 
    time_bounds=[None], 
    slice_values=None, 
    conversion_timestamp_col='conversion_timestamp',
    assignment_timestamp_col='first_assigned_at',
    conversion_col='conversion'
):
    """
    Run analysis across different time bounds and slices of data, handling nulls and edge cases.
    Calls calculate_significance for each group.
    assignment_timestamp_col: column name for assignment timestamp (default 'first_assigned_at')
    conversion_col: binary conversion column (default 'conversion')
    conversion_timestamp_col: timestamp column for conversion (default 'conversion_timestamp')
    """
    results = {}

    # Defensive: check required columns
    required_cols = {'shop_id', assignment_timestamp_col, 'variant', conversion_timestamp_col, 'total'}
    missing = required_cols - set(df.columns)
    if missing:
        print(f"Missing required columns: {missing}")
        return {}

    max_date = max(df[assignment_timestamp_col].max(), df[conversion_timestamp_col].max())

    # Handle multiple slice columns (optional)
    if isinstance(slice_column, list):
        for col in slice_column:
            if col in df.columns:
                print(f"\n--- Analyzing by slice: {col} ---")
                results[col] = run_time_bound_analysis(
                    df, slice_column=col, time_bounds=time_bounds, 
                    slice_values=None, conversion_timestamp_col=conversion_timestamp_col,
                    assignment_timestamp_col=assignment_timestamp_col,
                    conversion_col=conversion_col
                )
            else:
                print(f"Column '{col}' not found in DataFrame. Skipping.")
        return results

    # If no slice column specified, run for whole dataset
    if slice_column is None:
        for bound in time_bounds:
            bound_name = 'unbound' if bound is None else f'{bound}_days'
            if bound is not None:
                eligible_df = df[(max_date - df[assignment_timestamp_col]) >= pd.Timedelta(days=bound)].copy()
            else:
                eligible_df = df.copy()
            if len(eligible_df) == 0:
                print(f"\nResults for {bound_name}: No data available.")
                results[bound_name] = None
                continue
            # Add conversion column if needed
            if conversion_col not in eligible_df.columns and conversion_timestamp_col in eligible_df.columns:
                if bound is not None:
                    eligible_df[conversion_col] = ~eligible_df[conversion_timestamp_col].isna() & \
                        (eligible_df[conversion_timestamp_col] < (eligible_df[assignment_timestamp_col] + pd.Timedelta(days=bound)))
                else:
                    eligible_df[conversion_col] = ~eligible_df[conversion_timestamp_col].isna()
            # Call calculate_significance
            print(f"\nResults for {bound_name}:")
            result = calculate_significance(
                eligible_df,
                variant_col='variant',
                conversion_col=conversion_col,
                conversion_timestamp_col=conversion_timestamp_col if conversion_col not in eligible_df.columns else None
            )
            print(result)
            results[bound_name] = result
            print('-'*100)
        return results

    # If slice_column is provided but not in df, skip
    if slice_column not in df.columns:
        print(f"Column '{slice_column}' not found in DataFrame. Skipping slice analysis.")
        return results

    # Get slice values if not specified, including nulls
    if slice_values is None:
        slice_values = list(df[slice_column].unique())
        if df[slice_column].isnull().any():
            slice_values.append(None)

    for slice_value in slice_values:
        if slice_value is None:
            slice_df = df[df[slice_column].isnull()]
            display_value = "NULL"
        else:
            slice_df = df[df[slice_column] == slice_value]
            display_value = str(slice_value)

        if len(slice_df) == 0:
            print(f"\nNo data for {slice_column}: {display_value}")
            continue

        results[display_value] = {}

        print(f"\nResults for {slice_column}: {display_value}")
        print(f"Number of records: {len(slice_df)}")
        conversion_rate = (~slice_df[conversion_timestamp_col].isna()).mean()
        print(f"Overall conversion rate: {100 * conversion_rate:.2f}%")
        print('='*100)

        for bound in time_bounds:
            bound_name = 'unbound' if bound is None else f'{bound}_days'
            if bound is not None:
                eligible_slice_df = slice_df[(max_date - slice_df[assignment_timestamp_col]) >= pd.Timedelta(days=bound)].copy()
            else:
                eligible_slice_df = slice_df.copy()

            if len(eligible_slice_df) > 0:
                print(f"\n{bound_name} - Data Summary:")
                # Add conversion column if needed
                if conversion_col not in eligible_slice_df.columns and conversion_timestamp_col in eligible_slice_df.columns:
                    if bound is not None:
                        eligible_slice_df[conversion_col] = ~eligible_slice_df[conversion_timestamp_col].isna() & \
                            (eligible_slice_df[conversion_timestamp_col] < (eligible_slice_df[assignment_timestamp_col] + pd.Timedelta(days=bound)))
                    else:
                        eligible_slice_df[conversion_col] = ~eligible_slice_df[conversion_timestamp_col].isna()
                # Call calculate_significance
                result = calculate_significance(
                    eligible_slice_df,
                    variant_col='variant',
                    conversion_col=conversion_col,
                    conversion_timestamp_col=conversion_timestamp_col if conversion_col not in eligible_slice_df.columns else None
                )
                print(result)
                results[display_value][bound_name] = result
            else:
                print(f"\n{bound_name}: No data available")
                results[display_value][bound_name] = None
            print('-'*100)

    return results