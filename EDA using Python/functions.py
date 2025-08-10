# Import
import os
import pandas as pd
import re
import logging
import time
import functools

def timer(func):
    """
    Decorator to measure the execution time of a function.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()            # Record start time
        result = func(*args, **kwargs) # Call the original function
        end = time.time()              # Record end time
        print(f"{func.__name__} took {end - start:.4f} seconds")  # Print elapsed time
        return result                 # Return the function's result
    return wrapper

@timer   
def load_dataset(url):
    """
    Load dataset from a given URL into a pandas DataFrame.
    Validate mandatory columns exist.
    """
    mandatory_cols = ['statistic label', 'year', 'sex', 'unit', 'value']

    try:
        df = pd.read_csv(url)
        logging.info(f"Dataset loaded successfully with {df.shape[0]} rows and {df.shape[1]} columns")

        # Normalize columns for case-insensitive comparison
        cols_lower = [col.strip().lower() for col in df.columns]
        missing_cols = [col for col in mandatory_cols if col not in cols_lower]

        if missing_cols:
            error_msg = f"Schema error: missing mandatory columns {missing_cols}"
            logging.error(error_msg)
            return None, error_msg

        return df, None

    except Exception as e:
        error_msg = f"Error loading dataset: {e}"
        logging.error(error_msg)
        return None, error_msg

@timer
def process_first_year_stats(df):
    """
    Filter 'First Year' statistics, group years into 5-year ranges,
    and summarize counts and percentages by sex and year group.
    """
    try:
        # Normalize column names: lowercase and strip whitespace
        df.columns = df.columns.str.strip().str.lower()
        
        # Regex to filter rows containing "first year" or "1st year" in 'statistic label'
        pattern = r"\b(first year|1st year)\b"
        df = df[df['statistic label'].astype(str).str.contains(pattern, case=False, na=False, regex=True)].copy()
        
        # Normalize sex values (M/F to Male/Female)
        df['sex'] = df['sex'].replace({'M': 'Male', 'F': 'Female', 'Both': 'Both sexes'})
        
        df['value'] = pd.to_numeric(df['value'], errors='coerce').fillna(0)

        # Create 'year_group' column grouping years into 5-year intervals (e.g., 2000-2004)
        df['year_group'] = (df['year'] // 5) * 5
        df['year_range'] = df['year_group'].astype(str) + "-" + (df['year_group'] + 4).astype(str)

        # Split DataFrame into counts (unit == 'number') and percentages (unit == '%' or 'percentage')
        df_counts = df[df['unit'].str.lower() == 'number'].copy()
        df_perc = df[df['unit'].str.strip().str.lower().isin(['%', 'percentage'])].copy()

        # Summarize counts by summing 'value' grouped by year range and sex
        counts_summary = df_counts.groupby(['year_range', 'sex'], as_index=False)['value'].sum()
        # Summarize percentages by mean of 'value' grouped by year range and sex
        perc_summary = df_perc.groupby(['year_range', 'sex'], as_index=False)['value'].mean()

        # Pivot counts summary to wide format with separate columns per sex and suffix '_count'
        counts_wide = counts_summary.pivot(index='year_range', columns='sex', values='value').add_suffix('_count')
        # Pivot percentages summary similarly with suffix '_percent'
        perc_wide = perc_summary.pivot(index='year_range', columns='sex', values='value').add_suffix('_percent')

        # Round counts to nearest integer and convert to int type
        counts_wide = counts_wide.round(0).astype(int)
        # Fill missing percentage values with 0 and round to one decimal place
        perc_wide = perc_wide.fillna(0.0).round(1)

        # Merge counts and percentages DataFrames on 'year_range' column
        merged = pd.merge(counts_wide, perc_wide, on='year_range', how='inner').reset_index()

        return merged, None
    
    except Exception as e:
        error_msg = f"Error transforming dataset: {e}"
        logging.error(error_msg)
        return None, error_msg
@timer
def rename_header(df):
    """
    Rename DataFrame column headers   
    """
    try:
        if df is None:
            raise ValueError("No DataFrame provided for transformation.")
        
        # Apply regex substitutions on column names
        df.columns = [
            re.sub(r"_+$", "",                       # Remove trailing underscores
            re.sub(r"_+", "_",                        # Replace multiple underscores with single underscore
            re.sub(r"[\s,()]+", "_", col.strip().lower())  # Replace spaces, commas, parentheses with underscore
            ))
            for col in df.columns
        ]

        logging.info("Headers renamed successfully")
        return df, None

    except Exception as e:
        error_msg = f"Error renaming headers: {e}"
        logging.error(error_msg)
        return None, error_msg


@timer
def save_outputs(df, csv_path, parquet_path):
    """
    Save the DataFrame to CSV and Parquet file formats.
    """
    # Save CSV independently
    try:
        df.to_csv(csv_path, index=False)
        logging.info(f"Data saved to CSV: {csv_path}")
    except Exception as e:
        logging.error(f"Error saving CSV to {csv_path}: {e}")

    # Save Parquet independently
    try:
        df.to_parquet(parquet_path, index=False)
        logging.info(f"Data saved to Parquet: {parquet_path}")
    except Exception as e:
        logging.error(f"Error saving Parquet to {parquet_path}: {e}")


# main
if __name__ == "__main__":
    """
    Main execution flow
    """
    url = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/EDA14/CSV/1.0/en"
    
    # Determine base directory of this script to save files relative to it
    base_dir = os.path.dirname(os.path.abspath(__file__))
    csv_path = os.path.join(base_dir, "result.csv")
    parquet_path = os.path.join(base_dir, "result.parquet")


    # Step 1: Load dataset from the URL
    df, err = load_dataset(url)
    if df is not None:
        # Step 2: Process dataset for first year stats
        df_processed, err = process_first_year_stats(df)
        if df_processed is not None:
            # Step 3: Rename headers for consistency
            df_renamed, err = rename_header(df_processed)
            if df_renamed is not None:
                # Step 4: Save processed data to CSV and Parquet
                save_outputs(df_renamed, csv_path, parquet_path)
