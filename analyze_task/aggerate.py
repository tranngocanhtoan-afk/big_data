import os
import re
import pandas as pd
from .processdata import *
from .utils import *
def analyze_blocks(data_dir):
    """
    Analyze all CSV files in the given directory and upload results to server.
    """
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            try:
                file_path = os.path.join(data_dir, file)
                df = read_and_preprocess_csv(file_path) 
                df_norm = normalize_and_encode(df)
                analysis = analyze_dataframe(df_norm)
                file_size = os.path.getsize(file_path)
                header = f'==== Analyzing file: {file} (size: {file_size:.2f} MB) ===='
                result_path = os.path.join(data_dir, f'{file}_analysis.txt')
                write_analysis_to_txt(result_path, header, analysis, df_norm)
                print(f"Finished processing {file}, sending data to server")
            except Exception as e:
                print(f"Error processing file {file}: {e}")
def aggregate_ensemble_results(results_dir):
    """
    Read multiple .txt analysis files, extract statistical summaries, and compute
    simple averages for each column (dropping NaN values).
    """
    pattern = re.compile(
        r"(?P<column>.+?)\s+(?P<count>\d+\.?\d*)\s+(?P<mean>[-+]?\d*\.?\d+)\s+"
        r"(?P<std>[-+]?\d*\.?\d+)\s+(?P<min>[-+]?\d*\.?\d+)\s+(?P<q25>[-+]?\d*\.?\d+)\s+"
        r"(?P<q50>[-+]?\d*\.?\d+)\s+(?P<q75>[-+]?\d*\.?\d+)\s+(?P<max>[-+]?\d*\.?\d+)"
    )
    numeric_cols = ['count', 'mean', 'std', 'min', 'q25', 'q50', 'q75', 'max']
    all_stats = []

    for fname in os.listdir(results_dir):
        if not fname.endswith(".txt"):
            continue
        filepath = os.path.join(results_dir, fname)
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        section = re.search(r"Basic statistics:(.*?)Unique value statistics per column:", content, re.S)
        if not section:
            continue
        for line in section.group(1).strip().split("\n"):
            match = pattern.match(line.strip())
            if match:
                data = match.groupdict()
                data["source_file"] = fname
                all_stats.append(data)

    if not all_stats:
        raise ValueError("No valid stats found in any file.")

    df = pd.DataFrame(all_stats)
    df[numeric_cols] = df[numeric_cols].astype(float)
    
    # Compute both mean and std, keeping both columns
    ensemble = df.groupby("column")[numeric_cols].agg(['mean', 'std'])
    
    # Flatten the column names (e.g., 'count_mean', 'count_std', 'mean_mean', 'mean_std', etc.)
    ensemble.columns = ['_'.join(col).strip() for col in ensemble.columns.values]
    
    # Drop any columns that are all NaN
    ensemble = ensemble.dropna(axis=1, how='all')
    
    # Drop any rows that are all NaN
    ensemble = ensemble.dropna(axis=0, how='all')
    
    return ensemble
