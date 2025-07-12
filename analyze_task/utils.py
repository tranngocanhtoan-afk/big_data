import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.preprocessing import StandardScaler, LabelEncoder
import seaborn as sns
import warnings

import pandas as pd

def read_and_preprocess_csv(filepath, nrows=None):
    # Step 1: Read CSV
    if filepath.endswith('.csv'):
        df = pd.read_csv(filepath, nrows=nrows, encoding='utf-8')
    if filepath.endswith("json"):
        df = pd.read_json(filepath, nrows=nrows, encoding='utf-8')
    # Step 2: Drop columns ending with '_id'
    df = df.drop(columns=[col for col in df.columns if col.endswith('_id')], errors='ignore')

    # Step 3: Fill missing values with column median (numeric) or mode (categorical)
    for col in df.columns:
        if df[col].dtype in ['float64', 'int64']:
            df[col] = df[col].fillna(df[col].median())

    # Step 4: Drop any rows that still contain NaNs (just in case)
    df = df.dropna()

    # Step 5: Clip outliers (1st–99th percentile for numeric columns)
    for col in df.select_dtypes(include=['number']):
        lower = df[col].quantile(0.01)
        upper = df[col].quantile(0.99)
        df[col] = df[col].clip(lower, upper)

    return df


def analyze_dataframe(df):

    warnings.simplefilter(action='ignore', category=FutureWarning)
    desc = df.describe(include="all").transpose().fillna("").infer_objects(copy=False)

    info = [
        f'Number of rows: {df.shape[0]}',
        f'Number of columns: {df.shape[1]}',
        f'Column names: {list(df.columns)}',
        'Basic statistics:',
        desc.to_string(),
        '\nUnique value statistics per column:'
    ]
    for col in df.columns:
        unique_count = df[col].nunique(dropna=True)
        info.append(f'Column: {col} | Unique: {unique_count}')
    return '\n'.join(info)

def plot_histograms(df, filename, results_dir):
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        plt.figure(figsize=(8, 5))
        df[col].hist(bins=30, color='#3498db', edgecolor='black', alpha=0.85)
        plt.title(f'{filename} - {col} Histogram', fontsize=14, fontweight='bold')
        plt.xlabel(col, fontsize=12)
        plt.ylabel('Frequency', fontsize=12)
        plt.grid(True, linestyle='--', alpha=0.7)
        plt.tick_params(axis='both', which='major', labelsize=10)
        plt.tight_layout()
        plot_path = os.path.join(results_dir, f'{filename}_{col}_hist.png')
        plt.savefig(plot_path, dpi=120)
        plt.close()
def plot_correlation_matrix(df, filename, results_dir):
    numeric_cols = df.select_dtypes(include=['number']).columns
    corr_matrix = df[numeric_cols].corr()
    plt.figure(figsize=(12, 8))
    sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, linewidths=0.5)
    plt.title(f'{filename} - Correlation Matrix', fontsize=14, fontweight='bold')
    plt.tight_layout()
def normalize_and_encode(df):
    df_norm = df.copy()
    numeric_cols = df_norm.select_dtypes(include=['number']).columns
    if not numeric_cols.empty:
        scaler = StandardScaler()
        df_norm[numeric_cols] = scaler.fit_transform(df_norm[numeric_cols])
    cat_cols = df_norm.select_dtypes(include=['object', 'category']).columns
    for col in cat_cols:
        le = LabelEncoder()
        try:
            df_norm[col] = le.fit_transform(df_norm[col].astype(str))
        except Exception as e:
            df_norm[col] = df_norm[col]
    return df_norm

def write_analysis_to_txt(result_path, header, analysis, df_norm):
    """
    Write analysis results to a text file with proper formatting.
    """
    try:
        # Ensure the directory exists
        os.makedirs(os.path.dirname(result_path), exist_ok=True)
        
        with open(result_path, 'w', encoding='utf-8') as out:
            out.write(header + '\n\n')
            out.write('STATISTICAL ANALYSIS:\n')
            out.write('-' * 50 + '\n')
            out.write(analysis + '\n\n')
            out.write('SAMPLE DATA (First 20 rows after preprocessing):\n')
            out.write('-' * 50 + '\n')
            out.write(df_norm.head(20).to_string(index=False) + '\n\n')
            out.write('DATA TYPES:\n')
            out.write('-' * 50 + '\n')
            out.write(df_norm.dtypes.to_string() + '\n\n')
            out.write('MEMORY USAGE:\n')
            out.write('-' * 50 + '\n')
            out.write(f"Memory usage: {df_norm.memory_usage(deep=True).sum() / 1024:.2f} KB\n")
            out.write(f"Shape: {df_norm.shape[0]} rows x {df_norm.shape[1]} columns\n")
        
        print(f"Analysis written to: {result_path}")
        return result_path
        
    except Exception as e:
        print(f"Error writing analysis to file: {e}")
        return None 


if __name__ == '__main__':
    # This module is designed to be imported, not run directly
    print("Utils module loaded successfully") 