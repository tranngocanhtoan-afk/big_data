from .utils import *
import pandas as pd
import os
def process_single_file(file_path, results_dir=None):
    """
    Process a single data file and write results to a .txt file.
    
    Args:
        file_path: Path to the file to process
        results_dir: Directory to save results (defaults to same directory as file)
    
    Returns:
        Path to the result file or None if processing failed
    """
    if not os.path.isfile(file_path):
        print(f"Error: {file_path} is not a valid file")
        return None
        
    if not file_path.lower().endswith(('.csv', '.json')):
        print(f"Error: {file_path} is not a CSV or JSON file")
        return None
    
    # Create results directory if it doesn't exist
    if results_dir is None:
        results_dir = os.path.join(os.path.dirname(file_path), 'results')
    os.makedirs(results_dir, exist_ok=True)
    
    try:
        print(f"Processing file: {os.path.basename(file_path)}")
        
        # Read and preprocess the data
        df = read_and_preprocess_csv(file_path) 
        df_norm = normalize_and_encode(df)
        analysis = analyze_dataframe(df_norm)
        
        # Calculate file size in MB
        file_size_bytes = os.path.getsize(file_path)
        file_size_mb = file_size_bytes / (1024 * 1024)
        
        # Create header with file information
        file_name = os.path.basename(file_path)
        header = f'==== Analysis Report for: {file_name} ====\n'
        header += f'File size: {file_size_mb:.2f} MB ({file_size_bytes:,} bytes)\n'
        header += f'Analysis date: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}\n'
        header += f'Original rows: {df.shape[0]}, columns: {df.shape[1]}\n'
        header += '=' * 60
        
        # Define result file path
        file_base = os.path.splitext(file_name)[0]
        result_path = os.path.join(results_dir, f'{file_base}_analysis.txt')
        
        # Write analysis to txt file
        write_analysis_to_txt(result_path, header, analysis, df_norm)
        
        print(f"✓ Analysis completed -> {result_path}")
        return result_path
        
    except Exception as e:
        print(f"✗ Error processing file: {e}")
        return None



