import pandas as pd
import pandasql as ps
import os
import sys
from datetime import datetime
from typing import Dict, Tuple

def run_query(query: str, dataframes: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """
    Executes a SQL query on the provided dataframes.

    Parameters:
    query (str): The SQL query to execute.
    dataframes (dict): A dictionary where keys are table names and values are pandas DataFrames.

    Returns:
    pd.DataFrame: The result of the SQL query as a pandas DataFrame.
    """
    # Create a local namespace for the query execution
    local_namespace = {name: df for name, df in dataframes.items()}
    
    # Execute the query using pandasql
    result = ps.sqldf(query, local_namespace)
    
    # Ensure we always return a DataFrame
    if result is None:
        return pd.DataFrame()
    
    return result

def load_data() -> pd.DataFrame:
    """
    Load all_assignments data from either XLSX or CSV file.
    
    Returns:
    pd.DataFrame: The loaded dataframe.
    """
    # Check for xlsx first, then csv
    if os.path.exists('all_assignments.xlsx'):
        print("Loading data from all_assignments.xlsx...")
        return pd.read_excel('all_assignments.xlsx')
    elif os.path.exists('all_assignments.csv'):
        print("Loading data from all_assignments.csv...")
        return pd.read_csv('all_assignments.csv')
    else:
        raise FileNotFoundError("Neither all_assignments.xlsx nor all_assignments.csv found in current directory")

def save_results(df: pd.DataFrame, base_filename: str = 'query_results') -> Tuple[str, str]:
    """
    Save dataframe to both CSV and XLSX formats with timestamp in query_results folder.
    
    Parameters:
    df (pd.DataFrame): The dataframe to save.
    base_filename (str): Base name for output files.
    
    Returns:
    Tuple[str, str]: Paths to the CSV and XLSX files.
    """
    # Create query_results directory if it doesn't exist
    output_dir = 'query_results'
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_filename = os.path.join(output_dir, f"{base_filename}_{timestamp}.csv")
    xlsx_filename = os.path.join(output_dir, f"{base_filename}_{timestamp}.xlsx")
    
    df.to_csv(csv_filename, index=False)
    print(f"✓ Results saved to {csv_filename}")
    
    df.to_excel(xlsx_filename, index=False)
    print(f"✓ Results saved to {xlsx_filename}")
    
    return csv_filename, xlsx_filename

def print_example_query() -> None:
    """
    Print an example query to help users get started.
    """
    print("\n" + "="*80)
    print("EXAMPLE QUERY")
    print("="*80)
    print("""
This example finds patents with the following criteria:
- Application Status = Patented Case
- Entity Status = either Micro or Small
- Conveyance = ASSIGNMENT OF ASSIGNOR'S INTEREST

And includes only specific columns in the output:
- Patent Number, Inventors, Assignees, Correspondent Address, Attorney Name, Attorney Address

SQL Query:
-----------
SELECT 
    "Patent Number",
    Inventors,
    Assignees,
    "Correspondent Address",
    "Attorney Name",
    "Attorney Address"
FROM all_assignments
WHERE 
    "Application Status" LIKE '%Patented Case%'
    AND ("Entity Status" = 'Micro' OR "Entity Status" = 'Small')
    AND Conveyance = 'ASSIGNMENT OF ASSIGNOR''S INTEREST'
""")
    print("="*80 + "\n")

def main() -> None:
    """
    Main function to run the query tool from command line.
    """
    print("\n" + "="*80)
    print("PATENT ASSIGNMENT QUERY TOOL")
    print("="*80)
    
    # Print example query
    print_example_query()
    
    # Load data
    try:
        df = load_data()
        print(f"Loaded {len(df)} records\n")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    
    # Get query from user
    print("Enter your SQL query (table name: all_assignments)")
    print("Type your query and press Ctrl+D (Mac/Linux) or Ctrl+Z (Windows) when done:")
    print("-" * 80)
    
    try:
        query = sys.stdin.read().strip()
    except KeyboardInterrupt:
        print("\n\nQuery cancelled by user.")
        sys.exit(0)
    
    if not query:
        print("\nNo query entered. Exiting.")
        sys.exit(0)
    
    # Execute query
    print("\nExecuting query...")
    try:
        result = run_query(query, {'all_assignments': df})
        print(f"Query returned {len(result)} rows\n")
        
        # Display first few rows
        if len(result) > 0:
            print("Preview of results:")
            print(result.head(10).to_string())
            print(f"\n... ({len(result)} total rows)\n")
        else:
            print("Query returned no results.\n")
            return
        
        # Save results
        save_results(result)
        print("\n✓ Query completed successfully!\n")
        
    except Exception as e:
        print(f"\nError executing query: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()