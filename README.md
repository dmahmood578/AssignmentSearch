# Patent Assignment Search Tool

## Setup

### 1. Get API Keys
- Create a USPTO API KEY [How to get your Open Data Portal API Key](https://developer.uspto.gov/api-catalog/how-get-your-open-data-portal-api-key)

### 2. Download and Navigate to Project
- Download the patentsearch folder
- Open a terminal, cd to the patentsearch folder:
```bash
cd /path/to/AssignmentSearch
```

### 3. Set Up Virtual Environment

**If `patentsearch-venv` folder does NOT exist on your system**, create it from `requirements.txt`:

```bash
# Create virtual environment
python3 -m venv patentsearch-venv

# Activate it
source patentsearch-venv/bin/activate

# Install all required packages from requirements.txt
pip install -r requirements.txt
```

**If `patentsearch-venv` already exists**, just activate it:
```bash
source patentsearch-venv/bin/activate
```

### 4. Set Environment Variables
In the terminal, enter (spacing matters):
```bash
export USPTO_API_KEY=your_key_here 
export PATENTSVIEW_API_KEY=your_key_here
```

## Usage

### AssignmentSearch.py - Fetch Patent Assignments

#### Search by Patent Number
```bash
python AssignmentSearch.py bypatentnumber patentnumbers.txt --delay 0.2
```
Note that `patentnumbers.txt` is newline-delimited.

#### Search by Assignee
```bash
python AssignmentSearch.py byassignee assignees.txt --per-page 100 --max-pages 20 --debug
```
Note that `assignees.txt` is newline-delimited

The script successfully returns patent assignments in the terminal and as a CSV file: `all_patent_assignments.csv`

**Troubleshooting:**
- If you notice rate-limiting errors (code: 429), increase `--delay 0.2` to `--delay 0.5` or more
- If any fail through 404 errors, it will try them at the end again and place successful ones at the end of the CSV file

### queries.py - Query and Filter Patent Data

Run SQL queries on your patent assignment data and export results to CSV/XLSX files.

#### Run the Query Tool
```bash
python queries.py
```

The tool will:
1. Display an example query
2. Load data from `all_assignments.xlsx` or `all_assignments.csv`
3. Prompt you to enter your SQL query
4. Execute the query and save results to timestamped CSV and XLSX files

#### Example Query
The tool displays this example on startup:
```sql
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
```

**Note:** Column names with spaces must be wrapped in double quotes (e.g., `"Patent Number"`, `"Attorney Name"`).

#### Input Methods
Enter your query and press:
- **Mac/Linux:** Ctrl+D
- **Windows:** Ctrl+Z

Or pipe a query file:
```bash
python queries.py < myquery.sql
```

#### Output Files
Results are saved in the `query_results/` folder with timestamps:
- `query_results/query_results_YYYYMMDD_HHMMSS.csv`
- `query_results/query_results_YYYYMMDD_HHMMSS.xlsx`