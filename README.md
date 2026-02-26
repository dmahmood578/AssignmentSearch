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

The script returns patent assignments as timestamped CSV and XLSX files in `assignment_results/`:
- `assignment_results/all_assignments_YYYYMMDD_HHMMSS.csv`
- `assignment_results/all_assignments_YYYYMMDD_HHMMSS.xlsx`

#### Extract Patent Abstracts and Field of Invention (`--text`)

Append `--text` to either mode to fetch the **abstract** and **WIPO Field of Invention** (and primary **CPC code**) for each patent, instead of running the assignment pipeline. Results are written to a timestamped Excel file in `patent_text_results/`.

```bash
# By patent number
python AssignmentSearch.py bypatentnumber patentnumbers.txt --text

# By assignee — deduplicates patents across all assignees automatically
python AssignmentSearch.py byassignee assignees.txt --text
```

Output file: `patent_text_results/patent_text_YYYYMMDD_HHMMSS.xlsx`

| Column | Description |
|---|---|
| Patent Number | USPTO patent number |
| Patent Title | Title of the patent |
| Abstract | Full patent abstract |
| WIPO Field of Invention | WIPO IPC technology field (e.g. `Electrical Engineering — Computer technology`) |
| CPC Primary | Primary CPC classification code (e.g. `G06F30/28`) |

Patent numbers not found as granted patents (e.g. pre-grant publication numbers like `20230XXXXXX`) are automatically retried against the PatentsView pre-grant publications endpoint.

Requires `PATENTSVIEW_API_KEY` to be set (same key used for `byassignee` mode).

**Troubleshooting:**
- If you notice rate-limiting errors (code: 429), increase `--delay 0.2` to `--delay 0.5` or more
- If any fail through 404 errors, it will try them at the end again and place successful ones at the end of the CSV file

### queries.py - Query and Filter Patent Data

Run SQL queries across one or both result tables and export to CSV/XLSX.

#### Run the Query Tool
```bash
# Interactive — type your query after the date prompts
python queries.py

# Pass a .sql file directly — skips the interactive query prompt
python queries.py myquery.sql
python queries.py --query myquery.sql

# Pipe — loads ALL available files (no prompts), query comes from the file
python queries.py < myquery.sql
```

The tool will:
1. List all available result files in `assignment_results/` and `patent_text_results/`
2. Ask whether to load each table, and for what **date/time range** of run outputs to include
3. Concatenate all matching files into the selected tables
4. Show example queries, then prompt for your SQL
5. Save results to `query_results/query_results_YYYYMMDD_HHMMSS.{csv,xlsx}`

#### Available Tables

| Table name | Source folder | Populated by |
|---|---|---|
| `all_assignments` | `assignment_results/` | Running without `--text` |
| `patent_text` | `patent_text_results/` | Running with `--text` |

#### Date Range Selection

When prompted, enter a start and/or end to narrow which run files are loaded. Press Enter to include all files.

```
From (e.g. 2026-02-26 or 20260226_130000): 2026-02-26
To   (e.g. 2026-02-26 or 20260226_235959):          ← Enter = no upper bound
```

Multiple files within the range are concatenated automatically.

#### Example Queries

**Query assignments only:**
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

**Query patent text only:**
```sql
SELECT
    "Patent Number",
    "Patent Title",
    "WIPO Field of Invention",
    "CPC Primary",
    Abstract
FROM patent_text
WHERE "WIPO Field of Invention" LIKE '%Computer technology%'
```

**Join both tables:**
```sql
SELECT
    a."Patent Number",
    a.Assignees,
    t."WIPO Field of Invention",
    t."CPC Primary",
    t.Abstract
FROM all_assignments AS a
JOIN patent_text AS t ON a."Patent Number" = t."Patent Number"
WHERE a.Conveyance = 'ASSIGNMENT OF ASSIGNOR''S INTEREST'
```

**Note:** Column names with spaces must be wrapped in double quotes.

#### Input Methods
Enter your query and press:
- **Mac/Linux:** Ctrl+D
- **Windows:** Ctrl+Z then Enter

Or pass a query file as an argument (date-range prompts still apply):
```bash
python queries.py myquery.sql
```

Or pipe a query file (skips **all** prompts and loads every available file):
```bash
python queries.py < myquery.sql
```

#### Output Files
Results are saved in the `query_results/` folder with timestamps:
- `query_results/query_results_YYYYMMDD_HHMMSS.csv`
- `query_results/query_results_YYYYMMDD_HHMMSS.xlsx`