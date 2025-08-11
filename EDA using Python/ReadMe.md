# Overview

This project is a Python-based solution for processing First-Year student statistics from a dataset.  
It retrieves the dataset from a url, filters and aggregates the data, performs column normalization, and outputs the results in CSV and Parquet formats.  
The project also includes unit tests using `pytest` to verify the correctness of key functions.

# Features

- **Data Retrieval:** Loads CSV data directly from url.
  
- **Data Filtering:** Selects only rows with "First Year" or "1st Year" in the statistic label.

- **Data Transformation:**
  - Groups results into 5-year intervals.
  - Calculates sum of counts and average percentages for each sex (Male/Female/Both).

- **Column Renaming:** Normalizes all column headers to lowercase with underscores.

- **Output Formats:** Saves results in both `.csv` and `.parquet` formats.

- **Execution Time Logging:** Uses a decorator to measure function runtime.

- **Unit Testing:** Comprehensive test coverage for:
  - Missing columns
  - Load failures
  - Header renaming
  - Data processing logic
  - Mixed unit handling
  - Output saving behavior
 
# Project Structure
- functions.py - Core functions
- unittests.py - Pytest unit tests
- result.csv - Processed CSV output (generated)
- result.parquet - Processed Parquet output (generated)
- README.md - This file

# Installation & Setup

## Requirements

- Python 3.8+
- pandas
- pytest
- pyarrow or fastparquet (for Parquet output)

## **Install dependencies**
```bash
pip install pandas pytest pyarrow
```
## Usage
Run the processing script `functions.py`

This will:
- Download the dataset from:
https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/EDA14/CSV/1.0/en
- Filter and process First-Year statistics.
- Rename column headers.
- Save results to: `result.csv` and `result.parquet`

## Testing
Run all unit tests with:
```bash
pytest unittests.py -v
```
Tests include:
- Missing mandatory columns
- API load failure handling
- Header normalization
- Processing logic for counts & percentages
- Mixed-unit handling
- Output saving with partial failures

## Performance Logging
All major functions are wrapped with a @timer decorator that logs execution time to the console, helping assess runtime performance.





