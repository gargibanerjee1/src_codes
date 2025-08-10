"""
Unit tests for functions module using pytest
"""

# Import
import functions
import pandas as pd
import logging
import pytest
import builtins
from unittest.mock import patch


def test_load_dataset_missing_mandatory_cols():
    """
    This test case will verify the behavior of load_dataset() when the input DataFrame
    will be missing mandatory columns ('sex').
    It will expect the function to return None and an error message about missing columns.
    """
    # This will prepare a sample DataFrame missing the 'sex' column
    sample_df = pd.DataFrame({
        'statistic label': ['Entrants to First Year of Junior Cycle'],
        'year': [2015],
        'unit': ['Number'],
        'value': [30608]  # 'sex' column will be missing intentionally
    })

    # It will patch pandas.read_csv to return our sample DataFrame instead of reading a file
    with patch('pandas.read_csv', return_value=sample_df):
        df, err = functions.load_dataset("http://url123.com")

    # It will assert that DataFrame is None because mandatory columns will be missing
    assert df is None
    # It will assert error message is not None and will mention missing columns
    assert err is not None
    assert "missing mandatory columns" in err.lower()

def test_load_dataset_failure():
    """
    This test will verify the failure case when loading dataset raises an exception.
    It will mock pandas.read_csv to raise an Exception.
    The test will check if the function returns None and a proper error message.
    """
    def raise_error(url):
        raise Exception("Failed to read")

    test_url = "http://invalid-url.com"

    # It will patch pandas.read_csv to raise the custom exception
    with patch('pandas.read_csv', side_effect=raise_error):
        df, error = functions.load_dataset(test_url)

        # It will assert no DataFrame is returned
        assert df is None
        # It will assert an error message is returned
        assert error is not None
        # It will assert error message contains expected substring
        assert "Failed to read" in error

def test_rename_header_basic():
    """
    This test will verify the renaming of headers in a DataFrame with irregular column names.
    It will check if the output columns will be normalized: lowercase, stripped, and special characters replaced.
    """
    df = pd.DataFrame(columns=["Col   1", " Col(2)", "Col,,,,3", "col 4", "COL  5"])
    out, err = functions.rename_header(df)

    # It will expect no error returned
    assert err is None
    # It will check if columns were renamed correctly
    assert list(out.columns) == ["col_1", "col_2", "col_3", "col_4", "col_5"]

def test_rename_header_empty_df():
    """
    This test will verify renaming headers on an empty DataFrame (no columns, no rows).
    It will handle this case gracefully with no error and empty columns list.
    """
    df = pd.DataFrame()  # Empty DataFrame
    out_df, error = functions.rename_header(df)

    # It will assert no error is returned
    assert error is None, f"Unexpected error: {error}"
    # It will assert the returned object is a DataFrame
    assert isinstance(out_df, pd.DataFrame)
    # It will assert DataFrame is empty (no rows)
    assert out_df.empty
    # It will assert columns list is empty
    assert list(out_df.columns) == []

def test_process_first_year_stats_basic():
    """
    This test will verify processing of 'first year' statistics with various valid inputs.
    It will check that expected columns will be present after processing and no errors will occur.
    It will also verify that M/F values will normalize correctly to Male/Female.
    """
    data = {
        'statistic label': [
            'First Year data', '1st Year info', 'FIRST', 'First Year something'
        ],
        'year': [2000, 2002, 2005, 2007],
        'sex': ['Male', 'Female', 'M', 'F'],  # Include M/F short forms
        'unit': ['number', '%', 'number', 'percentage'],  # Mix number and percentage units
        'value': [10907, 89.5, 5000, 92.3]
    }
    df = pd.DataFrame(data)
    result_df, error = functions.process_first_year_stats(df)

    # It will check that no error was returned
    assert error is None
    # Output DataFrame will exist
    assert result_df is not None
    # Output will include the 'year_range' column
    assert 'year_range' in result_df.columns
    # Columns with Male counts and Female percentages will exist after normalization
    assert any(col.lower().startswith('male') and col.endswith('_count') for col in result_df.columns)
    assert any(col.lower().startswith('female') and col.endswith('_percent') for col in result_df.columns)


def test_save_outputs_unwritable_path(tmp_path, caplog):
    """
    This test will verify that parquet file will save successfully even if saving csv fails due to invalid path.
    It will use pytest tmp_path fixture and caplog to capture logging messages.
    """
    df = pd.DataFrame({'a': [1]})
    bad_path = tmp_path / "nonexistent_dir" / "file.csv"  # Invalid CSV path (directory won't exist)
    pq_path = tmp_path / "file.parquet"                  # Valid parquet path

    with caplog.at_level(logging.ERROR):
        functions.save_outputs(df, str(bad_path), str(pq_path))

    # CSV file will not exist because save will fail
    assert not bad_path.exists()
    # Parquet file will exist because save should succeed
    assert pq_path.exists()
    # Logging will include an error about saving CSV
    assert any("error saving csv" in m.lower() for m in caplog.messages)

def test_process_first_year_stats_mixed_units():
    """
    This test will verify that process_first_year_stats correctly handles mixed units within the same group:
    it will sum counts and average percentages by year range and sex.
    """
    df = pd.DataFrame({
        'statistic label': ['First Year Data', 'First Year Data'],
        'year': [2010, 2010],
        'sex': ['Male', 'Male'],
        'unit': ['Number', '%'],
        'value': [100, 50]
    })
    out_df, err = functions.process_first_year_stats(df)

    # It will expect no error
    assert err is None
    # Male_count will sum to 100
    assert out_df.loc[0, 'Male_count'] == 100
    # Male_percent will average to 50.0
    assert out_df.loc[0, 'Male_percent'] == 50.0
