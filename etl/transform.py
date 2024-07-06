import pandas as pd

from etl.utils import clean_df_column_names
from etl.cols_config import COLUMN_TYPES, CUSTOM_COLUMN_NAMES


def transform_timestamps(df: pd.DataFrame, unit: str = None) -> pd.DataFrame:
    '''
    Transforms naive timestamp columns to UTC timezone-aware timestamps
    Pass optional argument 'unit'
    if timestamp unit is other than the default (ns)
    '''

    TIMESTAMP_COLS = ['Naive_Timestamp ', 'Last Modified utc']

    # Cast timestamps to datetime and make them utc timezone aware
    for timestamp_col in TIMESTAMP_COLS:
        df[timestamp_col] = pd.to_datetime(
            df[timestamp_col],
            utc=True,
            unit=unit
        )

    # Rename Naive_Timestamp column since it's timezone-aware now
    df.rename(
        columns={'Naive_Timestamp ': 'Timezone-aware_Timestamp'},
        inplace=True
    )

    return df


def apply_type_casting(df: pd.DataFrame, column_types: dict) -> pd.DataFrame:
    '''
    Imposes defined types on specified columns
    '''

    for column, dtype in column_types.items():
        df[column] = df[column].astype(dtype)
    return df


def apply_custom_renaming(df: pd.DataFrame, column_names: dict) -> pd.DataFrame:  # noqa
    '''
    Renames columns based on specified old (key) and new (value) column names
    specified in the dictionary
    '''

    df.rename(columns=column_names, inplace=True)


def transform_data(
        wind_df: pd.DataFrame,
        solar_df: pd.DataFrame
) -> pd.DataFrame:
    '''
    Transform data by applying defined data transformation steps
    '''

    # 1. Transform naive timestamps to timezone-aware ones
    wind_df = transform_timestamps(wind_df)
    solar_df = transform_timestamps(solar_df, unit='ms')

    # 2. Cast other columns to specified data types
    wind_df = apply_type_casting(wind_df, COLUMN_TYPES)
    solar_df = apply_type_casting(solar_df, COLUMN_TYPES)

    # 3. Standardize column names
    clean_df_column_names(wind_df)
    clean_df_column_names(solar_df)

    # 4. Apply custom column renaming
    apply_custom_renaming(wind_df, CUSTOM_COLUMN_NAMES)
    apply_custom_renaming(solar_df, CUSTOM_COLUMN_NAMES)

    # 5. Sort data by timestamp
    wind_df.sort_values(by='Timezone-aware_Timestamp', inplace=True)
    solar_df.sort_values(by='Timezone-aware_Timestamp', inplace=True)

    return wind_df, solar_df
