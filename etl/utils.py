import pandas as pd
import datetime as dt


def clean_df_column_names(df: pd.DataFrame) -> pd.DataFrame:
    '''
    Renames column names to a standard format like: Abc_De_Fgh
    '''
    def format_column_name(column_name):
        # Remove leading and trailing whitespaces, replace
        # whitespaces by underscores and then split on underscores
        parts = column_name.strip().replace(' ', '_').split('_')
        # Capitalize first letter of each part and join by underscore
        formatted_name = '_'.join(part.capitalize() for part in parts)
        return formatted_name

    # Apply custom renaming to all columns
    df.rename(
        columns=lambda x: format_column_name(x),
        inplace=True
    )


def get_first_day_of_week() -> str:
    '''
    Returns string representation of the 1st day of last week (in UTC)
    '''

    today = dt.datetime.now(dt.UTC).date()
    start_of_week = today - dt.timedelta(days=7)

    return start_of_week.strftime('%Y-%m-%d')
