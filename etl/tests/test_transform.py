import pandas as pd
from etl.transform import transform_data


def test_transform_data():
    # Sample extracted data
    wind_data = {
        'Naive_Timestamp ': ['2023-06-01 00:00:00'],
        ' Variable': [123],
        'value': [45.67],
        'Last Modified utc': ['2023-06-01 00:00:00'],
    }
    wind_df = pd.DataFrame(wind_data)

    solar_data = {
        'Naive_Timestamp ': [1719878400000],
        ' Variable': [224],
        'value': [56.67],
        'Last Modified utc': [1719878400000],
    }
    solar_df = pd.DataFrame(solar_data)

    # Transform data
    wind_df, solar_df = transform_data(wind_df, solar_df)

    # Assert column name standardizing happened
    assert 'Variable' in wind_df.columns

    # Assert custom colum renaming happened
    assert 'Last_Modified_UTC' in solar_df.columns

    # Assert data has specified data types
    assert wind_df['Variable'].dtype == 'int64'
    assert solar_df['Value'].dtype == 'float64'

    # Assert timestamp is not naive
    assert wind_df['Timezone-aware_Timestamp'].dt.tz is not None
