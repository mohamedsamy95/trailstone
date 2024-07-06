import os
import pandas as pd
import datetime as dt

from etl.load import load_data, create_directory_structure
from etl.utils import get_first_day_of_week


def test_load_data(tmpdir):

    today = dt.datetime.now(tz=dt.UTC).date()

    # Sample transformed data
    data = {
        'Timezone-aware_Timestamp': [today],
        'Variable': [123],
        'Value': [45.67],
        'Last_Modified_UTC': [today],
    }
    df = pd.DataFrame(data)

    # Create temp directory
    output_dir = tmpdir.mkdir('output/')

    # Generate output paths for wind and solar data
    wind_output_path, solar_output_path = [
        create_directory_structure(
            output_dir,
            category,
            get_first_day_of_week()
        ) for category in ['wind', 'solar']
    ]

    # Load data into output directory
    load_data(df, df, output_dir)

    # Assert files are created in expected paths
    assert os.path.isfile(os.path.join(wind_output_path, 'wind_data.csv'))
    assert os.path.isfile(os.path.join(solar_output_path, 'solar_data.csv'))
