import os
import pandas as pd

from utils import get_first_day_of_week


def create_directory_structure(root: str, category: str, partition: str):
    '''
    Creates partitioned data where data will be stored
    '''
    path = os.path.join(root, category, partition)
    os.makedirs(path, exist_ok=True)

    return path


def load_data(wind_df: pd.DataFrame, solar_df: pd.DataFrame, output_dir: str):
    '''
    Loads data, in CSV format, into specified output dir after
    applying partioning
    '''

    # Generate output paths for wind and solar data
    wind_output_path, solar_output_path = [
        create_directory_structure(
            output_dir,
            category,
            get_first_day_of_week()
        ) for category in ['wind', 'solar']
    ]

    # Output data in CSV format to output paths
    wind_df.to_csv(
        os.path.join(wind_output_path, 'wind_data.csv'),
        index=False
    )
    solar_df.to_csv(
        os.path.join(solar_output_path, 'solar_data.csv'),
        index=False
    )
