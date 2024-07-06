import pandas as pd
from abc import ABC, abstractmethod


class DataQualityPolicy(ABC):
    @abstractmethod
    def check(self, data: pd.DataFrame) -> bool:
        pass

    @abstractmethod
    def error_message(self) -> str:
        pass


class NoGapsPolicy(DataQualityPolicy):
    '''
    Use this policy if you don't to allow gaps in the data
    '''
    def check(self, data: pd.DataFrame) -> bool:
        TIMESTAMP_COLUMN = 'Timezone-aware_Timestamp'

        # Timestamp column should exist in the data
        if TIMESTAMP_COLUMN not in data.columns:
            return False


        '''
        Check for gaps by..
            ..sorting the data by timestamp,
            ..calculating the time difference between rows in seconds,
            ..checking for any difference in seconds greater than 300 seconds (5 minutes)  # noqa
        '''

        # Sort data by timestamp ASC
        data_sorted = data.sort_values(TIMESTAMP_COLUMN)
        # Cast timestamp to datetime (just in case)
        data_sorted[TIMESTAMP_COLUMN] = pd.to_datetime(data_sorted[TIMESTAMP_COLUMN])  # noqa
        # Check for gaps
        gaps = data_sorted[TIMESTAMP_COLUMN].diff().dt.total_seconds().fillna(0) > 5 * 60  # noqa

        return not gaps.any()

    def error_message(self) -> str:
        return 'Data contains gaps.'


class NoNullValuesPolicy(DataQualityPolicy):
    '''
    Use this policy if you don't to allow null values in the data
    '''

    def check(self, data: pd.DataFrame) -> bool:
        return not data.isnull().values.any()

    def error_message(self) -> str:
        return 'Data contains null values.'
