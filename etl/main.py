import os
import asyncio
import logging
import time
from logging_config import setup_logging

from etl.extract import extract_last_week_data
from etl.transform import transform_data
from etl.quality_policies import NoGapsPolicy, NoNullValuesPolicy
from etl.load import load_data

API_KEY = os.getenv('API_KEY')
OUTPUT_DIR = 'output'


# Setup logging
setup_logging()
logger = logging.getLogger(__name__)


async def main():
    start_time = time.time()
    logger.info('ETL pipeline started')

    try:
        # Extract data
        extract_start = time.time()
        wind_data, solar_data = await extract_last_week_data(API_KEY)
        extract_end = time.time()
        logger.info(f'Data extraction completed in {extract_end - extract_start}s')  # noqa

        # Transform data
        wind_data, solar_data = transform_data(wind_data, solar_data)

        # Define data quality policies
        policies = [NoGapsPolicy(), NoNullValuesPolicy()]

        # Run data against quality policies
        data_sets = {
            'Wind': wind_data,
            'Solar': solar_data
        }
        for policy in policies:
            for key, data in data_sets.items():
                if not policy.check(data):
                    err_msg = f'{key} data quality check failed: {policy.error_message()}'  # noqa
                    logger.error(err_msg)
                    raise Exception(err_msg)

        # Load data
        logger.info('Data quality checks passed. Loading data...')
        load_data(wind_data, solar_data, OUTPUT_DIR)

    except Exception as e:
        logger.exception(f'ETL pipeline failed: {str(e)}')
        raise
    finally:
        end_time = time.time()
        logger.info(f'ETL pipeline finished in {end_time - start_time}s')


if __name__ == '__main__':
    asyncio.run(main())
