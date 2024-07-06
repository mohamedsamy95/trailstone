import httpx
import pandas as pd
import datetime as dt
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type  # noqa
from io import StringIO
import asyncio

from typing import Tuple


class TooManyRequestsException(Exception):
    '''
    Custom exception that covers the case where the API
    sends a "HTTP 429 Too Many Requests" response.
    '''
    pass


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=1, max=5),
    retry=retry_if_exception_type(TooManyRequestsException)
)
async def fetch_data(
        client: httpx.AsyncClient,
        endpoint: str,
        api_key: str,
        file_format: str
) -> pd.DataFrame:

    '''
    Fetches data from endpoint using an AsyncClient and with retry mechanism.
    If a response 429 is received from the server, a wait and retry mechanism
    using exponential backoff will be applied until request succeeds or
    maximum number of retries is achieved.

     Parameters:
    -----------
    client : httpx.AsyncClient
        An instance of the httpx.AsyncClient used to perform
        asynchronous HTTP requests.
    endpoint : str
        The endpoint from which to fetch the data. This should be a string
        representing the path of the API endpoint.
    api_key : str
        The API key used for authenticating the request.
    file_format : str
        The format of the response expected from the API.
        Can be either 'json' or 'csv'.
        Determines how the response data is parsed into a Pandas DataFrame.

    Returns:
    --------
    pd.DataFrame
        A Pandas DataFrame containing the fetched data, parsed according to
        the specified format.

    Raises:
    -------
    TooManyRequestsException
        If a 429 status code (Too Many Requests) is received, indicating
        that the API rate limit has been exceeded.
    httpx.RequestError
        If there is an issue with the request (e.g., network problems).
    httpx.HTTPStatusError
        If the API returns a non-200 status code.
    ValueError
        If the data cannot be parsed as expected.
    '''

    url = f'http://127.0.0.1:8000/{endpoint}?api_key={api_key}'
    response = await client.get(url)

    if response.status_code == 429:
        raise TooManyRequestsException('Received 429 Too Many Requests response')  # noqa
    response.raise_for_status()

    if file_format == 'json':
        data = response.json()
        return pd.DataFrame(data)

    elif file_format == 'csv':
        csv_file = StringIO(response.text)
        return pd.read_csv(csv_file)


async def fetch_data_with_sem(
        client: httpx.AsyncClient,
        endpoint: str,
        api_key: str,
        file_format: str,
        semaphore: asyncio.Semaphore
) -> pd.DataFrame:
    '''
    Wrapper for the fetch_data function to limit concurrency using
    a semaphore
    '''
    async with semaphore:
        return await fetch_data(client, endpoint, api_key, file_format)


async def extract_last_week_data(
        api_key: str
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    '''
    Extracts data of latest week from the endpoint.
    '''

    # Generate dates that we want the data of
    today = dt.datetime.now(dt.UTC).date()
    dates = [
        (
            today - dt.timedelta(days=i)
        ).strftime('%Y-%m-%d') for i in range(1, 8)
    ]

    # Initialize async client in a context manager
    # to be automatically closed when requests are done
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(100)  # Limit concurrency to 100

        # Create a list of wind data fetching tasks for each date
        wind_tasks = [
            fetch_data_with_sem(
                client,
                f'{date}/renewables/windgen.csv',
                api_key,
                'csv',
                semaphore
            )
            for date in dates
        ]

        # Create a list of solar data fetching tasks for each date
        solar_tasks = [
            fetch_data_with_sem(
                client,
                f'{date}/renewables/solargen.json',
                api_key,
                'json',
                semaphore
            )
            for date in dates
        ]

        # Await the completion of all wind data fetching tasks
        wind_results = await asyncio.gather(*wind_tasks)
        # Await the completion of all solar data fetching tasks
        solar_results = await asyncio.gather(*solar_tasks)

        # Concatenate the results into single DataFrames
        wind_data = pd.concat(wind_results)
        solar_data = pd.concat(solar_results)

    return wind_data, solar_data
