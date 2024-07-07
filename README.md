# ETL Project

## Purpose

The purpose of this ETL (Extract, Transform, Load) project is to extract weather data from an API, transform it into a standardized format, and load it into a structured directory. This ensures that data is ready for analysis and consumption. The ETL pipeline is designed to handle data quality checks, logging, and error handling to ensure robustness and reliability.

## Technologies Used

- **Python 3.12**: The main programming language used for the ETL pipeline.
- **Pandas**: For data manipulation and transformation.
- **httpx**: For asynchronous HTTP requests.
- **asyncio**: For asynchronous programming.
- **tenacity**: For retrying HTTP requests.
- **Docker**: For containerizing the application.
- **Pytest**: For unit testing.
- **GitHub Actions**: For continuous integration (CI) to run tests on pull requests.

## ETL Process

1. **Extract**: The pipeline extracts data for the latest week from both the `Solar` and `Wind` endpoints of the API. The data is fetched asynchronously using `httpx` and retries are implemented to handle throttling.
2. **Transform**: 
    - Naive timestamps are converted to timezone-aware UTC format.
    - Column names are standardized.
    - Data types are cast based on a predefined configuration.
3. **Load**: The transformed data is saved into the `/output` directory, partitioned by data type (`wind` and `solar`) and further by the date of the first day of the week.

## Getting Started

### Using Docker

1. **Build the Docker Image**:
    ```sh
    make build
    ```

2. **Run the ETL Pipeline**:
    ```sh
    make run
    ```

### Without Docker

1. **Setup Virtual Environment**:
    ```sh
    python3 -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    pip install -r requirements.txt
    ```

2. **Update PYTHONPATH Variable**:
   ```sh
   export PYTHONPATH=path/to/your/project/:$PYTHONPATH
   ```

3. **Set Environment Variables**:
    ```sh
    export API_KEY='your_api_key'
    export HOST=localhost
    ```

4. **Run the ETL Pipeline**:
    ```sh
    python -m etl.main
    ```

### Running tests
```sh
make test
```


## Logging

The ETL pipeline uses logging to track the duration of data extraction, transformation, and loading processes. It logs any failures with detailed error messages and confirms successful pipeline runs.

## Data Quality Policies

The pipeline includes data quality checks to ensure there are no gaps in the data. These checks are implemented in the `policies.py` file and are run after the data is transformed and before it is loaded.

## Configuration

The desired target column types are defined in a `config.py` file. The `transform.py` file includes a function that applies these type castings based on the configuration.

## Continuous Integration

The CI workflow is set up using GitHub Actions. It automatically runs the tests on every push and pull request to ensure code quality and functionality. The workflow performs the following steps:

1. Checks out the code from the repository.
2. Sets up Python 3.12.
3. Installs the required dependencies.
4. Sets the `PYTHONPATH` to the current working directory.
5. Navigates to the `etl` directory and runs the tests using `pytest`.

You can find the CI workflow configuration in the `.github/workflows/ci.yml` file.