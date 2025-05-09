# Great Expectations Integration for Flight Delay Prediction

This document outlines the implementation of Great Expectations (GX) for data validation within the MLOps flight delay prediction project.

## Overview

Great Expectations is used to ensure data quality and integrity throughout the data pipeline. Maintaining high-quality data is crucial for training reliable machine learning models and for the accuracy of predictions.

Initially, the focus is on validating the `routes` table data ingested from the AviationStack API into our PostgreSQL database. As the project progresses, Expectation Suites will be developed for other key tables like `airlines`, `airports`, and `flights`.

## Core Components

### 1. Validation Script: `run_gx_validations.py`

-   **Purpose**: This Python script automates the execution of Great Expectations validations.
-   **Key Features**:
    -   Loads database and GX configurations.
    -   Connects to the PostgreSQL database.
    -   Initializes the Great Expectations Data Context.
    -   Programmatically defines and runs a Checkpoint (e.g., `routes_validation_checkpoint`).
    -   Validates a specified data asset (e.g., `routes` table) against a predefined Expectation Suite (e.g., `routes_suite_v1`).
    -   Stores validation results within the GX project structure.
    -   Updates and builds Data Docs (HTML reports).
-   **Configuration**: Reads its primary settings from `gx_config.py`. Database credentials are loaded from the `.env` file.
-   **Execution Behavior**:
    -   The script is designed to **always exit with a status code of 0**, even if some data expectations fail.
    -   If expectations fail, it logs these failures to the console and includes them in the Data Docs.
    -   This behavior is intentional to allow a pipeline (like Metaflow) to proceed while still being alerted to data quality issues.

### 2. Configuration File: `gx_config.py`

-   **Purpose**: Provides a centralized location for configuring the `run_gx_validations.py` script.
-   **Variables**:
    -   `CONTEXT_ROOT_DIR`: Path to the Great Expectations project root (e.g., `"/Users/danielmak/Documents/MLOps_project/gx"`).
    -   `DATASOURCE_NAME`: Name of the GX Datasource (e.g., `"flight_data"`).
    -   `ASSET_TO_VALIDATE`: The specific data asset (table) to validate (e.g., `"routes"`).
    -   `EXPECTATION_SUITE_NAME`: The name of the Expectation Suite to use for validation (e.g., `"routes_suite_v1"`).
    -   `CHECKPOINT_NAME`: The name for the Checkpoint that will be run (e.g., `"routes_validation_checkpoint"`).

### 3. Great Expectations Project (`gx/` directory)

This directory contains the core Great Expectations project setup:

-   **`great_expectations.yml`**: The main configuration file for the GX project. Defines datasources, stores, and other project-level settings.
-   **`expectations/`**: This sub-directory stores Expectation Suites in JSON format. For example, `routes_suite_v1.json` contains the specific rules and assertions for the `routes` table.
-   **`checkpoints/`**: While our `run_gx_validations.py` script currently defines its Checkpoint programmatically, this directory is where Checkpoint configuration files (YAML) would typically be stored if defined separately.
-   **`uncommitted/data_docs/`**: Contains the generated Data Docs. These are HTML files providing human-readable reports of expectations and validation results. The main entry point is usually `uncommitted/data_docs/local_site/index.html`.
-   **`uncommitted/validations/`**: Stores the detailed results of validation runs.

## Prerequisites for Running Validation

1.  **Python Environment**: Ensure your Python environment (e.g., `flight-delay-env` conda environment) has the necessary libraries installed:
    -   `great-expectations`
    -   `psycopg2-binary` (or `psycopg2` if compiled) for PostgreSQL connection
    -   `python-dotenv` for loading environment variables
    -   `SQLAlchemy`
2.  **`.env` File**: A `.env` file must be present in the root directory of the project (or where `run_gx_validations.py` is executed) containing the following PostgreSQL database credentials:
    ```env
    POSTGRES_HOST=your_host
    POSTGRES_PORT=your_port
    POSTGRES_USER=your_user
    POSTGRES_PASSWORD=your_password
    POSTGRES_DB=your_db
    ```
3.  **Configuration Accuracy**: Verify that the paths and names in `gx_config.py` and `great_expectations.yml` are correct for your environment.
4.  **Expectation Suite**: The Expectation Suite specified in `gx_config.py` (e.g., `routes_suite_v1.json`) must exist in the `gx/expectations/` directory.

## How to Run Standalone Validation

1.  **Navigate** to your project\'s root directory in the terminal.
2.  **Ensure** your conda environment is activated.
3.  **Execute** the script:
    ```bash
    python run_gx_validations.py
    ```
4.  **Expected Output**:
    -   Console logs indicating the script\'s progress, including connection status, checkpoint execution, and a summary of whether expectations passed or failed.
    -   If expectations failed, a summary of these failures will be printed to the console.
    -   A message confirming that Data Docs have been built.
5.  **Checking Results**:
    -   **Console**: Review the script\'s output for a quick summary.
    -   **Data Docs**: Open `gx/uncommitted/data_docs/local_site/index.html` in a web browser for a detailed, human-readable report of the validation run, including which specific expectations passed or failed and the observed values.

## Integrating with Metaflow

The `run_gx_validations.py` script can be integrated as a step within a Metaflow pipeline. Since the script is designed to always exit with code 0, Metaflow will consider the step successful even if data expectations fail. This allows the pipeline to continue while still providing visibility into data quality issues.

### Example Metaflow Step

```python
from metaflow import FlowSpec, step, Parameter, current
import subprocess
import os
import sys # Required for sys.executable

class DataProcessingFlow(FlowSpec):

    # You might have other parameters or data artifacts from previous steps
    # For example, an S3 path if your ETL script outputs a trigger file

    @step
    def start(self):
        print("Starting data processing flow...")
        self.next(self.validate_routes_data)

    @step
    def validate_routes_data(self):
        print("Running Great Expectations validation for routes data...")
        
        # Determine the path to the validation script.
        # Assumes run_gx_validations.py and gx_config.py are in the same directory as the flow,
        # or part of the Metaflow code package.
        flow_dir = os.path.dirname(os.path.abspath(__file__)) 
        script_name = "run_gx_validations.py"
        script_full_path = os.path.join(flow_dir, script_name)

        # Use the same Python interpreter that Metaflow is using for the script
        python_executable = sys.executable

        # Ensure .env file is accessible by the script if it relies on it from its CWD.
        # Alternatively, manage DB credentials via Metaflow's secrets management (e.g., @secrets)
        # and modify the script to read from environment variables set by Metaflow.
        # For this example, we assume .env is co-located or handled by the script's `load_dotenv()`.

        try:
            process = subprocess.run(
                [python_executable, script_full_path],
                capture_output=True,
                text=True,
                check=False # Script is designed to exit 0, so check=False is appropriate.
                            # We will inspect stdout/stderr for script execution errors.
            )

            print("--- Great Expectations Script STDOUT ---")
            print(process.stdout)
            if process.stderr:
                print("--- Great Expectations Script STDERR ---")
                print(process.stderr)
            
            # Check for script execution errors (not expectation failures)
            if process.returncode != 0:
                # This indicates an error in the script itself (e.g., Python error, file not found by script)
                raise Exception(f"Great Expectations script execution failed with return code {process.returncode}. STDERR: {process.stderr}")

            # Check for expectation failures reported in stdout
            if "One or more expectations FAILED" in process.stdout:
                print("Metaflow Step: GX validation reported FAILED expectations. See logs and Data Docs for details.")
                self.gx_routes_expectations_passed = False
                # You could trigger alerts or specific actions here based on GX failure.
            elif "All expectations PASSED" in process.stdout:
                 print("Metaflow Step: GX validation reported all expectations PASSED.")
                 self.gx_routes_expectations_passed = True
            else:
                # This case might indicate an unexpected output from the GX script
                print("Metaflow Step: GX script output did not clearly indicate pass/fail status for expectations. Review script output.")
                self.gx_routes_expectations_passed = None # Undetermined

        except FileNotFoundError:
            print(f"Error: The validation script {script_full_path} was not found.")
            raise # Fail the Metaflow step
        except Exception as e:
            print(f"An error occurred during the Great Expectations validation step: {e}")
            raise # Fail the Metaflow step

        self.next(self.another_step) # Or your next relevant step

    @step
    def another_step(self):
        # Example: Access the validation status later in the flow
        if hasattr(self, 'gx_routes_expectations_passed'):
            print(f"Status of routes data validation: {'Passed' if self.gx_routes_expectations_passed else ('Failed' if self.gx_routes_expectations_passed is False else 'Undetermined')}")
        
        print("Continuing with the pipeline...")
        self.next(self.end)

    @step
    def end(self):
        print("Data processing flow finished.")

if __name__ == '__main__':
    DataProcessingFlow()
```

### Considerations for Metaflow Integration

1.  **Environment Consistency**: The Metaflow step's execution environment must have all necessary Python packages (`great-expectations`, `psycopg2-binary`, `python-dotenv`, `SQLAlchemy`) that `run_gx_validations.py` requires. This is typically handled via the `@conda` decorator in Metaflow or by ensuring the Docker image used for execution has these.
2.  **File Accessibility**:
    -   The `run_gx_validations.py` script, `gx_config.py`, and the entire `gx/` project directory must be accessible to the Metaflow step at runtime. This usually means including them in your Metaflow deployment package (e.g., `metaflow.IncludeFile` or by having them in the same directory as your flow script if not deploying remotely).
    -   The `.env` file for database credentials needs to be accessible by `run_gx_validations.py` in its expected location (usually the script's current working directory when `load_dotenv()` is called). For production, consider using Metaflow's built-in secrets management (like `@secrets` decorator with AWS Secrets Manager or similar) and modifying the script to fetch credentials from environment variables populated by Metaflow, rather than a `.env` file.
3.  **Interpreting Results in Metaflow**: Since `run_gx_validations.py` always exits with 0, Metaflow won't automatically fail the step on expectation failures. To make decisions in your flow based on validation outcomes:
    -   **Parse stdout**: As shown in the example, the Metaflow step can capture and parse the `stdout` of the script to look for messages like "One or more expectations FAILED" or "All expectations PASSED."
    -   **Output File (Alternative)**: Modify `run_gx_validations.py` to write a simple status file (e.g., `gx_status.json` with `{"expectations_passed": false}`) that the Metaflow step can then read.
    -   **Data Docs**: While not for direct programmatic control flow, ensure operators can easily access the generated Data Docs for detailed investigation.
4.  **Error Handling**: The Metaflow step should include `try/except` blocks to catch errors related to the execution of the script itself (e.g., script not found, Python errors within the script that are not handled internally), distinct from data expectation failures.

By implementing these components and strategies, you can effectively integrate Great Expectations into your MLOps workflow for robust data validation. 