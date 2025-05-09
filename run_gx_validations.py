import os
import sys
import great_expectations as gx
from dotenv import load_dotenv
import gx_config # Import the configuration file

# --- Configuration ---
# Configurations are now imported from gx_config.py
CONTEXT_ROOT_DIR = gx_config.CONTEXT_ROOT_DIR
DATASOURCE_NAME = gx_config.DATASOURCE_NAME
ASSET_TO_VALIDATE = gx_config.ASSET_TO_VALIDATE
EXPECTATION_SUITE_NAME = gx_config.EXPECTATION_SUITE_NAME
CHECKPOINT_NAME = gx_config.CHECKPOINT_NAME

def get_db_connection_string():
    """
    Loads database connection details from .env and constructs connection string.
    """
    load_dotenv()
    pg_host = os.getenv("POSTGRES_HOST")
    pg_port = os.getenv("POSTGRES_PORT")
    pg_user = os.getenv("POSTGRES_USER")
    pg_password = os.getenv("POSTGRES_PASSWORD")
    pg_db = os.getenv("POSTGRES_DB")

    if not all([pg_host, pg_port, pg_user, pg_password, pg_db]):
        print("Error: One or more PostgreSQL environment variables are not set or not loaded.")
        print("Please ensure POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, and POSTGRES_DB are in your .env file.")
        return None
    
    connection_string = f"postgresql+psycopg2://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}"
    print(f"Connection string constructed (password is redacted for display): postgresql+psycopg2://{pg_user}:********@{pg_host}:{pg_port}/{pg_db}")
    return connection_string

def run_validation():
    """
    Initializes Great Expectations, defines or loads a Checkpoint,
    and runs validation on the specified data asset.
    """
    print(f"Attempting to initialize Great Expectations Data Context from: {CONTEXT_ROOT_DIR}")
    try:
        context = gx.get_context(context_root_dir=CONTEXT_ROOT_DIR)
        print("Successfully initialized Data Context.")
    except Exception as e:
        print(f"Error initializing Data Context: {e}")
        return False

    connection_string = get_db_connection_string()
    if not connection_string:
        return False

    print(f"Ensuring datasource '{DATASOURCE_NAME}' exists in context...")
    try:
        # This matches your notebook's way of adding/updating the datasource
        datasource = context.sources.add_or_update_postgres(
            name=DATASOURCE_NAME,
            connection_string=connection_string
        )
        print(f"Datasource '{DATASOURCE_NAME}' ensured in context.")
    except Exception as e:
        print(f"Error ensuring datasource '{DATASOURCE_NAME}': {e}")
        return False
        
    # Define the Checkpoint configuration programmatically.
    # This Checkpoint will use your existing 'routes_suite_v1'.
    # Ensure 'routes_suite_v1.json' is in gx/expectations/
    print(f"Defining Checkpoint: {CHECKPOINT_NAME}")
    checkpoint_config = {
        "name": CHECKPOINT_NAME,
        "config_version": 1.0,
        "class_name": "SimpleCheckpoint",
        "run_name_template": f"%Y%m%d-%H%M%S-{ASSET_TO_VALIDATE}-validation",
        "validations": [
            {
                "batch_request": {
                    "datasource_name": DATASOURCE_NAME,
                    # For a table asset added via add_table_asset (like in your notebook if it was new)
                    # or an inferred asset if GX can find it by name.
                    # GX will infer 'default_inferred_data_connector_name' if not specified and it's a simple setup.
                    # Or 'default_runtime_data_connector_name' for runtime data.
                    # Given your notebook snippet `asset = datasource.get_asset(asset_name=table_name_to_test)`
                    # implies the asset is known to the datasource.
                    "data_connector_name": "default_inferred_data_connector_name", # Common default for table assets
                    "data_asset_name": ASSET_TO_VALIDATE,
                },
                "expectation_suite_name": EXPECTATION_SUITE_NAME,
            }
        ],
        "action_list": [
            {
                "name": "store_validation_result",
                "action": {"class_name": "StoreValidationResultAction"},
            },
            {
                "name": "update_data_docs",
                "action": {"class_name": "UpdateDataDocsAction", "site_names": []}, # site_names: [] ensures all sites are updated
            },
            # Consider adding a SlackNotificationAction or EmailAction here for failures
            # {
            #     "name": "send_slack_notification_on_failure",
            #     "action": {
            #         "class_name": "SlackNotificationAction",
            #         "slack_webhook": os.environ.get("SLACK_WEBHOOK_URL"), # Store in .env
            #         "notify_on": "failure", # "all", "success", "failure"
            #         "renderer": {
            #             "module_name": "great_expectations.render.renderer.slack_renderer",
            #             "class_name": "SlackRenderer"
            #         }
            #     }
            # }
        ],
    }

    try:
        context.add_or_update_checkpoint(**checkpoint_config)
        print(f"Checkpoint '{CHECKPOINT_NAME}' added/updated in context.")
    except Exception as e:
        print(f"Error adding/updating checkpoint: {e}")
        return False

    print(f"Running checkpoint: {CHECKPOINT_NAME} for asset: {ASSET_TO_VALIDATE} using suite: {EXPECTATION_SUITE_NAME}...")
    try:
        results = context.run_checkpoint(checkpoint_name=CHECKPOINT_NAME)
    except Exception as e:
        print(f"Error running checkpoint '{CHECKPOINT_NAME}': {e}")
        return False

    # Process and report results
    if results["success"]:
        print(f"Validation SUCCEEDED for checkpoint: {CHECKPOINT_NAME}")
        return True
    else:
        print(f"Validation FAILED for checkpoint: {CHECKPOINT_NAME}")
        print("Summary of failed expectations:")
        for run_id, run_result_data in results.run_results.items():
            validation_result = run_result_data.get("validation_result")
            if validation_result:
                for res in validation_result.results:
                    if not res.success:
                        print(f"  - Expectation Type: {res.expectation_config.expectation_type}")
                        print(f"    Column: {res.expectation_config.kwargs.get('column')}")
                        print(f"    Observed Value: {res.result.get('observed_value')}")
                        print(f"    Details: {res.result}")
        print("\\\\nFor full details, check your Data Docs.")
        return False

if __name__ == "__main__":
    print("Starting Great Expectations validation script...")
    
    # This variable captures if the EXPECTATIONS THEMSELVES passed or failed.
    expectations_passed = run_validation() 
    
    if expectations_passed:
        print("✅ Great Expectations validation completed: All expectations PASSED.")
    else:
        # This is the "alert" part for the console
        print("⚠️ Great Expectations validation completed: One or more expectations FAILED.")
        print("   The script itself will not fail the pipeline due to expectation failures. Check logs and Data Docs for details.")

    # Always attempt to build Data Docs regardless of expectation outcomes.
    # The run_validation function already prints detailed expectation failures if any.
    # Data Docs provide the comprehensive report.
    try:
        print("Building Data Docs...")
        # It's good practice to get a fresh context or ensure it's the same one used for the run.
        # For simplicity here, we re-initialize, assuming CONTEXT_ROOT_DIR is correctly set.
        context = gx.get_context(context_root_dir=CONTEXT_ROOT_DIR)
        context.build_data_docs()
        print(f"Data Docs build complete. Check them in {CONTEXT_ROOT_DIR}/uncommitted/data_docs/local_site/index.html")
    except Exception as e:
        print(f"Error building Data Docs: {e}")
        # This is a script operational error, not an expectation failure.
        # The primary request is that the script doesn't "fail" (exit non-zero) due to expectation failures.
        # If Data Docs build fails, it's a separate issue to investigate.
        # We will still proceed to exit with 0 as per the main requirement.
        
    print("Script execution finished. Exiting with status 0.")
    sys.exit(0) # Always exit with 0 as per user request 