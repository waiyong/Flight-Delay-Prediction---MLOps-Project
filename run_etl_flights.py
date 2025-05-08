#!/usr/bin/env python3
"""
ETL Script for AviationStack Flights Data

This script orchestrates the fetching and processing of flight data from the
AviationStack API for a defined date range (today to 2 weeks ago) 
into the PostgreSQL database.

It utilizes functions defined in scripts.flight_data_processor.

Prerequisites:
- A .env file with AVIATIONSTACK_API_KEY and DATABASE_URL.
- Database schema matching the models in scripts.models (specifically 'flights' table).
"""

import os
import logging
import sys
import time # For a small delay between processing dates
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add scripts directory to Python path to allow direct import
# project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) # Go up one level
project_root = os.path.dirname(os.path.abspath(__file__)) # This is the directory of the current script (e.g., MLOps_project)
scripts_path = os.path.join(project_root, 'scripts')
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

# Import the specific functions AFTER updating sys.path
try:
    from flight_data_processor import (
        fetch_paginated_data,
        process_flights_data # Assuming this function exists and handles Flight model
    )
except ImportError as e:
    print(f"Error importing from flight_data_processor: {e}")
    print(f"Please ensure '{scripts_path}' exists and contains flight_data_processor.py with process_flights_data function.")
    sys.exit(1)


# --- Configuration ---
# Configure logging (append to the same file as the processor uses)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s (%(levelname)s) - %(message)s',
    handlers=[
        logging.FileHandler("flight_data_processor.log", mode='a'), # Append mode
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__) # Get logger specific to this script


# --- TEMPORARY CHANGE FOR TESTING ---
# Define the specific end date for testing
end_date_str = "2025-05-06"
year, month, day = map(int, end_date_str.split('-'))
end_date = date(year, month, day)
start_date = end_date - timedelta(days=1) # For a 2-day period including the end_date
# --- END OF TEMPORARY CHANGE ---

# Original date range logic (commented out for testing)
# # Define the date range: today to 2 weeks ago
# end_date = date.today()
# start_date = end_date - timedelta(days=13) # Approx 2 weeks (14 days inclusive of end_date)

logger.info(f"TARGETING FLIGHT DATA FOR TESTING: Dates from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
# dates_to_process = generate_date_list(start_date, end_date, reverse=True) # Ensure generate_date_list is available
# Corrected line assuming generate_date_list is not in this specific file but imported
# For direct use in this file if it were defined here or for clarity:
dates_to_process = []
current_d = start_date
while current_d <= end_date:
    dates_to_process.append(current_d.strftime("%Y-%m-%d"))
    current_d += timedelta(days=1)
if True: # Assuming reverse=True from original call
    dates_to_process.reverse()

# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting ETL process for Flights...")

    # Load environment variables from .env file
    load_dotenv()

    # Verify necessary environment variables are set
    api_key = os.getenv("AVIATIONSTACK_API_KEY")
    db_url = os.getenv("DATABASE_URL")

    if not api_key:
        logger.error("FATAL: AVIATIONSTACK_API_KEY not found in environment variables.")
        sys.exit(1)
    if not db_url:
        logger.error("FATAL: DATABASE_URL not found in environment variables.")
        sys.exit(1)

    logger.info("Environment variables loaded.")

    # Setup Database Connection
    engine = None
    session = None
    try:
        engine = create_engine(db_url)
        Session = sessionmaker(bind=engine)
        session = Session()
        logger.info("Database session created successfully.")

        # Use the dates_to_process list generated earlier
        for date_str in dates_to_process:
            logger.info(f"--- Starting Flights Processing for date: {date_str} ---")
            
            try:
                # Fetch flights for the current date
                # fetch_paginated_data is designed to get all pages for the given endpoint/date
                flights_data_for_date = fetch_paginated_data(endpoint="flights", date=date_str)
                
                if flights_data_for_date:
                    logger.info(f"Fetched {len(flights_data_for_date)} total flight records for {date_str}.")
                    # process_flights_data should handle the transformation and DB storage,
                    # including the commit for the data of this specific date.
                    process_flights_data(flights_data_for_date, session)
                    logger.info(f"Finished processing flights for {date_str}.")
                else:
                    logger.warning(f"No flight data fetched for {date_str} or an error occurred during fetch.")
            
            except Exception as e:
                logger.error(f"Error during Flights processing for date {date_str}: {e}", exc_info=True)
                # Decide if you want to rollback for this specific date or let it be handled by outer try/except
                # For now, an error for one date won't stop processing of subsequent dates.
                # The session.commit() inside process_flights_data would handle rollback for that batch if it fails.

            # Optional: Add a small delay to be respectful to the API, 
            # though fetch_paginated_data already has a 1-second delay between pages.
            # Consider if many distinct date calls need an inter-date delay.
            # time.sleep(1) # Example: 1-second delay between processing each date

        logger.info(f"✅ ETL process for Flights from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} completed.")

    except Exception as e:
        # Catch connection errors or other major issues
        logger.error(f"❌ A critical error occurred during the Flights ETL process: {e}", exc_info=True)
        # The session might not exist or might be in a bad state.
        # process_flights_data should handle its own commit/rollback.
        # If a global session rollback is needed, it could be attempted here, but care is needed.
        sys.exit(1) # Exit with error status
    finally:
        # Ensure session is closed
        if session:
            session.close()
            logger.info("Database session closed.") 