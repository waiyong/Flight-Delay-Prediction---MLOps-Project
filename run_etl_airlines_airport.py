#!/usr/bin/env python3
"""
ETL Script for AviationStack Airlines and Airports Data

This script orchestrates the fetching and processing of data from the
AviationStack API (Airlines and Airports only) into the PostgreSQL database.

It utilizes the functions defined in scripts.flight_data_processor.

Prerequisites:
- A .env file with AVIATIONSTACK_API_KEY and DATABASE_URL.
- Database schema matching the models in scripts.models.
"""

import os
import logging
import sys
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

# Add scripts directory to Python path to allow direct import
project_root = os.path.dirname(os.path.abspath(__file__))
scripts_path = os.path.join(project_root, 'scripts')
if scripts_path not in sys.path:
    sys.path.append(scripts_path)

# Import the specific functions AFTER updating sys.path
try:
    from flight_data_processor import (
        fetch_paginated_data,
        process_airlines_data,
        process_airports_data
    )
except ImportError as e:
    print(f"Error importing from flight_data_processor: {e}")
    print(f"Please ensure '{scripts_path}' exists and contains flight_data_processor.py")
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


# --- Main Execution ---
if __name__ == "__main__":
    logger.info("Starting ETL process for Airlines and Airports...")

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

        # --- Process Airlines ---
        logger.info("--- Starting Airlines Processing ---")
        try:
            airlines_data = fetch_paginated_data(endpoint="airlines")
            if airlines_data:
                logger.info(f"Fetched {len(airlines_data)} total airline records.")
                process_airlines_data(airlines_data, session)
                logger.info("Finished processing airlines.")
            else:
                logger.warning("No airline data fetched or an error occurred during fetch.")
        except Exception as e:
            logger.error(f"Error during Airlines processing: {e}", exc_info=True)
            # Decide if you want to rollback here or continue to Airports
            # session.rollback()

        # --- Process Airports ---
        logger.info("--- Starting Airports Processing ---")
        try:
            airports_data = fetch_paginated_data(endpoint="airports")
            if airports_data:
                logger.info(f"Fetched {len(airports_data)} total airport records.")
                process_airports_data(airports_data, session)
                logger.info("Finished processing airports.")
            else:
                logger.warning("No airport data fetched or an error occurred during fetch.")
        except Exception as e:
            logger.error(f"Error during Airports processing: {e}", exc_info=True)
            # Decide if you want to rollback here
            # session.rollback()

        logger.info("✅ ETL process for Airlines and Airports completed.")

    except Exception as e:
        # Catch connection errors or other major issues
        logger.error(f"❌ An critical error occurred: {e}", exc_info=True)
        if session: # Attempt rollback if session exists
             try:
                 session.rollback()
             except Exception as rb_e:
                 logger.error(f"Error during rollback: {rb_e}")
        sys.exit(1) # Exit with error status
    finally:
        # Ensure session is closed
        if session:
            session.close()
            logger.info("Database session closed.") 