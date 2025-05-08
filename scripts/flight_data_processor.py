#!/usr/bin/env python3
"""
Flight Data Processor

This script manages the ETL process for flight data from AviationStack API to PostgreSQL.
It implements pagination to handle API result limits and processes data in batches.

Note: AviationStack API only provides historical data for the last 3 months.
"""

import os
import time
import json
import logging
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional, Tuple

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text, func, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Import the models
from scripts.models import Base, Flight, Airline, Airport, Route

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("flight_data_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
API_KEY = os.getenv("AVIATIONSTACK_API_KEY")
DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://flight_user:flight_pass@localhost:5432/flight_db")

# API Configuration
BASE_URL = "https://api.aviationstack.com/v1/"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds
# AviationStack only provides data for the last 3 months
HISTORICAL_DATA_WINDOW_DAYS = 90


def get_valid_date_range() -> Tuple[datetime, datetime]:
    """
    Calculate the valid date range for historical data (last 3 months)
    
    Returns:
        tuple: (start_date, end_date) as datetime objects
    """
    end_date = datetime.now()
    # Go back 3 months (90 days)
    start_date = end_date - timedelta(days=HISTORICAL_DATA_WINDOW_DAYS)
    
    return start_date, end_date


def is_date_in_valid_range(date_str: str) -> bool:
    """
    Check if a date is within the valid 3-month range
    
    Args:
        date_str: Date in YYYY-MM-DD format
        
    Returns:
        bool: True if date is within valid range
    """
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d")
        start_date, end_date = get_valid_date_range()
        return start_date <= date <= end_date
    except ValueError:
        logger.error(f"Invalid date format: {date_str}")
        return False


def generate_date_list(start_date: datetime, end_date: datetime, reverse: bool = True) -> List[str]:
    """
    Generate a list of dates between start_date and end_date
    
    Args:
        start_date: Start date (datetime object)
        end_date: End date (datetime object)
        reverse: If True, returns dates from newest to oldest
    
    Returns:
        list: List of date strings in YYYY-MM-DD format
    """
    dates = []
    current_date = start_date
    
    while current_date <= end_date:
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    if reverse:
        dates.reverse()  # Process newest dates first
    
    return dates


def call_aviationstack(endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make a call to AviationStack API with retry logic
    
    Args:
        endpoint: API endpoint (flights, airlines, etc.)
        params: Query parameters
        
    Returns:
        API response as a dictionary
    """
    url = f"{BASE_URL}{endpoint}"
    
    # Ensure API key is included
    if "access_key" not in params:
        params["access_key"] = API_KEY
    
    # Implement retry logic
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()  # Raise exception for 4XX/5XX responses
            
            data = response.json()
            
            # Check for API errors
            if "error" in data:
                error = data["error"]
                logger.error(f"API Error: {error}")
                
                # Handle rate limiting
                if "rate limit" in str(error).lower():
                    logger.info(f"Rate limited. Waiting before retry.")
                    time.sleep(RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                    continue
                
                # Check for date-related errors
                if "date" in str(error).lower() or "historical" in str(error).lower():
                    logger.warning(f"Date-related error: {error}")
                    return {"error": error, "type": "date_error"}
                    
                return {"error": error}
            
            return data
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed (attempt {attempt+1}/{MAX_RETRIES}): {str(e)}")
            
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY * (2 ** attempt)
                logger.info(f"Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                logger.error("Max retries exceeded")
                return {"error": str(e)}
    
    return {"error": "Max retries exceeded"}


def fetch_paginated_data(endpoint: str, date: Optional[str] = None, additional_params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Fetch all data for a specific date using pagination
    
    Args:
        endpoint: API endpoint (flights, airlines, etc.)
        date: Flight date in YYYY-MM-DD format
        additional_params: Additional parameters for the API call
        
    Returns:
        List of all data records for the specified date
    """
    # Check if date is within valid range for historical data
    if endpoint == "flights" and date:
        if not is_date_in_valid_range(date):
            logger.warning(f"Date {date} is outside the valid 3-month range. AviationStack only provides historical data for the last 3 months.")
            return []
    
    all_results = []
    offset = 0
    # limit = 100   # Adjusted based on user's plan limits - This was a general change

    # Set limit based on the endpoint
    if endpoint == "flights":
        limit = 100  # Specific limit for the /flights endpoint
    else:
        limit = 1000 # Default limit for other endpoints (airlines, airports, routes)
    
    # Prepare parameters
    params = {
        "limit": limit
    }
    
    # Add date parameter for flights endpoint
    if endpoint == "flights" and date:
        params["flight_date"] = date
    
    # Add additional parameters if provided
    if additional_params:
        params.update(additional_params)
    
    # Loop until all data is fetched
    while True:
        # Update offset for pagination
        params["offset"] = offset
        
        # Make API call
        date_info = f"for {date} " if date else ""
        logger.info(f"Fetching {endpoint} data {date_info}(offset: {offset}, limit: {limit})")
        response = call_aviationstack(endpoint, params)
        
        # Check for errors
        if "error" in response:
            error_type = response.get("type", "")
            if error_type == "date_error":
                logger.warning(f"Date-related error, likely outside valid range: {response['error']}")
                break
            else:
                logger.error(f"Error fetching data: {response['error']}")
                break
        
        # Process results
        results = response.get("data", [])
        pagination = response.get("pagination", {})
        
        count = len(results)
        logger.info(f"Received {count} {endpoint} records")
        
        # Add to results list
        all_results.extend(results)
        
        # Check if we've fetched all data
        total = pagination.get("total", 0)
        
        if count == 0 or (offset + count) >= total:
            date_info = f"for {date} " if date else ""
            logger.info(f"Completed fetching {len(all_results)}/{total} {endpoint} records {date_info}")
            break
        
        # Update offset for next page
        offset += limit
        
        # Respect API rate limits
        time.sleep(1)
    
    return all_results


def has_data_for_date(date_str: str, session) -> bool:
    """
    Check if we already have data for the given date
    
    Args:
        date_str: Date in YYYY-MM-DD format
        session: Database session
    
    Returns:
        bool: True if we have data for this date
    """
    try:
        date = datetime.strptime(date_str, "%Y-%m-%d").date()
        count = session.query(func.count()).select_from(Flight).filter(Flight.flight_date == date).scalar()
        return count > 0
    except Exception as e:
        logger.error(f"Error checking for existing data: {str(e)}")
        return False

# ... existing code ...

def process_flights_data(flights_data: List[Dict[str, Any]], session) -> int:
    """
    Process and store flights data in the database

    Args:
        flights_data: List of flight records from API
        session: SQLAlchemy database session

    Returns:
        int: Number of records processed
    """
    processed_count = 0

    for flight_record in flights_data:
        try:
            # Extract flight data safely, defaulting Nones to empty dicts for sub-access
            flight_date = flight_record.get("flight_date")
            flight_status = flight_record.get("flight_status")

            # Extract airline info safely
            airline_data = flight_record.get("airline") or {} # Handle None case
            airline_name = airline_data.get("name")
            airline_iata = airline_data.get("iata")
            airline_icao = airline_data.get("icao")

            # Extract flight info safely
            flight_info_data = flight_record.get("flight") or {} # Handle None case
            flight_number = flight_info_data.get("number")
            flight_iata = flight_info_data.get("iata")
            flight_icao = flight_info_data.get("icao")
            codeshared = flight_info_data.get("codeshared") # This might be None, handled by DB

            # Extract aircraft info safely
            aircraft_data = flight_record.get("aircraft") or {} # Handle None case
            aircraft_registration = aircraft_data.get("registration")
            aircraft_iata = aircraft_data.get("iata")
            aircraft_icao = aircraft_data.get("icao")
            aircraft_icao24 = aircraft_data.get("icao24")

            # Extract departure info safely
            departure_data = flight_record.get("departure") or {} # Handle None case
            departure_airport = departure_data.get("airport")
            departure_timezone = departure_data.get("timezone")
            departure_iata = departure_data.get("iata")
            departure_icao = departure_data.get("icao")
            departure_terminal = departure_data.get("terminal")
            departure_gate = departure_data.get("gate")
            departure_delay = departure_data.get("delay")
            departure_scheduled = departure_data.get("scheduled")
            departure_estimated = departure_data.get("estimated")
            departure_actual = departure_data.get("actual")
            departure_estimated_runway = departure_data.get("estimated_runway")
            departure_actual_runway = departure_data.get("actual_runway")

            # Extract arrival info safely
            arrival_data = flight_record.get("arrival") or {} # Handle None case
            arrival_airport = arrival_data.get("airport")
            arrival_timezone = arrival_data.get("timezone")
            arrival_iata = arrival_data.get("iata")
            arrival_icao = arrival_data.get("icao")
            arrival_terminal = arrival_data.get("terminal")
            arrival_gate = arrival_data.get("gate")
            arrival_baggage = arrival_data.get("baggage")
            arrival_delay = arrival_data.get("delay")
            arrival_scheduled = arrival_data.get("scheduled")
            arrival_estimated = arrival_data.get("estimated")
            arrival_actual = arrival_data.get("actual")
            arrival_estimated_runway = arrival_data.get("estimated_runway")
            arrival_actual_runway = arrival_data.get("actual_runway")

            # Check for required fields (adjust if your logic requires more/less)
            # Using departure_iata/arrival_iata as potentially more reliable than airline_iata
            if not all([flight_date, departure_iata, arrival_iata]):
                logger.warning(f"Missing essential fields (date, departure_iata, arrival_iata) for flight: {flight_record.get('flight', {}).get('iata', 'N/A')}")
                continue

            # Prepare flight object that matches the model's column names
            flight = {
                "flight_date": flight_date,
                "flight_status": flight_status,

                # Airline info
                "airline_name": airline_name,
                "airline_iata": airline_iata,
                "airline_icao": airline_icao,

                # Flight info
                "flight_number": flight_number,
                "flight_iata": flight_iata,
                "flight_icao": flight_icao,
                "codeshared": codeshared,

                # Aircraft info
                "aircraft_registration": aircraft_registration,
                "aircraft_iata": aircraft_iata,
                "aircraft_icao": aircraft_icao,
                "aircraft_icao24": aircraft_icao24,

                # Departure info
                "departure_airport": departure_airport,
                "departure_timezone": departure_timezone,
                "departure_iata": departure_iata,
                "departure_icao": departure_icao,
                "departure_terminal": departure_terminal,
                "departure_gate": departure_gate,
                "departure_delay": departure_delay,
                "departure_scheduled": departure_scheduled,
                "departure_estimated": departure_estimated,
                "departure_actual": departure_actual,
                "departure_estimated_runway": departure_estimated_runway,
                "departure_actual_runway": departure_actual_runway,

                # Arrival info
                "arrival_airport": arrival_airport,
                "arrival_timezone": arrival_timezone,
                "arrival_iata": arrival_iata,
                "arrival_icao": arrival_icao,
                "arrival_terminal": arrival_terminal,
                "arrival_gate": arrival_gate,
                "arrival_baggage": arrival_baggage,
                "arrival_delay": arrival_delay,
                "arrival_scheduled": arrival_scheduled,
                "arrival_estimated": arrival_estimated,
                "arrival_actual": arrival_actual,
                "arrival_estimated_runway": arrival_estimated_runway,
                "arrival_actual_runway": arrival_actual_runway,
                # Add the raw payload (ensure the model has this column)
                "raw_payload": flight_record
            }

            # Upsert flight data
            stmt = insert(Flight).values(**flight)
            # Ensure index elements match the unique constraint in the model
            stmt = stmt.on_conflict_do_update(
                index_elements=["flight_date", "flight_iata", "departure_iata", "arrival_iata", "departure_scheduled"],
                set_=flight # Update all fields on conflict
            )

            session.execute(stmt)
            processed_count += 1

        except Exception as e:
            # Log the error along with some identifying info if possible
            flight_ident = flight_record.get('flight', {}).get('iata', 'Unknown')
            logger.error(f"Error processing flight record {flight_ident}: {str(e)}", exc_info=True) # Add traceback

    # Commit all changes for the batch
    try:
        session.commit()
        logger.info(f"Committed {processed_count} flight records for the batch")
    except Exception as e:
        logger.error(f"Error committing batch: {str(e)}", exc_info=True)
        session.rollback() # Rollback the transaction on commit error
        return 0 # Indicate no records were successfully committed in this batch call

    return processed_count


def process_airlines_data(airlines_data: List[Dict[str, Any]], session) -> None:
    """
    Process and store airlines data in the database, using API's 'id' as PK

    Args:
        airlines_data: List of airline records from API
        session: SQLAlchemy database session
    """
    processed_count = 0
    skipped_count = 0
    for airline_record in airlines_data:
        try:
            # Extract API's id field for use as primary key
            api_id = airline_record.get("id")

            # Skip if no API ID (required as primary key)
            if not api_id:
                logger.warning(f"Missing API 'id' field for airline record: {airline_record}")
                skipped_count += 1
                continue

            # Extract other airline data
            iata_code = airline_record.get("iata_code") # Keep fetching this, now nullable

            # Prepare airline object that matches model column names
            airline = {
                "id": api_id, # This is now the PK
                "iata_code": iata_code,
                "airline_id": airline_record.get("airline_id"), # API's other ID
                "icao_code": airline_record.get("icao_code"),
                "iata_prefix_accounting": airline_record.get("iata_prefix_accounting"),
                "airline_name": airline_record.get("airline_name"),
                "callsign": airline_record.get("callsign"),
                "country_name": airline_record.get("country_name"),
                "country_iso2": airline_record.get("country_iso2"),
                "date_founded": airline_record.get("date_founded"),
                "hub_code": airline_record.get("hub_code"),
                "fleet_size": airline_record.get("fleet_size"),
                "fleet_average_age": airline_record.get("fleet_average_age"),
                "status": airline_record.get("status"),
                "type": airline_record.get("type"),
                "raw_payload": airline_record
            }

            # Upsert airline data based on the new primary key 'id'
            stmt = insert(Airline).values(**airline)
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"], # Use 'id' as the conflict target
                set_=airline # Update all fields on conflict
            )

            session.execute(stmt)
            processed_count += 1

        except Exception as e:
            logger.error(f"Error processing airline record with API ID {api_id}: {str(e)}", exc_info=True)
            # Depending on the error, you might want to rollback the specific record or the whole batch
            # For now, we let the outer loop handle batch commit/rollback

    # Commit all changes for the batch
    try:
        session.commit()
        logger.info(f"Processed {processed_count} airline records (skipped {skipped_count} due to missing API ID).")
    except Exception as e:
        logger.error(f"Error committing airlines batch: {str(e)}", exc_info=True)
        session.rollback() # Rollback the transaction on commit error


def process_airports_data(airports_data: List[Dict[str, Any]], session) -> None:
    """
    Process and store airports data in the database
    
    Args:
        airports_data: List of airport records from API
        session: SQLAlchemy database session
    """
    for airport_record in airports_data:
        try:
            # Extract airport data
            iata_code = airport_record.get("iata_code")
            
            # Skip if no IATA code (required as primary key)
            if not iata_code:
                logger.warning(f"Missing IATA code for airport: {airport_record}")
                continue
            
            # Prepare airport object that matches model column names
            airport = {
                "iata_code": iata_code,
                "airport_name": airport_record.get("airport_name"),
                "icao_code": airport_record.get("icao_code"),
                "latitude": airport_record.get("latitude"),
                "longitude": airport_record.get("longitude"),
                "geoname_id": airport_record.get("geoname_id"),
                "city_iata_code": airport_record.get("city_iata_code"),
                "country_name": airport_record.get("country_name"),
                "country_iso2": airport_record.get("country_iso2"),
                "timezone": airport_record.get("timezone"),
                "gmt": airport_record.get("gmt"),
                "phone_number": airport_record.get("phone_number"),
                "raw_payload": airport_record
            }
            
            # Upsert airport data
            stmt = insert(Airport).values(**airport)
            stmt = stmt.on_conflict_do_update(
                index_elements=["iata_code"],
                set_=airport
            )
            
            session.execute(stmt)
            
        except Exception as e:
            logger.error(f"Error processing airport record: {str(e)}")
    
    # Commit all changes
    session.commit()
    logger.info(f"Processed {len(airports_data)} airport records")


def process_routes_data(routes_data: List[Dict[str, Any]], session) -> None:
    """
    Process and store routes data in the database, including date_pulled

    Args:
        routes_data: List of route records from API
        session: SQLAlchemy database session
    """
    processed_count = 0
    skipped_count = 0
    missing_iata_count = 0
    # Get the current time ONCE for the entire batch, using UTC
    pulled_timestamp = datetime.now(timezone.utc)

    for route_record in routes_data:
        try:
            # Extract route data
            airline_data = route_record.get("airline") or {}
            flight_data = route_record.get("flight") or {}
            departure_data = route_record.get("departure") or {}
            arrival_data = route_record.get("arrival") or {}

            airline_iata = airline_data.get("iata")
            flight_number = flight_data.get("number")
            departure_iata = departure_data.get("iata")
            arrival_iata = arrival_data.get("iata")

            # Track routes with missing IATA codes
            if not airline_iata:
                missing_iata_count += 1
                logger.info(f"Route with missing airline IATA code: {airline_data.get('name')} ({airline_data.get('icao')}) - Flight {flight_number} from {departure_iata} to {arrival_iata}")

            # Skip if essential fields for the unique constraint are missing
            if not all([flight_number, departure_iata, arrival_iata]):
                logger.warning(f"Missing required fields (flight_number, dep_iata, arr_iata) for route: {route_record}")
                skipped_count += 1
                continue

            # Extract more details
            departure_airport = departure_data.get("airport")
            departure_timezone = departure_data.get("timezone")
            departure_icao = departure_data.get("icao")
            departure_terminal = departure_data.get("terminal")
            departure_time = departure_data.get("time")

            arrival_airport = arrival_data.get("airport")
            arrival_timezone = arrival_data.get("timezone")
            arrival_icao = arrival_data.get("icao")
            arrival_terminal = arrival_data.get("terminal")
            arrival_time = arrival_data.get("time")

            airline_name = airline_data.get("name")
            airline_callsign = airline_data.get("callsign")
            airline_icao = airline_data.get("icao")

            # Prepare route object that matches model column names
            route = {
                "airline_iata": airline_iata,
                "flight_number": flight_number,
                "departure_iata": departure_iata,
                "arrival_iata": arrival_iata,

                "departure_airport": departure_airport,
                "departure_timezone": departure_timezone,
                "departure_icao": departure_icao,
                "departure_terminal": departure_terminal,
                "departure_time": departure_time,

                "arrival_airport": arrival_airport,
                "arrival_timezone": arrival_timezone,
                "arrival_icao": arrival_icao,
                "arrival_terminal": arrival_terminal,
                "arrival_time": arrival_time,

                "airline_name": airline_name,
                "airline_callsign": airline_callsign,
                "airline_icao": airline_icao,

                "raw_payload": route_record,
                "date_pulled": pulled_timestamp
            }

            # Upsert route data
            stmt = insert(Route).values(**route)
            # Ensure index elements match the unique constraint in the model
            stmt = stmt.on_conflict_do_update(
                index_elements=["airline_iata", "flight_number", "departure_iata", "arrival_iata"],
                set_=route
            )

            session.execute(stmt)
            processed_count += 1

        except Exception as e:
            # Identify the route if possible for better error logging
            ident_str = f"{airline_iata or '-'}{flight_number or '-'}:{departure_iata or '-'}->{arrival_iata or '-'}"
            logger.error(f"Error processing route record ({ident_str}): {str(e)}", exc_info=True)

    # Commit all changes for the batch
    try:
        session.commit()
        logger.info(f"Processed {processed_count} route records (skipped {skipped_count} due to missing key fields).")
        logger.info(f"Found {missing_iata_count} routes with missing airline IATA codes.")
    except Exception as e:
        logger.error(f"Error committing routes batch: {str(e)}", exc_info=True)
        session.rollback()


def process_available_historical_data(fetch_supporting_data: bool = True) -> None:
    """
    Process all available historical data within the 3-month window
    
    Args:
        fetch_supporting_data: Whether to fetch airlines and airports data
    """
    # Calculate valid date range
    start_date, end_date = get_valid_date_range()
    logger.info(f"Processing historical data from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Generate list of dates (newest first)
    date_list = generate_date_list(start_date, end_date)
    
    # Create database session
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # First, fetch reference data (airlines, airports) if requested
        if fetch_supporting_data:
            logger.info("Fetching airlines data")
            airlines_data = fetch_paginated_data("airlines")
            process_airlines_data(airlines_data, session)
            
            logger.info("Fetching airports data")
            airports_data = fetch_paginated_data("airports")
            process_airports_data(airports_data, session)
            
            logger.info("Fetching routes data")
            routes_data = fetch_paginated_data("routes")
            process_routes_data(routes_data, session)
        
        # Process each date
        for date_str in date_list:
            # Check if we already have data for this date
            if has_data_for_date(date_str, session):
                logger.info(f"Already have data for {date_str}, skipping")
                continue
                
            logger.info(f"Processing flights for {date_str}")
            
            # Fetch and process flight data for this date
            flights_data = fetch_paginated_data("flights", date_str)
            if flights_data:
                records_processed = process_flights_data(flights_data, session)
                logger.info(f"Processed {records_processed} records for {date_str}")
            else:
                logger.warning(f"No data available for {date_str}")
            
            # Save checkpoint
            with open("checkpoint.txt", "w") as f:
                f.write(date_str)
            
            # Respect API rate limits
            time.sleep(2)
            
    except Exception as e:
        logger.error(f"Error processing historical data: {str(e)}")
        session.rollback()
    finally:
        session.close()


def process_date_range(start_date: datetime, end_date: datetime, fetch_supporting_data: bool = True) -> None:
    """
    Process flight data for a range of dates
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        fetch_supporting_data: Whether to fetch airlines and airports data
    """
    # Validate and adjust date range based on 3-month limitation
    valid_start, valid_end = get_valid_date_range()
    
    # Adjust start_date if it's before valid range
    if start_date < valid_start:
        logger.warning(f"Start date {start_date.strftime('%Y-%m-%d')} is before valid range start {valid_start.strftime('%Y-%m-%d')}")
        logger.warning("Adjusting start date to valid range start")
        start_date = valid_start
    
    # Adjust end_date if it's after valid range
    if end_date > valid_end:
        logger.warning(f"End date {end_date.strftime('%Y-%m-%d')} is after valid range end {valid_end.strftime('%Y-%m-%d')}")
        logger.warning("Adjusting end date to valid range end")
        end_date = valid_end
    
    # Check if range is valid
    if start_date > end_date:
        logger.error(f"Invalid date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        return
    
    logger.info(f"Processing date range from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    
    # Create database engine and session
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Fetch supporting data (airlines, airports) if requested
        if fetch_supporting_data:
            logger.info("Fetching airlines data")
            airlines_data = fetch_paginated_data("airlines")
            process_airlines_data(airlines_data, session)
            
            logger.info("Fetching airports data")
            airports_data = fetch_paginated_data("airports")
            process_airports_data(airports_data, session)
            
            logger.info("Fetching routes data")
            routes_data = fetch_paginated_data("routes")
            process_routes_data(routes_data, session)
        
        # Generate list of dates (newest first for better data quality)
        date_list = generate_date_list(start_date, end_date)
        
        # Process each date
        for date_str in date_list:
            # Check if we already have data for this date
            if has_data_for_date(date_str, session):
                logger.info(f"Already have data for {date_str}, skipping")
                continue
                
            logger.info(f"Processing flights for {date_str}")
            
            # Fetch flight data for this date
            flights_data = fetch_paginated_data("flights", date_str)
            
            # Process and store flight data
            if flights_data:
                records_processed = process_flights_data(flights_data, session)
                logger.info(f"Processed {records_processed} records for {date_str}")
            else:
                logger.warning(f"No data available for {date_str}")
            
            # Save checkpoint
            with open("checkpoint.txt", "w") as f:
                f.write(date_str)
            
            # Respect API rate limits
            time.sleep(1.5)
            
    except Exception as e:
        logger.error(f"Error processing date range: {str(e)}")
        session.rollback()
    finally:
        session.close()


def resume_processing(end_date: datetime, checkpoint_file: str = "checkpoint.txt") -> None:
    """
    Resume processing from the last checkpoint
    
    Args:
        end_date: End date (inclusive)
        checkpoint_file: File containing the last processed date
    """
    try:
        # Read checkpoint file
        with open(checkpoint_file, "r") as f:
            last_date_str = f.read().strip()
            
        # Parse date and add one day
        last_date = datetime.strptime(last_date_str, "%Y-%m-%d")
        start_date = last_date + timedelta(days=1)
        
        # Check if start_date is within valid range
        valid_start, valid_end = get_valid_date_range()
        if start_date < valid_start:
            logger.warning(f"Checkpoint date {start_date.strftime('%Y-%m-%d')} is before valid range start {valid_start.strftime('%Y-%m-%d')}")
            logger.warning("Adjusting to valid range start")
            start_date = valid_start
        
        # Continue processing
        logger.info(f"Resuming from {start_date.strftime('%Y-%m-%d')}")
        process_date_range(start_date, end_date, fetch_supporting_data=False)
        
    except FileNotFoundError:
        logger.error(f"Checkpoint file not found: {checkpoint_file}")
        logger.info("Starting new processing run with all available historical data")
        process_available_historical_data()
    except Exception as e:
        logger.error(f"Error resuming processing: {str(e)}")


def collect_daily_data() -> None:
    """
    Collect data for yesterday (to be run daily)
    """
    yesterday = datetime.now() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")
    
    logger.info(f"Collecting daily data for {yesterday_str}")
    
    # Create database session
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Check if we already have data for yesterday
        if has_data_for_date(yesterday_str, session):
            logger.info(f"Already have data for {yesterday_str}, skipping")
            return
            
        # Fetch flight data for yesterday
        flights_data = fetch_paginated_data("flights", yesterday_str)
        
        # Process and store flight data
        if flights_data:
            records_processed = process_flights_data(flights_data, session)
            logger.info(f"Processed {records_processed} records for {yesterday_str}")
        else:
            logger.warning(f"No data available for {yesterday_str}")
            
    except Exception as e:
        logger.error(f"Error collecting daily data: {str(e)}")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch and process flight data from AviationStack API")
    
    # Define command-line arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--daily", action="store_true", help="Collect data for yesterday")
    group.add_argument("--historical", action="store_true", help="Collect all available historical data (last 3 months)")
    group.add_argument("--date-range", nargs=2, metavar=("START_DATE", "END_DATE"), 
                      help="Collect data for a specific date range (YYYY-MM-DD format)")
    group.add_argument("--resume", action="store_true", help="Resume from last checkpoint")
    
    parser.add_argument("--skip-reference", action="store_true", help="Skip fetching reference data (airlines, airports, routes)")
    
    args = parser.parse_args()
    
    # Process based on arguments
    if args.daily:
        collect_daily_data()
    elif args.historical:
        process_available_historical_data(fetch_supporting_data=not args.skip_reference)
    elif args.date_range:
        try:
            start_date = datetime.strptime(args.date_range[0], "%Y-%m-%d")
            end_date = datetime.strptime(args.date_range[1], "%Y-%m-%d")
            process_date_range(start_date, end_date, fetch_supporting_data=not args.skip_reference)
        except ValueError as e:
            logger.error(f"Invalid date format: {str(e)}")
    elif args.resume:
        end_date = datetime.now()
        resume_processing(end_date)
    
    logger.info("Processing completed") 