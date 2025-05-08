# Data Processing Pipeline Documentation

## 1. Overview

This document outlines the implementation of the data processing pipeline for the Flight Delay Prediction project. The pipeline is responsible for fetching aviation data from the AviationStack API, transforming it, and loading it into a PostgreSQL database. It consists of separate ETL scripts for different data entities: Airlines, Airports, Routes, and Flights.

All scripts utilize common helper functions from `scripts.flight_data_processor` for tasks like API calls with pagination, data transformation, and database interaction.

## 2. Common ETL Components

### 2.1. Configuration
-   **Environment Variables**: API keys (`AVIATIONSTACK_API_KEY`) and database connection strings (`DATABASE_URL`) are loaded from a `.env` file using `python-dotenv`.
-   **Logging**: Centralized logging is configured to output to both the console and a `flight_data_processor.log` file. Each ETL script uses a logger specific to its module.

### 2.2. API Interaction (`scripts.flight_data_processor.call_aviationstack`)
-   **Base URL**: `https://api.aviationstack.com/v1/`
-   **Authentication**: `access_key` (API key) is automatically added to parameters.
-   **Error Handling**:
    -   Retries up to `MAX_RETRIES` (currently 3) with exponential backoff (`RETRY_DELAY` seconds).
    -   Raises HTTP errors for 4xx/5xx responses.
    -   Parses API-level errors from the JSON response.
    -   Specific handling for rate limit errors (waits and retries).
    -   Specific handling for date-related errors (returns an error marker).

### 2.3. Paginated Data Fetching (`scripts.flight_data_processor.fetch_paginated_data`)
-   Handles API pagination by iteratively calling the API with increasing `offset` values.
-   Uses a `limit` of 1000 records per page (suitable for paid plans).
-   Collects all results for a given endpoint (or endpoint + date for flights) into a single list in memory before returning.
-   Includes a 1-second `time.sleep(1)` between paginated calls to respect API rate limits.
-   For the "flights" endpoint, it can take a `date` parameter to fetch data for a specific day.
-   Includes a check using `is_date_in_valid_range` for the "flights" endpoint to ensure requested dates are within AviationStack's typical 3-month historical data window.

### 2.4. Database Interaction (`scripts.flight_data_processor` and individual `process_*_data` functions)
-   **SQLAlchemy**: Used for ORM and database interaction.
-   **Session Management**: Each main ETL script creates a SQLAlchemy session.
-   **Upsert Logic**: Data is inserted into tables using an "upsert" strategy (`INSERT ... ON CONFLICT DO UPDATE`) to handle new records and update existing ones based on defined unique constraints.
-   **Transaction Management**:
    -   For Airlines, Airports, and Routes: `session.commit()` is called after processing all records for that entity. If an error occurs during the batch commit, the session is rolled back.
    -   For Flights: `process_flights_data` commits data after processing all records *for a single day*. This means each day's flight data is a separate transaction.

## 3. ETL Pipelines by Data Entity

The general flow for each ETL script (`run_etl_*.py`) is:
1.  Load environment variables.
2.  Establish a database session.
3.  Call `fetch_paginated_data` for the specific API endpoint.
4.  Pass the fetched data to a dedicated `process_<entity>_data` function.
5.  The processing function transforms data and executes upserts.
6.  Close the database session.

---

### 3.1. Airlines Data Pipeline

-   **Script**: `run_etl_airlines_airport.py` (handles both airlines and airports)
-   **API Endpoint**: `/airlines`
-   **Processing Function**: `scripts.flight_data_processor.process_airlines_data`
-   **Database Table**: `airlines` (defined in `scripts.models.Airline`)

#### Flow:
1.  **Fetch**: `fetch_paginated_data(endpoint="airlines")` retrieves all airline records.
2.  **Transform & Load (`process_airlines_data`)**:
    *   Iterates through each airline record.
    *   **Primary Key**: Uses the `id` field from the API response as the primary key for the `airlines` table. Records missing this API `id` are skipped and logged.
    *   Extracts relevant fields (e.g., `iata_code`, `icao_code`, `airline_name`, `country_name`, `status`, `type`, `raw_payload`).
    *   Constructs an `Airline` object.
    *   Performs an upsert into the `airlines` table. Conflict is checked on the `id` column.
    *   A single `session.commit()` is called after attempting to process all fetched airline records.

#### Nuances:
-   The `iata_code` is fetched but is not the primary key, as the API's `id` field is used for better uniqueness.
-   Skipping records if the API `id` is missing is a key data quality step.

---

### 3.2. Airports Data Pipeline

-   **Script**: `run_etl_airlines_airport.py` (handles both airlines and airports)
-   **API Endpoint**: `/airports`
-   **Processing Function**: `scripts.flight_data_processor.process_airports_data`
-   **Database Table**: `airports` (defined in `scripts.models.Airport`)

#### Flow:
1.  **Fetch**: `fetch_paginated_data(endpoint="airports")` retrieves all airport records.
2.  **Transform & Load (`process_airports_data`)**:
    *   Iterates through each airport record.
    *   **Primary Key**: Uses `iata_code` from the API response as the primary key. Records missing `iata_code` are skipped and logged.
    *   Extracts relevant fields (e.g., `airport_name`, `icao_code`, `latitude`, `longitude`, `timezone`, `country_name`, `raw_payload`).
    *   Constructs an `Airport` object.
    *   Performs an upsert into the `airports` table. Conflict is checked on the `iata_code` column.
    *   A single `session.commit()` is called after attempting to process all fetched airport records.

#### Nuances:
-   Relies on `iata_code` being present and unique for each airport.

---

### 3.3. Routes Data Pipeline

-   **Script**: `run_etl_routes.py`
-   **API Endpoint**: `/routes` (Note: Requires a paid AviationStack plan)
-   **Processing Function**: `scripts.flight_data_processor.process_routes_data`
-   **Database Table**: `routes` (defined in `scripts.models.Route`)

#### Flow:
1.  **Fetch**: `fetch_paginated_data(endpoint="routes")` retrieves all route records.
2.  **Transform & Load (`process_routes_data`)**:
    *   Iterates through each route record.
    *   A `pulled_timestamp` (UTC) is generated once for the entire batch of routes being processed.
    *   Extracts `airline_iata`, `flight_number`, `departure_iata`, `arrival_iata` from nested API structures.
    *   **Data Quality & Skipping**:
        *   Logs an `INFO` message if `airline_data.get("iata")` is missing/empty, and increments `missing_iata_count`. The record is still processed.
        *   Logs a `WARNING` and **skips** the record if any of `flight_number`, `departure_data.get("iata")`, or `arrival_data.get("iata")` are missing/empty.
    *   Extracts other details like airport names, times, airline name, ICAO, etc.
    *   Constructs a `Route` object, including the `raw_payload` and the common `date_pulled` timestamp.
    *   Performs an upsert into the `routes` table.
        *   **Unique Constraint for Upsert**: `UniqueConstraint('airline_iata', 'flight_number', 'departure_iata', 'arrival_iata', name='uix_route_identity')`.
        *   The `airline_iata` column in the `routes` table is nullable. PostgreSQL's `UniqueConstraint` treats `NULL`s as distinct, so multiple routes can have `NULL` for `airline_iata` and still be inserted if other components differ, or even if they are the same (as `NULL` is not equal to `NULL` for constraint purposes). The `on_conflict_do_update` will effectively update rows where these four components are non-NULL and match an existing record.
    *   A single `session.commit()` is called after attempting to process all fetched route records. Final counts of processed, skipped, and missing IATA records are logged.

#### Nuances:
-   Requires a paid AviationStack plan for the `/routes` endpoint.
-   Special handling for missing `airline_iata`: records are ingested with `NULL` `airline_iata` but logged. This is to capture as much data as possible, with the understanding that these specific records might be harder to uniquely identify for updates or may require special handling during analysis/modeling.
-   Records are strictly skipped if `flight_number`, `departure_iata`, or `arrival_iata` are missing, as these are deemed essential for basic route identification.
-   The `date_pulled` column indicates when the batch of routes was processed.

---

### 3.4. Flights Data Pipeline

-   **Script**: `run_etl_flights.py`
-   **API Endpoint**: `/flights`
-   **Processing Function**: `scripts.flight_data_processor.process_flights_data`
-   **Database Table**: `flights` (defined in `scripts.models.Flight`)

#### Flow:
1.  **Date Range Calculation**: `run_etl_flights.py` calculates a date range from 14 days ago to the current day. The start and end dates are logged.
2.  **Iterate by Date**: The script loops through each date in this calculated range.
3.  **Fetch (per day)**: For each `date_str`:
    *   `fetch_paginated_data(endpoint="flights", date=date_str)` retrieves all flight records for that specific day.
4.  **Transform & Load (`process_flights_data`)**:
    *   This function is called with the flight data for a single day.
    *   Iterates through each flight record for that day.
    *   Extracts all relevant fields from the potentially nested API response (departure, arrival, airline, flight, aircraft details, `raw_payload`).
    *   **Data Quality & Skipping**:
        *   Logs a `WARNING` and **skips** the record if `flight_date`, `flight_status`, `departure_iata`, or `arrival_iata` are missing/empty. (Based on `process_flights_data` logic: `if not all([flight_date, flight_status, departure_iata, arrival_iata]): ... continue`)
    *   Constructs a `Flight` object.
    *   Performs an upsert into the `flights` table.
        *   **Unique Constraint for Upsert**: `UniqueConstraint('flight_date', 'flight_iata', 'departure_iata', 'arrival_iata', 'departure_scheduled', name='uix_flight_identity')`.
    *   `session.commit()` is called *within* `process_flights_data` after processing all records for that *single day's batch*. This makes each day's flight data an independent transaction.

#### Nuances:
-   **Date-Iterative Processing**: Unlike other entities, flights are fetched and processed day by day for a specified range.
-   **Historical Data Limit**: `fetch_paginated_data` includes a check (`is_date_in_valid_range`) to log a warning if a requested date is outside AviationStack's typical 3-month historical data window, and will return an empty list in such cases.
-   **Transactional Commits per Day**: Each day's flight data is committed separately, which can be beneficial for large volumes as it avoids one massive transaction.
-   The unique constraint for flights is more complex, including `flight_date`, `flight_iata`, airport IATAs, and `departure_scheduled` time to uniquely identify a flight instance.

## 4. Orchestration and Future Considerations
(This section can remain largely as is, but ensure it aligns with the above details if any specific scheduling or recovery logic has been implemented in `scripts/flight_data_processor.py`'s `main` block or helper functions like `resume_processing`.)

- **Initial Load**: Fetch reference data (Airlines, Airports, Routes) first. Then, fetch historical flight data (e.g., last 14 days as per `run_etl_flights.py`, or a wider 3-month window if using `process_available_historical_data` or `process_date_range` from `flight_data_processor.py`).
- **Daily Updates**: Schedule `run_etl_flights.py` (or `flight_data_processor.py --daily`) to fetch yesterday's flight data. Periodically refresh reference data (Airlines, Airports, Routes) as needed.
- **Error Handling & Recovery**: Each script has try-except blocks. `flight_data_processor.py` also includes functions for `resume_processing` using a checkpoint file, which is utilized if running it directly with `--resume`.

This updated structure should provide a clear and accurate overview of your data processing pipelines. 