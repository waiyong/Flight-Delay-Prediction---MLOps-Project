# Data Processing Pipeline Documentation

## Overview

This document outlines the implementation of the data processing pipeline for the Flight Delay Prediction project. The pipeline is responsible for fetching flight data from the AviationStack API, transforming it, and loading it into the PostgreSQL database.

## API Constraints and Solutions

AviationStack API has the following constraints:
- Maximum of 100 results per API call (Basic Plan)
- Maximum of 1000 results per API call (Professional Plan and above)
- Default limit is 100 results
- Historical data limited to the last 3 months only

To overcome these limitations and pull comprehensive flight data, we implement a paginated data fetching strategy.

## Processing Workflow for AviationStack APIs

AviationStack provides four main APIs that we use in this project. Each has distinct characteristics and processing requirements:

### API Endpoint Comparison

| API Endpoint | Historical Data | Update Frequency | Primary Key | Data Volume | Key Parameters |
|--------------|----------------|------------------|-------------|-------------|----------------|
| `/flights`   | Yes (3 months) | Real-time        | Composite   | Very High   | flight_date, flight_status |
| `/airlines`  | No             | Rarely changes   | iata_code   | Low         | None (reference data) |
| `/airports`  | No             | Rarely changes   | iata_code   | Low         | None (reference data) |
| `/routes`    | No             | Occasionally     | Composite   | Medium      | airline_iata, dep_iata, arr_iata |

### Processing Sequence

The optimal processing sequence accounts for dependencies between data types:

1. **Reference Data** (Airlines & Airports):
   - Process first as they change infrequently
   - Serve as lookups for flight and route data
   - No date filtering required

2. **Routes Data**:
   - Process after reference data
   - Provides context for flight patterns
   - No date filtering required

3. **Flight Data**:
   - Process last, with date-based processing
   - Highest volume that requires pagination
   - Must operate within 3-month historical window

### Step-by-Step Process by API

#### 1. Airlines API Process

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│ Fetch with  │     │ Extract IATA │     │ Transform   │     │ Upsert to  │
│ Pagination  │ ──► │ Codes & Info │ ──► │ Attributes  │ ──► │ airlines   │
└─────────────┘     └──────────────┘     └─────────────┘     └────────────┘
```

1. **Fetch**: Call `/airlines` endpoint with pagination until all records retrieved
2. **Extract**: Process airline records, ensuring IATA code (primary key) exists
3. **Transform**: Map API fields to database schema
4. **Load**: Upsert to `airlines` table with conflict resolution on iata_code

#### 2. Airports API Process

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│ Fetch with  │     │ Extract IATA │     │ Transform   │     │ Upsert to  │
│ Pagination  │ ──► │ Codes & Info │ ──► │ Attributes  │ ──► │ airports   │
└─────────────┘     └──────────────┘     └─────────────┘     └────────────┘
```

1. **Fetch**: Call `/airports` endpoint with pagination until all records retrieved
2. **Extract**: Process airport records, ensuring IATA code (primary key) exists
3. **Transform**: Map API fields to database schema, including geo coordinates
4. **Load**: Upsert to `airports` table with conflict resolution on iata_code

#### 3. Routes API Process

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐     ┌────────────┐
│ Fetch with  │     │ Validate     │     │ Transform   │     │ Upsert to  │
│ Pagination  │ ──► │ IATA Codes   │ ──► │ Attributes  │ ──► │ routes     │
└─────────────┘     └──────────────┘     └─────────────┘     └────────────┘
```

1. **Fetch**: Call `/routes` endpoint with pagination until all records retrieved
2. **Validate**: Ensure airline, departure, and arrival IATA codes exist
3. **Transform**: Map API fields to database schema
4. **Load**: Upsert to `routes` table with composite key conflict resolution

#### 4. Flights API Process (Date-Based)

```
┌─────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
│ Validate│     │ Fetch for│     │ Extract  │     │ Validate │     │ Transform│     │ Upsert to│
│  Date   │ ──► │ Date with│ ──► │ Flight   │ ──► │ Required │ ──► │ Nested   │ ──► │ flights  │
│  Range  │     │Pagination│     │ Records  │     │ Fields   │     │ Fields   │     │ Table    │
└─────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘     └──────────┘
```

1. **Validate Date**: Check if date is within 3-month window
2. **Fetch**: Call `/flights` endpoint with date parameter and pagination
3. **Extract**: Process flight records with nested departure/arrival data
4. **Validate**: Ensure required fields exist (date, status, airline_iata, etc.)
5. **Transform**: Map API fields to database schema, handling nested structures
6. **Load**: Upsert to `flights` table with composite key conflict resolution

### Orchestration Strategy

The optimal orchestration strategy for these APIs is:

1. **Initial Load**:
   - Load airlines and airports data first (reference data)
   - Load historical flight data (last 3 months) with date prioritization
   - Store checkpoints after each date processed

2. **Daily Updates**:
   - Update airlines and airports weekly (reference data changes infrequently)
   - Fetch new flight data daily for the previous day
   - Periodically check for gaps in recent flight data

3. **Recovery Process**:
   - If process fails, resume from last checkpoint
   - Verify data completeness with record counts by date
   - Implement retries for failed dates

This strategy ensures efficient data collection while respecting API constraints and maintaining data integrity in PostgreSQL.

## Implementation Components

### 1. Enhanced API Data Fetching with Pagination

#### Pagination Logic
- Use the `offset` parameter to fetch subsequent batches of data
- Set the `limit` parameter to the maximum allowed (100 or 1000)
- Track the `total` value from the pagination object to know when to stop
- Continue fetching until all data for a given date has been retrieved

```python
def fetch_paginated_data(endpoint, date, params=None):
    """
    Fetch all data for a specific date using pagination
    
    Args:
        endpoint (str): API endpoint (flights, airlines, etc.)
        date (str): Flight date in YYYY-MM-DD format
        params (dict): Additional parameters for the API call
    
    Returns:
        list: All data records for the specified date
    """
    all_results = []
    offset = 0
    limit = 100  # Maximum allowed for Basic Plan
    
    # Implementation details...
    
    return all_results
```

#### Date Range Processing
- Process data for each date in the target range (e.g., 2024-02-01 to 2024-04-30)
- For each date, fetch all available records using pagination
- Store the data in the database after all records for a date have been fetched

#### Rate Limiting
- Implement sleep intervals between API calls
- Use exponential backoff for error handling
- Cache responses to avoid duplicate calls

### 2. Data Transformation and Loading

#### Data Extraction and Normalization
- Parse JSON responses into appropriate data structures
- Extract relevant fields for each table (Flights, Airlines, Airports, Routes)
- Handle nested structures like departure/arrival information
- Apply data type conversions where needed

#### Data Validation
- Implement validation rules for each data type
- Check for missing required fields
- Validate date formats, numeric ranges, etc.
- Log validation errors for manual review

#### Database Loading
- Use SQLAlchemy's `insert().on_conflict_do_update()` for upsert operations
- Implement batch inserts for better performance
- Ensure transaction management for atomic operations

### 3. Orchestration and Scheduling

#### Workflow Script
- Main script that orchestrates the entire ETL process
- Implements checkpointing to resume interrupted runs
- Comprehensive logging and error handling

#### Scheduling with Airflow/Prefect
- Set up DAGs for regular data updates
- Configure incremental data loads
- Monitoring and notification for pipeline failures

### 4. Testing and Monitoring

#### Testing Framework
- Unit tests for individual components
- Integration tests for the entire pipeline
- Test with small data samples before production runs

#### Monitoring
- Track API call counts against your quota
- Monitor database size and growth
- Set up quality checks to detect data anomalies

## Execution Flow

1. **Initialize**: Set up connections and load configuration
2. **Process Date Range**: For each date in the range:
   - Fetch all flight data using pagination
   - Process and validate the data
   - Store the data in the database
   - Log progress and errors
3. **Finalize**: Clean up resources and generate report

## Example Usage

```python
from datetime import datetime, timedelta
from flight_data_processor import process_date_range

# Define date range
start_date = datetime(2024, 2, 1)
end_date = datetime(2024, 4, 30)

# Process all dates in the range
process_date_range(start_date, end_date)
```

## Error Handling

- **API Errors**: Implement retry logic with exponential backoff
- **Data Validation Errors**: Log errors and continue with valid records
- **Database Errors**: Implement transaction rollback and retry

## Performance Considerations

- Use bulk inserts to minimize database operations
- Consider parallel processing for different dates
- Implement caching to avoid redundant API calls
- Checkpoint progress to enable resuming after interruptions

## Maintenance

Regular maintenance tasks include:
- Monitoring API usage against quota limits
- Validating data quality and completeness
- Updating the pipeline for changes in the API
- Optimizing database performance

## Future Enhancements

- Implement real-time data processing
- Add support for additional AviationStack endpoints
- Enhance validation rules and data quality metrics
- Scale with distributed processing for larger data volumes 