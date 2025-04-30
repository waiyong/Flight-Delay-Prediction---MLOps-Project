# Setting Up Airflow for Data Pipeline Scheduling

This document outlines how to set up Apache Airflow to schedule and monitor the Flight Delay Prediction data processing pipeline.

## Overview

Apache Airflow is a platform to programmatically author, schedule, and monitor workflows. It's ideal for orchestrating our data pipeline that needs to fetch data from the AviationStack API on a regular schedule and process it through various stages.

## Installation and Setup

### 1. Install Airflow

```bash
# Create a virtual environment for Airflow
python -m venv airflow_env
source airflow_env/bin/activate  # On Windows: airflow_env\Scripts\activate

# Install Apache Airflow
pip install apache-airflow
pip install apache-airflow-providers-postgres

# Initialize the Airflow database
airflow db init
```

### 2. Configure Airflow

Edit the `airflow.cfg` file in the Airflow home directory:

```ini
# Set the executor to LocalExecutor for local development
executor = LocalExecutor

# Set the database connection
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow

# Set the dags directory to the project's dags folder
dags_folder = /path/to/flight-delay-prediction/airflow/dags
```

### 3. Create Airflow User

```bash
# Create an admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 4. Start Airflow Services

```bash
# Start the web server
airflow webserver --port 8080

# In a separate terminal, start the scheduler
airflow scheduler
```

## Creating DAGs for the Flight Delay Prediction Pipeline

Create the following DAG files in the `airflow/dags` directory:

### 1. `flight_data_daily_dag.py`

This DAG fetches flight data for the previous day:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import sys
import os

# Add the project root directory to the Python path
sys.path.append('/path/to/flight-delay-prediction')

# Import the project's flight data processor
from scripts.flight_data_processor import fetch_paginated_data, process_flights_data

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'flight_data_daily',
    default_args=default_args,
    description='Fetch and process flight data for the previous day',
    schedule_interval='0 2 * * *',  # Run at 2 AM every day
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['flight_data'],
)

# Python function to process flight data for the previous day
def process_previous_day():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    DB_URL = os.getenv("DATABASE_URL")
    
    # Calculate the previous day's date
    yesterday = datetime.now() - timedelta(days=1)
    date_str = yesterday.strftime("%Y-%m-%d")
    
    # Create database session
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Fetch flight data for the previous day
        flights_data = fetch_paginated_data("flights", date_str)
        
        # Process and store flight data
        process_flights_data(flights_data, session)
        
        return f"Processed {len(flights_data)} flight records for {date_str}"
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# Define the task
fetch_process_task = PythonOperator(
    task_id='fetch_process_flight_data',
    python_callable=process_previous_day,
    dag=dag,
)

# Optional: Add a task to create a report or send notifications
```

### 2. `flight_data_backfill_dag.py`

This DAG processes historical data for a specific date range:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

# Add the project root directory to the Python path
sys.path.append('/path/to/flight-delay-prediction')

# Import the project's flight data processor
from scripts.flight_data_processor import process_date_range

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'flight_data_backfill',
    default_args=default_args,
    description='Backfill flight data for a specified date range',
    schedule_interval=None,  # This is a manually triggered DAG
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['flight_data', 'backfill'],
)

# Python function to process a date range
def backfill_date_range(**kwargs):
    # Get date range from DAG parameters
    start_date_str = kwargs.get('params', {}).get('start_date')
    end_date_str = kwargs.get('params', {}).get('end_date')
    
    if not start_date_str or not end_date_str:
        raise ValueError("start_date and end_date parameters are required")
    
    # Parse dates
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    
    # Process date range
    process_date_range(start_date, end_date, fetch_supporting_data=True)
    
    return f"Processed data from {start_date_str} to {end_date_str}"

# Define the task
backfill_task = PythonOperator(
    task_id='backfill_flight_data',
    python_callable=backfill_date_range,
    provide_context=True,
    dag=dag,
)
```

### 3. `reference_data_update_dag.py`

This DAG updates reference data (airlines, airports) weekly:

```python
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
import os

# Add the project root directory to the Python path
sys.path.append('/path/to/flight-delay-prediction')

# Import the project's flight data processor
from scripts.flight_data_processor import fetch_paginated_data, process_airlines_data, process_airports_data

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'reference_data_update',
    default_args=default_args,
    description='Update reference data (airlines, airports) weekly',
    schedule_interval='0 0 * * 0',  # Run at midnight every Sunday
    start_date=datetime(2024, 2, 1),
    catchup=False,
    tags=['reference_data'],
)

# Python function to update airlines data
def update_airlines():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    DB_URL = os.getenv("DATABASE_URL")
    
    # Create database session
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Fetch airlines data
        airlines_data = fetch_paginated_data("airlines", None)
        
        # Process and store airlines data
        process_airlines_data(airlines_data, session)
        
        return f"Updated {len(airlines_data)} airline records"
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# Python function to update airports data
def update_airports():
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    import os
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    DB_URL = os.getenv("DATABASE_URL")
    
    # Create database session
    engine = create_engine(DB_URL)
    Session = sessionmaker(bind=engine)
    session = Session()
    
    try:
        # Fetch airports data
        airports_data = fetch_paginated_data("airports", None)
        
        # Process and store airports data
        process_airports_data(airports_data, session)
        
        return f"Updated {len(airports_data)} airport records"
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

# Define the tasks
update_airlines_task = PythonOperator(
    task_id='update_airlines',
    python_callable=update_airlines,
    dag=dag,
)

update_airports_task = PythonOperator(
    task_id='update_airports',
    python_callable=update_airports,
    dag=dag,
)

# Set task dependencies
update_airlines_task >> update_airports_task
```

## Triggering DAGs

### Daily Data Collection

The `flight_data_daily` DAG runs automatically at 2 AM every day.

### Historical Data Backfill

To trigger a backfill for a specific date range:

1. Go to the Airflow UI at http://localhost:8080
2. Navigate to the `flight_data_backfill` DAG
3. Click on "Trigger DAG w/ config"
4. Enter the date range parameters in JSON format:
   ```json
   {
     "start_date": "2024-02-01",
     "end_date": "2024-04-30"
   }
   ```
5. Click "Trigger"

## Monitoring and Logging

### Viewing Logs

1. Go to the Airflow UI
2. Navigate to the DAG run
3. Click on the task
4. Click on "Log" to view the execution logs

### Setting Up Alerts

Configure email alerts in the `airflow.cfg` file:

```ini
[email]
email_backend = airflow.utils.email.send_email_smtp
smtp_host = smtp.gmail.com
smtp_user = your.email@gmail.com
smtp_password = your_app_password
smtp_port = 587
smtp_mail_from = your.email@gmail.com
```

## Best Practices

1. **Idempotency**: All tasks should be idempotent (can be run multiple times without side effects)
2. **Error Handling**: Implement proper error handling and logging
3. **Resource Management**: Configure resource pools to avoid overloading the system
4. **Testing**: Test DAGs using Airflow's testing utilities before deploying
5. **Monitoring**: Set up monitoring for DAG status and task duration

## Maintenance

1. Regularly review Airflow logs for any issues
2. Monitor database size growth
3. Set up log rotation to prevent disk space issues
4. Periodically clean up old DAG runs and task instances

## Scaling Up

As the project grows, consider scaling the Airflow installation:

1. Use CeleryExecutor for distributed task execution
2. Deploy Airflow on Docker or Kubernetes
3. Set up a separate metadata database for Airflow
4. Configure worker nodes for parallel task execution 