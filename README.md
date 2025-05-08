# Flight Delay Prediction - MLOps Project

A comprehensive end-to-end MLOps project for predicting flight delays using historical aviation data from the [AviationStack API](https://aviationstack.com/documentation).

## Project Overview

This project aims to provide hands-on experience with MLOps practices by building a complete machine learning pipeline for flight delay prediction. The system ingests flight data from AviationStack API, processes it, trains models to predict flight delays, and serves these predictions via an API.

## Project Structure

The project is organized into the following stages:

### Stage 1: Data Ingestion and Storage
- PostgreSQL database running in Docker for storing flight data
- Data ingestion scripts for fetching data from AviationStack API with pagination
- Normalized data storage in separate tables (Flights, Airports, Airlines, Routes)
- Scheduled data ingestion pipeline

### Stage 2: Data Processing and Feature Engineering
- Data cleaning and preprocessing pipeline
- Feature engineering for flight delay prediction
- Data validation and quality checks with Great Expectations
- Feature store implementation with Feast or Hopsworks

### Stage 3: Model Training and Versioning
- Experiment tracking with MLflow
- Model training pipeline
- Hyperparameter optimization
- Model versioning and registry
- Model performance evaluation

### Stage 4: Model Packaging and Serving
- Model packaging with Docker
- REST API for model serving with FastAPI
- Real-time and batch prediction endpoints
- Model A/B testing capabilities with BentoML

### Stage 5: CI/CD Pipeline
- Automated testing
- Continuous integration with GitHub Actions
- Continuous deployment pipeline
- Model deployment automation

### Stage 6: Kubernetes and Infrastructure Orchestration
- Kubernetes deployment for all components
- Infrastructure as Code (IaC) with Terraform
- Auto-scaling and resource management
- Secrets management

### Stage 7: Monitoring and Observability
- Model performance monitoring with Prometheus/Grafana
- Data drift detection with Evidently AI
- System health metrics
- Logging and alerting
- Dashboards for visualization

## MLOps Tools

This project implements a comprehensive MLOps stack with the following tools:

### Data Engineering & Pipeline Orchestration
- **PostgreSQL**: Database for structured storage
- **Docker**: Containerization of database and services
- **Metaflow**: Workflow orchestration and scheduling
- **DVC**: Data version control

### Model Development & Experiment Tracking
- **MLflow**: Track experiments, parameters, and metrics
- **Weights & Biases** (alternative): Visual experiment tracking
- **Great Expectations**: Data validation and quality checks

### Model Serving & Deployment
- **FastAPI**: API framework for model serving
- **BentoML**: Model packaging and serving
- **Kubernetes**: Orchestration for scaling components
- **Terraform**: Infrastructure as code

### Monitoring & Observability
- **Prometheus/Grafana**: Metrics collection and visualization
- **Evidently AI**: Model and data drift monitoring
- **Seldon/KServe**: Advanced model serving

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.10+
- AviationStack API key (get it from [AviationStack](https://aviationstack.com/documentation))

### Setup

1. Clone the repository
```
git clone https://github.com/yourusername/flight-delay-prediction.git
cd flight-delay-prediction
```

2. Set up environment variables
Create a `.env` file with your AviationStack API key:
```
AVIATIONSTACK_API_KEY=your_api_key_here
```

3. Start PostgreSQL using Docker Compose
```
docker-compose up -d
```

4. Accessing the Database with Adminer (Optional)
This project includes Adminer, a web-based database management tool. Once Docker Compose is running, you can access Adminer in your web browser:
- URL: http://localhost:8080
- System: PostgreSQL
- Server: `postgres` (this is the service name defined in `docker-compose.yml`)
- Username: `silverlineage` (from `docker-compose.yml`)
- Password: `Fusion199!` (from `docker-compose.yml`)
- Database: `flight_db` (from `docker-compose.yml`)

5. Run the data ingestion script
```
python scripts/ingest_data.py
```

## Current Progress

- [x] PostgreSQL database setup with Docker
- [x] Database schema design and table creation
- [x] Data ingestion from AviationStack API
- [ ] Scheduled data ingestion
- [ ] Data processing pipeline
- [ ] Feature engineering pipeline
- [ ] Model training pipeline
- [ ] Model serving API
- [ ] CI/CD pipeline setup
- [ ] Kubernetes deployment
- [ ] Monitoring and observability

## Data Source

This project uses the [AviationStack API](https://aviationstack.com/documentation) to fetch real-time and historical flight data. The API provides comprehensive aviation data including flights, airlines, airports, and routes information.

## Data Relationships and Temporal Considerations

### Dataset Characteristics

1. **Airports and Airlines Data**
   - Static reference data that changes infrequently
   - Safe to use for joins and lookups
   - Primary keys: `iata_code` for both tables

2. **Routes Data**
   - Represents current/planned flight schedules
   - Refreshed daily via AviationStack API
   - Contains typical/planned schedule information
   - Primary key: combination of `airline_iata`, `flight_number`, `departure_iata`, `arrival_iata`
   - Includes `date_pulled` timestamp to track data freshness

3. **Flights Data**
   - Historical and real-time operational data
   - Contains actual flight instances with specific dates and times
   - Rich in operational details (delays, actual times, gates, etc.)
   - Each record represents a unique flight occurrence

### Data Relationship Challenges

When working with these datasets, particularly when joining Flights data with Routes data, consider the following:

1. **Temporal Mismatch**
   - Routes data represents current schedules
   - Flights data contains historical records
   - Direct joins between historical flights and current routes may lead to:
     - Missing matches (discontinued routes)
     - Incorrect schedule information (changed schedules)

2. **Recommended Join Strategies**

   a. **Primary Approach: Use Flight Data as Source of Truth**
   - Rely on operational details within the flights record
   - Use for analysis of specific flight instances
   - Most accurate for historical analysis

   b. **Static Joins (Recommended)**
   - Join flights to airlines and airports tables
   - Provides stable enrichment data
   - Example joins:
     ```sql
     flights.airline_iata -> airlines.iata_code
     flights.departure_iata -> airports.iata_code
     flights.arrival_iata -> airports.iata_code
     ```

   c. **Current Context Joins (Use with Caution)**
   - LEFT JOIN flights to routes using current snapshot
   - Useful for understanding current route status
   - Must be interpreted carefully
   - High chance of nulls for older flights

3. **Best Practices**
   - Use flights data as primary source for historical analysis
   - Join to static reference data (airports, airlines) for enrichment
   - Be cautious when joining to routes data
   - Consider the temporal context of your analysis
   - Document any assumptions about data relationships

### Example Use Cases

1. **Historical Flight Analysis**
   - Use flights data directly
   - Join to airports/airlines for context
   - Avoid routes joins unless specifically needed

2. **Current Route Status**
   - Use routes data directly
   - Join to airports/airlines for context
   - Consider date_pulled for data freshness

3. **Route Evolution Analysis**
   - Would require historical routes snapshots
   - Complex to implement
   - Consider if this level of detail is necessary

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
