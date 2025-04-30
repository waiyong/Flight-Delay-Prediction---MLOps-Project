# Flight Delay Prediction - MLOps Project

A comprehensive end-to-end MLOps project for predicting flight delays using historical aviation data from the AviationStack API.

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
- Data validation and quality checks
- Feature store implementation

### Stage 3: Model Training and Versioning
- Experiment tracking with MLflow
- Model training pipeline
- Hyperparameter optimization
- Model versioning and registry
- Model performance evaluation

### Stage 4: Model Packaging and Serving
- Model packaging with Docker
- REST API for model serving
- Real-time and batch prediction endpoints
- Model A/B testing capabilities

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
- Model performance monitoring
- Data drift detection
- System health metrics
- Logging and alerting
- Dashboards for visualization

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Python 3.10+
- AviationStack API key

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

4. Run the data ingestion script
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

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
