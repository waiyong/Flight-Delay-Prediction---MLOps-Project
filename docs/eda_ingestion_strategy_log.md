# EDA for Optimal Data Ingestion Frequency

**Date:** 2025-05-07 (Based on last conversation context)

**Current Status & Train of Thought:**

*   **Project Goal Context:** The primary project objective is to gain familiarity and hands-on experience with MLOps tools and the end-to-end deployment pipeline for a flight delay prediction system. While data accuracy is valuable, the emphasis for this project phase is on the MLOps lifecycle processes rather than achieving perfect data fidelity.
*   **Challenge Identified - API Costs & Data Freshness:** We've recognized that fetching all flights data daily could lead to excessive API call costs. Similarly, for routes data, daily pulls might be an over-fetch if the underlying data doesn't change with high frequency.
*   **Proposed Approach - Exploratory Data Analysis (EDA):** To make data-driven decisions about the optimal ingestion frequency for both routes and flights data, we plan to conduct targeted EDA. This will help balance the need for fresh data against API usage costs.

    *   **EDA Strategy for Routes Data:**
        *   **Objective:** Quantify how often the set of available flight routes changes.
        *   **Methodology:**
            1.  Execute `run_etl_routes.py` to get an initial snapshot of routes, noting the `date_pulled`.
            2.  After a defined interval (e.g., 1 day, then perhaps a longer period like 3-7 days), re-run `run_etl_routes.py`.
            3.  Analyze changes by comparing record counts for each `date_pulled` and identifying routes present in one pull but not the other. This will help estimate the churn rate (newly added or discontinued routes).

    *   **EDA Strategy for Flights Data:**
        *   **Objective:** Determine how quickly critical flight information (e.g., flight status, actual departure/arrival times, delay information) stabilizes after a flight's scheduled time.
        *   **Methodology (once `run_etl_flights.py` is ready for initial data pulls):**
            1.  Perform an initial data fetch using `run_etl_flights.py` for a specific day or a narrow date range.
            2.  Re-fetch data for the *same specific day(s)* at subsequent intervals (e.g., a few hours later, then the next day).
            3.  Analyze the evolution of individual flight records by comparing fields like `status`, `departure_actual`, `arrival_actual`, `departure_delay`, `arrival_delay` across the different `date_pulled` timestamps for the same flight instance. This will indicate the typical timeframe for data stabilization.

*   **Immediate Next Decision Point:**
    *   Option 1: Proceed with the EDA for routes data first by scheduling and executing the subsequent runs of `run_etl_routes.py`.
    *   Option 2: Prioritize finalizing `run_etl_flights.py` and `scripts/flight_data_processor.py` to a state where initial flights data can be ingested, enabling the flights data EDA.

This structured EDA approach will inform the design of our scheduled data ingestion pipelines. 