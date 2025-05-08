from sqlalchemy import Column, Integer, String, Date, DateTime, Float, Boolean, UniqueConstraint, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy import func 


Base = declarative_base()

class Flight(Base):
    __tablename__ = "flights"

    # Synthetic primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Flight info
    flight_date          = Column(Date, nullable=False)  
    flight_status        = Column(String(50), nullable=True)  # scheduled, active, landed, cancelled, incident, diverted
    # Departure info
    departure_airport    = Column(String(255), nullable=True)
    departure_timezone   = Column(String(50), nullable=True)
    departure_iata       = Column(String(8), nullable=True)
    departure_icao       = Column(String(8), nullable=True)
    departure_terminal   = Column(String(50), nullable=True)
    departure_gate       = Column(String(20), nullable=True)
    departure_delay      = Column(Integer, nullable=True)
    departure_scheduled  = Column(DateTime, nullable=True)
    departure_estimated  = Column(DateTime, nullable=True)
    departure_actual     = Column(DateTime, nullable=True)
    departure_estimated_runway = Column(DateTime, nullable=True)
    departure_actual_runway = Column(DateTime, nullable=True)
    # Arrival info
    arrival_airport      = Column(String(255), nullable=True)
    arrival_timezone     = Column(String(50), nullable=True)
    arrival_iata         = Column(String(8), nullable=True)
    arrival_icao         = Column(String(8), nullable=True)
    arrival_terminal     = Column(String(50), nullable=True)
    arrival_gate         = Column(String(20), nullable=True)
    arrival_baggage      = Column(String(20), nullable=True)
    arrival_delay        = Column(Integer, nullable=True)
    arrival_scheduled    = Column(DateTime, nullable=True)
    arrival_estimated    = Column(DateTime, nullable=True)
    arrival_actual       = Column(DateTime, nullable=True)
    arrival_estimated_runway = Column(DateTime, nullable=True)
    arrival_actual_runway = Column(DateTime, nullable=True)
    # Airline info
    airline_name         = Column(String(255), nullable=True)
    airline_iata         = Column(String(8), nullable=True)
    airline_icao         = Column(String(8), nullable=True)
    
    # Flight info
    flight_number        = Column(String(10), nullable=True)
    flight_iata          = Column(String(10), nullable=True)
    flight_icao          = Column(String(10), nullable=True)
    codeshared           = Column(JSONB, nullable=True)  # This can contain nested info
    
    # Aircraft info
    aircraft_registration = Column(String(20), nullable=True)
    aircraft_iata        = Column(String(10), nullable=True)
    aircraft_icao        = Column(String(10), nullable=True)
    aircraft_icao24      = Column(String(10), nullable=True)

    # Add the raw_payload column
    raw_payload          = Column(JSONB, nullable=True) # Allow null initially if needed, or set nullable=False

    # Add UNIQUE constraint
    __table_args__ = (
        UniqueConstraint('flight_date', 'flight_iata', 'departure_iata', 'arrival_iata', 
                        'departure_scheduled', name='uix_flight_identity'),
    )

class Airline(Base):
    __tablename__ = "airlines"
    
    # Primary key - Changed from iata_code to API's id field
    id                   = Column(String(10), primary_key=True)  # Another ID field in the response, now PK
    
    # Original primary key, now just a regular column (can be null)
    iata_code            = Column(String(8), nullable=True)

    # Identifier fields
    airline_id           = Column(String(10), nullable=True)  # API-specific ID
    # id                   = Column(String(10), nullable=True)  # Removed this duplicate definition
    icao_code            = Column(String(8), nullable=True)
    iata_prefix_accounting = Column(String(10), nullable=True)
    
    # Airline details
    airline_name         = Column(String(200), nullable=True)
    callsign             = Column(String(50), nullable=True)
    country_name         = Column(String(100), nullable=True)
    country_iso2         = Column(String(10), nullable=True)
    date_founded         = Column(Integer, nullable=True)  # Year as integer
    hub_code             = Column(String(8), nullable=True)  # Airport IATA code
    
    # Fleet information
    fleet_size           = Column(Integer, nullable=True)
    fleet_average_age    = Column(Float, nullable=True)
    
    # Status information
    status               = Column(String(50), nullable=True) 
    type                 = Column(String(50), nullable=True)  
    
    # Store the complete JSON response
    raw_payload          = Column(JSONB, nullable=True)

    # Optionally add unique constraints if needed for iata_code/icao_code where they exist
    # __table_args__ = (
    #     UniqueConstraint('iata_code', name='uq_airline_iata_code'),
    #     UniqueConstraint('icao_code', name='uq_airline_icao_code'),
    # )

class Airport(Base):
    __tablename__ = "airports"
    
    # Primary key
    iata_code            = Column(String(8), primary_key=True)
    
    # Airport details
    airport_name         = Column(String(200), nullable=True)
    icao_code            = Column(String(8), nullable=True)
    
    # Location information
    latitude             = Column(Float, nullable=True)
    longitude            = Column(Float, nullable=True)
    geoname_id           = Column(String(10), nullable=True)
    
    # Regional information
    city_iata_code       = Column(String(8), nullable=True)
    country_name         = Column(String(100), nullable=True)
    country_iso2         = Column(String(10), nullable=True)
    
    # Time information
    timezone             = Column(String(50), nullable=True)
    gmt                  = Column(String(10), nullable=True)  # GMT offset
    
    # Contact information
    phone_number         = Column(String(50), nullable=True)
    
    # Store the complete JSON response
    raw_payload          = Column(JSONB, nullable=True)

class Route(Base):
    __tablename__ = "routes"
    
    # Synthetic primary key
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Route identifiers
    airline_iata         = Column(String(4), nullable=True)
    flight_number        = Column(String(6), nullable=True)
    departure_iata       = Column(String(4), nullable=True)
    arrival_iata         = Column(String(4), nullable=True)
    
    # Departure info
    departure_airport    = Column(String(200), nullable=True)
    departure_timezone   = Column(String(50), nullable=True)
    departure_icao       = Column(String(8), nullable=True)
    departure_terminal   = Column(String(50), nullable=True)
    departure_time       = Column(String(8), nullable=True)
    
    # Arrival info
    arrival_airport      = Column(String(200), nullable=True)
    arrival_timezone     = Column(String(50), nullable=True)
    arrival_icao         = Column(String(8), nullable=True)
    arrival_terminal     = Column(String(50), nullable=True)
    arrival_time         = Column(String(8), nullable=True)
    
    # Airline info
    airline_name         = Column(String(200), nullable=True)
    airline_callsign     = Column(String(50), nullable=True)
    airline_icao         = Column(String(8), nullable=True)
    
    # Store the complete JSON response
    raw_payload          = Column(JSONB, nullable=True)

    # Add a column to store when the record was pulled/updated
    date_pulled = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    
    # Create a unique constraint on the natural key components
    __table_args__ = (
        UniqueConstraint('airline_iata', 'flight_number', 'departure_iata', 'arrival_iata', 
                        name='uix_route_identity'),
    )

# Create tables if running this file directly
if __name__ == "__main__":
    from sqlalchemy import create_engine
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    DB_URL = os.getenv("DATABASE_URL")
    engine = create_engine(DB_URL)
    Base.metadata.create_all(engine)
    print("✅ All tables created!")