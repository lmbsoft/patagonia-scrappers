import pytest
import csv
from io import StringIO
from datetime import datetime
from unittest.mock import patch, mock_open

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

# Adjust imports based on your project structure
# Assuming populate_db.py is at the root and models/database are in src
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from populate_db import populate_market_data
# Import db setup from src.models
from src.models import Base, Empresas, CotizacionXEmpresa, SessionLocal, engine, init_db as original_init_db # Rename to avoid conflict

# Use an in-memory SQLite database for testing
TEST_DATABASE_URL = "sqlite:///:memory:"

@pytest.fixture(scope="function")
def db_session() -> Session:
    """Fixture to set up an in-memory SQLite database for each test function."""
    engine = create_engine(TEST_DATABASE_URL)
    TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    # Create tables
    Base.metadata.create_all(bind=engine)
    
    db = TestingSessionLocal()
    
    yield db
    
    # Clean up after test
    db.close()
    Base.metadata.drop_all(bind=engine)
    engine.dispose()

@pytest.fixture
def mock_csv_data_valid():
    """Provides valid mock CSV data as a string."""
    csv_content = """Date,Price,Volume,Opening,Min,Max,ticker,settlement,instrument_type
2023-01-02T00:00:00-03:00,12690.0,2954318.0,12450.0,12100.0,12763.0,TESTA,A-24HS,CEDEARS
2023-01-03T00:00:00-03:00,11661.5,144802144.0,11979.5,11410.0,12056.5,TESTB,A-24HS,CEDEARS
2023-01-02T00:00:00-03:00,500.0,10000.0,490.0,480.0,510.0,TESTB,A-24HS,CEDEARS
"""
    return csv_content

@pytest.fixture
def mock_csv_data_invalid_row():
    """Provides mock CSV data with one invalid numeric value."""
    csv_content = """Date,Price,Volume,Opening,Min,Max,ticker,settlement,instrument_type
2023-01-04T00:00:00-03:00,10952.5,69833248.0,11200.0,10840.0,11263.0,TESTC,A-24HS,CEDEARS
2023-01-05T00:00:00-03:00,invalid,98606872.0,11080.0,11080.0,11245.5,TESTD,A-24HS,CEDEARS
2023-01-06T00:00:00-03:00,11258.0,100996304.0,11350.0,11210.0,11435.5,TESTC,A-24HS,CEDEARS
"""
    return csv_content

@pytest.fixture
def mock_csv_data_missing_column():
    """Provides mock CSV data missing the 'Price' column."""
    csv_content = """Date,Volume,Opening,Min,Max,ticker,settlement,instrument_type
2023-01-04T00:00:00-03:00,69833248.0,11200.0,10840.0,11263.0,TESTC,A-24HS,CEDEARS
"""
    return csv_content

# --- Test Cases ---

def test_populate_market_data_success(db_session: Session, mock_csv_data_valid):
    """Test successful population with valid data."""
    mock_file = StringIO(mock_csv_data_valid)
    
    # Use patch to mock the open function
    with patch('builtins.open', mock_open(read_data=mock_csv_data_valid)) as mocked_file:
        populate_market_data(db_session, "dummy_path.csv")
        
    # Assertions
    empresas = db_session.query(Empresas).order_by(Empresas.ticker).all()
    assert len(empresas) == 2
    assert empresas[0].ticker == "TESTA"
    assert empresas[1].ticker == "TESTB"
    
    cotizaciones = db_session.query(CotizacionXEmpresa).order_by(CotizacionXEmpresa.id_empresa, CotizacionXEmpresa.fecha).all()
    assert len(cotizaciones) == 3
    
    # Check TESTA cotizacion
    assert cotizaciones[0].id_empresa == empresas[0].id_empresa
    assert cotizaciones[0].fecha == datetime.fromisoformat("2023-01-02T00:00:00-03:00")
    assert cotizaciones[0].precio_cierre == 12690.0
    assert cotizaciones[0].volumen_operado == 2954318.0
    
    # Check TESTB cotizaciones
    assert cotizaciones[1].id_empresa == empresas[1].id_empresa
    assert cotizaciones[1].fecha == datetime.fromisoformat("2023-01-02T00:00:00-03:00")
    assert cotizaciones[1].precio_cierre == 500.0
    
    assert cotizaciones[2].id_empresa == empresas[1].id_empresa
    assert cotizaciones[2].fecha == datetime.fromisoformat("2023-01-03T00:00:00-03:00")
    assert cotizaciones[2].precio_cierre == 11661.5

def test_populate_market_data_with_existing_empresas(db_session: Session, mock_csv_data_valid):
    """Test population when some Empresas already exist."""
    # Pre-populate an existing empresa
    existing_empresa = Empresas(ticker="TESTA", nombre="Existing Test A")
    db_session.add(existing_empresa)
    db_session.commit()
    
    existing_id = existing_empresa.id_empresa
    assert existing_id is not None

    # Use patch to mock the open function
    with patch('builtins.open', mock_open(read_data=mock_csv_data_valid)) as mocked_file:
         populate_market_data(db_session, "dummy_path.csv")

    # Assertions
    empresas = db_session.query(Empresas).order_by(Empresas.ticker).all()
    assert len(empresas) == 2 # Should only add TESTB
    assert empresas[0].ticker == "TESTA"
    assert empresas[0].id_empresa == existing_id # Verify it used the existing one
    assert empresas[0].nombre == "Existing Test A" # Verify name wasn't overwritten
    assert empresas[1].ticker == "TESTB"

    cotizaciones = db_session.query(CotizacionXEmpresa).order_by(CotizacionXEmpresa.id_empresa, CotizacionXEmpresa.fecha).all()
    assert len(cotizaciones) == 3
    
    # Check that TESTA cotizacion is linked to the existing empresa
    assert cotizaciones[0].id_empresa == existing_id

def test_populate_market_data_invalid_row_skipped(db_session: Session, mock_csv_data_invalid_row, caplog):
    """Test that rows with invalid data are skipped and logged."""
    with patch('builtins.open', mock_open(read_data=mock_csv_data_invalid_row)) as mocked_file:
        populate_market_data(db_session, "dummy_path.csv")

    # Assertions
    empresas = db_session.query(Empresas).all()
    assert len(empresas) == 1 # Only TESTC should be added
    assert empresas[0].ticker == "TESTC"

    cotizaciones = db_session.query(CotizacionXEmpresa).all()
    assert len(cotizaciones) == 2 # Only the two valid rows for TESTC

    # Check logs for warning about skipped row
    assert "Skipping row 2 for ticker TESTD: Error parsing data" in caplog.text
    assert "invalid literal for float()" in caplog.text # Check specific error if possible

def test_populate_market_data_missing_column(db_session: Session, mock_csv_data_missing_column, caplog):
    """Test behavior when CSV is missing a required column."""
    with patch('builtins.open', mock_open(read_data=mock_csv_data_missing_column)) as mocked_file:
        populate_market_data(db_session, "dummy_path.csv")

    # Assertions
    empresas = db_session.query(Empresas).all()
    assert len(empresas) == 0 # No empresas should be added
    cotizaciones = db_session.query(CotizacionXEmpresa).all()
    assert len(cotizaciones) == 0 # No cotizaciones should be added

    # Check logs for error about missing columns
    assert "CSV file is missing required columns: ['Price']" in caplog.text

def test_populate_market_data_file_not_found(db_session: Session, caplog):
    """Test behavior when the CSV file does not exist."""
    # No need to mock open, let it raise FileNotFoundError
    populate_market_data(db_session, "non_existent_file.csv")
    
    # Assertions
    assert "Error: CSV file not found at non_existent_file.csv" in caplog.text
    
    # Ensure no data was added
    empresas = db_session.query(Empresas).all()
    assert len(empresas) == 0
    cotizaciones = db_session.query(CotizacionXEmpresa).all()
    assert len(cotizaciones) == 0

def test_populate_market_data_empty_csv(db_session: Session, caplog):
    """Test behavior with an empty CSV file (only headers)."""
    # Correctly define the multi-line string with headers and newline
    empty_csv = """Date,Price,Volume,Opening,Min,Max,ticker,settlement,instrument_type\n"""
    with patch('builtins.open', mock_open(read_data=empty_csv)) as mocked_file:
        populate_market_data(db_session, "empty.csv")

    # Assertions
    empresas = db_session.query(Empresas).all()
    assert len(empresas) == 0
    cotizaciones = db_session.query(CotizacionXEmpresa).all()
    assert len(cotizaciones) == 0
    
    # Check the final log message counts
    assert "Processed: 0, Added Cotizaciones: 0, Skipped: 0" in caplog.text

# Add more tests if needed, e.g., for boundary conditions, specific data formats, etc.
# (Ensure no extra characters or incorrect indentation below this line) 