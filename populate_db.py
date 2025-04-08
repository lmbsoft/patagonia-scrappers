import csv
from datetime import datetime
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Assuming your models and db session setup are in src
# Adjust the import path if your structure is different
try:
    from src.models import Base, Empresas, CotizacionXEmpresa
    from src.database import SessionLocal, engine, init_db
except ImportError as e:
    print(f"Error importing database modules: {e}")
    print("Please ensure src/models.py and src/database.py exist and are correctly structured.")
    exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def populate_market_data(db: Session, csv_file_path: str):
    """
    Reads market data from a CSV file and populates the Empresas and
    CotizacionXEmpresa tables in the database.

    Args:
        db: The SQLAlchemy database session.
        csv_file_path: The path to the market data CSV file.
    """
    logging.info(f"Starting database population from {csv_file_path}...")
    empresas_cache = {} # Cache to avoid repeated DB lookups for tickers
    processed_rows = 0
    added_cotizaciones = 0
    skipped_rows = 0

    try:
        # Pre-load existing tickers to optimize lookups
        existing_empresas = db.query(Empresas).all()
        for emp in existing_empresas:
            if emp.ticker:
                empresas_cache[emp.ticker] = emp.id_empresa
        logging.info(f"Loaded {len(empresas_cache)} existing companies into cache.")

        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            
            # Ensure expected columns are present
            expected_columns = ['Date', 'Price', 'Volume', 'Opening', 'Min', 'Max', 'ticker']
            if not all(col in reader.fieldnames for col in expected_columns):
                missing = [col for col in expected_columns if col not in reader.fieldnames]
                logging.error(f"CSV file is missing required columns: {missing}")
                return

            cotizaciones_to_add = []
            empresas_to_add = {} # Keep track of new empresas to add in batch

            for row in reader:
                processed_rows += 1
                try:
                    ticker = row.get('ticker')
                    date_str = row.get('Date')
                    
                    # Basic validation
                    if not ticker or not date_str:
                        logging.warning(f"Skipping row {processed_rows}: Missing ticker or date.")
                        skipped_rows += 1
                        continue

                    # Get or create Empresa
                    id_empresa = empresas_cache.get(ticker)
                    if not id_empresa:
                        # Check if we already marked this ticker for addition
                        if ticker not in empresas_to_add:
                             # Create new Empresa object but don't add to session yet
                            new_empresa = Empresas(
                                nombre=ticker,  # Use ticker as name if no other name available
                                ticker=ticker,
                                # Add default/None values for other fields if needed
                                # sector=None,
                                # capitalizacion_bursatil=None
                            )
                            empresas_to_add[ticker] = new_empresa
                            logging.info(f"Marked new company for addition: {ticker}")
                        # We'll get the ID after committing new empresas

                    # Parse date and numeric values safely
                    try:
                        fecha = datetime.fromisoformat(date_str)
                        precio_cierre = float(row['Price']) if row.get('Price') else None
                        volumen = float(row['Volume']) if row.get('Volume') else None
                        precio_apertura = float(row['Opening']) if row.get('Opening') else None
                        precio_min = float(row['Min']) if row.get('Min') else None
                        precio_max = float(row['Max']) if row.get('Max') else None
                    except (ValueError, TypeError) as e:
                        logging.warning(f"Skipping row {processed_rows} for ticker {ticker}: Error parsing data - {e}. Row: {row}")
                        skipped_rows += 1
                        continue
                        
                    # Create Cotizacion object but don't add to session yet
                    cotizacion = CotizacionXEmpresa(
                        # id_empresa will be set later
                        fecha=fecha,
                        precio_apertura=precio_apertura,
                        precio_cierre=precio_cierre,
                        precio_max=precio_max,
                        precio_min=precio_min,
                        volumen_operado=volumen,
                        # Calculate variacion_porcentaje if needed, otherwise None
                        # variacion_porcentaje=None 
                    )
                    # Store cotizacion temporarily with its ticker
                    cotizaciones_to_add.append({'ticker': ticker, 'cotizacion_obj': cotizacion})
                    
                    # Batch add empresas to the session periodically or at the end
                    if len(empresas_to_add) >= 100: # Commit new companies in batches of 100
                         logging.info(f"Adding batch of {len(empresas_to_add)} new companies...")
                         db.add_all(empresas_to_add.values())
                         db.flush() # Flush to assign IDs
                         for ticker, emp in empresas_to_add.items():
                             empresas_cache[ticker] = emp.id_empresa # Update cache with new IDs
                         empresas_to_add.clear()
                         logging.info("Batch added.")
                         
                except Exception as e:
                    logging.error(f"Error processing row {processed_rows}: {e}. Row: {row}", exc_info=True)
                    skipped_rows += 1
                    continue # Skip to the next row on unexpected error

            # Add any remaining new empresas
            if empresas_to_add:
                logging.info(f"Adding final batch of {len(empresas_to_add)} new companies...")
                db.add_all(empresas_to_add.values())
                db.flush() # Flush to assign IDs
                for ticker, emp in empresas_to_add.items():
                    empresas_cache[ticker] = emp.id_empresa # Update cache
                logging.info("Final batch added.")

            # Now add all cotizaciones, linking them to the correct empresa ID
            final_cotizaciones_list = []
            for item in cotizaciones_to_add:
                ticker = item['ticker']
                cotizacion = item['cotizacion_obj']
                id_empresa = empresas_cache.get(ticker)
                if id_empresa:
                    cotizacion.id_empresa = id_empresa
                    final_cotizaciones_list.append(cotizacion)
                    added_cotizaciones += 1
                else:
                    # This case should ideally not happen if logic is correct
                    logging.error(f"Could not find or create empresa ID for ticker {ticker}. Skipping cotizacion.")
                    skipped_rows += 1

            logging.info(f"Adding {len(final_cotizaciones_list)} cotizaciones to the database session...")
            db.add_all(final_cotizaciones_list) # Use add_all for potential performance benefit

            logging.info("Committing changes to the database...")
            db.commit()
            logging.info("Database commit successful.")

    except FileNotFoundError:
        logging.error(f"Error: CSV file not found at {csv_file_path}")
    except IntegrityError as e:
        db.rollback()
        logging.error(f"Database integrity error: {e}. Changes rolled back.", exc_info=True)
    except Exception as e:
        db.rollback()
        logging.error(f"An unexpected error occurred: {e}. Changes rolled back.", exc_info=True)
    finally:
        logging.info(f"Database population finished. Processed: {processed_rows}, Added Cotizaciones: {added_cotizaciones}, Skipped: {skipped_rows}")


if __name__ == "__main__":
    # Define the path to your CSV file
    # Assumes the script is run from the project root where 'nico' folder exists
    CSV_FILE = "nico/market_data.csv" 

    logging.info("Initializing database...")
    try:
        # Create tables if they don't exist
        init_db() 
        logging.info("Database initialized successfully.")
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}", exc_info=True)
        exit(1)

    # Create a new database session
    db = SessionLocal()
    try:
        populate_market_data(db, CSV_FILE)
    finally:
        # Ensure the session is closed
        db.close()
        logging.info("Database session closed.") 