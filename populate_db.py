import csv
from datetime import datetime
import logging
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Assuming your models and db session setup are in src
# Adjust the import path if your structure is different
try:
    # Import models and db setup from src.models
    from src.models import Base, Empresas, CotizacionXEmpresa, Usuario, TipoUsuario, NotasXUsuario, SessionLocal, engine, init_db
except ImportError as e:
    print(f"Error importing database modules from src.models: {e}")
    print("Please ensure src/models.py exists and contains SessionLocal, engine, and init_db.")
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


def populate_tipo_usuarios(db: Session):
    """
    Populates the tipo_usuarios table with predefined data.
    """
    logging.info("Populating tipo_usuarios table...")
    tipo_usuarios_data = [
        {"cod_tipo_usuario": "news_ext", "descripcion": "News External"},
        {"cod_tipo_usuario": "news_local", "descripcion": "News Local"},
        {"cod_tipo_usuario": "others", "descripcion": "Others"},
        {"cod_tipo_usuario": "influencers", "descripcion": "Influencers"},
    ]

    try:
        for tipo in tipo_usuarios_data:
            existing_tipo = db.query(TipoUsuario).filter_by(cod_tipo_usuario=tipo["cod_tipo_usuario"]).first()
            if not existing_tipo:
                new_tipo = TipoUsuario(
                    cod_tipo_usuario=tipo["cod_tipo_usuario"],
                    descripcion=tipo["descripcion"]
                )
                db.add(new_tipo)
                logging.info(f"Added tipo_usuario: {tipo['cod_tipo_usuario']} - {tipo['descripcion']}")
        db.commit()
        logging.info("tipo_usuarios table populated successfully.")
    except Exception as e:
        db.rollback()
        logging.error(f"An error occurred while populating tipo_usuarios: {e}", exc_info=True)


def populate_users_from_csv(db: Session, csv_file_path: str):
    """
    Reads user data from a CSV file and populates the Usuarios table in the database.

    Args:
        db: The SQLAlchemy database session.
        csv_file_path: The path to the user data CSV file.
    """
    logging.info(f"Starting user population from {csv_file_path}...")
    processed_rows = 0
    added_users = 0
    skipped_rows = 0

    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            # Ensure expected columns are present
            expected_columns = ['id_usuario', 'nombre', 'handle', 'cod_tipo_usuario', 'verificado',
                                'seguidores', 'cod_pais', 'idioma_principal', 'Score_credibilidad']
            if not all(col in reader.fieldnames for col in expected_columns):
                missing = [col for col in expected_columns if col not in reader.fieldnames]
                logging.error(f"CSV file is missing required columns: {missing}")
                return

            for row in reader:
                processed_rows += 1
                try:
                    # Parse and validate data
                    id_usuario = row.get('id_usuario')
                    nombre = row.get('nombre')
                    handle = row.get('handle')
                    cod_tipo_usuario = row.get('cod_tipo_usuario') or 'unknown'  # Asegurar que sea texto
                    verificado = row.get('verificado') == 'True'
                    seguidores = int(row['seguidores']) if row.get('seguidores') else 0
                    cod_pais = row.get('cod_pais') or None  # Reemplazar valores vacíos con None
                    if cod_pais == '':
                        cod_pais = None  # Asegurar que los valores vacíos sean tratados como nulos
                    idioma_principal = row.get('idioma_principal') or 'unknown'
                    score_credibilidad = (
                        float(row['Score_credibilidad']) if row.get('Score_credibilidad') else 0.0
                    )

                    # Skip rows with missing mandatory fields
                    if not id_usuario or not nombre or not handle:
                        logging.warning(f"Skipping row {processed_rows}: Missing mandatory fields.")
                        skipped_rows += 1
                        continue

                    # Check if the user already exists
                    existing_user = db.query(Usuario).filter_by(id_usuario=id_usuario).first()
                    if existing_user:
                        logging.info(f"User {id_usuario} already exists. Skipping.")
                        skipped_rows += 1
                        continue

                    # Create a new Usuario object
                    new_user = Usuario(
                        id_usuario=id_usuario,
                        nombre=nombre,
                        handle=handle,
                        cod_tipo_usuario=cod_tipo_usuario,
                        verificado=verificado,
                        seguidores=seguidores,
                        cod_pais=cod_pais,  # Usar None para valores nulos
                        idioma_principal=idioma_principal,
                        score_credibilidad=score_credibilidad,
                    )
                    db.add(new_user)
                    added_users += 1

                except Exception as e:
                    logging.error(f"Error processing row {processed_rows}: {e}. Row: {row}", exc_info=True)
                    skipped_rows += 1
                    continue

            # Commit changes to the database
            logging.info("Committing user data to the database...")
            db.commit()
            logging.info("User data commit successful.")

    except FileNotFoundError:
        logging.error(f"Error: CSV file not found at {csv_file_path}")
    except Exception as e:
        db.rollback()
        logging.error(f"An unexpected error occurred: {e}. Changes rolled back.", exc_info=True)
    finally:
        logging.info(f"User population finished. Processed: {processed_rows}, Added: {added_users}, Skipped: {skipped_rows}")


def populate_posts_with_sentiment(db: Session, csv_file_path: str):
    """
    Reads post data with sentiment analysis from a CSV file and populates the notas_x_usuario table.

    Args:
        db: The SQLAlchemy database session.
        csv_file_path: The path to the posts with sentiment CSV file.
    """
    logging.info(f"Starting post population from {csv_file_path}...")
    processed_rows = 0
    added_posts = 0
    skipped_rows = 0

    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)

            # Ensure expected columns are present
            expected_columns = [
                'actor_handle', 'uri', 'text', 'created_at', 'likes', 'reposts', 'replies',
                'engagement', 'sentiment', 'sentiment_vader', 'interpretacion_sentimiento'
            ]
            if not all(col in reader.fieldnames for col in expected_columns):
                missing = [col for col in expected_columns if col not in reader.fieldnames]
                logging.error(f"CSV file is missing required columns: {missing}")
                return

            for row in reader:
                processed_rows += 1
                try:
                    # Parse and validate data
                    actor_handle = row.get('actor_handle')
                    uri = row.get('uri')
                    text = row.get('text')
                    created_at = row.get('created_at')
                    likes = int(row['likes']) if row.get('likes') else 0
                    reposts = int(row['reposts']) if row.get('reposts') else 0
                    replies = int(row['replies']) if row.get('replies') else 0
                    engagement = int(row['engagement']) if row.get('engagement') else 0
                    sentiment = row.get('sentiment')
                    sentiment_vader = float(row['sentiment_vader']) if row.get('sentiment_vader') else None
                    interpretacion_sentimiento = row.get('interpretacion_sentimiento')

                    # Convert created_at to datetime
                    try:
                        fecha_publicacion = datetime.fromisoformat(created_at)
                    except ValueError as e:
                        logging.warning(f"Skipping row {processed_rows}: Invalid date format - {e}. Row: {row}")
                        skipped_rows += 1
                        continue

                    # Find the user associated with the actor_handle
                    usuario = db.query(Usuario).filter_by(handle=actor_handle).first()
                    if not usuario:
                        logging.warning(f"Skipping row {processed_rows}: No user found for handle {actor_handle}.")
                        skipped_rows += 1
                        continue

                    # Create a new NotasXUsuario object
                    new_post = NotasXUsuario(
                        contenido=text,
                        fecha_publicacion=fecha_publicacion,
                        id_usuario=usuario.id_usuario,
                        cod_tipo_nota=1,  # Assuming 1 corresponds to "texto" in tipo_notas
                        url_nota=uri,
                        engagement_total=engagement,
                        score_analisis_sentimiento_nlp=sentiment_vader,
                        sentimiento=interpretacion_sentimiento,
                        score_sentimiento=sentiment_vader
                    )
                    db.add(new_post)
                    added_posts += 1

                except Exception as e:
                    logging.error(f"Error processing row {processed_rows}: {e}. Row: {row}", exc_info=True)
                    skipped_rows += 1
                    continue

            # Commit changes to the database
            logging.info("Committing post data to the database...")
            db.commit()
            logging.info("Post data commit successful.")

    except FileNotFoundError:
        logging.error(f"Error: CSV file not found at {csv_file_path}")
    except Exception as e:
        db.rollback()
        logging.error(f"An unexpected error occurred: {e}. Changes rolled back.", exc_info=True)
    finally:
        logging.info(f"Post population finished. Processed: {processed_rows}, Added: {added_posts}, Skipped: {skipped_rows}")


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

    # Define the path to the usuarios_bluesky.csv file
    USERS_CSV_FILE = "fede/usuarios_bluesky.csv"

    # Populate tipo_usuarios before loading users
    logging.info("Populating tipo_usuarios...")
    db = SessionLocal()
    try:
        populate_tipo_usuarios(db)
    finally:
        db.close()
        logging.info("Database session closed.")

    # Populate users from the CSV file
    logging.info("Populating users from CSV...")
    db = SessionLocal()
    try:
        populate_users_from_csv(db, USERS_CSV_FILE)
    finally:
        db.close()
        logging.info("Database session closed.")

    # Define the path to the posts_con_sentimiento.csv file
    POSTS_CSV_FILE = "franco/API-connect/posts_con_sentimiento.csv"

    # Populate posts with sentiment data
    logging.info("Populating posts with sentiment data...")
    db = SessionLocal()
    try:
        populate_posts_with_sentiment(db, POSTS_CSV_FILE)
    finally:
        db.close()
        logging.info("Database session closed.")
