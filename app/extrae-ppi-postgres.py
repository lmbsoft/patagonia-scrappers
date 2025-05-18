#!/usr/bin/env python3
import os
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import func
from sqlalchemy.orm import Session
from ppi_client.ppi import PPI
from ppi_client.api.constants import (
    ACCOUNTDATA_TYPE_ACCOUNT_NOTIFICATION,
    ACCOUNTDATA_TYPE_PUSH_NOTIFICATION,
    ACCOUNTDATA_TYPE_ORDER_NOTIFICATION
)
from models import SessionLocal, TablaPPI

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extrae_ppi_postgres.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Claves de acceso a la API (deberías considerar mover esto a variables de entorno en producción)
KEY_PUBLICA = os.environ.get("PPI_KEY_PUBLICA")
KEY_PRIVADA = os.environ.get("PPI_KEY_PRIVADA")

# Constantes
DEFAULT_START_DATE = datetime(2025, 1, 1)
INSTRUMENT_TYPE = "CEDEARS"
SETTLEMENTS = "A-24HS"
TICKERS = ["TEND", "AAPLD", "VISTD", "DESPD", "MELID", "XOMD", "NVDAD", "MSFTD", "KOD"]


def parse_args():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description="Extrae datos de la API PPI y los guarda en PostgreSQL")
    parser.add_argument(
        "--desde", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help="Fecha desde la cual extraer datos (YYYY-MM-DD). Por defecto: última fecha en BD o 2025-01-01"
    )
    parser.add_argument(
        "--hasta", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help="Fecha hasta la cual extraer datos (YYYY-MM-DD). Por defecto: fecha actual"
    )
    return parser.parse_args()


def get_latest_date_from_db():
    """Obtiene la última fecha disponible en la base de datos"""
    try:
        with SessionLocal() as db:
            latest_date = db.query(func.max(TablaPPI.Date)).scalar()
            
            if latest_date:
                # Sumamos un día para no duplicar datos
                next_date = latest_date + timedelta(days=1)
                logger.info(f"Última fecha en la base de datos: {latest_date.strftime('%Y-%m-%d')}")
                return next_date
    
    except Exception as e:
        logger.error(f"Error al consultar la última fecha en la base de datos: {e}")
    
    # Si hay algún problema o no hay datos, usa la fecha por defecto
    return DEFAULT_START_DATE


def authenticate_ppi():
    """Autenticación con la API de PPI"""
    try:
        ppi = PPI(sandbox=False)
        ppi.account.login_api(KEY_PUBLICA, KEY_PRIVADA)
        logger.info("Autenticación exitosa con la API de PPI")
        return ppi
    except Exception as e:
        logger.error(f"Error de autenticación con la API de PPI: {e}")
        raise


def extract_ticker_data(ppi, ticker, date_from, date_to):
    """Extrae datos históricos para un ticker específico"""
    logger.info(f"Extrayendo datos para {ticker} desde {date_from.strftime('%Y-%m-%d')} hasta {date_to.strftime('%Y-%m-%d')}")
    
    try:
        market_data = ppi.marketdata.search(
            ticker=ticker,
            instrument_type=INSTRUMENT_TYPE,
            settlement=SETTLEMENTS,
            date_from=date_from,
            date_to=date_to
        )
        
        data = []
        for ins in market_data:
            data.append({
                'Date': ins['date'],
                'Price': ins['price'],
                'Volume': ins['volume'],
                'Opening': ins['openingPrice'],
                'Min': ins['min'],
                'Max': ins['max'],
                'ticker': ticker,
                'settlement': SETTLEMENTS,
                'instrument_type': INSTRUMENT_TYPE,
                'currency': "USD"
            })
        
        if data:
            logger.info(f"Se encontraron {len(data)} registros para {ticker}")
            return pd.DataFrame(data)
        else:
            logger.info(f"No se encontraron datos para {ticker} en el rango de fechas especificado")
            return None
    
    except Exception as e:
        logger.error(f"Error al extraer datos para {ticker}: {e}")
        return None


def process_and_save_to_db(df):
    """Procesa y guarda los datos en la base de datos PostgreSQL"""
    if df is None or df.empty:
        logger.info("No hay nuevos datos para guardar")
        return 0
    
    try:
        # Convertir fechas
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values(by='Date')
        
        # Iniciar sesión de base de datos
        with SessionLocal() as db:
            # Calcular variación diaria por ticker
            for ticker in df['ticker'].unique():
                ticker_mask = df['ticker'] == ticker
                df.loc[ticker_mask, 'variacion_diaria'] = df.loc[ticker_mask, 'Price'].pct_change() * 100
            
            # Verificar registros existentes para evitar duplicados
            records_added = 0
            
            for _, row in df.iterrows():
                # Verificar si ya existe un registro con la misma fecha y ticker
                existing = db.query(TablaPPI).filter(
                    func.date(TablaPPI.Date) == func.date(row['Date']),
                    TablaPPI.ticker == row['ticker']
                ).first()
                
                if not existing:
                    # Crear nuevo registro en la base de datos
                    new_record = TablaPPI(
                        Date=row['Date'],
                        Price=row['Price'],
                        Volume=row['Volume'],
                        Opening=row['Opening'],
                        Min=row['Min'],
                        Max=row['Max'],
                        ticker=row['ticker'],
                        settlement=row['settlement'],
                        instrument_type=row['instrument_type'],
                        currency=row['currency'],
                        variacion_diaria=row['variacion_diaria']
                    )
                    db.add(new_record)
                    records_added += 1
            
            # Commit de los cambios a la base de datos
            db.commit()
            
            logger.info(f"{records_added} registros agregados a la tabla tabla_ppi")
            return records_added
            
    except Exception as e:
        logger.error(f"Error al guardar datos en la base de datos: {e}")
        return 0


def main():
    """Función principal del script"""
    args = parse_args()
    
    # Determinar fechas de inicio y fin
    date_to = args.hasta if args.hasta else datetime.now()
    date_from = args.desde if args.desde else get_latest_date_from_db()

    # Validar rango de fechas
    if date_from > date_to:
        logger.warning(f"La fecha inicial ({date_from.strftime('%Y-%m-%d')}) es posterior a la fecha final ({date_to.strftime('%Y-%m-%d')})")
        logger.info("No hay nuevos datos para extraer.")
        return    
    
    logger.info(f"Iniciando extracción de datos desde {date_from.strftime('%Y-%m-%d')} hasta {date_to.strftime('%Y-%m-%d')}")
    
    try:
        # Autenticarse con la API
        ppi = authenticate_ppi()
        
        # Inicializar DataFrame combinado
        all_data = pd.DataFrame()
        
        # Extraer datos para cada ticker
        for ticker in TICKERS:
            df = extract_ticker_data(ppi, ticker, date_from, date_to)
            if df is not None and not df.empty:
                all_data = pd.concat([all_data, df])
        
        # Guardar resultados en la base de datos
        records_added = process_and_save_to_db(all_data)
        logger.info(f"Extracción finalizada: {records_added} registros nuevos agregados a la base de datos")
    
    except Exception as e:
        logger.error(f"Error durante la ejecución: {e}")


if __name__ == "__main__":
    main()