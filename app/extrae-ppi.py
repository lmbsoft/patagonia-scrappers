#!/usr/bin/env python3
import pandas as pd
import os
import argparse
import logging
from datetime import datetime, timedelta
from ppi_client.ppi import PPI
from ppi_client.api.constants import (
    ACCOUNTDATA_TYPE_ACCOUNT_NOTIFICATION,
    ACCOUNTDATA_TYPE_PUSH_NOTIFICATION,
    ACCOUNTDATA_TYPE_ORDER_NOTIFICATION
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extrae_ppi.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Claves de acceso a la API (deberías considerar mover esto a variables de entorno en producción)
KEY_PUBLICA = os.environ.get("PPI_KEY_PUBLICA")
KEY_PRIVADA = os.environ.get("PPI_KEY_PRIVADA")

# Constantes
DEFAULT_START_DATE = datetime(2025, 1, 1)
CSV_FILE = "market_data.csv"
INSTRUMENT_TYPE = "CEDEARS"
SETTLEMENTS = "A-24HS"
TICKERS = ["TEND", "AAPLD", "VISTD", "DESPD", "MELID", "XOMD", "NVDAD", "MSFTD", "KOD"]


def parse_args():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description="Extrae datos de la API PPI")
    parser.add_argument(
        "--desde", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help="Fecha desde la cual extraer datos (YYYY-MM-DD). Por defecto: última fecha en CSV o 2025-01-01"
    )
    parser.add_argument(
        "--hasta", 
        type=lambda s: datetime.strptime(s, '%Y-%m-%d'),
        help="Fecha hasta la cual extraer datos (YYYY-MM-DD). Por defecto: fecha actual"
    )
    return parser.parse_args()


def get_latest_date_from_csv():
    """Obtiene la última fecha disponible en el CSV si existe"""
    try:
        if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
            df = pd.read_csv(CSV_FILE)
            if not df.empty and 'Date' in df.columns:
                df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
                
                # Normalizar los datos quitando la información de timezone
                df['Date'] = df['Date'].dt.tz_localize(None)
                
                # Filtra fechas válidas (no futuras)
                today = datetime.now()
                valid_dates = df[df['Date'] <= today]
                
                if not valid_dates.empty:
                    latest_date = valid_dates['Date'].max()
                    # Sumamos un día para no duplicar datos
                    next_date = latest_date + timedelta(days=1)
                    
                    logger.info(f"Última fecha en CSV: {latest_date.strftime('%Y-%m-%d')}")
                    return next_date
    
    except Exception as e:
        logger.error(f"Error al leer el CSV existente: {e}")
    
    # Si hay algún problema o el archivo no existe, usa la fecha por defecto
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


def process_and_save_data(df):
    """Procesa y guarda los datos en el archivo CSV"""
    if df is None or df.empty:
        logger.info("No hay nuevos datos para guardar")
        return 0
    
    try:
        # Convertir fechas
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        df = df.sort_values(by='Date')
        
        # Si el archivo existe, verificar duplicados
        if os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0:
            existing_df = pd.read_csv(CSV_FILE)
            existing_df['Date'] = pd.to_datetime(existing_df['Date'], errors='coerce')
            
            # Normalizar los datos quitando la información de timezone para la comparación
            existing_df_normalized = existing_df.copy()
            existing_df_normalized['Date'] = existing_df_normalized['Date'].dt.tz_localize(None)
            
            df_normalized = df.copy()
            df_normalized['Date'] = df_normalized['Date'].dt.tz_localize(None)
            
            # Crear una clave única para cada registro usando los dataframes normalizados
            df_normalized['key'] = df_normalized['Date'].dt.strftime('%Y-%m-%d') + '_' + df_normalized['ticker']
            existing_df_normalized['key'] = existing_df_normalized['Date'].dt.strftime('%Y-%m-%d') + '_' + existing_df_normalized['ticker']
            
            # Filtrar registros que ya existen (usando las claves normalizadas pero manteniendo los datos originales)
            new_keys = ~df_normalized['key'].isin(existing_df_normalized['key'])
            df = df.iloc[new_keys.values]
            
            if df.empty:
                logger.info("Todos los registros ya existen en el CSV")
                return 0
        
        # IMPORTANTE: Estas líneas deben estar fuera del bloque condicional anterior
        # Calcular variación diaria por ticker
        for ticker in df['ticker'].unique():
            ticker_mask = df['ticker'] == ticker
            df.loc[ticker_mask, 'variacion_diaria'] = df.loc[ticker_mask, 'Price'].pct_change() * 100
        
        # Guardar en CSV
        file_exists = os.path.exists(CSV_FILE) and os.path.getsize(CSV_FILE) > 0
        df.to_csv(CSV_FILE, mode='a', header=not file_exists, index=False)
        
        records_added = len(df)
        logger.info(f"{records_added} registros agregados a {CSV_FILE}")
        return records_added
            
    except Exception as e:
        logger.error(f"Error al guardar datos: {e}")
        return 0
      

def main():
    """Función principal del script"""
    args = parse_args()
    
    # Determinar fechas de inicio y fin
    date_to = args.hasta if args.hasta else datetime.now()
    date_from = args.desde if args.desde else get_latest_date_from_csv()

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
        
        # Guardar resultados
        records_added = process_and_save_data(all_data)
        logger.info(f"Extracción finalizada: {records_added} registros nuevos agregados")
    
    except Exception as e:
        logger.error(f"Error durante la ejecución: {e}")


if __name__ == "__main__":
    main()