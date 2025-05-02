#!/usr/bin/env python3
import os
import argparse
import logging
import pandas as pd
from datetime import datetime
from sqlalchemy import func
from sqlalchemy.orm import Session
from models import SessionLocal, TablaPPI

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('importar_csv_pg_ppi.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def parse_args():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description="Importa datos desde un archivo CSV a la tabla tabla_ppi en PostgreSQL")
    parser.add_argument(
        "--archivo", 
        type=str,
        default="market_data_nico.csv",
        help="Ruta al archivo CSV para importar. Por defecto: market_data_nico.csv"
    )
    parser.add_argument(
        "--skip_duplicates",
        action="store_true",
        help="Si se activa, omite los registros que ya existen en la base de datos"
    )
    return parser.parse_args()

def read_csv(file_path):
    """Lee el archivo CSV y devuelve un DataFrame de pandas"""
    try:
        logger.info(f"Leyendo archivo CSV: {file_path}")
        # Verificar si el archivo existe
        if not os.path.exists(file_path):
            logger.error(f"El archivo {file_path} no existe")
            return None
            
        # Leer el archivo CSV
        df = pd.read_csv(file_path)
        
        # Verificar que el CSV tenga todas las columnas necesarias
        required_columns = ['Date', 'Price', 'Volume', 'Opening', 'Min', 'Max', 'ticker', 
                           'settlement', 'instrument_type', 'currency', 'variacion_diaria']
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"Faltan columnas en el CSV: {', '.join(missing_columns)}")
            return None
            
        logger.info(f"CSV leído correctamente con {len(df)} filas")
        return df
        
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {e}")
        return None

def process_dataframe(df):
    """Procesa el DataFrame para asegurar que los tipos de datos sean correctos"""
    try:
        if df is None or df.empty:
            logger.warning("No hay datos para procesar")
            return None
            
        # Convertir fechas
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Convertir columnas numéricas
        numeric_cols = ['Price', 'Volume', 'Opening', 'Min', 'Max', 'variacion_diaria']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
            
        # Eliminar filas con fechas inválidas
        invalid_dates = df['Date'].isna().sum()
        if invalid_dates > 0:
            logger.warning(f"Se eliminaron {invalid_dates} filas con fechas inválidas")
            df = df.dropna(subset=['Date'])
            
        # Ordenar por fecha
        df = df.sort_values(by=['ticker', 'Date'])
        
        logger.info(f"DataFrame procesado correctamente: {len(df)} filas válidas")
        return df
        
    except Exception as e:
        logger.error(f"Error al procesar el DataFrame: {e}")
        return None

def import_to_database(df, skip_duplicates=True):
    """Importa los datos del DataFrame a la base de datos"""
    if df is None or df.empty:
        logger.warning("No hay datos para importar")
        return 0
        
    try:
        # Iniciar sesión de base de datos
        with SessionLocal() as db:
            records_added = 0
            records_skipped = 0
            
            # Procesar cada fila
            for _, row in df.iterrows():
                # Verificar si el registro ya existe
                if skip_duplicates:
                    existing = db.query(TablaPPI).filter(
                        func.date(TablaPPI.Date) == func.date(row['Date']),
                        TablaPPI.ticker == row['ticker']
                    ).first()
                    
                    if existing:
                        records_skipped += 1
                        continue
                
                # Crear nuevo registro
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
                
                # Hacer commit cada 100 registros para evitar transacciones muy grandes
                if records_added % 100 == 0:
                    db.commit()
                    logger.info(f"Progreso: {records_added} registros importados")
            
            # Commit final
            db.commit()
            
            logger.info(f"Importación completada: {records_added} registros agregados, {records_skipped} registros omitidos")
            return records_added
            
    except Exception as e:
        logger.error(f"Error al importar datos a la base de datos: {e}")
        return 0

def main():
    """Función principal del script"""
    args = parse_args()
    
    # Leer el archivo CSV
    df = read_csv(args.archivo)
    
    # Procesar el DataFrame
    df = process_dataframe(df)
    
    # Importar a la base de datos
    records_added = import_to_database(df, args.skip_duplicates)
    
    # Resumen final
    if records_added > 0:
        logger.info(f"Importación exitosa: {records_added} registros agregados a la tabla tabla_ppi")
    else:
        logger.warning("No se importaron nuevos registros a la base de datos")

if __name__ == "__main__":
    main()