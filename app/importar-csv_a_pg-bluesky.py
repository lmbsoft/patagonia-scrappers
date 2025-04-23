#!/usr/bin/env python3
# filepath: /app/importar-csv_a_pg-bluesky.py
import pandas as pd
from datetime import datetime
import os
import logging
import argparse
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import func
from models import SessionLocal, TablaPostsBluesky, init_db

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('importar_csv_pg_bluesky.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constantes
DEFAULT_CSV_PATH = "/app/posts_con_sentimiento_historia.csv"

# Umbrales de sentimiento
POSITIVE_THRESHOLD = 0.05
NEGATIVE_THRESHOLD = -0.05


def parse_args():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='Importa posts desde un CSV a la base de datos PostgreSQL')
    parser.add_argument(
        '--csv-path', 
        default=DEFAULT_CSV_PATH,
        help=f'Ruta al archivo CSV. Por defecto: {DEFAULT_CSV_PATH}'
    )
    parser.add_argument(
        '--start-date', 
        help='Filtrar posts desde esta fecha (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date', 
        help='Filtrar posts hasta esta fecha (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--batch-size', 
        type=int, 
        default=1000, 
        help='Número de registros a procesar por lote. Por defecto: 1000'
    )
    return parser.parse_args()


def read_csv_file(csv_path):
    """Lee el archivo CSV y convierte los datos a un DataFrame"""
    logger.info(f"Leyendo archivo CSV: {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        logger.info(f"Archivo CSV leído: {len(df)} registros encontrados")
        
        # Convertir fecha a datetime usando un método más robusto
        try:
            # Intentar usar pd.to_datetime con format='mixed' para detectar el formato automáticamente
            df['created_at'] = pd.to_datetime(df['created_at'], format='mixed', utc=True)
        except:
            # Si falla, intentar convertir cada fecha individualmente
            logger.info("Intentando convertir fechas manualmente...")
            created_at_list = []
            for date_str in df['created_at']:
                try:
                    # Probar varios formatos comunes
                    dt = pd.to_datetime(date_str, utc=True)
                except:
                    logger.warning(f"No se pudo convertir la fecha: {date_str}, usando None")
                    dt = None
                created_at_list.append(dt)
            df['created_at'] = created_at_list
        
        # Filtrar filas con fechas inválidas
        valid_rows = df['created_at'].notna()
        if not all(valid_rows):
            logger.warning(f"Se encontraron {(~valid_rows).sum()} filas con fechas inválidas que serán eliminadas")
            df = df[valid_rows]
        
        # Extraer fecha de created_at si no existe la columna 'date'
        if 'date' not in df.columns:
            df['date'] = df['created_at'].dt.date
        
        logger.info(f"Archivo CSV procesado correctamente. Total de registros válidos: {len(df)}")
        return df
    except Exception as e:
        logger.error(f"Error al leer el archivo CSV: {e}")
        return None


def filter_posts_by_date(df, start_date_str, end_date_str):
    """Filtra posts por rango de fechas"""
    logger.info(f"Filtrando posts entre {start_date_str} y {end_date_str}")
    
    try:
        if not pd.api.types.is_datetime64_any_dtype(df['created_at']):
            df['created_at'] = pd.to_datetime(df['created_at'])
        
        if df['created_at'].dt.tz is None:
            df['created_at'] = df['created_at'].dt.tz_localize('UTC')
        
        start_date = pd.to_datetime(start_date_str + ' 00:00:00+00:00')
        end_date = pd.to_datetime(end_date_str + ' 23:59:59+00:00')
        
        filtered_df = df[(df['created_at'] >= start_date) & (df['created_at'] <= end_date)].copy()
        logger.info(f"Posts después del filtrado: {len(filtered_df)} (de {len(df)} posts originales)")
        
        return filtered_df
    except Exception as e:
        logger.error(f"Error al filtrar posts por fecha: {e}")
        return df


def get_latest_date_from_db():
    """Obtiene la fecha más reciente de los posts almacenados en la base de datos"""
    try:
        with SessionLocal() as db:
            latest_date = db.query(func.max(TablaPostsBluesky.created_at)).scalar()
            if latest_date:
                logger.info(f"Último post en la base de datos: {latest_date.strftime('%Y-%m-%d %H:%M:%S')}")
            else:
                logger.info("No hay posts previos en la base de datos")
            return latest_date
    except Exception as e:
        logger.error(f"Error al consultar la última fecha en la base de datos: {e}")
        return None


def prepare_data_for_db(df):
    """Prepara los datos del CSV para la estructura de la base de datos"""
    logger.info("Preparando datos para importar a la base de datos...")
    
    try:
        # Mapeo de columnas CSV a columnas de la base de datos
        db_df = pd.DataFrame()
        
        # Columnas comunes
        db_df['actor_handle'] = df['actor_handle']
        db_df['uri'] = df['uri']
        db_df['text'] = df['text']
        db_df['created_at'] = df['created_at']
        
        # Asegurar que los campos numéricos son realmente números
        db_df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0).astype(int)
        db_df['reposts'] = pd.to_numeric(df['reposts'], errors='coerce').fillna(0).astype(int)
        db_df['replies'] = pd.to_numeric(df['replies'], errors='coerce').fillna(0).astype(int)
        
        # Columna engagement (puede estar en el CSV o calcularse)
        if 'engagement' in df.columns:
            db_df['engagement'] = pd.to_numeric(df['engagement'], errors='coerce').fillna(0).astype(int)
        else:
            db_df['engagement'] = db_df['likes'] + db_df['reposts'] + db_df['replies']
        
        # Mapeo de columnas de sentimiento
        if 'sentiment' in df.columns:
            db_df['textblob_sentiment'] = pd.to_numeric(df['sentiment'], errors='coerce').fillna(0).astype(float)
            
            # Determinar etiqueta basada en el valor del sentimiento
            textblob_labels = []
            for sentiment in db_df['textblob_sentiment']:
                if sentiment > POSITIVE_THRESHOLD:
                    label = 'Positivo'
                elif sentiment < NEGATIVE_THRESHOLD:
                    label = 'Negativo'
                else:
                    label = 'Neutral'
                textblob_labels.append(label)
            
            db_df['textblob_sentiment_label'] = textblob_labels
        else:
            db_df['textblob_sentiment'] = 0.0
            db_df['textblob_sentiment_label'] = 'Neutral'
        
        # Mapeo de columnas de sentimiento VADER
        if 'sentiment_vader' in df.columns:
            db_df['vader_sentiment'] = pd.to_numeric(df['sentiment_vader'], errors='coerce').fillna(0).astype(float)
            
            # Determinar etiqueta basada en el valor del sentimiento de VADER
            vader_labels = []
            for sentiment in db_df['vader_sentiment']:
                if sentiment >= POSITIVE_THRESHOLD:
                    label = 'Positivo'
                elif sentiment <= NEGATIVE_THRESHOLD:
                    label = 'Negativo'
                else:
                    label = 'Neutral'
                vader_labels.append(label)
            
            db_df['vader_sentiment_label'] = vader_labels
        else:
            db_df['vader_sentiment'] = 0.0
            db_df['vader_sentiment_label'] = 'Neutral'
        
        # Si hay interpretación directa, usarla para sobreescribir las etiquetas generadas
        if 'interpretacion_sentimiento' in df.columns:
            for i, interp in enumerate(df['interpretacion_sentimiento']):
                if pd.notna(interp):
                    if 'Positivo' in str(interp):
                        db_df.at[i, 'textblob_sentiment_label'] = 'Positivo'
                        db_df.at[i, 'vader_sentiment_label'] = 'Positivo'
                    elif 'Negativo' in str(interp):
                        db_df.at[i, 'textblob_sentiment_label'] = 'Negativo'
                        db_df.at[i, 'vader_sentiment_label'] = 'Negativo'
                    elif 'Neutral' in str(interp):
                        db_df.at[i, 'textblob_sentiment_label'] = 'Neutral'
                        db_df.at[i, 'vader_sentiment_label'] = 'Neutral'
        
        logger.info(f"Datos preparados correctamente. Total de registros: {len(db_df)}")
        return db_df
    except Exception as e:
        logger.error(f"Error al preparar los datos: {e}")
        return None


def save_to_database(df, batch_size=1000):
    """Guarda los posts en la base de datos PostgreSQL por lotes"""
    if df is None or len(df) == 0:
        logger.info("No hay posts para guardar")
        return 0
    
    total_posts = len(df)
    logger.info(f"Intentando guardar {total_posts} posts en la base de datos en lotes de {batch_size}")
    
    total_saved = 0
    
    try:
        with SessionLocal() as db:
            # Obtener URIs existentes para evitar duplicados
            existing_uris = set()
            existing_records = db.query(TablaPostsBluesky.uri).all()
            for record in existing_records:
                existing_uris.add(record[0])
                
            logger.info(f"Se encontraron {len(existing_uris)} posts existentes en la base de datos")
            
            # Procesar lotes de datos
            for start_idx in range(0, total_posts, batch_size):
                end_idx = min(start_idx + batch_size, total_posts)
                batch_df = df.iloc[start_idx:end_idx]
                
                logger.info(f"Procesando lote {start_idx//batch_size + 1}/{(total_posts//batch_size) + 1} ({start_idx} a {end_idx-1})")
                
                # Filtrar posts que ya existen en la base de datos
                new_posts = []
                
                for _, row in batch_df.iterrows():
                    uri = row['uri']
                    if uri not in existing_uris:
                        # Asegurarnos de que los valores numéricos son del tipo correcto
                        try:
                            new_post = TablaPostsBluesky(
                                actor_handle=str(row['actor_handle']),
                                uri=str(uri),
                                text=str(row['text']),
                                created_at=row['created_at'],
                                likes=int(row['likes']),
                                reposts=int(row['reposts']),
                                replies=int(row['replies']),
                                engagement=int(row['engagement']),
                                textblob_sentiment=float(row['textblob_sentiment']),
                                textblob_sentiment_label=str(row['textblob_sentiment_label']),
                                vader_sentiment=float(row['vader_sentiment']),
                                vader_sentiment_label=str(row['vader_sentiment_label'])
                            )
                            new_posts.append(new_post)
                            # Actualizar el conjunto de URIs existentes para evitar duplicados en lotes siguientes
                            existing_uris.add(uri)
                        except Exception as e:
                            logger.warning(f"Error al crear objeto para URI {uri}: {e}")
                
                # Añadir nuevos posts a la base de datos
                if new_posts:
                    try:
                        db.add_all(new_posts)
                        db.commit()
                        logger.info(f"Se guardaron {len(new_posts)} nuevos posts en el lote")
                        total_saved += len(new_posts)
                    except Exception as e:
                        db.rollback()
                        logger.error(f"Error al guardar el lote en la base de datos: {e}")
                        
                        # Intentar insertar uno por uno para identificar registros problemáticos
                        successful_inserts = 0
                        for post in new_posts:
                            try:
                                db.add(post)
                                db.commit()
                                successful_inserts += 1
                                # Mantenemos la URI en el conjunto para evitar duplicados
                            except Exception as e_individual:
                                db.rollback()
                                logger.warning(f"Error al insertar post individual {post.uri}: {e_individual}")
                        
                        logger.info(f"Se guardaron {successful_inserts} posts individuales después del error")
                        total_saved += successful_inserts
                else:
                    logger.info("No se encontraron nuevos posts para guardar en este lote")
            
            logger.info(f"Importación completada. Total de posts guardados: {total_saved}/{total_posts}")
            return total_saved
                
    except Exception as e:
        logger.error(f"Error al guardar los posts en la base de datos: {e}")
        return 0


def main():
    """Función principal del script"""
    args = parse_args()
    
    try:
        logger.info("Iniciando script de importación de CSV a PostgreSQL")
        
        # Inicializar la base de datos
        init_db()
        logger.info("Base de datos inicializada")
        
        # Leer archivo CSV
        csv_path = args.csv_path
        if not os.path.exists(csv_path):
            logger.error(f"El archivo CSV no existe: {csv_path}")
            return
        
        df = read_csv_file(csv_path)
        if df is None:
            logger.error("No se pudo leer el archivo CSV")
            return
        
        # Filtrar por fecha si se especificó
        if args.start_date and args.end_date:
            df = filter_posts_by_date(df, args.start_date, args.end_date)
        
        # Preparar datos para la base de datos
        db_df = prepare_data_for_db(df)
        if db_df is None:
            logger.error("No se pudieron preparar los datos para la base de datos")
            return
        
        # Ordenar por fecha (más recientes primero)
        db_df = db_df.sort_values(by='created_at', ascending=False)
        
        # Guardar en la base de datos PostgreSQL
        posts_saved = save_to_database(db_df, args.batch_size)
        
        logger.info(f"Proceso finalizado. Se importaron {posts_saved} posts nuevos.")
        
    except Exception as e:
        logger.error(f"Error durante la ejecución del script: {e}")


if __name__ == "__main__":
    main()