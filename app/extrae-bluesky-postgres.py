#!/usr/bin/env python3
# filepath: /home/ubuntu/docker/webmining/patagonia-scrappers-src/app/extrae-bluesky-postgres.py
import requests
import pandas as pd
from datetime import datetime
import os
import logging
import argparse
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import func
from models import SessionLocal, TablaPostsBluesky, init_db
from textblob import TextBlob

# Instalar vaderSentiment si no está disponible
try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
except ImportError:
    import subprocess
    import sys
    logging.info("Instalando vaderSentiment...")
    subprocess.check_call([sys.executable, "-m", "pip", "install", "vaderSentiment"])
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('extrae_bluesky_postgres.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Constantes
DEFAULT_POSTS_LIMIT = 100
API_BASE_URL = "https://bsky.social/xrpc"
API_AUTH_ENDPOINT = "com.atproto.server.createSession"
API_SEARCH_ENDPOINT = "app.bsky.actor.searchActors"
API_FEED_ENDPOINT = "app.bsky.feed.getAuthorFeed"
USERNAME = "grupo18.bsky.social"
PASSWORD = "Grupo18*BS"  # En producción, usar variables de entorno
SEARCH_TERMS = ["bloomberg", "aoc", "economist"]

# Umbrales de sentimiento
POSITIVE_THRESHOLD = 0.05
NEGATIVE_THRESHOLD = -0.05


def parse_args():
    """Parsea los argumentos de línea de comandos"""
    parser = argparse.ArgumentParser(description='Extrae posts de la API de Bluesky y los guarda en PostgreSQL')
    parser.add_argument(
        '--start-date', 
        help='Filtrar posts desde esta fecha (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--end-date', 
        help='Filtrar posts hasta esta fecha (YYYY-MM-DD)'
    )
    parser.add_argument(
        '--limit', 
        type=int, 
        default=DEFAULT_POSTS_LIMIT, 
        help=f'Máximo de posts por actor. Por defecto: {DEFAULT_POSTS_LIMIT}'
    )
    return parser.parse_args()


def connect_to_bluesky():
    """Conecta a la API de Bluesky y obtiene el token de autenticación"""
    logger.info("Conectando a la API de Bluesky")
    url_se = f"{API_BASE_URL}/{API_AUTH_ENDPOINT}"
    payload = {
        "identifier": USERNAME,
        "password": PASSWORD
    }
    
    try:
        response_tk = requests.post(url_se, json=payload)
        response_tk.raise_for_status()
        token = response_tk.json()["accessJwt"]
        logger.info("Conexión exitosa con la API de Bluesky")
        return token
    except requests.exceptions.RequestException as e:
        logger.error(f"Error al conectar con la API de Bluesky: {e}")
        return None


def search_actors(token, search_terms):
    """Busca actores basado en una lista de términos de búsqueda"""
    logger.info(f"Buscando actores con los términos: {search_terms}")
    search_url = f"{API_BASE_URL}/{API_SEARCH_ENDPOINT}"
    headers = {"Authorization": f"Bearer {token}"}
    
    found_actors = []
    actor_handles = []
    
    for term in search_terms:
        logger.info(f"Buscando término: '{term}'")
        params = {
            "term": term,
            "limit": 1  # Solo obtenemos el resultado más relevante
        }
        try:
            response = requests.get(search_url, headers=headers, params=params)
            response.raise_for_status()
            search_data = response.json()
            
            if search_data.get('actors'):
                actor = search_data['actors'][0]
                logger.info(f"Actor encontrado: {actor['handle']}")
                actor_handles.append(actor['handle'])
            else:
                logger.warning(f"No se encontraron resultados para el término: '{term}'")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al buscar el término '{term}': {e}")
    
    # Eliminar duplicados
    actor_handles = list(set(actor_handles))
    logger.info(f"Se encontraron {len(actor_handles)} actores únicos para procesar")
    
    return actor_handles


def get_actor_feeds(token, actor_handles, posts_limit_per_actor=DEFAULT_POSTS_LIMIT):
    """Obtiene los feeds para una lista de handles de actores"""
    logger.info(f"Obteniendo feeds para {len(actor_handles)} actores (límite: {posts_limit_per_actor} posts por actor)")
    feed_url = f"{API_BASE_URL}/{API_FEED_ENDPOINT}"
    headers = {"Authorization": f"Bearer {token}"}
    
    all_posts = []
    
    for handle in actor_handles:
        logger.info(f"Obteniendo feed de '{handle}'")
        params = {
            "actor": handle,
            "limit": posts_limit_per_actor
        }
        try:
            response = requests.get(feed_url, headers=headers, params=params)
            response.raise_for_status()
            feed_data = response.json()
            
            for item in feed_data.get('feed', []):
                post = item.get('post')
                if not post or 'record' not in post:
                    logger.warning(f"Post incompleto encontrado en el feed de {handle}, omitiendo")
                    continue
                
                record = post.get('record', {})
                created_at_str = record.get('createdAt', '')
                created_at_dt = None
                
                if created_at_str:
                    try:
                        created_at_dt = datetime.fromisoformat(created_at_str.replace('Z', '+00:00'))
                    except ValueError:
                        logger.warning(f"Formato de fecha inválido en post de {handle}, omitiendo")
                        continue
                
                likes = post.get('likeCount', 0)
                reposts = post.get('repostCount', 0)
                replies = post.get('replyCount', 0)
                
                post_data = {
                    'actor_handle': handle,
                    'uri': post.get('uri', ''),
                    'text': record.get('text', ''),
                    'created_at': created_at_dt,
                    'likes': likes,
                    'reposts': reposts,
                    'replies': replies,
                    'engagement': likes + reposts + replies
                }
                
                # Solo agregar posts con fechas válidas
                if post_data['created_at']:
                    all_posts.append(post_data)
                    
        except requests.exceptions.RequestException as e:
            logger.error(f"Error al obtener feed de '{handle}': {e}")
        except Exception as e:
            logger.error(f"Error al procesar feed de '{handle}': {e}")
    
    logger.info(f"Total de posts obtenidos: {len(all_posts)}")
    return all_posts


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


def analyze_sentiment_textblob(text):
    """Analiza el sentimiento usando TextBlob"""
    try:
        sentiment = TextBlob(str(text)).sentiment.polarity
        # Clasificar el sentimiento
        if sentiment > POSITIVE_THRESHOLD:
            label = 'Positivo'
        elif sentiment < NEGATIVE_THRESHOLD:
            label = 'Negativo'
        else:
            label = 'Neutral'
        return sentiment, label
    except:
        return 0, 'Neutral'


def analyze_sentiment_vader(text):
    """Analiza el sentimiento usando VADER"""
    try:
        analyzer = SentimentIntensityAnalyzer()
        vs = analyzer.polarity_scores(str(text))
        sentiment = vs['compound']
        # Clasificar el sentimiento
        if sentiment >= POSITIVE_THRESHOLD:
            label = 'Positivo'
        elif sentiment <= NEGATIVE_THRESHOLD:
            label = 'Negativo'
        else:
            label = 'Neutral'
        return sentiment, label
    except:
        return 0, 'Neutral'


def analyze_sentiments(df):
    """Analiza el sentimiento de todos los posts usando TextBlob y VADER"""
    logger.info("Analizando sentimiento de los posts...")
    
    # Analizar sentimiento con TextBlob
    textblob_results = []
    for text in df['text']:
        score, label = analyze_sentiment_textblob(text)
        textblob_results.append((score, label))
    
    df['textblob_sentiment'] = [r[0] for r in textblob_results]
    df['textblob_sentiment_label'] = [r[1] for r in textblob_results]
    
    # Analizar sentimiento con VADER
    vader_results = []
    for text in df['text']:
        score, label = analyze_sentiment_vader(text)
        vader_results.append((score, label))
    
    df['vader_sentiment'] = [r[0] for r in vader_results]
    df['vader_sentiment_label'] = [r[1] for r in vader_results]
    
    # Conteo de resultados
    textblob_counts = df['textblob_sentiment_label'].value_counts()
    vader_counts = df['vader_sentiment_label'].value_counts()
    
    logger.info(f"Análisis de sentimiento completado. Resultados TextBlob: {textblob_counts.to_dict()}")
    logger.info(f"Análisis de sentimiento completado. Resultados VADER: {vader_counts.to_dict()}")
    
    return df


def save_to_database(post_data):
    """Guarda los posts en la base de datos PostgreSQL"""
    if not post_data or len(post_data) == 0:
        logger.info("No hay posts para guardar")
        return 0
    
    logger.info(f"Guardando {len(post_data)} posts en la base de datos")
    
    try:
        with SessionLocal() as db:
            # Convertir a DataFrame para facilitar el procesamiento
            df = pd.DataFrame(post_data)
            
            # Verificar URIs existentes para evitar duplicados
            existing_uris = {}
            existing_records = db.query(TablaPostsBluesky.uri).all()
            for record in existing_records:
                existing_uris[record[0]] = True
                
            # Filtrar posts que ya existen en la base de datos
            new_posts = []
            for _, row in df.iterrows():
                if row['uri'] not in existing_uris:
                    new_post = TablaPostsBluesky(
                        actor_handle=row['actor_handle'],
                        uri=row['uri'],
                        text=row['text'],
                        created_at=row['created_at'],
                        likes=row['likes'],
                        reposts=row['reposts'],
                        replies=row['replies'],
                        engagement=row['engagement'],
                        textblob_sentiment=row['textblob_sentiment'],
                        textblob_sentiment_label=row['textblob_sentiment_label'],
                        vader_sentiment=row['vader_sentiment'],
                        vader_sentiment_label=row['vader_sentiment_label']
                    )
                    new_posts.append(new_post)
            
            # Añadir nuevos posts a la base de datos
            if new_posts:
                db.add_all(new_posts)
                db.commit()
                logger.info(f"Se guardaron {len(new_posts)} nuevos posts en la base de datos")
                return len(new_posts)
            else:
                logger.info("No se encontraron nuevos posts para guardar")
                return 0
                
    except Exception as e:
        logger.error(f"Error al guardar los posts en la base de datos: {e}")
        return 0


def main():
    """Función principal del script"""
    args = parse_args()
    
    try:
        logger.info("Iniciando script de extracción de Bluesky para PostgreSQL")
        
        # Inicializar la base de datos
        init_db()
        
        # Conectar a la API de Bluesky
        token = connect_to_bluesky()
        if not token:
            logger.error("No se pudo obtener el token de autenticación, terminando la ejecución")
            return
        
        # Buscar actores
        actor_handles = search_actors(token, SEARCH_TERMS)
        if not actor_handles:
            logger.warning("No se encontraron actores, terminando la ejecución")
            return
        
        # Obtener feeds para los actores encontrados
        all_posts = get_actor_feeds(token, actor_handles, args.limit)
        if not all_posts:
            logger.warning("No se obtuvieron posts, terminando la ejecución")
            return
        
        # Convertir a DataFrame
        df = pd.DataFrame(all_posts)
        
        # Filtrar por fecha si se especificó
        if args.start_date and args.end_date:
            df = filter_posts_by_date(df, args.start_date, args.end_date)
        
        # Ordenar por fecha (más recientes primero)
        df = df.sort_values(by='created_at', ascending=False)
        
        # Analizar sentimiento
        df = analyze_sentiments(df)
        
        # Guardar en la base de datos PostgreSQL
        save_to_database(df.to_dict('records'))
        
    except Exception as e:
        logger.error(f"Error durante la ejecución del script: {e}")


if __name__ == "__main__":
    main()