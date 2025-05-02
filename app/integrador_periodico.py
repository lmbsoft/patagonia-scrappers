#!/usr/bin/env python3
# filepath: /home/ubuntu/docker/webmining/patagonia-scrappers-src/app/integrador_periodico.py
"""
Script para integración periódica de datos desde tabla_ppi y tabla_posts_bluesky
hacia las tablas principales del modelo de datos.

- Actualiza empresas y cotizaciones desde tabla_ppi
- Actualiza usuarios y notas desde tabla_posts_bluesky
- Está diseñado para ejecutarse periódicamente sin duplicar registros
"""

import logging
import os
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func

# Importar modelos 
from models import (
    Base, Empresas, CotizacionXEmpresa, Usuario, TipoUsuario, NotasXUsuario, 
    TablaPostsBluesky, TablaPPI, TipoNota, SessionLocal, engine, init_db
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('integrador_periodico.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

POSITIVE_THRESHOLD = 0.05
NEGATIVE_THRESHOLD = -0.05

def integrar_cotizaciones_desde_tablaPPI(db: Session):
    """
    Integra datos desde tabla_ppi a las tablas Empresas y CotizacionXEmpresa.
    Verifica existencia previa para evitar duplicados.
    """
    logger.info("Iniciando integración de cotizaciones desde tabla_ppi...")
    
    try:
        # Obtener último registro procesado para evitar procesar datos antiguos
        # (opcional, depende de requerimientos específicos)
        ultima_cotizacion = db.query(func.max(CotizacionXEmpresa.fecha)).scalar()
        
        if ultima_cotizacion:
            logger.info(f"Procesando cotizaciones posteriores a: {ultima_cotizacion}")
            registros_ppi = db.query(TablaPPI).filter(TablaPPI.Date > ultima_cotizacion).all()
        else:
            logger.info("No hay cotizaciones previas, procesando todos los registros")
            registros_ppi = db.query(TablaPPI).all()
        
        logger.info(f"Se encontraron {len(registros_ppi)} registros nuevos para procesar")
        
        # Caché para evitar consultas repetidas de empresas
        empresas_cache = {}  
        for empresa in db.query(Empresas).all():
            if empresa.ticker:
                empresas_cache[empresa.ticker] = empresa.id_empresa
        
        empresas_nuevas = {}
        cotizaciones_nuevas = []
        procesados = 0
        agregados = 0
        
        for registro in registros_ppi:
            procesados += 1
            
            # Verificar si existe la empresa o crear una nueva
            ticker = registro.ticker
            if ticker not in empresas_cache and ticker not in empresas_nuevas:
                # Crear nueva empresa si no existe
                nueva_empresa = Empresas(
                    nombre=ticker,  # Usar ticker como nombre provisional
                    ticker=ticker
                )
                empresas_nuevas[ticker] = nueva_empresa
                db.add(nueva_empresa)
                
                if procesados % 100 == 0:
                    # Hacer flush cada 100 registros para obtener IDs
                    db.flush()
                    for tk, emp in empresas_nuevas.items():
                        if emp.id_empresa:  # Asegurar que tiene ID asignado
                            empresas_cache[tk] = emp.id_empresa
                    empresas_nuevas = {}  # Limpiar caché temporal
            
            # Obtener ID de empresa (desde caché o después de crear nueva)
            id_empresa = None
            if ticker in empresas_cache:
                id_empresa = empresas_cache[ticker]
            elif ticker in empresas_nuevas and empresas_nuevas[ticker].id_empresa:
                id_empresa = empresas_nuevas[ticker].id_empresa
            
            # Solo agregar cotización si tenemos ID de empresa
            if id_empresa:
                # Verificar si ya existe esta cotización para esta empresa y fecha
                existe_cotizacion = db.query(CotizacionXEmpresa).filter(
                    CotizacionXEmpresa.id_empresa == id_empresa,
                    CotizacionXEmpresa.fecha == registro.Date
                ).first()
                
                if not existe_cotizacion:
                    # Crear nueva cotización
                    nueva_cotizacion = CotizacionXEmpresa(
                        id_empresa=id_empresa,
                        fecha=registro.Date,
                        precio_apertura=registro.Opening,
                        precio_cierre=registro.Price,
                        precio_max=registro.Max,
                        precio_min=registro.Min,
                        volumen_operado=registro.Volume,
                        variacion_porcentaje=registro.variacion_diaria
                    )
                    cotizaciones_nuevas.append(nueva_cotizacion)
                    agregados += 1
                    
                    # Cada 1000 cotizaciones, agregar al batch y limpiar lista
                    if len(cotizaciones_nuevas) >= 1000:
                        db.add_all(cotizaciones_nuevas)
                        db.flush()
                        cotizaciones_nuevas = []
                        logger.info(f"Procesados {procesados} registros. Agregadas {agregados} cotizaciones.")
        
        # Hacer flush final de empresas pendientes
        if empresas_nuevas:
            db.flush()
            for tk, emp in empresas_nuevas.items():
                if emp.id_empresa:
                    empresas_cache[tk] = emp.id_empresa
        
        # Agregar últimas cotizaciones pendientes
        if cotizaciones_nuevas:
            db.add_all(cotizaciones_nuevas)
        
        # Commit final
        db.commit()
        logger.info(f"Integración de cotizaciones completada. Procesados: {procesados}, Agregados: {agregados}")
        
        return agregados
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error al integrar cotizaciones: {e}", exc_info=True)
        return 0

def integrar_posts_desde_tablaBluesky(db: Session):
    """
    Integra datos desde tabla_posts_bluesky a las tablas Usuario y NotasXUsuario.
    Verifica existencia previa para evitar duplicados.
    """
    logger.info("Iniciando integración de posts desde tabla_posts_bluesky...")
    
    try:
        # Obtener última nota procesada para evitar procesar datos antiguos
        ultima_nota_fecha = db.query(func.max(NotasXUsuario.fecha_publicacion)).scalar()
        
        if ultima_nota_fecha:
            logger.info(f"Procesando posts posteriores a: {ultima_nota_fecha}")
            posts = db.query(TablaPostsBluesky).filter(TablaPostsBluesky.created_at > ultima_nota_fecha).all()
        else:
            logger.info("No hay notas previas, procesando todos los posts")
            posts = db.query(TablaPostsBluesky).all()
        
        logger.info(f"Se encontraron {len(posts)} posts nuevos para procesar")
        
        # Caché de usuarios para evitar consultas repetidas
        usuarios_cache = {}
        for usuario in db.query(Usuario).all():
            if usuario.handle:
                usuarios_cache[usuario.handle] = usuario.id_usuario
        
        # Valores por defecto para usuarios
        cod_tipo_usuario_default = "others"  # Categoría por defecto
        
        # Verificar si existe el tipo de nota para texto
        tipo_nota_texto = db.query(TipoNota).filter_by(cod_tipo_nota=1).first()
        if not tipo_nota_texto:
            logger.warning("No se encontró el tipo de nota para texto (cod_tipo_nota=1), creando...")
            tipo_nota_texto = TipoNota(cod_tipo_nota=1, descripcion="Texto")
            db.add(tipo_nota_texto)
            db.flush()
        
        usuarios_nuevos = {}
        notas_nuevas = []
        procesados = 0
        usuarios_agregados = 0
        notas_agregadas = 0
        
        for post in posts:
            procesados += 1
            handle = post.actor_handle
            
            # Verificar si ya existe el usuario o crear uno nuevo
            if handle not in usuarios_cache and handle not in usuarios_nuevos:
                # Buscar si ya existe en la BD
                usuario_existente = db.query(Usuario).filter_by(handle=handle).first()
                
                if not usuario_existente:
                    # Crear nuevo usuario
                    nuevo_usuario = Usuario(
                        id_usuario=handle,  # Usar handle como ID
                        nombre=handle,      # Usar handle como nombre provisional
                        handle=handle,
                        cod_tipo_usuario=cod_tipo_usuario_default,
                        verificado=False,
                        seguidores=0,       # Valor por defecto
                        idioma_principal="en"  # Valor por defecto
                    )
                    db.add(nuevo_usuario)
                    usuarios_nuevos[handle] = handle  # Usar handle como ID
                    usuarios_agregados += 1
                else:
                    # Si ya existe, agregar al caché
                    usuarios_cache[handle] = usuario_existente.id_usuario
            
            # Obtener ID de usuario
            id_usuario = None
            if handle in usuarios_cache:
                id_usuario = usuarios_cache[handle]
            elif handle in usuarios_nuevos:
                id_usuario = usuarios_nuevos[handle]
            
            # Verificar si tenemos ID de usuario
            if id_usuario:
                # Verificar si ya existe esta nota (por URI)
                existe_nota = db.query(NotasXUsuario).filter_by(url_nota=post.uri).first()
                
                if not existe_nota:
                    # Determinar sentimiento
                    # Preferir VADER si está disponible, sino usar TextBlob
                    score_sentimiento = post.vader_sentiment if post.vader_sentiment is not None else post.textblob_sentiment
                    etiqueta_sentimiento = post.vader_sentiment_label if post.vader_sentiment_label else post.textblob_sentiment_label
                    
                    # Normalizar etiqueta de sentimiento
                    if not etiqueta_sentimiento:
                        if score_sentimiento > POSITIVE_THRESHOLD:
                            etiqueta_sentimiento = "Positivo"
                        elif score_sentimiento < NEGATIVE_THRESHOLD:
                            etiqueta_sentimiento = "Negativo"
                        else:
                            etiqueta_sentimiento = "Neutral"
                    
                    # Crear nueva nota
                    nueva_nota = NotasXUsuario(
                        contenido=post.text,
                        fecha_publicacion=post.created_at,
                        id_usuario=id_usuario,
                        cod_tipo_nota=1,  # Tipo texto
                        url_nota=post.uri,
                        engagement_total=post.engagement,
                        score_analisis_sentimiento_nlp=None,  # No tenemos este valor
                        sentimiento=etiqueta_sentimiento,
                        score_sentimiento=score_sentimiento
                    )
                    notas_nuevas.append(nueva_nota)
                    notas_agregadas += 1
                    
                    # Cada 1000 notas, agregar al batch y limpiar lista
                    if len(notas_nuevas) >= 1000:
                        db.add_all(notas_nuevas)
                        db.flush()
                        notas_nuevas = []
                        logger.info(f"Procesados {procesados} posts. Agregados {usuarios_agregados} usuarios y {notas_agregadas} notas.")
        
        # Agregar últimas notas pendientes
        if notas_nuevas:
            db.add_all(notas_nuevas)
        
        # Commit final
        db.commit()
        logger.info(f"Integración de posts completada. Procesados: {procesados}, Usuarios nuevos: {usuarios_agregados}, Notas nuevas: {notas_agregadas}")
        
        return notas_agregadas
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error al integrar posts: {e}", exc_info=True)
        return 0

def ejecutar_integracion():
    """
    Ejecuta el proceso completo de integración periódica.
    """
    try:
        # Crear sesión de base de datos
        db = SessionLocal()
        try:
            logger.info("=== INICIANDO INTEGRACIÓN PERIÓDICA ===")
            # Integrar datos desde TablaPPI
            cotizaciones_agregadas = integrar_cotizaciones_desde_tablaPPI(db)
            logger.info(f"Se agregaron {cotizaciones_agregadas} nuevas cotizaciones.")
            
            # Integrar datos desde TablaPostsBluesky
            notas_agregadas = integrar_posts_desde_tablaBluesky(db)
            logger.info(f"Se agregaron {notas_agregadas} nuevas notas.")
            
            logger.info("=== INTEGRACIÓN PERIÓDICA COMPLETADA ===")
            return cotizaciones_agregadas + notas_agregadas
            
        finally:
            db.close()
            logger.info("Sesión de base de datos cerrada.")
            
    except Exception as e:
        logger.error(f"Error durante el proceso de integración: {e}", exc_info=True)
        return 0

if __name__ == "__main__":
    logger.info("Iniciando script de integración periódica...")
    total_registros = ejecutar_integracion()
    logger.info(f"Proceso de integración completado. Se agregaron {total_registros} registros en total.")