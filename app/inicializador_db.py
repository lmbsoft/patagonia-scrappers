#!/usr/bin/env python3
# filepath: /home/ubuntu/docker/webmining/patagonia-scrappers-src/app/inicializador_db.py
"""
Script de inicialización de la base de datos.
Crea las tablas principales y carga datos iniciales estáticos.
No está diseñado para ejecución periódica sino para el setup inicial.
"""

import logging
import os
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

# Importar los modelos desde app.models
from models import (
    Base, Empresas, CotizacionXEmpresa, Usuario, TipoUsuario, 
    NotasXUsuario, Paises, TipoNota, SessionLocal, engine, init_db
)

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('inicializador_db.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def populate_tipo_usuarios(db: Session):
    """
    Puebla la tabla tipo_usuarios con datos predefinidos.
    """
    logger.info("Poblando tabla tipo_usuarios...")
    tipo_usuarios_data = [
        {"cod_tipo_usuario": "news_ext", "descripcion": "Noticias externas"},
        {"cod_tipo_usuario": "news_local", "descripcion": "Noticias locales"},
        {"cod_tipo_usuario": "others", "descripcion": "Otros"},
        {"cod_tipo_usuario": "influencers", "descripcion": "Influencers"},
    ]

    try:
        added_count = 0
        for data in tipo_usuarios_data:
            existing_tipo = db.query(TipoUsuario).filter_by(cod_tipo_usuario=data["cod_tipo_usuario"]).first()
            if not existing_tipo:
                nuevo_tipo = TipoUsuario(**data)
                db.add(nuevo_tipo)
                added_count += 1
        
        if added_count > 0:
            db.commit()
            logger.info(f"Tabla tipo_usuarios poblada exitosamente. Se añadieron {added_count} registros.")
        else:
            logger.info("No fue necesario añadir nuevos tipos de usuario.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error al poblar la tabla tipo_usuarios: {e}", exc_info=True)

def populate_tipo_notas(db: Session):
    """
    Puebla la tabla tipo_notas con datos predefinidos.
    """
    logger.info("Poblando tabla tipo_notas...")
    tipo_notas_data = [
        {"cod_tipo_nota": 1, "descripcion": "Texto"},
        {"cod_tipo_nota": 2, "descripcion": "Imagen"},
        {"cod_tipo_nota": 3, "descripcion": "Video"},
        {"cod_tipo_nota": 4, "descripcion": "Enlace"},
    ]

    try:
        added_count = 0
        for data in tipo_notas_data:
            existing_tipo = db.query(TipoNota).filter_by(cod_tipo_nota=data["cod_tipo_nota"]).first()
            if not existing_tipo:
                nuevo_tipo = TipoNota(**data)
                db.add(nuevo_tipo)
                added_count += 1
        
        if added_count > 0:
            db.commit()
            logger.info(f"Tabla tipo_notas poblada exitosamente. Se añadieron {added_count} registros.")
        else:
            logger.info("No fue necesario añadir nuevos tipos de notas.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error al poblar la tabla tipo_notas: {e}", exc_info=True)

def populate_paises(db: Session):
    """
    Puebla la tabla paises con algunos datos básicos.
    """
    logger.info("Poblando tabla paises...")
    paises_data = [
        {"cod_pais": "AR", "descripcion": "Argentina"},
        {"cod_pais": "US", "descripcion": "Estados Unidos"},
        {"cod_pais": "GB", "descripcion": "Reino Unido"},
        {"cod_pais": "ES", "descripcion": "España"},
        {"cod_pais": "MX", "descripcion": "México"},
        {"cod_pais": "BR", "descripcion": "Brasil"},
        {"cod_pais": "CL", "descripcion": "Chile"},
        {"cod_pais": "UY", "descripcion": "Uruguay"},
        {"cod_pais": "CO", "descripcion": "Colombia"},
        {"cod_pais": "PE", "descripcion": "Perú"},
    ]

    try:
        added_count = 0
        for data in paises_data:
            existing_pais = db.query(Paises).filter_by(cod_pais=data["cod_pais"]).first()
            if not existing_pais:
                nuevo_pais = Paises(**data)
                db.add(nuevo_pais)
                added_count += 1
        
        if added_count > 0:
            db.commit()
            logger.info(f"Tabla paises poblada exitosamente. Se añadieron {added_count} registros.")
        else:
            logger.info("No fue necesario añadir nuevos países.")
    except Exception as e:
        db.rollback()
        logger.error(f"Error al poblar la tabla paises: {e}", exc_info=True)

def init_database():
    """
    Inicializa la base de datos y puebla las tablas con datos iniciales.
    """
    logger.info("Inicializando base de datos...")
    
    try:
        # Crear todas las tablas definidas en models.py
        logger.info("Creando tablas...")
        init_db()
        logger.info("Tablas creadas exitosamente.")
        
        # Crear sesión de base de datos
        db = SessionLocal()
        try:
            # Poblar tablas de referencia
            populate_tipo_usuarios(db)
            populate_tipo_notas(db)
            populate_paises(db)
            
            logger.info("Inicialización de base de datos completada exitosamente.")
        finally:
            db.close()
            logger.info("Sesión de base de datos cerrada.")
    
    except Exception as e:
        logger.error(f"Error durante la inicialización de la base de datos: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    logger.info("Iniciando script de inicialización de base de datos...")
    init_database()
    logger.info("Proceso de inicialización completado.")