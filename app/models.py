import os
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, Table, Float, Boolean
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from datetime import datetime

# Configuración de la base de datos PostgreSQL mediante variables de entorno
USER = os.getenv("DB_USER", "tu_usuario")
PASSWORD = os.getenv("DB_PASSWORD", "tu_contraseña")
HOST = os.getenv("DB_HOST", "db")
DB = os.getenv("DB_NAME", "patagonia_db")

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DB}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class TipoUsuario(Base):
    __tablename__ = 'tipo_usuarios'
    cod_tipo_usuario = Column(String, primary_key=True)
    descripcion = Column(String, nullable=False)

class TipoNota(Base):
    __tablename__ = 'tipo_notas'
    cod_tipo_nota = Column(Integer, primary_key=True)
    descripcion = Column(String, nullable=False)  # (texto, imagen, video)

class Paises(Base):
    __tablename__ = 'paises'
    cod_pais = Column(String, primary_key=True)
    descripcion = Column(String, nullable=False)

class Usuario(Base):
    __tablename__ = 'usuarios'
    id_usuario = Column(String, primary_key=True)  # Cambiado a String para coincidir con los datos
    nombre = Column(String, nullable=False)
    handle = Column(String, nullable=False)
    cod_tipo_usuario = Column(String, ForeignKey('tipo_usuarios.cod_tipo_usuario'))  # Asegurado como String
    verificado = Column(Boolean, default=False)
    seguidores = Column(Integer)
    cod_pais = Column(String, ForeignKey('paises.cod_pais'), nullable=True)  # Permitir valores nulos explícitamente
    idioma_principal = Column(String)
    score_credibilidad = Column(Float, nullable=True)  # Permitir valores nulos

    # Relationships
    tipo_usuario = relationship("TipoUsuario")
    pais = relationship("Paises")
    notas = relationship("NotasXUsuario", back_populates="usuario")

class NotasXUsuario(Base):
    __tablename__ = 'notas_x_usuario'
    id_nota = Column(Integer, primary_key=True)
    contenido = Column(Text, nullable=False)
    fecha_publicacion = Column(DateTime, nullable=False)
    id_usuario = Column(String, ForeignKey('usuarios.id_usuario'), nullable=False)
    cod_tipo_nota = Column(Integer, ForeignKey('tipo_notas.cod_tipo_nota'), nullable=False)
    url_nota = Column(String)
    engagement_total = Column(Integer)  # likes + reposts + replies + (shares si existieran)
    score_analisis_sentimiento_nlp = Column(Float)
    sentimiento = Column(String)  # (positivo, negativo, neutral)
    score_sentimiento = Column(Float)  # valor entre -1 y 1

    # Relationships
    usuario = relationship("Usuario", back_populates="notas")
    tipo_nota = relationship("TipoNota")
    empresas = relationship("EmpresasXNota", back_populates="nota", cascade="all, delete-orphan")
    llm_outputs = relationship(
        "NotasXUsuarioGemma",
        back_populates="nota",
        cascade="all, delete-orphan"
    )
        

# New table to store LLM outputs
class NotasXUsuarioGemma(Base):
    __tablename__ = 'notas_x_usuario_gemma'
    id = Column(Integer, primary_key=True, autoincrement=True)
    id_nota = Column(Integer, ForeignKey('notas_x_usuario.id_nota', ondelete='CASCADE'), nullable=False)
    valoracion_llm = Column(String, nullable=False)  # 'positivo','negativo','neutral'
    relevante_economia = Column(Boolean, nullable=False)  # True=1/False=0
    AAPLD = Column(Boolean, default=False)
    DESPD = Column(Boolean, default=False)
    KOD = Column(Boolean, default=False)
    MELID = Column(Boolean, default=False)
    MSFTD = Column(Boolean, default=False)
    NVDAD = Column(Boolean, default=False)
    TEND = Column(Boolean, default=False)
    VISTD = Column(Boolean, default=False)
    XOMD = Column(Boolean, default=False)

    nota = relationship("NotasXUsuario", back_populates="llm_outputs")

class Empresas(Base):
    __tablename__ = 'empresas'
    id_empresa = Column(Integer, primary_key=True)
    nombre = Column(String, nullable=False)
    ticker = Column(String)
    sector = Column(String)
    capitalizacion_bursatil = Column(Float)

    # Relationships
    cotizaciones = relationship("CotizacionXEmpresa", back_populates="empresa")
    eventos = relationship("EventosFinancierosXEmpresa", back_populates="empresa")

class EmpresasXNota(Base):
    __tablename__ = 'empresas_x_nota'
    id_nota = Column(Integer, ForeignKey('notas_x_usuario.id_nota', ondelete='CASCADE'), primary_key=True)
    id_empresa = Column(Integer, ForeignKey('empresas.id_empresa'), primary_key=True)
    fuente_extraccion = Column(String)  # modelo que detectó la mención
    tipo_mencion = Column(String)  # (directa, ticker, similar)
    contexto = Column(Text)  # fragmento donde aparece
    impacto_calculado = Column(Float)  # Variación del precio después de la nota (ej: +3.5%)
    tipo_impacto = Column(String)  # (positivo, negativo, neutro)
    tiempo_reaccion = Column(Float)  # Minutos/hora después de la nota donde se observó el impacto

    # Relationships
    nota = relationship("NotasXUsuario", back_populates="empresas")
    empresa = relationship("Empresas")

class TipoEvento(Base):
    __tablename__ = 'tipo_evento'
    id_tipo_evento = Column(Integer, primary_key=True)
    nombre_evento = Column(String, nullable=False)  # ej: resultados trimestrales, cambio de CEO, demanda legal
    descripcion = Column(String)

class EventosFinancierosXEmpresa(Base):
    __tablename__ = 'eventos_financieros_x_empresa'
    id_evento = Column(Integer, primary_key=True)
    id_empresa = Column(Integer, ForeignKey('empresas.id_empresa'))
    id_tipo_evento = Column(Integer, ForeignKey('tipo_evento.id_tipo_evento'))
    fecha_evento = Column(DateTime)
    descripcion = Column(Text)
    impacto_esperado = Column(String)  # valores posibles: positivo,negativo,neutral,incierto,depende

    # Relationships
    empresa = relationship("Empresas", back_populates="eventos")
    tipo_evento = relationship("TipoEvento")

class CotizacionXEmpresa(Base):
    __tablename__ = 'cotizacion_x_empresa'
    id_cotizacion = Column(Integer, primary_key=True)
    id_empresa = Column(Integer, ForeignKey('empresas.id_empresa'))
    fecha = Column(DateTime, nullable=False)
    precio_apertura = Column(Float)
    precio_cierre = Column(Float)
    precio_max = Column(Float)
    precio_min = Column(Float)
    volumen_operado = Column(Float)
    variacion_porcentaje = Column(Float)

    # Relationships
    empresa = relationship("Empresas", back_populates="cotizaciones")

class TablaPPI(Base):
    __tablename__ = 'tabla_ppi'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    Date = Column(DateTime, nullable=False)
    Price = Column(Float)
    Volume = Column(Float)
    Opening = Column(Float)
    Min = Column(Float)
    Max = Column(Float)
    ticker = Column(String, nullable=False)
    settlement = Column(String)
    instrument_type = Column(String)
    currency = Column(String)
    variacion_diaria = Column(Float)

    def __repr__(self):
        return f"<TablaPPI(Date='{self.Date}', ticker='{self.ticker}', Price={self.Price})>"

class TablaPostsBluesky(Base):
    __tablename__ = 'tabla_posts_bluesky'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    actor_handle = Column(String, nullable=False)
    uri = Column(String, nullable=False)
    text = Column(Text)
    created_at = Column(DateTime, nullable=False)
    likes = Column(Integer, default=0)
    reposts = Column(Integer, default=0)
    replies = Column(Integer, default=0)
    
    # Nuevos campos para análisis de sentimiento y engagement
    engagement = Column(Integer, default=0)
    textblob_sentiment = Column(Float)
    textblob_sentiment_label = Column(String)
    vader_sentiment = Column(Float)
    vader_sentiment_label = Column(String)
    
    def __repr__(self):
        return f"<TablaPostsBluesky(actor_handle='{self.actor_handle}', created_at='{self.created_at}')>"

def init_db():
    Base.metadata.create_all(bind=engine)
