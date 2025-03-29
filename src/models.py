from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, ForeignKey, Table, Float, Boolean
from sqlalchemy.orm import sessionmaker, relationship, declarative_base
from datetime import datetime

Base = declarative_base()

class TipoUsuario(Base):
    __tablename__ = 'tipo_usuarios'
    cod_tipo_usuario = Column(Integer, primary_key=True)
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
    id_usuario = Column(Integer, primary_key=True)
    nombre = Column(String, nullable=False)
    handle = Column(String, nullable=False)
    cod_tipo_usuario = Column(Integer, ForeignKey('tipo_usuarios.cod_tipo_usuario'))
    verificado = Column(Boolean, default=False)
    seguidores = Column(Integer)
    cod_pais = Column(String, ForeignKey('paises.cod_pais'))
    idioma_principal = Column(String)
    score_credibilidad = Column(Float)  # (0,1)

    # Relationships
    tipo_usuario = relationship("TipoUsuario")
    pais = relationship("Paises")
    notas = relationship("NotasXUsuario", back_populates="usuario")

class NotasXUsuario(Base):
    __tablename__ = 'notas_x_usuario'
    id_nota = Column(Integer, primary_key=True)
    contenido = Column(Text, nullable=False)
    fecha_publicacion = Column(DateTime, nullable=False)
    id_usuario = Column(Integer, ForeignKey('usuarios.id_usuario'), nullable=False)
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

# Database configuration
DATABASE_URL = "sqlite:///patagonia_datos.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine) 