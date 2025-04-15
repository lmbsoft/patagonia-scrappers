import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

USER = os.getenv("DB_USER", "tu_usuario")
PASSWORD = os.getenv("DB_PASSWORD", "tu_contraseña")
HOST = os.getenv("DB_HOST", "db")
DB = os.getenv("DB_NAME", "patagonia_db")

DATABASE_URL = f"postgresql://{USER}:{PASSWORD}@{HOST}/{DB}"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
