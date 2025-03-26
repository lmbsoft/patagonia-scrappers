from sqlalchemy import create_engine, Column, Integer, String, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Post(Base):
    __tablename__ = 'post'
    id = Column(Integer, primary_key=True)
    user = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    timestamp = Column(String, nullable=False)

DATABASE_URL = "sqlite:///patagonia_datos.db"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def init_db():
    Base.metadata.create_all(bind=engine)