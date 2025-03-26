import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from src.post import Base, Post

DATABASE_URL = "sqlite:///test_patagonia_datos.db"

@pytest.fixture(scope='module')
def engine():
    return create_engine(DATABASE_URL)

@pytest.fixture(scope='module')
def tables(engine):
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)

@pytest.fixture(scope='function')
def dbsession(engine, tables):
    connection = engine.connect()
    transaction = connection.begin()
    Session = sessionmaker(bind=connection)
    session = Session()
    yield session
    session.close()
    transaction.rollback()
    connection.close()

def test_create_social_network_post(dbsession):
    new_post = Post(user="test_user", content="This is a test post", timestamp="2023-01-01T00:00:00Z")
    dbsession.add(new_post)
    dbsession.commit()
    assert new_post.id is not None