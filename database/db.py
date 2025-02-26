from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

connector = Connector()

def get_engine():
    def getconn():
        conn = connector.connect(
            "innate-works-442820-f3:europe-west4:taxibee-db",
            "pymysql",
            user="root",
            password="yxPgnQKog|G2:+E*",
            db="taxibee-database",
        )
        return conn
    engine = create_engine("mysql+pymysql://",creator=getconn)
    return engine

engine = get_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()