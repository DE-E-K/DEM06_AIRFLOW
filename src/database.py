"""
Database connection and utility functions for MySQL and PostgreSQL
"""
import os
from typing import Optional
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
import logging
import pandas as pd
from io import StringIO
import csv
import psycopg2.extras

logger = logging.getLogger(__name__)


class DatabaseConnection:
    """Base class for database connections"""
    
    def __init__(self, user: str, password: str, host: str, port: int, database: str, dialect: str):
        """
        Initialize database connection parameters
        
        Args:
            user: Database user
            password: Database password
            host: Database host
            port: Database port
            database: Database name
            dialect: SQLAlchemy dialect (mysql+pymysql, postgresql, etc.)
        """
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database
        self.dialect = dialect
        self.engine: Optional[Engine] = None
    
    def get_connection_string(self) -> str:
        """Generate connection string for SQLAlchemy"""
        raise NotImplementedError("Subclasses must implement get_connection_string()")
    
    def create_engine(self) -> Engine:
        """Create and return SQLAlchemy engine with connection pooling"""
        raise NotImplementedError("Subclasses must implement create_engine()")
    
    def get_engine(self) -> Engine:
        """Get or create engine"""
        if self.engine is None:
            self.engine = self.create_engine()
        return self.engine
    
    def close(self):
        """Close the engine connection pool"""
        if self.engine:
            self.engine.dispose()
            logger.info(f"Closed {self.dialect} connection")


class MySQLConnection(DatabaseConnection):
    """MySQL database connection handler"""
    
    def __init__(self, user: str, password: str, host: str = "localhost", port: int = 3306, database: str = "flight_staging"):
        """Initialize MySQL connection"""
        super().__init__(user, password, host, port, database, "mysql+mysqldb")
    
    def get_connection_string(self) -> str:
        """Generate MySQL connection string"""
        return f"{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def create_engine(self) -> Engine:
        """Create MySQL engine with pooling and timeouts"""
        conn_string = self.get_connection_string()
        logger.info(f"Creating MySQL engine for {self.host}:{self.port}/{self.database}")
        
        engine = create_engine(
            conn_string,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            connect_args={"connect_timeout": 10}
        )
        return engine


class PostgreSQLConnection(DatabaseConnection):
    """PostgreSQL database connection handler"""
    
    def __init__(self, user: str, password: str, host: str = "localhost", port: int = 5432, database: str = "flight_analytics"):
        """Initialize PostgreSQL connection"""
        super().__init__(user, password, host, port, database, "postgresql+psycopg2")
    
    def get_connection_string(self) -> str:
        """Generate PostgreSQL connection string"""
        return f"{self.dialect}://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def create_engine(self) -> Engine:
        """Create PostgreSQL engine with pooling and timeouts"""
        conn_string = self.get_connection_string()
        logger.info(f"Creating PostgreSQL engine for {self.host}:{self.port}/{self.database}")
        
        engine = create_engine(
            conn_string,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            pool_recycle=3600,
            connect_args={"connect_timeout": 10}
        )
        return engine


def get_mysql_engine(
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: int = 3306,
    database: str = "flight_staging"
) -> Engine:
    """
    Get MySQL SQLAlchemy engine
    
    Uses environment variables if not provided:
    - MYSQL_USER
    - MYSQL_PASSWORD
    - MYSQL_HOST
    - MYSQL_PORT
    - MYSQL_DATABASE
    """
    user = user or os.getenv("MYSQL_USER", "airflow_user")
    password = password or os.getenv("MYSQL_PASSWORD", "airflow_password")
    host = host or os.getenv("MYSQL_HOST", "localhost")
    port = int(os.getenv("MYSQL_PORT", port))
    database = database or os.getenv("MYSQL_DATABASE", database)
    
    mysql_conn = MySQLConnection(user, password, host, port, database)
    return mysql_conn.get_engine()


def get_postgres_engine(
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: int = 5432,
    database: str = "flight_analytics"
) -> Engine:
    """
    Get PostgreSQL SQLAlchemy engine
    
    Uses environment variables if not provided:
    - POSTGRES_USER
    - POSTGRES_PASSWORD
    - POSTGRES_HOST
    - POSTGRES_PORT
    - POSTGRES_DATABASE
    """
    user = user or os.getenv("POSTGRES_USER", "airflow_user")
    password = password or os.getenv("POSTGRES_PASSWORD", "airflow_password")
    host = host or os.getenv("POSTGRES_HOST", "localhost")
    port = int(os.getenv("POSTGRES_PORT", port))
    database = database or os.getenv("POSTGRES_DATABASE", database)
    
    postgres_conn = PostgreSQLConnection(user, password, host, port, database)
    return postgres_conn.get_engine()


def table_exists(engine: Engine, table_name: str, schema: Optional[str] = None) -> bool:
    """
    Check if a table exists in the database
    
    Args:
        engine: SQLAlchemy engine
        table_name: Name of the table
        schema: Schema name (optional)
    
    Returns:
        True if table exists, False otherwise
    """
    inspector = inspect(engine)
    tables = inspector.get_table_names(schema=schema)
    return table_name in tables


def execute_query(engine: Engine, query: str, params: Optional[dict] = None):
    """
    Execute a SQL query
    
    Args:
        engine: SQLAlchemy engine
        query: SQL query string
        params: Query parameters
    
    Returns:
        Query result
    """
    with engine.begin() as connection:
        result = connection.execute(text(query), params or {})
        return result


def bulk_insert_postgres(engine: Engine, df: pd.DataFrame, table_name: str, if_exists: str = 'append') -> int:
    """
    Bulk insert DataFrame into PostgreSQL using COPY or execute_values for performance
    
    Args:
        engine: SQLAlchemy engine
        df: DataFrame to insert
        table_name: Target table
        if_exists: 'append', 'replace', or 'fail'
        
    Returns:
        Number of rows inserted
    """
    if df.empty:
        return 0
        
    # Get raw connection
    raw_conn = engine.raw_connection()
    try:
        cur = raw_conn.cursor()
        
        # Handle table truncation if replace
        if if_exists == 'replace':
            cur.execute(f"TRUNCATE TABLE {table_name}")
            raw_conn.commit()
        
        # Prepare data columns to match table
        # This assumes DF columns match table columns
        columns = list(df.columns)
        
        # Convert DataFrame to list of tuples for execute_values
        # This is safer than COPY for potential escaping issues, and efficient enough for batch loading
        data = [tuple(x) for x in df.to_numpy()]
        
        # Generate INSERT query
        cols_str = ','.join(columns)
        base_query = f"INSERT INTO {table_name} ({cols_str}) VALUES %s"
        
        if if_exists == 'ignore':
            # Use ON CONFLICT DO NOTHING
            # Note: This requires a unique constraint on the table matching the conflict
            query = f"{base_query} ON CONFLICT DO NOTHING"
        else:
            query = base_query
        
        # Use execute_values 
        psycopg2.extras.execute_values(
            cur, query, data, template=None, page_size=1000
        )
        
        raw_conn.commit()
        logger.info(f"Bulk inserted {len(data)} rows into {table_name}")
        return len(data)
        
    except Exception as e:
        raw_conn.rollback()
        logger.error(f"Error in bulk insert: {e}")
        raise e
    finally:
        raw_conn.close()



if __name__ == "__main__":
    # Test connection
    logging.basicConfig(level=logging.INFO)
    
    try:
        mysql_engine = get_mysql_engine()
        print("MySQL connection successful")
        mysql_engine.dispose()
    except Exception as e:
        print(f"MySQL connection failed: {e}")
    
    try:
        postgres_engine = get_postgres_engine()
        print("PostgreSQL connection successful")
        postgres_engine.dispose()
    except Exception as e:
        print(f"PostgreSQL connection failed: {e}")
