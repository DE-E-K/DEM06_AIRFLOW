"""
Data ingestion - Load CSV data into MySQL staging table
"""
import pandas as pd
from pathlib import Path
from typing import Tuple, Dict, Any
from sqlalchemy.engine import Engine
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

# Explicit mapping from CSV headers to Internal DB columns
COLUMN_MAPPING = {
    'Airline': 'airline',
    'Source': 'source',
    'Source Name': 'source_name',
    'Destination': 'destination',
    'Destination Name': 'destination_name',
    'Departure Date & Time': 'departure_date',
    'Arrival Date & Time': 'arrival_date',
    'Duration (hrs)': 'duration_hours',
    'Stopovers': 'stopovers',
    'Aircraft Type': 'aircraft_type',
    'Class': 'class',
    'Booking Source': 'booking_source',
    'Base Fare (BDT)': 'base_fare',
    'Tax & Surcharge (BDT)': 'tax_surcharge',
    'Total Fare (BDT)': 'total_fare',
    'Seasonality': 'seasonality',
    'Days Before Departure': 'days_before_departure'
}

def load_csv_to_mysql(
    csv_path: str,
    target_table: str,
    mysql_engine: Engine,
    chunksize: int = 1000,
    if_exists: str = "append"
) -> Dict[str, Any]:
    """
    Load CSV data into MySQL staging table
    
    Args:
        csv_path: Path to CSV file
        target_table: Target table name in MySQL
        mysql_engine: SQLAlchemy engine for MySQL
        chunksize: Number of rows per batch insert
        if_exists: 'fail', 'replace', or 'append'
    
    Returns:
        Dictionary with ingestion metadata:
        - total_rows: Total rows loaded
        - ingestion_timestamp: When ingestion occurred
        - source_file: CSV filename
        - status: 'SUCCESS' or 'FAILED'
        - error_message: Error details if failed
    """
    
    metadata = {
        "total_rows": 0,
        "ingestion_timestamp": datetime.now().isoformat(),
        "source_file": Path(csv_path).name,
        "status": "FAILED",
        "error_message": None
    }
    
    try:
        # Validate CSV exists
        csv_file = Path(csv_path)
        if not csv_file.exists():
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
        
        if not csv_file.is_file():
            raise ValueError(f"Path is not a file: {csv_path}")
        
        logger.info(f"Loading CSV from {csv_path} to table {target_table}")
        
        # Read CSV in chunks
        total_rows = 0
        
        for chunk_num, chunk_df in enumerate(pd.read_csv(csv_path, chunksize=chunksize), 1):
            # Rename columns using explicit mapping
            # First clean columns to match mapping keys if necessary, or just use mapping directly
            # The CSV likely has exact keys as in COLUMN_MAPPING
            
            # Create a normalized version of mapping keys to handle potential whitespace issues
            current_cols = chunk_df.columns.tolist()
            new_cols = {}
            
            for col in current_cols:
                if col in COLUMN_MAPPING:
                    new_cols[col] = COLUMN_MAPPING[col]
                else:
                    # Fallback normalization just in case
                    normalized = col.lower().replace(' ', '_').replace('&', 'and')
                    new_cols[col] = normalized
            
            chunk_df.rename(columns=new_cols, inplace=True)
            
            # Add metadata columns
            chunk_df["source_file"] = csv_file.name
            chunk_df["ingestion_timestamp"] = datetime.utcnow()
            chunk_df["record_status"] = "VALID"  # Will be updated during validation
            chunk_df["validation_errors"] = None
            
            # Insert chunk
            chunk_df.to_sql(
                target_table,
                con=mysql_engine,
                if_exists="append" if chunk_num > 1 else if_exists,
                index=False,
                method="multi"
            )
            
            chunk_rows = len(chunk_df)
            total_rows += chunk_rows
            logger.info(f"  Chunk {chunk_num}: {chunk_rows} rows inserted")
        
        metadata["total_rows"] = total_rows
        metadata["status"] = "SUCCESS"
        
        logger.info(f"Successfully loaded {total_rows} rows into {target_table}")
        
    except Exception as e:
        error_msg = f"Error loading CSV: {str(e)}"
        metadata["error_message"] = error_msg
        logger.error(error_msg)
    
    return metadata


def get_staging_data(mysql_engine: Engine, table_name: str = "raw_flight_data") -> pd.DataFrame:
    """
    Retrieve all data from staging table
    
    Args:
        mysql_engine: SQLAlchemy engine for MySQL
        table_name: Name of staging table
    
    Returns:
        DataFrame with all staging data
    """
    query = f"SELECT * FROM {table_name} WHERE record_status != 'INVALID'"
    df = pd.read_sql(query, con=mysql_engine)
    logger.info(f"Retrieved {len(df)} rows from {table_name}")
    return df


def get_staging_data_for_validation(mysql_engine: Engine, table_name: str = "raw_flight_data") -> pd.DataFrame:
    """
    Retrieve all data from staging table for validation (including flagged records)
    
    Args:
        mysql_engine: SQLAlchemy engine for MySQL
        table_name: Name of staging table
    
    Returns:
        DataFrame with all staging data
    """
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, con=mysql_engine)
    logger.info(f"Retrieved {len(df)} rows from {table_name} for validation")
    return df


def update_staging_record_status(
    mysql_engine: Engine,
    record_ids: list,
    status: str,
    error_message: str = None,
    table_name: str = "raw_flight_data"
) -> int:
    """
    Update record status in staging table
    
    Args:
        mysql_engine: SQLAlchemy engine for MySQL
        record_ids: List of record IDs to update
        status: New status ('VALID', 'INVALID', 'FLAGGED')
        error_message: Error details
        table_name: Name of staging table
    
    Returns:
        Number of rows updated
    """
    if not record_ids:
        return 0
    
    ids_str = ",".join(map(str, record_ids))
    query = f"""
    UPDATE {table_name}
    SET record_status = %s, validation_errors = %s
    WHERE id IN ({ids_str})
    """
    
    with mysql_engine.connect() as conn:
        result = conn.execute(query, (status, error_message))
        conn.commit()
        rows_updated = result.rowcount
    
    logger.info(f"Updated {rows_updated} records to status '{status}'")
    return rows_updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    from database import get_mysql_engine
    
    # Test ingestion
    mysql_engine = get_mysql_engine()
    metadata = load_csv_to_mysql(
        csv_path="./data/Flight_Price_Dataset_of_Bangladesh.csv",
        target_table="raw_flight_data",
        mysql_engine=mysql_engine
    )
    print(f"Ingestion metadata: {metadata}")
    mysql_engine.dispose()
