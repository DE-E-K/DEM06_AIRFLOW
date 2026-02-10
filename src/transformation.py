"""
Data transformation - Clean, enrich, and prepare data for KPI computation
"""
import pandas as pd
from datetime import datetime
from typing import Tuple
import logging

logger = logging.getLogger(__name__)


def calculate_total_fare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Total Fare if missing or verify existing calculation
    
    Args:
        df: Input DataFrame with base_fare and tax_surcharge columns
    
    Returns:
        DataFrame with total_fare column populated/verified
    """
    df = df.copy()
    
    try:
        base = pd.to_numeric(df['base_fare'], errors='coerce')
        tax = pd.to_numeric(df['tax_surcharge'], errors='coerce')
        
        # If total_fare exists, verify it
        if 'total_fare' in df.columns:
            total = pd.to_numeric(df['total_fare'], errors='coerce')
            
            # Check for inconsistencies
            tolerance = 0.01
            incorrect = abs((base + tax) - total) > tolerance
            
            if incorrect.any():
                logger.warning(f"Found {incorrect.sum()} rows with inconsistent totals, recalculating...")
                df.loc[incorrect, 'total_fare'] = (base + tax)
            
            # Fill any missing total_fare values
            missing = df['total_fare'].isna()
            df.loc[missing, 'total_fare'] = base + tax
        else:
            # Calculate total_fare
            df['total_fare'] = base + tax
            logger.info("Created total_fare column from base_fare + tax_surcharge")
        
        logger.info("Total fare calculation completed")
        
    except Exception as e:
        logger.error(f"Error calculating total fare: {e}")
    
    return df


def classify_season(date_obj) -> str:
    """
    Classify date into season for fare analysis
    
    Seasons:
    - PEAK_EID: Around Islamic Eid holidays (~2-3 weeks in May and July, approximate)
    - PEAK_WINTER: December 1 - January 31 (winter holidays)
    - NON_PEAK: All other dates
    
    Args:
        date_obj: Date object or datetime
    
    Returns:
        Season classification string
    """
    try:
        if pd.isna(date_obj):
            return 'UNKNOWN'
        
        # Convert to datetime if string
        if isinstance(date_obj, str):
            date_obj = pd.to_datetime(date_obj)
        
        month = date_obj.month
        day = date_obj.day
        
        # Eid al-Fitr (May) - approximate: May 1-31 for MVP
        if month == 5:
            return 'PEAK_EID'
        
        # Eid al-Adha (July) - approximate: July 1-31 for MVP
        if month == 7:
            return 'PEAK_EID'
        
        # Winter holidays (Dec 1 - Jan 31)
        if month in [12, 1]:
            return 'PEAK_WINTER'
        
        # Non-peak
        return 'NON_PEAK'
    
    except Exception as e:
        logger.warning(f"Error classifying season for {date_obj}: {e}")
        return 'UNKNOWN'


def clean_and_enrich(df: pd.DataFrame, extract_date_col: str = None) -> pd.DataFrame:
    """
    Clean and enrich data with derived columns
    
    Args:
        df: Input DataFrame
        extract_date_col: Column name containing date information (optional)
    
    Returns:
        Enriched DataFrame with transformation applied
    """
    df = df.copy()
    
    logger.info(f"Starting data transformation on {len(df)} records...")
    
    # 1. Calculate/verify total fare
    df = calculate_total_fare(df)
    
    # 2. Clean string columns
    string_cols = ['airline', 'source', 'destination']
    for col in string_cols:
        if col in df.columns:
            # Trim whitespace
            df[col] = df[col].astype(str).str.strip()
            # Title case
            df[col] = df[col].astype(str).str.title()
            logger.info(f"Cleaned {col} column (trimmed, title-cased)")
    
    # 3. Ensure numeric columns are float
    numeric_cols = ['base_fare', 'tax_surcharge', 'total_fare']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # 4. Create or extract flight_date column
    if 'flight_date' not in df.columns:
        if extract_date_col and extract_date_col in df.columns:
            try:
                df['flight_date'] = pd.to_datetime(df[extract_date_col]).dt.date
                logger.info(f"Extracted flight_date from {extract_date_col}")
            except Exception as e:
                logger.warning(f"Could not extract flight_date: {e}")
                df['flight_date'] = None
        else:
            # Use current date as placeholder if no date column
            df['flight_date'] = datetime.now().date()
            logger.info("Used current date as flight_date (no date column in source)")
    
    # 5. Classify season
    if 'flight_date' in df.columns:
        df['season'] = df['flight_date'].apply(classify_season)
        logger.info("Added season classification")
    else:
        df['season'] = 'UNKNOWN'
    
    # 6. Mark data validity (will be updated from validation module)
    if 'is_valid' not in df.columns:
        df['is_valid'] = True
        logger.info("Added is_valid column (default: True)")
    
    # 7. Add transformation timestamp
    df['loaded_timestamp'] = datetime.utcnow()
    
    logger.info(f"Transformation completed for {len(df)} records")
    
    return df


def remove_duplicates(df: pd.DataFrame, subset: list = None, keep: str = 'first') -> Tuple[pd.DataFrame, int]:
    """
    Remove duplicate records
    
    Args:
        df: Input DataFrame
        subset: Columns to consider for identifying duplicates
        keep: 'first', 'last', or False (remove all duplicates)
    
    Returns:
        Tuple of (deduplicated_df, num_duplicates_removed)
    """
    df = df.copy()
    
    if subset is None:
        subset = ['airline', 'source', 'destination', 'base_fare', 'tax_surcharge']
    
    initial_count = len(df)
    df = df.drop_duplicates(subset=subset, keep=keep)
    final_count = len(df)
    removed_count = initial_count - final_count
    
    if removed_count > 0:
        logger.warning(f"Removed {removed_count} duplicate records")
    else:
        logger.info("No duplicate records found")
    
    return df, removed_count


def handle_missing_values(df: pd.DataFrame, strategy: str = 'drop') -> Tuple[pd.DataFrame, int]:
    """
    Handle missing/null values
    
    Args:
        df: Input DataFrame
        strategy: 'drop' (remove rows), 'median' (impute with median for numeric), 
                 'forward_fill', or 'skip'
    
    Returns:
        Tuple of (processed_df, num_rows_changed)
    """
    df = df.copy()
    initial_count = len(df)
    
    if strategy == 'drop':
        # Drop rows with any null in required columns
        required_cols = ['airline', 'source', 'destination', 'base_fare', 'tax_surcharge']
        df = df.dropna(subset=required_cols)
        rows_changed = initial_count - len(df)
        logger.info(f"Dropped {rows_changed} rows with missing required values")
    
    elif strategy == 'median':
        # Impute numeric columns with median
        numeric_cols = ['base_fare', 'tax_surcharge', 'total_fare']
        for col in numeric_cols:
            if col in df.columns:
                median_val = pd.to_numeric(df[col], errors='coerce').median()
                if pd.notna(median_val):
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(median_val)
        
        # Drop rows with missing categorical values
        cat_cols = ['airline', 'source', 'destination']
        rows_before = len(df)
        df = df.dropna(subset=cat_cols)
        rows_changed = rows_before - len(df)
        logger.info(f"Imputed numeric columns, dropped {rows_changed} rows with missing categorical values")
    
    elif strategy == 'forward_fill':
        df = df.fillna(method='ffill')
        rows_changed = initial_count - len(df[df.isna().any(axis=1)])
        logger.info(f"Applied forward fill for missing values")
    
    elif strategy == 'skip':
        logger.info("Skipping missing value handling")
        rows_changed = 0
    
    else:
        logger.warning(f"Unknown strategy: {strategy}, skipping")
        rows_changed = 0
    
    return df, rows_changed


def generate_transformation_summary(original_df: pd.DataFrame, transformed_df: pd.DataFrame) -> dict:
    """
    Generate summary statistics of transformation
    
    Args:
        original_df: Original DataFrame before transformation
        transformed_df: Transformed DataFrame
    
    Returns:
        Dictionary with transformation summary
    """
    summary = {
        "original_record_count": len(original_df),
        "final_record_count": len(transformed_df),
        "records_removed": len(original_df) - len(transformed_df),
        "new_columns_added": list(set(transformed_df.columns) - set(original_df.columns)),
        "transformation_timestamp": datetime.utcnow().isoformat()
    }
    
    # Calculate fare statistics
    if 'total_fare' in transformed_df.columns:
        fare_col = pd.to_numeric(transformed_df['total_fare'], errors='coerce')
        summary["fare_statistics"] = {
            "min": fare_col.min(),
            "max": fare_col.max(),
            "mean": fare_col.mean(),
            "median": fare_col.median(),
            "std_dev": fare_col.std()
        }
    
    # Season distribution
    if 'season' in transformed_df.columns:
        summary["season_distribution"] = transformed_df['season'].value_counts().to_dict()
    
    logger.info(f"Transformation summary: {summary['original_record_count']} records -> {summary['final_record_count']} records")
    
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
