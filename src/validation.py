"""
Data validation - Check data quality and integrity
"""
import pandas as pd
from typing import Dict, List, Tuple, Any
from sqlalchemy.engine import Engine
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class ValidationReport:
    """Container for validation check results"""
    
    def __init__(self):
        self.checks_performed: List[Dict[str, Any]] = []
        self.total_records = 0
        self.valid_records = 0
        self.invalid_records = 0
        self.flagged_records = 0
        self.validation_timestamp = datetime.utcnow()
    
    def add_check(self, check_name: str, check_type: str, passed: int, failed: int, 
                  error_message: str = None):
        """Add a validation check result"""
        self.checks_performed.append({
            "check_name": check_name,
            "check_type": check_type,
            "records_passed": passed,
            "records_failed": failed,
            "error_message": error_message
        })
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert report to dictionary"""
        return {
            "validation_timestamp": self.validation_timestamp.isoformat(),
            "total_records": self.total_records,
            "valid_records": self.valid_records,
            "invalid_records": self.invalid_records,
            "flagged_records": self.flagged_records,
            "validity_percentage": (self.valid_records / self.total_records * 100) 
                                  if self.total_records > 0 else 0,
            "checks_performed": self.checks_performed
        }


def validate_required_columns(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate that all required columns exist
    
    Args:
        df: Input DataFrame
    
    Returns:
        Tuple of (is_valid, error_message)
    """
    # Expected internal column names after ingestion mapping
    required_columns = {
        'airline', 'source', 'destination', 
        'base_fare', 'tax_surcharge', 'total_fare',
        'departure_date'
    }
    
    actual_columns = set(df.columns)
    missing_columns = required_columns - actual_columns
    
    if missing_columns:
        error_msg = f"Missing required columns: {missing_columns}"
        logger.warning(error_msg)
        return False, error_msg
    
    logger.info("All required columns present")
    return True, ""


def validate_data_types(df: pd.DataFrame) -> Dict[str, List[int]]:
    """
    Validate data types for all columns
    
    Args:
        df: Input DataFrame
    
    Returns:
        Dictionary with field-level validation results
    """
    invalid_rows = {
        "airline": [],
        "source": [],
        "destination": [],
        "base_fare": [],
        "tax_surcharge": [],
        "total_fare": []
    }
    
    # Check numeric columns
    numeric_cols = ['base_fare', 'tax_surcharge', 'total_fare']
    for col in numeric_cols:
        if col in df.columns:
            # Try to convert to numeric
            non_numeric = pd.to_numeric(df[col], errors='coerce').isna() & df[col].notna()
            invalid_rows[col] = df[non_numeric].index.tolist()
            if len(invalid_rows[col]) > 0:
                logger.warning(f"{col}: {len(invalid_rows[col])} non-numeric values found")
    
    # Check string columns
    string_cols = ['airline', 'source', 'destination']
    for col in string_cols:
        if col in df.columns:
            # Check for empty strings
            empty_strings = (df[col].astype(str).str.strip() == '')
            invalid_rows[col] = df[empty_strings].index.tolist()
            if len(invalid_rows[col]) > 0:
                logger.warning(f"{col}: {len(invalid_rows[col])} empty values found")
    
    logger.info("Data type validation completed")
    return invalid_rows


def check_null_values(df: pd.DataFrame, allow_null_cols: List[str] = None) -> Dict[str, List[int]]:
    """
    Check for null/missing values
    
    Args:
        df: Input DataFrame
        allow_null_cols: Columns where nulls are allowed
    
    Returns:
        Dictionary with null value locations per column
    """
    allow_null_cols = allow_null_cols or []
    null_locations = {}
    
    for col in df.columns:
        if col not in allow_null_cols and col not in ['id', 'source_file', 'ingestion_timestamp', 
                                                        'record_status', 'validation_errors']:
            null_rows = df[df[col].isna()].index.tolist()
            if null_rows:
                null_locations[col] = null_rows
                logger.warning(f"{col}: {len(null_rows)} null values found")
    
    logger.info("Null value check completed")
    return null_locations


def check_negative_values(df: pd.DataFrame) -> Dict[str, List[int]]:
    """
    Check for negative values in fare columns
    
    Args:
        df: Input DataFrame
    
    Returns:
        Dictionary with negative value locations per column
    """
    negative_locations = {}
    fare_cols = ['base_fare', 'tax_surcharge', 'total_fare']
    
    for col in fare_cols:
        if col in df.columns:
            try:
                numeric_col = pd.to_numeric(df[col], errors='coerce')
                neg_rows = df[numeric_col < 0].index.tolist()
                if neg_rows:
                    negative_locations[col] = neg_rows
                    logger.warning(f"{col}: {len(neg_rows)} negative values found")
            except Exception as e:
                logger.error(f"Error checking {col} for negative values: {e}")
    
    logger.info("Negative value check completed")
    return negative_locations


def check_valid_cities(
    df: pd.DataFrame,
    valid_cities: List[str] = None
) -> Dict[str, List[int]]:
    """
    Check if source and destination cities are valid
    
    Args:
        df: Input DataFrame
        valid_cities: List of valid city names (optional whitelist)
    
    Returns:
        Dictionary with invalid city locations
    """
    invalid_locations = {}
    
    if valid_cities:
        valid_cities_set = set(city.lower() for city in valid_cities)
        
        for col in ['source', 'destination']:
            if col in df.columns:
                df_col_lower = df[col].astype(str).str.lower()
                invalid_rows = df[~df_col_lower.isin(valid_cities_set)].index.tolist()
                if invalid_rows:
                    invalid_locations[col] = invalid_rows
                    logger.warning(f"{col}: {len(invalid_rows)} invalid cities found")
    
    if not invalid_locations:
        logger.info("City validation completed (no whitelist provided, skipped check)")
    else:
        logger.info("City validation completed")
    
    return invalid_locations


def check_fare_consistency(df: pd.DataFrame) -> List[int]:
    """
    Check if Total Fare = Base Fare + Tax & Surcharge
    
    Args:
        df: Input DataFrame
    
    Returns:
        List of row indices with inconsistent fares
    """
    inconsistent_rows = []
    
    if all(col in df.columns for col in ['base_fare', 'tax_surcharge', 'total_fare']):
        try:
            base = pd.to_numeric(df['base_fare'], errors='coerce')
            tax = pd.to_numeric(df['tax_surcharge'], errors='coerce')
            total = pd.to_numeric(df['total_fare'], errors='coerce')
            
            # Allow small floating point differences
            tolerance = 0.01
            inconsistent = (abs((base + tax) - total) > tolerance)
            inconsistent_rows = df[inconsistent].index.tolist()
            
            if inconsistent_rows:
                logger.warning(f"Fare consistency: {len(inconsistent_rows)} rows with inconsistent totals")
        except Exception as e:
            logger.error(f"Error checking fare consistency: {e}")
    
    logger.info("Fare consistency check completed")
    return inconsistent_rows


def validate_data_quality(df: pd.DataFrame, valid_cities: List[str] = None) -> ValidationReport:
    """
    Run all validation checks and generate report
    
    Args:
        df: Input DataFrame
        valid_cities: List of valid city names (optional)
    
    Returns:
        ValidationReport object with all check results
    """
    report = ValidationReport()
    report.total_records = len(df)
    
    logger.info(f"Starting validation checks for {report.total_records} records...")
    
    # Column validation
    columns_valid, error_msg = validate_required_columns(df)
    if not columns_valid:
        report.add_check("Required Columns", "COLUMN_VALIDATION", 0, report.total_records, error_msg)
        report.invalid_records = report.total_records
        return report
    
    # Data type validation
    invalid_by_type = validate_data_types(df)
    type_invalid_count = sum(len(v) for v in invalid_by_type.values())
    report.add_check("Data Types", "TYPE_CHECK", report.total_records - type_invalid_count, type_invalid_count)
    
    # Null value check
    null_locations = check_null_values(df)
    null_invalid_count = len(set(val for vals in null_locations.values() for val in vals))
    report.add_check("Null Values", "NULLS", report.total_records - null_invalid_count, null_invalid_count)
    
    # Negative values check
    negative_locations = check_negative_values(df)
    negative_invalid_count = len(set(val for vals in negative_locations.values() for val in vals))
    report.add_check("Negative Values", "BUSINESS_RULE", report.total_records - negative_invalid_count, 
                    negative_invalid_count)
    
    # City validation check
    invalid_cities = check_valid_cities(df, valid_cities)
    city_invalid_count = len(set(val for vals in invalid_cities.values() for val in vals))
    report.add_check("Valid Cities", "BUSINESS_RULE", report.total_records - city_invalid_count, 
                    city_invalid_count)
    
    # Fare consistency check
    fare_inconsistent = check_fare_consistency(df)
    report.add_check("Fare Consistency", "BUSINESS_RULE", report.total_records - len(fare_inconsistent), 
                    len(fare_inconsistent))
    
    # Calculate overall validity
    all_invalid_indices = set()
    all_invalid_indices.update(set(val for vals in invalid_by_type.values() for val in vals))
    all_invalid_indices.update(set(val for vals in null_locations.values() for val in vals))
    all_invalid_indices.update(set(val for vals in negative_locations.values() for val in vals))
    all_invalid_indices.update(set(val for vals in invalid_cities.values() for val in vals))
    all_invalid_indices.update(fare_inconsistent)
    
    report.invalid_records = len(all_invalid_indices)
    report.valid_records = report.total_records - report.invalid_records
    
    validity_pct = (report.valid_records / report.total_records * 100) if report.total_records > 0 else 0
    logger.info(f"Validation complete: {report.valid_records}/{report.total_records} valid ({validity_pct:.2f}%)")
    
    return report


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
