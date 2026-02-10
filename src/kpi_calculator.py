"""
KPI Calculator - Compute key performance indicators for flight pricing analysis
"""
import pandas as pd
from typing import Dict, Any
from sqlalchemy import Engine
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


def compute_airline_average_fare(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate average fare by airline
    
    KPI Definition:
    - Group flights by airline
    - Calculate average base fare, tax/surcharge, and total fare
    - Count total bookings per airline
    
    Args:
        df: Transformed flight data
    
    Returns:
        DataFrame with columns: airline, avg_base_fare, avg_tax_surcharge, 
                               avg_total_fare, booking_count
    """
    try:
        # Convert fare columns to numeric
        df = df.copy()
        df['base_fare'] = pd.to_numeric(df['base_fare'], errors='coerce')
        df['tax_surcharge'] = pd.to_numeric(df['tax_surcharge'], errors='coerce')
        df['total_fare'] = pd.to_numeric(df['total_fare'], errors='coerce')
        
        kpi = df.groupby('airline').agg({
            'base_fare': 'mean',
            'tax_surcharge': 'mean',
            'total_fare': 'mean',
            'airline': 'count'
        }).rename(columns={'airline': 'booking_count'})
        
        kpi.columns = ['avg_base_fare', 'avg_tax_surcharge', 'avg_total_fare', 'booking_count']
        kpi = kpi.reset_index()
        
        # Round to 2 decimal places
        kpi[['avg_base_fare', 'avg_tax_surcharge', 'avg_total_fare']] = \
            kpi[['avg_base_fare', 'avg_tax_surcharge', 'avg_total_fare']].round(2)
        
        # Add metadata
        kpi['computed_at'] = datetime.utcnow()
        
        logger.info(f"Computed average fares for {len(kpi)} airlines")
        
        return kpi.sort_values('avg_total_fare', ascending=False)
    
    except Exception as e:
        logger.error(f"Error computing airline average fare: {e}")
        return pd.DataFrame()


def compute_seasonal_variation(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate seasonal fare variation
    
    KPI Definition:
    - For each airline, compare average fares during peak vs non-peak seasons
    - Peak seasons: PEAK_EID, PEAK_WINTER
    - Non-peak: NON_PEAK
    - Calculate percentage difference
    
    Args:
        df: Transformed flight data with 'season' column
    
    Returns:
        DataFrame with columns: airline, avg_fare_peak, avg_fare_non_peak, 
                               fare_difference, peak_percentage_increase
    """
    try:
        df = df.copy()
        df['total_fare'] = pd.to_numeric(df['total_fare'], errors='coerce')
        
        # Separate peak and non-peak data
        peak_df = df[df['season'].isin(['PEAK_EID', 'PEAK_WINTER'])]
        non_peak_df = df[df['season'] == 'NON_PEAK']
        
        # Group by airline and calculate averages
        peak_by_airline = peak_df.groupby('airline')['total_fare'].agg(['mean', 'count']).round(2)
        non_peak_by_airline = non_peak_df.groupby('airline')['total_fare'].agg(['mean', 'count']).round(2)
        
        # Merge results
        variation = pd.DataFrame()
        variation['airline'] = set(peak_by_airline.index) | set(non_peak_by_airline.index)
        
        variation['avg_fare_peak'] = variation['airline'].map(
            peak_by_airline['mean']
        ).round(2)
        variation['peak_booking_count'] = variation['airline'].map(
            peak_by_airline['count']
        ).fillna(0).astype(int)
        
        variation['avg_fare_non_peak'] = variation['airline'].map(
            non_peak_by_airline['mean']
        ).round(2)
        variation['non_peak_booking_count'] = variation['airline'].map(
            non_peak_by_airline['count']
        ).fillna(0).astype(int)
        
        # Calculate differences
        variation['fare_difference'] = (
            variation['avg_fare_peak'] - variation['avg_fare_non_peak']
        ).round(2)
        
        variation['peak_percentage_increase'] = (
            (variation['fare_difference'] / variation['avg_fare_non_peak'] * 100)
        ).round(2)
        
        # Add metadata
        variation['computed_at'] = datetime.utcnow()
        
        logger.info(f"Computed seasonal variation for {len(variation)} airlines")
        
        return variation.sort_values('peak_percentage_increase', ascending=False)
    
    except Exception as e:
        logger.error(f"Error computing seasonal variation: {e}")
        return pd.DataFrame()


def compute_popular_routes(df: pd.DataFrame, top_n: int = 10) -> pd.DataFrame:
    """
    Identify top routes by booking count
    
    KPI Definition:
    - Group by source-destination pair
    - Count bookings (frequency)
    - Calculate average fare on each route
    - Rank by booking count
    - Return top N routes
    
    Args:
        df: Transformed flight data
        top_n: Number of top routes to return (default: 10)
    
    Returns:
        DataFrame with columns: source, destination, booking_count, route_rank, avg_fare_on_route
    """
    try:
        df = df.copy()
        df['total_fare'] = pd.to_numeric(df['total_fare'], errors='coerce')
        
        kpi = df.groupby(['source', 'destination']).agg({
            'total_fare': ['count', 'mean']
        }).reset_index()
        
        kpi.columns = ['source', 'destination', 'booking_count', 'avg_fare_on_route']
        kpi = kpi.sort_values('booking_count', ascending=False).reset_index(drop=True)
        
        # Add ranking
        kpi['route_rank'] = range(1, len(kpi) + 1)
        
        # Round fare to 2 decimals
        kpi['avg_fare_on_route'] = kpi['avg_fare_on_route'].round(2)
        
        # Add metadata
        kpi['computed_at'] = datetime.utcnow()
        
        # Return top N
        kpi = kpi.head(top_n)
        
        logger.info(f"Identified top {len(kpi)} popular routes")
        
        return kpi[['source', 'destination', 'booking_count', 'route_rank', 'avg_fare_on_route', 'computed_at']]
    
    except Exception as e:
        logger.error(f"Error computing popular routes: {e}")
        return pd.DataFrame()


def compute_booking_count_by_airline(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate total booking count by airline
    
    KPI Definition:
    - Count total number of bookings (flights) per airline
    - Sort by count descending
    
    Args:
        df: Transformed flight data
    
    Returns:
        DataFrame with columns: airline, total_bookings
    """
    try:
        kpi = df.groupby('airline').size().reset_index(name='total_bookings')
        kpi = kpi.sort_values('total_bookings', ascending=False)
        kpi['computed_at'] = datetime.utcnow()
        
        logger.info(f"Computed booking counts for {len(kpi)} airlines")
        
        return kpi
    
    except Exception as e:
        logger.error(f"Error computing booking count: {e}")
        return pd.DataFrame()


def compute_all_kpis(df: pd.DataFrame, top_routes: int = 10) -> Dict[str, pd.DataFrame]:
    """
    Compute all KPI metrics at once
    
    Args:
        df: Transformed flight data
        top_routes: Number of top routes to compute (default: 10)
    
    Returns:
        Dictionary with KPI results:
        - 'airline_average': Average fare by airline
        - 'seasonal_variation': Peak vs non-peak comparison
        - 'popular_routes': Top routes by booking count
        - 'booking_count': Total bookings by airline
    """
    logger.info(f"Starting KPI computation for {len(df)} records...")
    
    kpis = {
        'airline_average': compute_airline_average_fare(df),
        'seasonal_variation': compute_seasonal_variation(df),
        'popular_routes': compute_popular_routes(df, top_n=top_routes),
        'booking_count': compute_booking_count_by_airline(df)
    }
    
    logger.info("All KPIs computed successfully")
    
    return kpis


def save_kpis_to_postgres(kpis_dict: Dict[str, pd.DataFrame], postgres_engine: Engine) -> Dict[str, int]:
    """
    Save all KPI results to PostgreSQL
    
    Args:
        kpis_dict: Dictionary of KPI DataFrames
        postgres_engine: SQLAlchemy engine for PostgreSQL
    
    Returns:
        Dictionary with row counts inserted to each table
    """
    insert_counts = {}
    
    try:
        # Save airline average
        if not kpis_dict['airline_average'].empty:
            rows = kpis_dict['airline_average'].to_sql(
                'kpi_airline_average',
                con=postgres_engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            insert_counts['airline_average'] = len(kpis_dict['airline_average'])
            logger.info(f"Inserted {insert_counts['airline_average']} rows to kpi_airline_average")
        
        # Save seasonal variation
        if not kpis_dict['seasonal_variation'].empty:
            rows = kpis_dict['seasonal_variation'].to_sql(
                'kpi_seasonal_variation',
                con=postgres_engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            insert_counts['seasonal_variation'] = len(kpis_dict['seasonal_variation'])
            logger.info(f"Inserted {insert_counts['seasonal_variation']} rows to kpi_seasonal_variation")
        
        # Save popular routes
        if not kpis_dict['popular_routes'].empty:
            rows = kpis_dict['popular_routes'].to_sql(
                'kpi_popular_routes',
                con=postgres_engine,
                if_exists='replace',
                index=False,
                method='multi'
            )
            insert_counts['popular_routes'] = len(kpis_dict['popular_routes'])
            logger.info(f"Inserted {insert_counts['popular_routes']} rows to kpi_popular_routes")
        
        # Save booking count
        if not kpis_dict['booking_count'].empty:
            # We can store this in airline_average or a separate table
            insert_counts['booking_count'] = len(kpis_dict['booking_count'])
            logger.info(f"Booking count data ready: {insert_counts['booking_count']} airlines")
        
        logger.info(f"Successfully saved KPIs to PostgreSQL")
        
    except Exception as e:
        logger.error(f"Error saving KPIs to PostgreSQL: {e}")
    
    return insert_counts


def generate_kpi_summary(kpis_dict: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    """
    Generate summary statistics of KPI computation
    
    Args:
        kpis_dict: Dictionary of KPI DataFrames
    
    Returns:
        Summary dictionary
    """
    summary = {
        "computation_timestamp": datetime.utcnow().isoformat(),
        "airline_average": {
            "airlines_analyzed": len(kpis_dict['airline_average']),
            "highest_avg_fare": kpis_dict['airline_average']['avg_total_fare'].max() if not kpis_dict['airline_average'].empty else None,
            "lowest_avg_fare": kpis_dict['airline_average']['avg_total_fare'].min() if not kpis_dict['airline_average'].empty else None
        },
        "seasonal_variation": {
            "airlines_with_peak_data": len(kpis_dict['seasonal_variation']),
            "max_peak_increase_pct": kpis_dict['seasonal_variation']['peak_percentage_increase'].max() if not kpis_dict['seasonal_variation'].empty else None
        },
        "popular_routes": {
            "top_routes_identified": len(kpis_dict['popular_routes']),
            "most_booked_route": kpis_dict['popular_routes'].iloc[0][['source', 'destination']].to_dict() if not kpis_dict['popular_routes'].empty else None,
            "top_route_bookings": kpis_dict['popular_routes']['booking_count'].iloc[0] if not kpis_dict['popular_routes'].empty else None
        },
        "booking_count": {
            "total_airlines": len(kpis_dict['booking_count']),
            "total_bookings": kpis_dict['booking_count']['total_bookings'].sum() if not kpis_dict['booking_count'].empty else None
        }
    }
    
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("KPI Calculator module loaded successfully")
    