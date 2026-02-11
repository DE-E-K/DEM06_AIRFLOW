# 📊 KPI Definitions & Computation Logic

## Overview

This document describes the key performance indicators (KPIs) computed by the pipeline, their mathematical definitions, business interpretation, and computation logic based on the **Bangladesh Flight Price Dataset**.

### Key Data Fields
- **Time Basis**: `Departure Date & Time` is used as the primary timestamp for all temporal logic (seasonality, flight date).
- **Booking Date**: Derived as `Departure Date & Time` minus `Days Before Departure`.
- **Fares**: All monetary values are in BDT (Bangladeshi Taka).

---

## KPI 1: Average Fare by Airline

### Definition

**Average Fare Metrics per Airline**

For each airline operating routes in the dataset, calculate the mean of fare components.

### Mathematical Formula

```
For airline A with bookings B1, B2, ..., Bn:

avg_base_fare(A) = ∑(Base Fare (BDT)_i) / n
avg_tax_surcharge(A) = ∑(Tax & Surcharge (BDT)_i) / n
avg_total_fare(A) = ∑(Total Fare (BDT)_i) / n
booking_count(A) = n
```

### Business Interpretation

- **Identify Premium Carriers**: Airlines with highest `avg_total_fare`.
- **Market Share**: `booking_count` indicates airline popularity.
- **Revenue Estimation**: `avg_total_fare` × `booking_count`.

### Implementation

**Source Columns**: `Airline`, `Base Fare (BDT)`, `Tax & Surcharge (BDT)`, `Total Fare (BDT)`

```python
kpi = df.groupby('Airline').agg({
    'Base Fare (BDT)': 'mean',
    'Tax & Surcharge (BDT)': 'mean',
    'Total Fare (BDT)': 'mean',
    'Airline': 'count'
}).rename(columns={'Airline': 'booking_count'})
```

**Output Table**: `kpi_airline_average` (PostgreSQL)

---

## KPI 2: Seasonal Fare Variation

### Definition

**Peak vs Non-Peak Fare Comparison per Airline**

Compare average fares during peak travel seasons versus off-peak periods, based on the `Seasonality` column or derived from `Departure Date & Time`.

#### Season Definitions
1.  **Provided**: The dataset includes a `Seasonality` column (e.g., "Regular", "Eid", "Winter").
2.  **Derived**: If `Seasonality` is missing, logic is applied to `Departure Date & Time`:
    - **PEAK_EID**: Approx. dates for Eid al-Fitr & Eid al-Adha.
    - **PEAK_WINTER**: Dec 1 - Jan 31.
    - **NON_PEAK**: All other dates.

### Mathematical Formula

```
avg_fare_peak(A) = Mean(Total Fare (BDT)) where Seasonality != 'Regular'
avg_fare_non_peak(A) = Mean(Total Fare (BDT)) where Seasonality == 'Regular'

peak_percentage_increase(A) = ((avg_fare_peak - avg_fare_non_peak) / avg_fare_non_peak) * 100
```

### Business Interpretation

- **Pricing Power**: Higher percentage increase during peak seasons indicates strong demand elasticity.
- **Seasonal Strategy**: Helps airlines optimize peak-season surcharges.

### Implementation

```python
# Use 'Seasonality' column directly if available, else derive from 'Departure Date & Time'
peak_df = df[df['Seasonality'] != 'Regular']
non_peak_df = df[df['Seasonality'] == 'Regular']

# Aggregate by Airline
```

**Output Table**: `kpi_seasonal_variation` (PostgreSQL)

---

## KPI 3: Popular Routes

### Definition

**Top Routes by Booking Frequency**

Identify the most booked routes defined by `Source` (IATA) to `Destination` (IATA) pairs.

### Mathematical Formula

```
rank(route) = ROW_NUMBER() OVER (ORDER BY Count(*) DESC)
avg_fare(route) = Mean(Total Fare (BDT))
```

### Business Interpretation

- **High-Demand Corridors**: Routes with highest volume (e.g., DAC-CGP).
- **Route Profitability**: High volume + High avg_fare = Key revenue driver.

### Implementation

**Source Columns**: `Source`, `Destination`, `Total Fare (BDT)`

```python
kpi = df.groupby(['Source', 'Destination']).agg({
    'Total Fare (BDT)': ['count', 'mean']
}).reset_index()
# Sort by count descending
```

**Output Table**: `kpi_popular_routes` (PostgreSQL)

---

## KPI 4: Booking Logic (Derived)

### Definition

**Estimated Booking Date**

Since the dataset does not have a raw "Booking Date" field, it is derived to analyze booking behavior (e.g., "Advance Booking Window").

### Formula

```
Booking Date = (Departure Date & Time) - (Days Before Departure) days
```

### Business Interpretation

- **Advance Booking Curve**: Analyze how fare changes as `Days Before Departure` decreases.
- **Last Minute Surges**: Identify price spikes for bookings < 7 days before departure.

---

## Data Quality & Assumptions

### Assumptions

1.  **Date Format**: `Departure Date & Time` is parsable (e.g., "2025-01-15 10:30").
2.  **Currency**: All fare columns are in BDT.
3.  **Completeness**: `Source` and `Destination` codes are valid IATA codes.
4.  **Consistency**: `Total Fare (BDT)` matches `Base Fare` + `Tax & Surcharge` (validated in pipeline).

### Validation Rules

- **Negative Fares**: `Total Fare (BDT)` < 0 is flagged as invalid.
- **Missing Dates**: Rows with null `Departure Date & Time` are rejected.
