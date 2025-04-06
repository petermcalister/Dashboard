import duckdb
import pandas as pd
from datetime import datetime, timedelta

# --- Configuration ---
DB_FILE = 'spend_data.duckdb'
TABLE_NAME = 'CLEAN_SPEND' # Make sure this matches schemaSetup

# --- Constants for Spend Type Classification ---
STORAGE_KEYWORDS = ['GB', 'Dollar'] # Case-insensitive check below
COMPUTE_KEYWORDS = ['VM', 'VSI', 'CPU', 'Instance', 'Server', 'Core Hour'] # Case-insensitive check below

# --- Private Helper Function ---
def _func_execute_query(p_str_sql, p_tuple_params=None, p_str_calling_func=""):
    """Executes a SQL query and returns results as a list of dictionaries."""
    l_con = None
    l_list_results = []
    try:
        l_con = duckdb.connect(database=DB_FILE, read_only=True)
        if p_tuple_params:
            l_cursor = l_con.execute(p_str_sql, p_tuple_params)
        else:
            l_cursor = l_con.execute(p_str_sql)

        # Fetch column names for dict conversion
        l_list_colnames = [desc[0] for desc in l_cursor.description]
        l_list_results = [dict(zip(l_list_colnames, row)) for row in l_cursor.fetchall()]

    except Exception as e:
        print(f"Error in _func_execute_query called from {p_str_calling_func}: {e}")
        print(f"SQL:\n{p_str_sql}")
        if p_tuple_params:
            print(f"Parameters: {p_tuple_params}")
        # Optionally re-raise or return empty list
        # raise
    finally:
        if l_con:
            l_con.close()
    return l_list_results

def _func_build_pricing_unit_filter(p_list_keywords):
    """Builds a CASE WHEN statement part for pricingUnit classification."""
    # DuckDB's lower() function works for case-insensitivity
    return " OR ".join([f"lower(pricingUnit) LIKE '%{keyword.lower()}%'" for keyword in p_list_keywords])

# --- Public Query Functions ---

def func_get_unique_seal_id():
    """Fetches distinct seal IDs."""
    l_str_sql = f"""
        SELECT DISTINCT
            chargedApplicationId AS sealId
        FROM
            {TABLE_NAME}
        WHERE Exclude != 'Y'
        ORDER BY
            sealId;
    """
    return _func_execute_query(l_str_sql, p_str_calling_func="func_get_unique_seal_id")

def func_get_ppg_product_summary(p_list_seal_ids=None):
    """
    Fetches aggregated data per sealId and ppgProduct across all time.
    Calculates TotalSpend and SpendGrowthOverYear.
    """
    l_str_where_clause = "WHERE Exclude != 'Y'"
    l_tuple_params = []

    if p_list_seal_ids:
        l_str_placeholders = ', '.join(['?'] * len(p_list_seal_ids))
        l_str_where_clause += f" AND chargedApplicationId IN ({l_str_placeholders})"
        l_tuple_params.extend(p_list_seal_ids)

    # Calculate date range for growth calculation (last 12 vs previous 12)
    # We need the latest date in the data to anchor this reliably
    l_str_max_date_sql = f"SELECT MAX(CAST(printf('%d-%02d-01', year, month) AS DATE)) FROM {TABLE_NAME}"
    l_con_temp = None
    l_dt_max_date = None
    try:
        l_con_temp = duckdb.connect(database=DB_FILE, read_only=True)
        l_dt_max_date_result = l_con_temp.execute(l_str_max_date_sql).fetchone()
        if l_dt_max_date_result and l_dt_max_date_result[0]:
             l_dt_max_date = l_dt_max_date_result[0]
        else: # Default if no data
            l_dt_max_date = datetime.now().date().replace(day=1)

    except Exception as e:
         print(f"Warning: Could not determine max date for growth calc: {e}. Using current date.")
         l_dt_max_date = datetime.now().date().replace(day=1)
    finally:
        if l_con_temp:
            l_con_temp.close()

    l_dt_one_year_ago = (l_dt_max_date - timedelta(days=365)).replace(day=1)
    l_dt_two_years_ago = (l_dt_one_year_ago - timedelta(days=365)).replace(day=1)

    l_str_date_col = "CAST(printf('%d-%02d-01', year, month) AS DATE)" # Construct date

    l_str_sql = f"""
        SELECT
            chargedApplicationId AS sealId,
            ppgProduct,
            SUM(amount) AS TotalSpend,
            -- Calculate Spend Growth YoY
            CAST(SUM(CASE WHEN {l_str_date_col} >= ? AND {l_str_date_col} <= ? THEN amount ELSE 0 END) AS DOUBLE)
                AS SpendLast12Months,
            CAST(SUM(CASE WHEN {l_str_date_col} >= ? AND {l_str_date_col} < ? THEN amount ELSE 0 END) AS DOUBLE)
                AS SpendPrevious12Months
        FROM
            {TABLE_NAME}
        {l_str_where_clause}
        GROUP BY
            chargedApplicationId, ppgProduct
        HAVING
            SUM(amount) > 0 -- Exclude products with zero total spend in the filtered period
        ORDER BY
            sealId, ppgProduct;
    """
    # Add date parameters for growth calculation
    l_tuple_params_final = [l_dt_one_year_ago, l_dt_max_date, l_dt_two_years_ago, l_dt_one_year_ago] + l_tuple_params

    l_list_results = _func_execute_query(l_str_sql, tuple(l_tuple_params_final), p_str_calling_func="func_get_ppg_product_summary")

    # Calculate growth percentage in Python to handle division by zero safely
    for row in l_list_results:
        l_float_last_12 = row.get('SpendLast12Months', 0.0)
        l_float_prev_12 = row.get('SpendPrevious12Months', 0.0)
        if l_float_prev_12 and l_float_prev_12 != 0:
            row['SpendGrowthOverYear'] = round(((l_float_last_12 - l_float_prev_12) / l_float_prev_12) * 100, 1)
        elif l_float_last_12 > 0:
             row['SpendGrowthOverYear'] = None # Indicate infinite growth / new spend
        else:
            row['SpendGrowthOverYear'] = 0.0 # No spend in either period
        # Remove intermediate columns
        row.pop('SpendLast12Months', None)
        row.pop('SpendPrevious12Months', None)

    return l_list_results


def func_get_total_spend_over_time(p_list_seal_ids=None, p_list_ppg_products=None):
    """Fetches total spend aggregated by sealId and month/year."""
    l_str_where_clause = "WHERE Exclude != 'Y'"
    l_tuple_params = []

    if p_list_seal_ids:
        l_str_placeholders = ', '.join(['?'] * len(p_list_seal_ids))
        l_str_where_clause += f" AND chargedApplicationId IN ({l_str_placeholders})"
        l_tuple_params.extend(p_list_seal_ids)

    if p_list_ppg_products:
        l_str_placeholders = ', '.join(['?'] * len(p_list_ppg_products))
        l_str_where_clause += f" AND ppgProduct IN ({l_str_placeholders})"
        l_tuple_params.extend(p_list_ppg_products)

    l_str_sql = f"""
        SELECT
            chargedApplicationId AS sealId,
            CAST(printf('%d-%02d-01', year, month) AS DATE) AS date,
            SUM(amount) AS TotalSpend
        FROM
            {TABLE_NAME}
        {l_str_where_clause}
        GROUP BY
            sealId, date
        ORDER BY
            sealId, date;
    """
    return _func_execute_query(l_str_sql, tuple(l_tuple_params) if l_tuple_params else None, p_str_calling_func="func_get_total_spend_over_time")


def func_get_spend_per_gb_over_time(p_list_seal_ids=None, p_list_ppg_products=None):
    """
    Fetches spend per GB over time, aggregated by sealId and month/year.
    Spend per GB = SUM(Amount) / SUM(Volume_GB)
    Volume_GB is derived from storage rows: Amount / pricePerUnit
    """
    l_str_where_clause = "WHERE Exclude != 'Y'"
    l_tuple_params = []

    if p_list_seal_ids:
        l_str_placeholders = ', '.join(['?'] * len(p_list_seal_ids))
        l_str_where_clause += f" AND chargedApplicationId IN ({l_str_placeholders})"
        l_tuple_params.extend(p_list_seal_ids)

    if p_list_ppg_products:
        l_str_placeholders = ', '.join(['?'] * len(p_list_ppg_products))
        l_str_where_clause += f" AND ppgProduct IN ({l_str_placeholders})"
        l_tuple_params.extend(p_list_ppg_products)

    l_str_storage_filter = _func_build_pricing_unit_filter(STORAGE_KEYWORDS)

    l_str_sql = f"""
        WITH MonthlyVolume AS (
            SELECT
                chargedApplicationId,
                year,
                month,
                SUM(CASE
                        WHEN ({l_str_storage_filter}) AND pricePerUnit != 0 THEN amount / pricePerUnit
                        ELSE 0
                    END) AS StorageVolumeGB,
                SUM(amount) AS MonthlyTotalSpend
            FROM
                {TABLE_NAME}
            {l_str_where_clause}
            GROUP BY
                chargedApplicationId, year, month
        )
        SELECT
            chargedApplicationId AS sealId,
            CAST(printf('%d-%02d-01', year, month) AS DATE) AS date,
            MonthlyTotalSpend,
            StorageVolumeGB,
            CASE
                WHEN StorageVolumeGB > 0 THEN CAST(MonthlyTotalSpend AS DOUBLE) / StorageVolumeGB
                ELSE NULL -- Or 0, or MonthlyTotalSpend depending on desired handling of zero volume
            END AS SpendPerGB
        FROM
            MonthlyVolume
        WHERE MonthlyTotalSpend > 0 -- Only show months with spend
        ORDER BY
            sealId, date;

    """
    return _func_execute_query(l_str_sql, tuple(l_tuple_params) if l_tuple_params else None, p_str_calling_func="func_get_spend_per_gb_over_time")

# Note: GetYearlyChangeInSpend query is integrated into func_get_ppg_product_summary