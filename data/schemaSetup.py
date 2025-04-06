import duckdb
import random
import pandas as pd
from datetime import date, timedelta

# --- Configuration ---
DB_FILE = 'spend_data.duckdb'
TABLE_NAME = 'CLEAN_SPEND'

# --- Helper Functions ---

def func_define_schema(p_con):
    """Creates the CLEAN_SPEND table."""
    l_sql = f"""
    CREATE OR REPLACE TABLE {TABLE_NAME} (
        chargedApplicationId INTEGER,
        ppgProduct VARCHAR,
        pricePerUnit DOUBLE,
        pricingUnit VARCHAR,
        dcRegion VARCHAR,
        assetEnvironment VARCHAR,
        month INTEGER,
        year INTEGER,
        amount DOUBLE,
        Exclude VARCHAR(1),
        cpof VARCHAR,
        workloadType VARCHAR
    );
    """
    try:
        p_con.execute(l_sql)
        print(f"Table '{TABLE_NAME}' created or replaced successfully.")
    except Exception as e:
        print(f"Error in func_define_schema: {e}")
        print(f"SQL: {l_sql}")
        raise

def func_generate_data(p_num_seals=5, p_start_year=2023, p_end_year=2024):
    """Generates sample data matching the requirements."""
    l_list_data = []
    l_list_seal_ids = [random.randint(10000000, 99999999) for _ in range(p_num_seals)]
    l_list_products = [
        ('Compute Instance', 0.02, ['VM', 'VSI', 'Instance', 'Server'], 'Compute'),
        ('High CPU Compute', 0.025, ['vCPU'], 'Compute'),
        ('Standard Block Storage', 0.01, ['GB', 'GB Allocated'], 'Storage'),
        ('Premium Block Storage', 0.015, ['GB', 'GB Allocated', 'Dollar'], 'Storage'),
        ('Mongo Atlas Cluster', 150, ['Instance', 'Server'], 'Compute'),
        ('Mongo Atlas Storage', 0.012, ['GB', 'Dollar'], 'Storage'),
        ('Oracle RDS Instance', 200, ['Instance'], 'Compute'),
        ('Oracle RDS Storage', 0.011, ['GB'], 'Storage'),
        ('Gaia Postgres Instance', 180, ['Instance'], 'Compute'),
        ('Gaia Postgres Storage', 0.01, ['GB'], 'Storage'),
        ('Aurora Postgres Compute', 0.022, ['vCPU hour', 'Instance'], 'Compute'), # Note: pricing unit complexity ignored for simplicity here
        ('Aurora Postgres Storage', 0.01, ['GB month', 'Dollar'], 'Storage'),
        ('ElasticSearch Node', 100, ['Instance', 'Server'], 'Compute'),
        ('ElasticSearch Storage', 0.009, ['GB'], 'Storage'),
        ('Data Protection', 0.008, ['GB per Month'], 'Storage'),
        ('Network Transfer', 0.05, ['GB Transferred'], 'Other'), # Add some non-compute/storage
    ]
    l_list_regions = ['NAMR', 'EMEA', 'APAC', '']
    l_list_envs = ['PROD', 'UAT', 'DEV', '']
    l_list_workload_types = ['oltp', 'olap', 'mixed', 'unknown']
    l_list_cpof = ['cpof', 'unknown'] # Simplified

    l_date_current = date(p_start_year, 1, 1)
    l_date_end = date(p_end_year, 12, 1)

    # Base storage size per seal (in GB) - random walk
    dict_seal_storage_gb = {seal_id: random.randint(1000, 12000) for seal_id in l_list_seal_ids}
    # Base compute units per seal (e.g., instances/vcpu) - random walk
    dict_seal_compute_units = {seal_id: random.randint(5, 50) for seal_id in l_list_seal_ids}

    while l_date_current <= l_date_end:
        l_int_month = l_date_current.month
        l_int_year = l_date_current.year

        for l_int_seal_id in l_list_seal_ids:
            # Simulate some churn/new products for a seal
            l_list_current_products = random.sample(l_list_products, random.randint(5, len(l_list_products) - 2))

            for l_tpl_product_info in l_list_current_products:
                l_str_product, l_float_base_price, l_list_units, l_str_category = l_tpl_product_info
                l_str_pricing_unit = random.choice(l_list_units)
                l_float_price_per_unit = l_float_base_price * random.uniform(0.9, 1.1) # Slight variation

                l_float_amount = 0
                if l_str_category == 'Storage':
                    # Update storage size slightly each month
                    dict_seal_storage_gb[l_int_seal_id] *= random.uniform(0.98, 1.03)
                    dict_seal_storage_gb[l_int_seal_id] = max(100, dict_seal_storage_gb[l_int_seal_id]) # Min size
                    # Calculate amount based on storage size
                    l_float_volume = dict_seal_storage_gb[l_int_seal_id] * random.uniform(0.3, 0.8) # Portion used by this product
                    l_float_amount = l_float_volume * l_float_price_per_unit
                elif l_str_category == 'Compute':
                     # Update compute units slightly
                    dict_seal_compute_units[l_int_seal_id] *= random.uniform(0.97, 1.04)
                    dict_seal_compute_units[l_int_seal_id] = max(1, dict_seal_compute_units[l_int_seal_id]) # Min units
                    # Calculate amount based on compute units
                    l_float_units = dict_seal_compute_units[l_int_seal_id] * random.uniform(0.2, 0.7) # Portion used by this product
                    l_float_amount = l_float_units * l_float_price_per_unit
                else: # Other category
                    l_float_amount = random.uniform(1, 500) # Smaller random amount

                # Add noise / zero spend sometimes
                if random.random() < 0.05:
                    l_float_amount = 0
                else:
                     l_float_amount = round(max(0, l_float_amount * random.uniform(0.8, 1.2)), 2)


                l_list_data.append({
                    'chargedApplicationId': l_int_seal_id,
                    'ppgProduct': l_str_product,
                    'pricePerUnit': l_float_price_per_unit,
                    'pricingUnit': l_str_pricing_unit,
                    'dcRegion': random.choice(l_list_regions),
                    'assetEnvironment': random.choice(l_list_envs),
                    'month': l_int_month,
                    'year': l_int_year,
                    'amount': l_float_amount,
                    'Exclude': 'Y' if random.random() < 0.1 else 'N', # Randomly exclude some
                    'cpof': random.choice(l_list_cpof),
                    'workloadType': random.choice(l_list_workload_types)
                })

        # Move to the next month
        l_dt_next_month_start = (l_date_current.replace(day=28) + timedelta(days=4)).replace(day=1)
        l_date_current = l_dt_next_month_start

    return pd.DataFrame(l_list_data)


def func_populate_database():
    """Generates data and populates the DuckDB database."""
    l_con = None
    try:
        print(f"Connecting to DuckDB file: {DB_FILE}")
        l_con = duckdb.connect(database=DB_FILE, read_only=False)

        print("Defining schema...")
        func_define_schema(l_con)

        print("Generating data...")
        l_df_data = func_generate_data()
        print(f"Generated {len(l_df_data)} rows.")

        # Use DuckDB's efficient DataFrame insertion
        print(f"Inserting data into {TABLE_NAME}...")
        l_con.register('df_data_view', l_df_data)
        l_con.execute(f'INSERT INTO {TABLE_NAME} SELECT * FROM df_data_view')
        l_con.unregister('df_data_view') # Clean up view

        print("Data population complete.")

    except Exception as e:
        print(f"An error occurred during database population: {e}")
    finally:
        if l_con:
            l_con.close()
            print("Database connection closed.")

# --- Main Execution ---
if __name__ == "__main__":
    func_populate_database()
    # Example verification query (optional)
    try:
        con = duckdb.connect(DB_FILE)
        count = con.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}").fetchone()[0]
        print(f"\nVerification: Table '{TABLE_NAME}' contains {count} rows.")
        con.close()
    except Exception as e:
        print(f"Verification failed: {e}")