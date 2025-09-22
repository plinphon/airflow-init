from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import logging
import psycopg2
from psycopg2.extras import execute_values
from generate_fuel_exports import setup_logging, ensure_data_dir, build_schema, make_batch, write_parquet

ROWS_PER_FILE = 300
OUT_DIR = "/opt/airflow/data"

with DAG(
    dag_id="create_data",
    start_date=datetime(2025, 1, 1),
    schedule_interval="*/1 * * * *",  #1min
    catchup=False,
    tags=["example"],
) as dag:

    @task
    def create_table():
        """Create the fuel_transactions table if it doesn't exist"""
        conn = psycopg2.connect(
            host="postgres",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fuel_transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            station_id VARCHAR(255),
            dock_bay VARCHAR(255),
            dock_level VARCHAR(255),
            ship_name VARCHAR(255),
            franchise VARCHAR(255),
            captain_name VARCHAR(255),
            species VARCHAR(255),
            fuel_type VARCHAR(255),
            fuel_units INTEGER,
            price_per_unit DECIMAL(10,2),
            total_cost DECIMAL(10,2),
            services TEXT[],
            is_emergency BOOLEAN,
            visited_at TIMESTAMP,
            arrival_date DATE,
            coords_x DECIMAL(10,6),
            coords_y DECIMAL(10,6)
        );
        """
        
        cursor.execute(create_table_sql)
        conn.commit()
        logging.info("Table fuel_transactions created or already exists")
        
        cursor.close()
        conn.close()

    @task
    def export_file(rows_per_file: int = ROWS_PER_FILE, out_dir: str = OUT_DIR):
        setup_logging()
        ensure_data_dir(out_dir)
        schema = build_schema()

        logging.info(f"Generating {rows_per_file} rows into {out_dir}")
        records = make_batch(rows_per_file)
        write_parquet(records, out_dir, schema)
        logging.info("File generation completed")

        conn = psycopg2.connect(
            host="postgres", 
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow"
        )
        cursor = conn.cursor()
        
        sql = """
        INSERT INTO fuel_transactions (
            transaction_id, station_id, dock_bay, dock_level, ship_name, franchise,
            captain_name, species, fuel_type, fuel_units, price_per_unit, total_cost,
            services, is_emergency, visited_at, arrival_date, coords_x, coords_y
        )
        VALUES %s
        ON CONFLICT (transaction_id) DO NOTHING
        """
        
        values = [
            (
                r["transaction_id"],
                r["station_id"],
                r["dock"]["bay"],
                r["dock"]["level"],
                r["ship_name"],
                r["franchise"],
                r["captain_name"],
                r["species"],
                r["fuel_type"],
                r["fuel_units"],
                float(r["price_per_unit"]),
                float(r["total_cost"]),
                r["services"],     
                r["is_emergency"],
                r["visited_at"],
                r["arrival_date"],
                r["coords_x"],
                r["coords_y"]
            ) for r in records
        ]

        execute_values(cursor, sql, values)
        conn.commit()
        logging.info(f"Inserted {len(values)} rows into Postgres")

        cursor.close()
        conn.close()

    # Define task dependencies
    create_table() >> export_file()