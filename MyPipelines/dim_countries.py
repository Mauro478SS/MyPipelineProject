import pandas as pd
import os
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import logging

# Configurar el registro
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
                         
def main():
    logging.info("Actualizando la dimensión (dim_countries)...")
    
    # Asignar variables desde las variables de entorno    
    logging.info("Cargando variables de entorno...")
    load_dotenv()
    
    redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_port = os.getenv('REDSHIFT_PORT')
    redshift_schema = os.getenv('REDSHIFT_SCHEMA')
    redshift_dim_table = os.getenv('TABLE_DIM_COUNTRIES')
    redshift_stg_table = os.getenv('TABLE_COUNTRIES')

    conn_str = f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}'
    engine = create_engine(conn_str)
    
    try:
        with engine.connect() as connection:
            logging.info("Marcando registros actuales como históricos si hay cambios...")
            result = connection.execute(f"""
                UPDATE "{redshift_schema}".{redshift_dim_table}
                SET end_date = CURRENT_TIMESTAMP,
                    current_record = FALSE
                FROM "{redshift_schema}".{redshift_stg_table}
                WHERE {redshift_dim_table}.countrycode = {redshift_stg_table}.countrycode
                AND ({redshift_dim_table}.countryname <> {redshift_stg_table}.countryname
                OR {redshift_dim_table}.countrydescription <> {redshift_stg_table}.countrydescription)
                AND {redshift_dim_table}.current_record = TRUE;
            """)            
            logging.info(f"Registros actualizados: {result.rowcount}")
                         
            logging.info("Insertando nuevos registros o registros actualizados...")
            result = connection.execute(f"""
                INSERT INTO "{redshift_schema}".{redshift_dim_table} (countrycode, countryname, countrydescription, regionid, genccode, start_date, end_date, current_record)
                SELECT countrycode::integer, countryname, countrydescription, regionid::integer, genccode, CURRENT_TIMESTAMP, NULL, TRUE
                FROM "{redshift_schema}".{redshift_stg_table}
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "{redshift_schema}".{redshift_dim_table}
                    WHERE {redshift_dim_table}.countrycode = {redshift_stg_table}.countrycode
                    AND {redshift_dim_table}.countryname = {redshift_stg_table}.countryname
                    AND {redshift_dim_table}.countrydescription = {redshift_stg_table}.countrydescription
                    AND {redshift_dim_table}.current_record = TRUE
                );
            """)
            logging.info(f"Registros insertados: {result.rowcount}")
            logging.info("Dimensión actualizada.")
    except Exception as e:
        logging.error(f"Error al actualizar la dimensión: {e}")

if __name__ == '__main__':
    logging.info("Inicio pipeline dim_countries")
    main()
    logging.info("Fin pipeline dim_countries")