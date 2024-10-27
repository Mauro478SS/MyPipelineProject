import os
import logging
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import logging

# Configurar el registro
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_fact_table():
    logging.info("Cargando tabla de hechos (ft_export)...")
    
    logging.info("Cargando variables de entorno...")
    load_dotenv()

    # Variables de conexión
    redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_port = os.getenv('REDSHIFT_PORT')
    redshift_schema = os.getenv('REDSHIFT_SCHEMA')
    redshift_stg_table = os.getenv('TABLE_RECORDS')
    redshift_fact_table=os.getenv('TABLE_FT_EXPORT')
    
    conn_str = f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}'
    engine = create_engine(conn_str)
    
    try:
        with engine.connect() as connection:  
            
            #logging.info(f"Borrando registros existentes en la tabla {redshift_fact_table}...")
            #connection.execute(f'DELETE FROM "{redshift_schema}"."{redshift_fact_table}"')
                        
            logging.info(f"Poblando la tabla de hechos {redshift_fact_table}...")
            result = connection.execute(f"""
                INSERT INTO "{redshift_schema}".{redshift_fact_table} (sk_commodities, sk_countries, weeklyexports, accumulatedexports, outstandingsales,
                        currentmynetsales, currentmytotalcommitment, nextmyoutstandingsales, nextmynetsales, unitid, weekendingdate)
                SELECT
                    c.sk_commodities,
                    co.sk_countries,
                    s.weeklyexports,
                    s.accumulatedexports::decimal,
                    s.outstandingsales::decimal,
                    s.currentmynetsales::decimal,
                    s.currentmytotalcommitment::decimal,
                    s.nextmyoutstandingsales::decimal,
                    s.nextmynetsales::decimal,
                    s.unitid::integer,
                    s.weekendingdate::TIMESTAMP
                FROM "{redshift_schema}".{redshift_stg_table} s
                JOIN "{redshift_schema}".dim_commodities c ON s.commoditycode = c.commoditycode
                JOIN "{redshift_schema}".dim_countries co ON s.countrycode = co.countrycode;
            """)
            
            logging.info(f"Datos insertados : {result.rowcount} en la tabla de hechos {redshift_fact_table}.")
    except Exception as e:
        logging.error(f"Error al crear o poblar la tabla de hechos: {e}")

if __name__ == "__main__":
    logging.info("Inicio creación de la tabla de hechos")
    create_fact_table()
    logging.info("Fin creación de la tabla de hechos")
