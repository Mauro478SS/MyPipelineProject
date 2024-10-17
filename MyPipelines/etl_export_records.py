import requests
import pandas as pd
import sys
import os
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv
import logging

# Configurar el registro
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    
    # Asignar variables desde las variables de entorno    
    logging.info("Cargando variables de entorno...")
    load_dotenv()

    redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
    redshift_db = os.getenv('REDSHIFT_DB')
    redshift_user = os.getenv('REDSHIFT_USER')
    redshift_password = os.getenv('REDSHIFT_PASSWORD')
    redshift_port = os.getenv('REDSHIFT_PORT')
    redshift_schema = os.getenv('REDSHIFT_SCHEMA')
    redshift_table = os.getenv('TABLE_RECORDS')
    api_url = os.getenv('API_REC')
    api_key = os.getenv('API_KEY')
    
    # Hacer la solicitud GET a la API con los encabezados
    logging.info(f"Realizando solicitud a la API: {api_url}")
    response = requests.get(api_url,  headers={'X-Api-Key': api_key})

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        logging.info("Solicitud a la API exitosa")
        
        # Convertir la respuesta JSON a un DataFrame de pandas
        data = response.json()
        df = pd.DataFrame(data)        
        df_limited= df.head(5)
        
        # Crear la cadena de conexión
        conn_str = f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}'

        # Crear el motor de SQLAlchemypip 
        engine = create_engine(conn_str)

        try:
            logging.info(f"Borrando registros existentes en la tabla {redshift_table}...")
            with engine.connect() as connection:
                connection.execute(f'DELETE FROM "{redshift_schema}"."{redshift_table}"')
                    
            # Cargar el DataFrame en la tabla de Redshift               
            logging.info(f"Cargando registros en la tabla de {redshift_table} ...")
            df_limited.to_sql(name=redshift_table, con=engine, schema=redshift_schema,if_exists='append', index=False)        
            
            # Loguear la cantidad de registros insertados
            registros_insertados = len(df_limited)
            logging.info(f'Se insertaron {registros_insertados} registros en la tabla {redshift_table} de Redshift.')
        except Exception as e:
            logging.error(f'Error al cargar los datos en Redshift: {e}')
    else:
        logging.error(f"Error en la solicitud a la API: {response.status_code}")    

if __name__ == '__main__':
    logging.info("Inicio pipeline etl_export_records")
    main()
    logging.info("Fin pipeline etl_export_records")