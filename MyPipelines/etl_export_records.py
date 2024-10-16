import requests
import pandas as pd
import sys
import os
from sqlalchemy.engine import create_engine
from dotenv import load_dotenv

# Cargar las variables de entorno desde un archivo .env
load_dotenv()

# Asignar variables desde las variables de entorno
redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
redshift_db = os.getenv('REDSHIFT_DB')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_schema = os.getenv('REDSHIFT_SCHEMA')
redshift_table = os.getenv('REDSHIFT_TABLE')
api_url = os.getenv('API_URL')
api_key = os.getenv('API_KEY')

def main():
    
    # Hacer la solicitud GET a la API con los encabezados
    response = requests.get(api_url,  headers={'X-Api-Key': api_key})

    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        # Convertir la respuesta JSON a un DataFrame de pandas
        data = response.json()
        df = pd.DataFrame(data)        
        df_limited= df.head(5)
        
        # Crear la cadena de conexión
        conn_str = f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}'

        # Crear el motor de SQLAlchemypip 
    engine = create_engine(conn_str)

    try:
        # Cargar el DataFrame en la tabla de Redshift               
        df_limited.to_sql(name=redshift_table, con=engine, schema=redshift_schema,if_exists='replace', index=False)        
        # Loguear la cantidad de registros insertados
        registros_insertados = len(df_limited)
        print(f'Se insertaron {registros_insertados} registros en la tabla {redshift_table} de Redshift.')
    except Exception as e:
        print(f'Error al cargar los datos en Redshift: {e}')

if __name__ == '__main__':
    main()