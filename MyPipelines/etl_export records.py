import requests
import pandas as pd
from sqlalchemy.engine import create_engine

def main():
    
    # URL de la API
    url = 'https://api.fas.usda.gov/api/esr/exports/commodityCode/104/allCountries/marketYear/2024'

    #  API key
    api_key = 'A7e4QEAsGHJcySTf4gumahDPpCStKel6lsKhEG6v'

    # Encabezados de la solicitud
    headers = {
        'X-Api-Key': api_key
    }

    # Hacer la solicitud GET a la API con los encabezados
    response = requests.get(url, headers=headers)



    # Verificar que la solicitud fue exitosa
    if response.status_code == 200:
        # Convertir la respuesta JSON a un DataFrame de pandas
        data = response.json()
        df = pd.DataFrame(data)
        print(df.head(10))
        df_limited= df.head(10)
        
        # Conexión a Redshift
        redshift_endpoint = 'redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com'
        redshift_db = 'pda'
        redshift_user = '2024_mauro_sebastian_sanchez'
        redshift_password = 'L4!&9^2$xQ'
        redshift_port = '5439'
        redshift_table = 'stg_export_records'
        redshift_schema = '2024_mauro_sebastian_sanchez_schema'

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