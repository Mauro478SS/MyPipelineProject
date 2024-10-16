import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.append(os.path.abspath('./MyPipelines'))
import etl_export_records
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

class TestEtlExportData(unittest.TestCase):

    @patch('etl_export_records.requests.get')
    def test_fetch_data(self, mock_get):
        # Configurar el mock para la respuesta de la API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'commodity': 'Wheat', 'price': 200}]
        mock_get.return_value = mock_response
                   
        # Llamar a la función principal del script
        etl_export_records.main()

        # Verificar que la solicitud GET fue llamada con la URL correcta
        mock_get.assert_called_once_with(api_url, headers={'X-Api-Key': api_key})
             
    @patch('etl_export_records.create_engine')
    def test_redshift_connection(self,  mock_create_engine):
        # Configurar el mock para el motor de SQLAlchemy
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connection_string = f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}'        

        # Llamar a la función principal del script
        etl_export_records.main()

        # Verificar que la cadena de conexión fue creada correctamente
        mock_create_engine.assert_called_once_with(connection_string)
        
if __name__ == '__main__':
    unittest.main()
