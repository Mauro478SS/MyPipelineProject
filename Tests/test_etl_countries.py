import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.append(os.path.abspath('./MyPipelines'))
import etl_countries
from dotenv import load_dotenv

# Asignar variables desde las variables de entorno    
load_dotenv()

redshift_endpoint = os.getenv('REDSHIFT_ENDPOINT')
redshift_db = os.getenv('REDSHIFT_DB')
redshift_user = os.getenv('REDSHIFT_USER')
redshift_password = os.getenv('REDSHIFT_PASSWORD')
redshift_port = os.getenv('REDSHIFT_PORT')
redshift_schema = os.getenv('REDSHIFT_SCHEMA')
redshift_table = os.getenv('TABLE_COUNTRIES')
api_url = os.getenv('API_COU')
api_key = os.getenv('API_KEY')

class TestEtlCountries(unittest.TestCase):

    @patch('etl_countries.requests.get')
    def test_fetch_data(self, mock_get):
        # Configurar el mock para la respuesta de la API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "countryCode": 1220,
                "countryName": "CANADA  ",
                "countryDescription": "CANADA",
                "regionId": 11,
                "gencCode": "CAN"               
            }
        ]
        mock_get.return_value = mock_response
                   
        # Llamar a la función principal del script
        etl_countries.main()
        
        # Verificar que la solicitud GET fue llamada con la URL correcta         
        mock_get.assert_called_once_with('https://api.fas.usda.gov/api/esr/countries',headers={'X-Api-Key': 'A7e4QEAsGHJcySTf4gumahDPpCStKel6lsKhEG6v'})
             
    @patch('etl_countries.create_engine')
    def test_redshift_connection(self,  mock_create_engine):
        # Configurar el mock para el motor de SQLAlchemy
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        connection_string = f'redshift+redshift_connector://{redshift_user}:{redshift_password}@{redshift_endpoint}:{redshift_port}/{redshift_db}'        

        # Llamar a la función principal del script
        etl_countries.main()

        # Verificar que la cadena de conexión fue creada correctamente
        mock_create_engine.assert_called_once_with('redshift+redshift_connector://2024_mauro_sebastian_sanchez:L4!&9^2$xQ@redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com:5439/pda')
        
if __name__ == '__main__':
    unittest.main()
