import unittest
from unittest.mock import patch, MagicMock
import sys
import os

sys.path.append(os.path.abspath('./MyPipelines'))
import etl_export_records 

class TestEtlCommodities(unittest.TestCase):

    @patch('etl_export_records.requests.get')
    def test_fetch_data(self, mock_get):
        # Configurar el mock para la respuesta de la API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'commodity': 'Wheat', 'price': 200}]
        mock_get.return_value = mock_response

        api_url = 'https://api.fas.usda.gov/api/esr/exports/commodityCode/104/allCountries/marketYear/2024'
        api_key = 'A7e4QEAsGHJcySTf4gumahDPpCStKel6lsKhEG6v'
        data = etl_export_records.fetch_data(api_url, api_key)

        # Verificar que la solicitud GET fue llamada con la URL correcta
        mock_get.assert_called_once_with(api_url, headers={'X-Api-Key': api_key})
        self.assertEqual(data, [{'commodity': 'Wheat', 'price': 200}])

    @patch('etl_export_records.create_engine')
    @patch('etl_export_records.pd.DataFrame.to_sql')
    def test_load_to_redshift(self, mock_to_sql, mock_create_engine):
        # Configurar el mock para el motor de SQLAlchemy
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        data = [{'commodity': 'Wheat', 'price': 200}]
        connection_string = 'redshift+redshift_connector://2024_mauro_sebastian_sanchez:L4!&9^2$xQ@redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com:5439/pda'
        table = 'stg_export_records'
        schema = '2024_mauro_sebastian_sanchez_schema'

        etl_export_records.load_to_redshift(data, connection_string, table, schema)

        # Verificar que la cadena de conexión fue creada correctamente
        mock_create_engine.assert_called_once_with(connection_string)
        mock_to_sql.assert_called_once_with(name=table, con=mock_engine, schema=schema, if_exists='replace', index=False)

if __name__ == '__main__':
    unittest.main()
