import unittest
from unittest.mock import patch, MagicMock
import mi_script

class TestMiScript(unittest.TestCase):

    @patch('mi_script.requests.get')
    def test_api_request(self, mock_get):
        # Configurar el mock para la respuesta de la API
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{'commodity': 'Wheat', 'price': 200}]
        mock_get.return_value = mock_response

        # Llamar a la función principal del script
        mi_script.main()

        # Verificar que la solicitud GET fue llamada con la URL correcta
        mock_get.assert_called_once_with('https://api.fas.usda.gov/api/esr/commodities', headers={'X-Api-Key': 'A7e4QEAsGHJcySTf4gumahDPpCStKel6lsKhEG6v'})

    @patch('mi_script.create_engine')
    def test_redshift_connection(self, mock_create_engine):
        # Configurar el mock para el motor de SQLAlchemy
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Llamar a la función principal del script
        mi_script.main()

        # Verificar que la cadena de conexión fue creada correctamente
        mock_create_engine.assert_called_once_with('redshift+redshift_connector://2024_mauro_sebastian_sanchez:L4!&9^2$xQ@redshift-pda-cluster.cnuimntownzt.us-east-2.redshift.amazonaws.com:5439/pda')

if __name__ == '__main__':
    unittest.main()