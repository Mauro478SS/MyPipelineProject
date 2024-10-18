import unittest
from unittest.mock import patch, MagicMock
import os
import sys


sys.path.append(os.path.abspath('./MyPipelines'))
import dim_regions
class TestDimRegions(unittest.TestCase):

    @patch('dim_regions.create_engine')
    def test_main(self, mock_create_engine):

        # Configurar el mock para el motor de SQLAlchemy
        mock_engine = MagicMock()
        mock_create_engine.return_value = mock_engine

        # Mock para la conexión y ejecución de consultas
        mock_connection = mock_engine.connect.return_value.__enter__.return_value
        mock_connection.execute.return_value.rowcount = 5

        # Capturar las consultas ejecutadas
        executed_queries = []

        def capture_execute(query):
            executed_queries.append(query)
            return mock_connection.execute.return_value

        mock_connection.execute.side_effect = capture_execute

        # Ejecutar el script principal
        dim_regions.main()

if __name__ == '__main__':
    unittest.main()