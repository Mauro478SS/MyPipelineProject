import unittest
from unittest.mock import patch, MagicMock
import os
import sys


sys.path.append(os.path.abspath('./MyPipelines'))
import dim_regions
class TestDimRegions(unittest.TestCase):

    @patch('dim_regions.create_engine')
    def test_main(self, mock_create_engine):
        # Configurar las variables de entorno simuladas
        os.environ['REDSHIFT_ENDPOINT'] = 'test-endpoint'
        os.environ['REDSHIFT_DB'] = 'test-db'
        os.environ['REDSHIFT_USER'] = 'test-user'
        os.environ['REDSHIFT_PASSWORD'] = 'test-password'
        os.environ['REDSHIFT_PORT'] = '5439'
        os.environ['REDSHIFT_SCHEMA'] = 'test-schema'
        os.environ['TABLE_DIM_REGIONS'] = 'dim_regions'
        os.environ['TABLE_REGIONS'] = 'stg_regions'

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

        # Imprimir las consultas ejecutadas
        for query in executed_queries:
            print(query)

        # Verificar que las consultas SQL fueron ejecutadas correctamente
        mock_connection.execute.assert_any_call("""
                UPDATE "test-schema".dim_regions
                SET end_date = CURRENT_TIMESTAMP,
                    current_record = FALSE
                FROM "test-schema".stg_regions
                WHERE dim_regions.regionid = stg_regions.regionid
                AND dim_regions.regionname <> stg_regions.regionname
                AND dim_regions.current_record = TRUE;
            """)
        
        mock_connection.execute.assert_any_call("""
                INSERT INTO "test-schema".dim_regions (regionid, regionname, start_date, end_date, current_record)
                SELECT regionid::integer, regionname, CURRENT_TIMESTAMP, NULL, TRUE
                FROM "test-schema".stg_regions
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM "test-schema".dim_regions
                    WHERE dim_regions.regionid = stg_regions.regionid
                    AND dim_regions.regionname = stg_regions.regionname
                    AND dim_regions.current_record = TRUE
                );
            """)

if __name__ == '__main__':
    unittest.main()