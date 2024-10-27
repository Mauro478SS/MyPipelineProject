# Script: trigger_dag_in_docker.ps1

# Nombre del servicio configurado en docker-compose.yml
$service_name = 'airflow-scheduler'

# ID del DAG que quieres ejecutar
$dag_id = "DAG_etl_pipeline"

# Ejecutar el DAG manualmente dentro del contenedor
docker-compose exec $service_name airflow dags trigger $dag_id

# Verificar el estado de la ejecución
Start-Sleep -Seconds 5  # Esperar unos segundos para que el DAG comience a ejecutarse
docker-compose exec $service_name airflow dags list-runs -d $dag_id
