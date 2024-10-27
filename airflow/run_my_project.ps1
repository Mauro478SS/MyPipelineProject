# Levantar el contenedor con Docker Compose
docker-compose up -d --build

# Esperar unos segundos para asegurarse de que el contenedor esté corriendo
Start-Sleep -Seconds 60

# Ejecutar el DAG dentro del contenedor
.\trigger_dag_in_docker.ps1 

