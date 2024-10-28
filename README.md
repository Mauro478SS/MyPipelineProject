# MyPipelineProject
El proyecto consume diferentes APIs para cargar cuatro tablas de stage y luego actualiza las dimensiones correspondientes y una tabla de hechos.
    API Origen Foreign Agricultural Service Data APIs
    ESR Data API - United States Weekly Export Sales of Agricultural Commodity Data

    /api/esr/regions 
        Returns a set of records with Region Codes and Region Names. Use it to associate Region Name with Country records obtained by querying Country end point
    /api/esr/countries 
        Returns a set of records with Countries and their corresponding Regions Codes the Country belongs to. Use it to associate Country Name with Commodity Data records obtained by querying Commodity Data End point
    /api/esr/commodities 
        Returns a set of records with Commodity Information. Use it to associate Commodity Name with Commodity Data records obtained by querying Commodity Data End point

    ​/api​/esr​/exports​/commodityCode​/{commodityCode}​/allCountries​/marketYear​/{marketYear} 
        Given Commodity Code (Ex: 104 for Wheat - White ) and MarketYear (Ex: 2017) this API End point will return a list of US Export records of White Wheat to all applicable countries from USA for the given Market Year. Please see DataReleaseDates end point to get a list of all Commodities and the corresponding Market Year data.

# Pasos de Ejecucion:

1) Clonar el Repositorio https://github.com/Mauro478SS/MyPipelineProject
2) Modificar las variables del proyecto en el archivo <ruta_repo_local>/airflow/dags/MyPipelineProject/MyPipelines/.env donde se debera actualizar el usuario, clave y esquema de la base REDSHIFT_USER/	REDSHIFT_PASSWORD / REDSHIFT_SCHEMA
3) Ejecutar el script <ruta_repo_local>/airflow/CreateDatabaseModel.sql en Redshift para crear el modelo. Previamente editar el archivo indicando el esquema target <Esquema_target>
3) Abrir en windows el programa Docker-Desktop
4) Abrir PowerShell en modo administrador 
5) cd <ruta_repo_local>\airflow
6) Ejecutar el script .\run_my_project.ps1 que levantara la instancia de docker y ejecutara el DAG con los procesos ETL.
7) Monitorear la ejecucion del proceso mediando Airflow ingresando al link http://localhost:8080/home usr: airflow y pass: airflow

# Documentacion

Procesos de ingesta STAGE y tabla target:
    etl_commodities.py >> stg_commodities
    etl_countries.py   >> stg_countries
    etl_regions.py     >> stg_regions
    etl_export_records >> stg_export_records

Procesos de ingesta DIM/FACT y tabla target:
    dim_commodities.py >> dim_commodities 
    dim_countries.py   >> dim_countries 
    dim_regions.py     >> dim_regions 
    ft_export.py       >> ft_export

Configuración
    Se creo un archivo .env en el directorio raíz del proyecto con las siguientes variables de entorno:
    REDSHIFT_ENDPOINT=your_redshift_endpoint
    REDSHIFT_DB=your_redshift_db
    REDSHIFT_USER=your_redshift_user
    REDSHIFT_PASSWORD=your_redshift_password
    REDSHIFT_PORT=your_redshift_port
    REDSHIFT_SCHEMA=your_redshift_schema
    TABLE_COMMODITIES=your_table_name
    API_COM=your_api_url
    API_KEY=your_api_key

Documentación
La documentación del proyecto se ha generado utilizando Sphinx y se encuentra en el directorio _build/html

Registro
    El proyecto utiliza la biblioteca logging para registrar las actividades del ETL. Los mensajes se registran con niveles de información y error.

AirFlow-DAG:
En la carpeta dags se encuentra el archivo DAG.PY para ejecutar los pipelines diaramente a las 20hs en el siguiente orden :
    
    start >> load_stg_commodities >> update_dim_commodities 
    start >> load_stg_countries >> update_dim_countries 
    start >> load_stg_regions >> update_dim_regions 
    start >> load_stg_export_records >> load_ft_export >> end
Docker:
    En la carpeta dags/MyPipelineProject se encuentran los archivos de configuracion de docker docker-compose y requirements necesarios para ejecucion del proyecto.

GITActions: 
    Se configuro el workflow test.ymal en la carpeta /workflows con la ejecucion de todos los test del proyecto.
	
Dashboard: 
	Se creó un dashboard en powerBI que consume el modelo estrella y genera unos gráficos. Ruta: \MyPipelineProject\DashboardTPFinal.pbix. Los datos están importados desde Redshift.


