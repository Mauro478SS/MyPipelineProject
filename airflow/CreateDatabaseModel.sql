-- Establecer el esquema predeterminado
SET search_path TO <Esquema_target>;

-- Crear tablas de STAGE
-- stg_countries definition

DROP TABLE IF EXISTS stg_countries;

CREATE TABLE IF NOT EXISTS stg_countries
(
	countrycode VARCHAR(256)   ENCODE lzo
	,countryname VARCHAR(256)   ENCODE lzo
	,countrydescription VARCHAR(256)   ENCODE lzo
	,regionid VARCHAR(256)   ENCODE lzo
	,genccode VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;

-- stg_export_records definition

DROP TABLE IF EXISTS stg_export_records;

CREATE TABLE IF NOT EXISTS stg_export_records
(
	commoditycode VARCHAR(256)   ENCODE lzo
	,countrycode VARCHAR(256)   ENCODE lzo
	,weeklyexports VARCHAR(256)   ENCODE lzo
	,accumulatedexports VARCHAR(256)   ENCODE lzo
	,outstandingsales VARCHAR(256)   ENCODE lzo
	,grossnewsales VARCHAR(256)   ENCODE lzo
	,currentmynetsales VARCHAR(256)   ENCODE lzo
	,currentmytotalcommitment VARCHAR(256)   ENCODE lzo
	,nextmyoutstandingsales VARCHAR(256)   ENCODE lzo
	,nextmynetsales VARCHAR(256)   ENCODE lzo
	,unitid VARCHAR(256)   ENCODE lzo
	,weekendingdate VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;

-- stg_regions definition

DROP TABLE IF EXISTS stg_regions;

CREATE TABLE IF NOT EXISTS stg_regions
(
	regionid VARCHAR(256)   ENCODE lzo
	,regionname VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;

-- stg_commodities definition

DROP TABLE IF EXISTS stg_commodities;

CREATE TABLE IF NOT EXISTS stg_commodities
(
	commoditycode VARCHAR(256)   ENCODE lzo
	,commodityname VARCHAR(256)   ENCODE lzo
	,unitid VARCHAR(256)   ENCODE lzo
)
DISTSTYLE AUTO
;

-- Crear Dimensiones
-- dim_commodities definition

DROP TABLE IF EXISTS dim_commodities;

CREATE TABLE IF NOT EXISTS dim_commodities
(
	sk_commodities INTEGER  DEFAULT "identity"(324106, 0, '1,1'::text) ENCODE az64
	,commoditycode INTEGER   ENCODE az64
	,commodityname VARCHAR(255)   ENCODE lzo
	,unitid INTEGER   ENCODE az64
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_record BOOLEAN   ENCODE RAW
)
DISTSTYLE AUTO
;

-- dim_countries definition
DROP TABLE IF EXISTS dim_countries;

CREATE TABLE IF NOT EXISTS dim_countries
(
	sk_countries INTEGER  DEFAULT "identity"(324058, 0, '1,1'::text) ENCODE az64
	,countrycode INTEGER   ENCODE az64
	,countryname VARCHAR(255)   ENCODE lzo
	,countrydescription VARCHAR(255)   ENCODE lzo
	,regionid INTEGER   ENCODE az64
	,genccode VARCHAR(255)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_record BOOLEAN   ENCODE RAW
)
DISTSTYLE AUTO
;


-- dim_regions definition

DROP TABLE IF EXISTS dim_regions;

CREATE TABLE IF NOT EXISTS dim_regions
(
	sk_regions INTEGER  DEFAULT "identity"(324110, 0, '1,1'::text) ENCODE az64
	,regionid INTEGER   ENCODE az64
	,regionname VARCHAR(256)   ENCODE lzo
	,start_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,end_date TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
	,current_record BOOLEAN   ENCODE RAW
)
DISTSTYLE AUTO
;


-- Crear tabla de hechos
-- ft_export definition

DROP TABLE IF EXISTS ft_export;

CREATE TABLE IF NOT EXISTS ft_export
(
	sk_export INTEGER  DEFAULT "identity"(325677, 0, '1,1'::text) ENCODE az64
	,sk_commodities VARCHAR(256)   ENCODE lzo
	,sk_countries VARCHAR(256)   ENCODE lzo
	,weeklyexports NUMERIC(10,2)   ENCODE az64
	,accumulatedexports NUMERIC(10,2)   ENCODE az64
	,outstandingsales NUMERIC(10,2)   ENCODE az64
	,currentmynetsales NUMERIC(10,2)   ENCODE az64
	,currentmytotalcommitment NUMERIC(10,2)   ENCODE az64
	,nextmyoutstandingsales VARCHAR(256)   ENCODE lzo
	,nextmynetsales NUMERIC(10,2)   ENCODE az64
	,unitid INTEGER   ENCODE az64
	,weekendingdate TIMESTAMP WITHOUT TIME ZONE   ENCODE az64
)
DISTSTYLE AUTO
;

