-- Crear base de datos de trabajo y activarla
DROP DATABASE IF EXISTS gamezone;
CREATE DATABASE gamezone;
USE gamezone;


-- Crear tabla con datos crudos
DROP TABLE IF EXISTS staging_orders;
CREATE TABLE staging_orders (
	user_id VARCHAR(255),
    order_id VARCHAR(255),
    purchase_ts VARCHAR(255),
    ship_ts VARCHAR(255),
    product_name VARCHAR(255),
    product_id VARCHAR(255),
    usd_price VARCHAR(255),
    purchase_platform VARCHAR(255),
    marketing_channel VARCHAR(255),
    account_creation_method VARCHAR(255),
    country_code VARCHAR(255)
);


DROP TABLE IF EXISTS staging_geo_lookup;
CREATE TABLE staging_geo_lookup (
	country_code VARCHAR(255),
    region VARCHAR(255)
);


-- Cargar los datos crudos en la tabla staging_orders
TRUNCATE staging_orders;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/orders.csv'
INTO TABLE staging_orders
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- Cargar los datos crudos en la tabla staging_geo_lookup table
TRUNCATE staging_geo_lookup;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/geo_lookup.csv'
INTO TABLE staging_geo_lookup
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- Clean orders table
DROP TABLE IF EXISTS clean_orders;
CREATE TABLE clean_orders AS
WITH clean_orders AS
(
	SELECT 
		TRIM(user_id) AS user_id,
		TRIM(order_id) AS order_id,
		CASE
			WHEN purchase_ts LIKE '%:%' THEN STR_TO_DATE(NULLIF(TRIM(SUBSTRING(purchase_ts, 1, 10)), ''), '%m-%d-%Y')
			ELSE STR_TO_DATE(NULLIF(TRIM(purchase_ts), ''), '%m/%d/%Y')
		END AS purchase_ts,
		STR_TO_DATE(NULLIF(TRIM(ship_ts), ''), '%m/%d/%Y') AS ship_ts,
		CASE
			WHEN TRIM(product_name) = '27inches 4k gaming monitor' THEN '27in 4K gaming monitor'
			ELSE TRIM(product_name)
		END AS product_name,
		TRIM(product_id) AS product_id,
		CASE
			WHEN TRIM(usd_price) = '' THEN NULL
			ELSE CAST(TRIM(usd_price) AS DECIMAL(10, 2))
		END AS usd_price,
		TRIM(purchase_platform) AS purchase_platform,
		CASE
			WHEN TRIM(marketing_channel) = '' THEN 'unknown'
			ELSE TRIM(marketing_channel)
		END AS marketing_channel,
		CASE
			WHEN TRIM(account_creation_method) = '' THEN 'unknown'
			ELSE TRIM(account_creation_method)
		END AS account_creation_method,
		CASE
			WHEN REPLACE(TRIM(country_code), '\r', '') = '' THEN NULL
			ELSE REPLACE(TRIM(country_code), '\r', '')
		END AS country_code
	FROM staging_orders
),

duplicates_to_delete AS (
SELECT
	*,
	ROW_NUMBER() OVER (
		PARTITION BY 
			order_id,
			user_id,
			purchase_ts,
            ship_ts,
			product_name,
            product_id,
			usd_price,
            purchase_platform,
			marketing_channel,
			account_creation_method,
			country_code
	) as row_num
    FROM clean_orders
)
SELECT
	user_id,
	order_id,
	purchase_ts,
	ship_ts,
	product_name,
	product_id,
	usd_price,
	purchase_platform,
	marketing_channel,
	account_creation_method,
	country_code,
    datediff(ship_ts, purchase_ts) AS time_to_ship
FROM duplicates_to_delete
WHERE row_num = 1
;


-- Registro de problemas

-- Duplicados
WITH duplicate_details AS (
    SELECT 
        order_id,
        COUNT(*) AS row_count,
        -- Revisar qué columnas tienen diferencias
        CASE WHEN COUNT(DISTINCT user_id) > 1 THEN 1 ELSE 0 END AS user_diff,
        CASE WHEN COUNT(DISTINCT purchase_ts) > 1 THEN 1 ELSE 0 END AS ts_diff,
        CASE WHEN COUNT(DISTINCT ship_ts) > 1 THEN 1 ELSE 0 END AS ship_ts_diff,
        CASE WHEN COUNT(DISTINCT product_name) > 1 THEN 1 ELSE 0 END AS product_diff,
        CASE WHEN COUNT(DISTINCT product_id) > 1 THEN 1 ELSE 0 END AS product_id_diff,
        CASE WHEN COUNT(DISTINCT usd_price) > 1 THEN 1 ELSE 0 END AS price_diff,
        CASE WHEN COUNT(DISTINCT purchase_platform) > 1 THEN 1 ELSE 0 END AS purchase_platform_diff,
        CASE WHEN COUNT(DISTINCT marketing_channel) > 1 THEN 1 ELSE 0 END AS channel_diff,
        CASE WHEN COUNT(DISTINCT account_creation_method) > 1 THEN 1 ELSE 0 END AS method_diff,
        CASE WHEN COUNT(DISTINCT country_code) > 1 THEN 1 ELSE 0 END AS country_diff
    FROM staging_orders
    GROUP BY order_id
    HAVING COUNT(*) > 1
),

duplicate_total AS
(
SELECT 
    CASE 
        WHEN user_diff = 1 AND 
             ts_diff = 0 AND 
             ship_ts_diff = 0  AND
             product_diff = 0 AND 
             product_id_diff = 0  AND
             price_diff = 0 AND 
             purchase_platform_diff = 0  AND
             channel_diff = 0 AND 
             method_diff = 0 AND 
             country_diff = 0 
        THEN 'Solo user_id difiere'
        
        WHEN user_diff = 0 AND 
             ts_diff = 0 AND 
             ship_ts_diff = 0  AND
             product_diff = 0 AND 
             product_id_diff = 0  AND
             price_diff = 0 AND 
             purchase_platform_diff = 0  AND
             channel_diff = 0 AND 
             method_diff = 1 AND 
             country_diff = 0 
        THEN 'Solo account_creation_method difiere'
        
        WHEN user_diff = 0 AND 
             ts_diff = 0 AND 
             ship_ts_diff = 0  AND
             product_diff = 0 AND 
             product_id_diff = 0  AND
             price_diff = 0 AND 
             purchase_platform_diff = 0  AND
             channel_diff = 0 AND 
             method_diff = 0 AND 
             country_diff = 0 
        THEN 'Duplicados exactos'
        
        ELSE 'Múltiples columnas difieren'
    END AS conflict_type,
    COUNT(DISTINCT order_id) AS order_count
FROM duplicate_details
GROUP BY conflict_type
ORDER BY order_count DESC
)

-- Fechas en blanco
SELECT
	'staging_orders' AS 'Tabla',
	'purchase_ts' AS 'Columna',
	'Fechas de compra en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM staging_orders
WHERE TRIM(purchase_ts) = ''

UNION ALL

-- Formato de fechas inconsistentes
SELECT
	'staging_orders' AS 'Tabla',
	'purchase_ts' AS 'Columna',
	'Formatos de fecha inconsistentes (MM/DD/YYYY vs MM-DD-YYYY HH:MM:SS)' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Aplicar el formato de fecha correcto' AS 'Solución'
FROM staging_orders
WHERE TRIM(purchase_ts) LIKE '__-__-____ __:__:__'

UNION ALL

-- Ortografía inconsistente
SELECT
	'staging_orders' AS 'Tabla',
	'product_name' AS 'Columna',
	'Ortografía inconsistente: "27inches 4k gaming monitor"' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar con "27in 4K gaming monitor"' AS 'Solución'
FROM staging_orders
WHERE TRIM(product_name) = '27inches 4k gaming monitor'

UNION ALL

-- Columna usd_price en blanco
SELECT
	'staging_orders' AS 'Tabla',
	'usd_price' AS 'Columna',
	'Valores de precio en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM staging_orders
WHERE TRIM(usd_price) = ''

UNION ALL

-- Columna usd_price con valor cero (0)
SELECT
	'staging_orders' AS 'Tabla',
	'usd_price' AS 'Columna',
	'Precio con valor cero' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM staging_orders
WHERE CAST(TRIM(usd_price) AS DECIMAL) = 0

UNION ALL

-- Columna marketing_channel con valores en blanco
SELECT
	'staging_orders' AS 'Tabla',
	'marketing_channel' AS 'Columna',
	'Canal de marketing en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar por "desconocido"' AS 'Solución'
FROM staging_orders
WHERE TRIM(marketing_channel) = ''

UNION ALL

-- Columna account_creation_method con valores en blanco
SELECT
	'staging_orders' AS 'Tabla',
	'account_creation_method' AS 'Columna',
	'Método de creación de cuenta en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar por "desconocido"' AS 'Solución'
FROM staging_orders
WHERE TRIM(account_creation_method) = ''

UNION ALL

-- Columna country_code con valores en blanco
SELECT
	'staging_orders' AS 'Tabla',
	'country_code' AS 'Columna',
	'Código de país en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM staging_orders
WHERE REPLACE(TRIM(country_code), '\r', '') = ''

UNION ALL

-- Región en blanco
SELECT
	'staging_geo_lookup' AS 'Tabla',
	'region' AS 'Columna',
	'Código de región en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_geo_lookup))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Asignar de acuerdo con el código del país' AS 'Solución'
FROM staging_geo_lookup
WHERE REPLACE(TRIM(region), '\r', '') = ''

UNION ALL

-- Ortografía inconsistente
SELECT
	'staging_geo_lookup' AS 'Tabla',
	'region' AS 'Columna',
	'Ortografía inconsistente: "North America"' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_geo_lookup))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar por "NA"' AS 'Solución'
FROM staging_geo_lookup
WHERE REPLACE(TRIM(region), '\r', '') = 'North America'

UNION ALL

-- Valores sin sentido
SELECT
	'staging_geo_lookup' AS 'Tabla',
	'region' AS 'Columna',
	'Valor sin sentido: "X.x"' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM staging_geo_lookup))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Asignar de acuerdo con el código del país' AS 'Solución'
FROM staging_geo_lookup
WHERE REPLACE(region, '\r', '') = 'X.x'

UNION ALL

-- Duplicados
SELECT
	'staging_orders' AS 'Tabla',
	'Todas' AS 'Columna',
	'Duplicados exactos' AS 'Problema',
    order_count AS 'Número de filas',
    CONCAT(CAST((order_count/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Eliminar duplicados, conservar una fila' AS 'Solución'
FROM duplicate_total
WHERE conflict_type = 'Duplicados exactos'

UNION ALL

SELECT
	'staging_orders' AS 'Tabla',
	'Todas' AS 'Columna',
	'Duplicados parciales: solo user_id difiere' AS 'Problema',
    order_count AS 'Número de filas',
    CONCAT(CAST((order_count/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM duplicate_total
WHERE conflict_type = 'Solo user_id difiere'

UNION ALL

SELECT
	'staging_orders' AS 'Tabla',
	'Todas' AS 'Columna',
	'Duplicados parciales: solo account_creation_method difiere' AS 'Problema',
    order_count AS 'Número de filas',
    CONCAT(CAST((order_count/(SELECT COUNT(*) FROM staging_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM duplicate_total
WHERE conflict_type = 'Solo account_creation_method difiere'

UNION ALL

SELECT
	'clean_orders' AS 'Tabla',
	'ship_ts' AS 'Columna',
	'Fecha de envío anterior a la fecha de compra' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM clean_orders))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM clean_orders
WHERE time_to_ship < 0
;


-- Tabla geo_lookup limpia
DROP TABLE IF EXISTS clean_geo_lookup;
CREATE TABLE clean_geo_lookup AS
SELECT
	TRIM(country_code) AS country_code,
	CASE
	WHEN country_code = 'IE' THEN 'EMEA'
    WHEN country_code = 'LB' THEN 'EMEA'
	WHEN country_code = 'MH' THEN 'APAC'
	WHEN country_code = 'PG' THEN 'APAC'
    WHEN REPLACE(TRIM(region), '\r', '') = 'North America' THEN 'NA'
    ELSE REPLACE(TRIM(region), '\r', '')
    END AS region
FROM staging_geo_lookup
;


-- Exportar tablas como CSV
-- Tabla clean_orders
SELECT 'user_id',
	'order_id',
	'purchase_ts',
	'ship_ts',
	'product_name',
	'product_id',
	'usd_price',
	'purchase_platform',
	'marketing_channel',
	'account_creation_method',
	'country_code',
    'time_to_ship'

UNION

SELECT user_id,
	order_id,
	IFNULL(purchase_ts, "N/A"),
	ship_ts,
	product_name,
	product_id,
	IFNULL(usd_price, "N/A"),
	purchase_platform,
	marketing_channel,
	account_creation_method,
	IFNULL(country_code, "N/A"),
    time_to_ship
FROM clean_orders
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/clean_orders.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';


-- Tabla clean_geo_lookup
SELECT 'country_code',
	'region'

UNION

SELECT country_code,
	region
FROM clean_geo_lookup
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/clean_geo_lookup.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';