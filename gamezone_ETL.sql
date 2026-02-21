-- Crear base de datos de trabajo y activarla
DROP DATABASE IF EXISTS gamezone;
CREATE DATABASE gamezone;
USE gamezone;


-- Crear tabla con datos crudos
DROP TABLE IF EXISTS pedidos_staging ;
CREATE TABLE pedidos_staging (
	cliente_id VARCHAR(255),
    pedido_id  VARCHAR(255),
    fecha_compra  VARCHAR(255),
    fecha_envio  VARCHAR(255),
    nombre_producto  VARCHAR(255),
    producto_id VARCHAR(255),
    precio_usd  VARCHAR(255),
    plataforma_compra  VARCHAR(255),
    canal_marketing VARCHAR(255),
    metodo_registro VARCHAR(255),
    codigo_pais  VARCHAR(255)
);


DROP TABLE IF EXISTS referencia_geo_staging;
CREATE TABLE referencia_geo_staging (
	codigo_pais  VARCHAR(255),
    region VARCHAR(255)
);


-- Cargar los datos crudos en la tabla pedidos_staging
TRUNCATE pedidos_staging;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/pedidos.csv'
INTO TABLE pedidos_staging
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- Cargar los datos crudos en la tabla referencia_geo_staging table
TRUNCATE referencia_geo_staging;
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/referencia_geo.csv'
INTO TABLE referencia_geo_staging
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
IGNORE 1 LINES;


-- Clean orders table
DROP TABLE IF EXISTS pedidos_limpia;
CREATE TABLE pedidos_limpia AS
WITH pedidos_limpia AS
(
	SELECT 
		TRIM(cliente_id) AS cliente_id,
		TRIM(pedido_id) AS pedido_id,
		CASE
			WHEN fecha_compra LIKE '%:%' THEN STR_TO_DATE(NULLIF(TRIM(SUBSTRING(fecha_compra, 1, 10)), ''), '%m-%d-%Y')
			ELSE STR_TO_DATE(NULLIF(TRIM(fecha_compra), ''), '%m/%d/%Y')
		END AS fecha_compra,
		STR_TO_DATE(NULLIF(TRIM(fecha_envio), ''), '%m/%d/%Y') AS fecha_envio,
		CASE
			WHEN TRIM(nombre_producto) = '27inches 4k gaming monitor' THEN '27in 4K gaming monitor'
			ELSE TRIM(nombre_producto)
		END AS nombre_producto,
		TRIM(producto_id) AS producto_id,
		CASE
			WHEN TRIM(precio_usd) = '' THEN NULL
			ELSE CAST(TRIM(precio_usd) AS DECIMAL(10, 2))
		END AS precio_usd,
		TRIM(plataforma_compra) AS plataforma_compra,
		CASE
			WHEN TRIM(canal_marketing) = '' THEN 'unknown'
			ELSE TRIM(canal_marketing)
		END AS canal_marketing,
		CASE
			WHEN TRIM(metodo_registro) = '' THEN 'unknown'
			ELSE TRIM(metodo_registro)
		END AS metodo_registro,
		CASE
			WHEN REPLACE(TRIM(codigo_pais), '\r', '') = '' THEN NULL
			ELSE REPLACE(TRIM(codigo_pais), '\r', '')
		END AS codigo_pais
	FROM pedidos_staging
),

duplicates_to_delete AS (
SELECT
	*,
	ROW_NUMBER() OVER (
		PARTITION BY 
			pedido_id,
			cliente_id,
			fecha_compra,
            fecha_envio,
			nombre_producto,
            producto_id,
			precio_usd,
            plataforma_compra,
			canal_marketing,
			metodo_registro,
			codigo_pais
	) as row_num
    FROM pedidos_limpia
)
SELECT
	cliente_id,
	pedido_id,
	fecha_compra,
	fecha_envio,
	nombre_producto,
	producto_id,
	precio_usd,
	plataforma_compra,
	canal_marketing,
	metodo_registro,
	codigo_pais,
    datediff(fecha_envio, fecha_compra) AS dias_para_envio
FROM duplicates_to_delete
WHERE row_num = 1
;


-- Registro de problemas
-- Duplicados
WITH detalles_duplicado AS (
    SELECT 
        pedido_id,
        COUNT(*) AS row_count,
        -- Revisar qué columnas tienen diferencias
        CASE WHEN COUNT(DISTINCT cliente_id) > 1 THEN 1 ELSE 0 END AS cliente_id_dif,
        CASE WHEN COUNT(DISTINCT fecha_compra) > 1 THEN 1 ELSE 0 END AS fecha_dif,
        CASE WHEN COUNT(DISTINCT fecha_envio) > 1 THEN 1 ELSE 0 END AS fecha_envio_dif,
        CASE WHEN COUNT(DISTINCT nombre_producto) > 1 THEN 1 ELSE 0 END AS nombre_producto_dif,
        CASE WHEN COUNT(DISTINCT producto_id) > 1 THEN 1 ELSE 0 END AS producto_id_dif,
        CASE WHEN COUNT(DISTINCT precio_usd) > 1 THEN 1 ELSE 0 END AS precio_usd_dif,
        CASE WHEN COUNT(DISTINCT plataforma_compra) > 1 THEN 1 ELSE 0 END AS plataforma_compra_dif,
        CASE WHEN COUNT(DISTINCT canal_marketing) > 1 THEN 1 ELSE 0 END AS canal_marketing_dif,
        CASE WHEN COUNT(DISTINCT metodo_registro) > 1 THEN 1 ELSE 0 END AS metodo_registro_dif,
        CASE WHEN COUNT(DISTINCT codigo_pais) > 1 THEN 1 ELSE 0 END AS codigo_pais_dif
    FROM pedidos_staging
    GROUP BY pedido_id
    HAVING COUNT(*) > 1
),

duplicate_total AS
(
SELECT 
    CASE 
        WHEN cliente_id_dif = 1 AND 
             fecha_dif = 0 AND 
             fecha_envio_dif = 0  AND
             nombre_producto_dif = 0 AND 
             producto_id_dif = 0  AND
             precio_usd_dif = 0 AND 
             plataforma_compra_dif = 0  AND
             canal_marketing_dif = 0 AND 
             metodo_registro_dif = 0 AND 
             codigo_pais_dif = 0 
        THEN 'Solo cliente_id difiere'
        
        WHEN cliente_id_dif = 0 AND 
             fecha_dif = 0 AND 
             fecha_envio_dif = 0  AND
             nombre_producto_dif = 0 AND 
             producto_id_dif = 0  AND
             precio_usd_dif = 0 AND 
             plataforma_compra_dif = 0  AND
             canal_marketing_dif = 0 AND 
             metodo_registro_dif = 1 AND 
             codigo_pais_dif = 0 
        THEN 'Solo metodo_registro difiere'
        
        WHEN cliente_id_dif = 0 AND 
             fecha_dif = 0 AND 
             fecha_envio_dif = 0  AND
             nombre_producto_dif = 0 AND 
             producto_id_dif = 0  AND
             precio_usd_dif = 0 AND 
             plataforma_compra_dif = 0  AND
             canal_marketing_dif = 0 AND 
             metodo_registro_dif = 0 AND 
             codigo_pais_dif = 0 
        THEN 'Duplicados exactos'
        
        ELSE 'Múltiples columnas difieren'
    END AS tipo_conflicto,
    COUNT(DISTINCT pedido_id) AS cantidad_pedido
FROM detalles_duplicado
GROUP BY tipo_conflicto
ORDER BY cantidad_pedido DESC
)

-- Fechas en blanco
SELECT
	'pedidos_staging' AS 'Tabla',
	'fecha_compra' AS 'Columna',
	'Fechas de compra en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM pedidos_staging
WHERE TRIM(fecha_compra) = ''

UNION ALL

-- Formato de fechas inconsistentes
SELECT
	'pedidos_staging' AS 'Tabla',
	'fecha_compra' AS 'Columna',
	'Formatos de fecha inconsistentes (MM/DD/YYYY vs MM-DD-YYYY HH:MM:SS)' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Aplicar el formato de fecha correcto' AS 'Solución'
FROM pedidos_staging
WHERE TRIM(fecha_compra) LIKE '__-__-____ __:__:__'

UNION ALL

-- Ortografía inconsistente
SELECT
	'pedidos_staging' AS 'Tabla',
	'nombre_producto' AS 'Columna',
	'Ortografía inconsistente: "27inches 4k gaming monitor"' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar con "27in 4K gaming monitor"' AS 'Solución'
FROM pedidos_staging
WHERE TRIM(nombre_producto) = '27inches 4k gaming monitor'

UNION ALL

-- Columna precio_usd en blanco
SELECT
	'pedidos_staging' AS 'Tabla',
	'precio_usd' AS 'Columna',
	'Valores de precio en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM pedidos_staging
WHERE TRIM(precio_usd) = ''

UNION ALL

-- Columna precio_usd con valor cero (0)
SELECT
	'pedidos_staging' AS 'Tabla',
	'precio_usd' AS 'Columna',
	'Precio con valor cero' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM pedidos_staging
WHERE CAST(TRIM(precio_usd) AS DECIMAL) = 0

UNION ALL

-- Columna canal_marketing con valores en blanco
SELECT
	'pedidos_staging' AS 'Tabla',
	'canal_marketing' AS 'Columna',
	'Canal de marketing en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar por "desconocido"' AS 'Solución'
FROM pedidos_staging
WHERE TRIM(canal_marketing) = ''

UNION ALL

-- Columna metodo_registro con valores en blanco
SELECT
	'pedidos_staging' AS 'Tabla',
	'metodo_registro' AS 'Columna',
	'Método de creación de cuenta en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar por "desconocido"' AS 'Solución'
FROM pedidos_staging
WHERE TRIM(metodo_registro) = ''

UNION ALL

-- Columna codigo_pais con valores en blanco
SELECT
	'pedidos_staging' AS 'Tabla',
	'codigo_pais' AS 'Columna',
	'Código de país en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM pedidos_staging
WHERE REPLACE(TRIM(codigo_pais), '\r', '') = ''

UNION ALL

-- Región en blanco
SELECT
	'referencia_geo_staging' AS 'Tabla',
	'region' AS 'Columna',
	'Código de región en blanco' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM referencia_geo_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Asignar de acuerdo con el código del país' AS 'Solución'
FROM referencia_geo_staging
WHERE REPLACE(TRIM(region), '\r', '') = ''

UNION ALL

-- Ortografía inconsistente
SELECT
	'referencia_geo_staging' AS 'Tabla',
	'region' AS 'Columna',
	'Ortografía inconsistente: "North America"' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM referencia_geo_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Reemplazar por "NA"' AS 'Solución'
FROM referencia_geo_staging
WHERE REPLACE(TRIM(region), '\r', '') = 'North America'

UNION ALL

-- Valores sin sentido
SELECT
	'referencia_geo_staging' AS 'Tabla',
	'region' AS 'Columna',
	'Valor sin sentido: "X.x"' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM referencia_geo_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Asignar de acuerdo con el código del país' AS 'Solución'
FROM referencia_geo_staging
WHERE REPLACE(region, '\r', '') = 'X.x'

UNION ALL

-- Duplicados
SELECT
	'pedidos_staging' AS 'Tabla',
	'Todas' AS 'Columna',
	'Duplicados exactos' AS 'Problema',
    cantidad_pedido AS 'Número de filas',
    CONCAT(CAST((cantidad_pedido/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'Sí' AS '¿Resoluble?',
	'Eliminar duplicados, conservar una fila' AS 'Solución'
FROM duplicate_total
WHERE tipo_conflicto = 'Duplicados exactos'

UNION ALL

SELECT
	'pedidos_staging' AS 'Tabla',
	'Todas' AS 'Columna',
	'Duplicados parciales: solo cliente_id difiere' AS 'Problema',
    cantidad_pedido AS 'Número de filas',
    CONCAT(CAST((cantidad_pedido/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM duplicate_total
WHERE tipo_conflicto = 'Solo cliente_id difiere'

UNION ALL

SELECT
	'pedidos_staging' AS 'Tabla',
	'Todas' AS 'Columna',
	'Duplicados parciales: solo metodo_registro difiere' AS 'Problema',
    cantidad_pedido AS 'Número de filas',
    CONCAT(CAST((cantidad_pedido/(SELECT COUNT(*) FROM pedidos_staging))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM duplicate_total
WHERE tipo_conflicto = 'Solo metodo_registro difiere'

UNION ALL

SELECT
	'pedidos_limpia' AS 'Tabla',
	'fecha_envio' AS 'Columna',
	'Fecha de envío anterior a la fecha de compra' AS 'Problema',
    COUNT(*) AS 'Número de filas',
    CONCAT(CAST((COUNT(*)/(SELECT COUNT(*) FROM pedidos_limpia))*100 AS NCHAR(5)), '%') AS 'Porcentaje',
	'No' AS '¿Resoluble?',
	'Requiere más información de la lógica y contexto del negocio y/o forma de captura de los datos. Dejar como está' AS 'Solución'
FROM pedidos_limpia
WHERE dias_para_envio < 0
;


-- Tabla geo_lookup limpia
DROP TABLE IF EXISTS referencia_geo_staging_limpia;
CREATE TABLE referencia_geo_staging_limpia AS
SELECT
	TRIM(codigo_pais) AS codigo_pais,
	CASE
	WHEN codigo_pais = 'IE' THEN 'EMEA'
    WHEN codigo_pais = 'LB' THEN 'EMEA'
	WHEN codigo_pais = 'MH' THEN 'APAC'
	WHEN codigo_pais = 'PG' THEN 'APAC'
    WHEN REPLACE(TRIM(region), '\r', '') = 'North America' THEN 'NA'
    ELSE REPLACE(TRIM(region), '\r', '')
    END AS region
FROM referencia_geo_staging
;


-- Exportar tablas como CSV
-- Tabla pedidos_limpia
SELECT 'cliente_id',
	'pedido_id',
	'fecha_compra',
	'fecha_envio',
	'nombre_producto',
	'producto_id',
	'precio_usd',
	'plataforma_compra',
	'canal_marketing',
	'metodo_registro',
	'codigo_pais'

UNION ALL

SELECT cliente_id,
	pedido_id,
	IFNULL(fecha_compra, NULL),
	fecha_envio,
	nombre_producto,
	producto_id,
	IFNULL(precio_usd, NULL),
	plataforma_compra,
	canal_marketing,
	metodo_registro,
	IFNULL(codigo_pais, NULL)
FROM pedidos_limpia
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/pedidos_limpia.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';


-- Tabla clean_geo_lookup
SELECT 'codigo_pais',
	'region'

UNION ALL

SELECT codigo_pais,
	region
FROM referencia_geo_staging_limpia
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/referencia_geo_staging_limpia.csv'
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\r\n';
