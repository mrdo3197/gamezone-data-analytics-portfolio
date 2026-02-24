# Análisis de Ventas para Gamezone

# Contexto del Proyecto
**Gamezone**, fundada en 2018, es una empresa global de comercio electrónico especializada en la venta de productos de videojuegos nuevos y reacondicionados. Opera principalmente a través de su sitio web y aplicación móvil, atendiendo a clientes en múltiples regiones del mundo. El modelo de negocio se centra en la venta directa al consumidor, con un enfoque en productos populares de gaming como consolas, accesorios y equipos especializados.

La compañía cuenta con datos históricos de ventas, esfuerzos de marketing, eficiencia operativa y portafolio de productos que han sido subutilizados. Este proyecto analiza y sintetiza dicha información para identificar insights críticos que impulsen el éxito comercial de Gamezone.

Los hallazgos y recomendaciones son realizados en la siguientes areas clave:

**- Análisis de tendencias de ventas:** Evaluación de patrones históricos de venta, con foco en ingresos, pedidos y valor promedio del pedido.

**- Performance por producto:** Análisis del portafolio de Gamezone para entender su contribución e impacto en los resultados comerciales.

Las consultas de SQL utilizados para inspeccionar y limpiar los datos para este análisis se pueden encontrar aquí [link](https://github.com/mrdo3197/gamezone-data-analytics-portfolio/blob/main/gamezone_ETL.sql).

Un dashboard interactivo de Tableau utilizado para reportar y explorar tendencias de ventas se puede encontrar aquí [link].

# Estructura de Datos y Chequeos Iniciales

Los datos de pedidos de Gamezone están alojados en una sola tabla que contiene 21,864 registros, donde cada registro representa un ítem único en el pedido de un cliente. Esta estructura es ideal para analizar el rendimiento de productos desde múltiples dimensiones.

Métricas Principales:

- ```usd_price```: Valor monetario de cada transacción

- ```purchase_ts```: Marca de tiempo de compra

- ```ship_ts```: Marca de tiempo de envío

Dimensiones Clave para Segmentación:

- ```product_name``` y ```product_id```: Para análisis de rendimiento de productos

- ```country_code```: Para análisis geográfico de ventas por producto

- ```purchase_platform```: Para entender el comportamiento de compra por dispositivo

- ```marketing_channel```: Para atribución de ventas (secundario para inventario)

# Executive Summary

### Overview of Findings

Explain the overarching findings, trends, and themes in 2-3 sentences here. This section should address the question: "If a stakeholder were to take away 3 main insights from your project, what are the most important things they should know?" You can put yourself in the shoes of a specific stakeholder - for example, a marketing manager or finance director - to think creatively about this section.

[Visualization, including a graph of overall trends or snapshot of a dashboard]



# Insights Deep Dive
### Category 1:

* **Main insight 1.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 2.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 3.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 4.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.

[Visualization specific to category 1]


### Category 2:

* **Main insight 1.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 2.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 3.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 4.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.

[Visualization specific to category 2]


### Category 3:

* **Main insight 1.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 2.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 3.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 4.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.

[Visualization specific to category 3]


### Category 4:

* **Main insight 1.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 2.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 3.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.
  
* **Main insight 4.** More detail about the supporting analysis about this insight, including time frames, quantitative values, and observations about trends.

[Visualization specific to category 4]



# Recommendations:

Based on the insights and findings above, we would recommend the [stakeholder team] to consider the following: 

* Specific observation that is related to a recommended action. **Recommendation or general guidance based on this observation.**
  
* Specific observation that is related to a recommended action. **Recommendation or general guidance based on this observation.**
  
* Specific observation that is related to a recommended action. **Recommendation or general guidance based on this observation.**
  
* Specific observation that is related to a recommended action. **Recommendation or general guidance based on this observation.**
  
* Specific observation that is related to a recommended action. **Recommendation or general guidance based on this observation.**
  


# Assumptions and Caveats:

Throughout the analysis, multiple assumptions were made to manage challenges with the data. These assumptions and caveats are noted below:

* Assumption 1 (ex: missing country records were for customers based in the US, and were re-coded to be US citizens)
  
* Assumption 1 (ex: data for December 2021 was missing - this was imputed using a combination of historical trends and December 2020 data)
  
* Assumption 1 (ex: because 3% of the refund date column contained non-sensical dates, these were excluded from the analysis)
