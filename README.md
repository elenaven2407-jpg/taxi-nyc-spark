# Análisis en batch de viajes en taxi amarillo de Nueva York con Apache Spark

Proyecto de la **Tarea 3 – Procesamiento de Datos con Apache Spark** del curso Big Data (código 202016911), Universidad Nacional Abierta y a Distancia (UNAD).

La aplicación `taxi_batch.py` procesa en lote (batch) los registros de viajes en taxi amarillo de la ciudad de Nueva York de **enero de 2025** con PySpark. Realiza la carga, la limpieza, la transformación, el análisis exploratorio (EDA) y la visualización de los resultados.

## Problema

¿Cómo varían la demanda, el costo y las propinas de los viajes en taxi amarillo de Nueva York según la hora, el día de la semana y la zona de recogida?

## Conjunto de datos

- **Nombre:** NYC TLC Trip Record Data – Yellow Taxi Trip Records
- **Fuente:** Comisión de Taxis y Limusinas de Nueva York (TLC): <https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page>
- **Archivo usado:** `yellow_tripdata_2025-01.parquet` (3.475.226 registros)
- **Formato:** Parquet

El archivo de datos **no se incluye** en este repositorio por su tamaño. Descárguelo desde la página de la TLC y guárdelo en la misma carpeta que `taxi_batch.py`.

## Requisitos

- Python 3
- Apache Spark 3.5.x con PySpark (probado con Spark 3.5.8, Java 11)
- Bibliotecas de Python: `pandas` y `matplotlib`

## Ejecución

```bash
spark-submit taxi_batch.py yellow_tripdata_2025-01.parquet 2025-01
```

Argumentos:

1. Ruta del archivo Parquet.
2. Mes del archivo en formato `AAAA-MM`. Se usa para descartar registros con fecha fuera del periodo.

Si no se indican argumentos, el programa usa por defecto `yellow_tripdata_2025-01.parquet` y `2025-01`.

## Qué hace el código

1. **Carga:** lee el archivo Parquet en un DataFrame y muestra el esquema y el número de registros.
2. **Limpieza:** elimina duplicados y registros con nulos en columnas críticas; excluye viajes con distancia, tarifa o total no positivos, con duración menor o igual a 0 o de 360 minutos o más, y con fecha de recogida fuera del mes.
3. **Transformación:** crea las columnas `hora`, `dia_num`, `duracion_min`, `velocidad_mph` y `pct_propina`.
4. **Análisis exploratorio:** estadísticas descriptivas, viajes por hora y por día de la semana, zonas de recogida y de destino más frecuentes, correlaciones con la tarifa, y propina por tipo de pago y por hora.
5. **Visualización:** genera cuatro gráficos PNG en la carpeta `salida_batch/`.

## Salida

Las tablas se muestran en la consola y los gráficos se guardan en `salida_batch/`:

| Archivo | Contenido |
| --- | --- |
| `1_viajes_por_hora.png` | Viajes por hora de recogida |
| `2_top_zonas_recogida.png` | Diez zonas de recogida con más viajes |
| `3_mapa_calor_dia_hora.png` | Viajes por día de la semana y hora |
| `4_histograma_tarifa.png` | Distribución de la tarifa (hasta 100 USD) |

## Resultados principales (enero de 2025)

- Registros cargados: 3.475.226. Registros válidos tras la limpieza: 3.251.302 (se descartó el 6,44 %).
- La hora con más viajes es las 18 h (236.408 viajes) y la de menos es las 4 h (17.926).
- El jueves es el día con más viajes (571.397) y el lunes el de menos (343.356).
- La zona de recogida más frecuente es la 161 (161.256 viajes), seguida de la 237 y la 236.
- Con pago en tarjeta la propina promedio es de 4,10 USD (26,3 % de la tarifa); en efectivo no se registra.

## Limitaciones

- La limpieza no elimina valores extremos de distancia y tarifa (por ejemplo, distancias de miles de millas), por lo que las correlaciones con la tarifa salen cercanas a cero. Una mejora sería filtrar distancias y tarifas fuera de un rango razonable.
- La TLC no garantiza la exactitud de los datos.
- Las propinas en efectivo no se registran.

## Autora

María Elena Cárdenas Avendaño – Tutor: Elkin Andrés Ladino Ordoñez – UNAD, 2026.
