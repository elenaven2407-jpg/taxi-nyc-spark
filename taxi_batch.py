"""
Tarea 3 - Big Data (UNAD)
Procesamiento en batch con PySpark: NYC TLC Yellow Taxi Trip Records.

Uso:
    spark-submit taxi_batch.py yellow_tripdata_2025-01.parquet 2025-01

Argumentos:
    1) ruta del archivo parquet
    2) mes del archivo en formato AAAA-MM (se usa para filtrar fechas fuera del periodo)

Salida: tablas en consola y 4 graficos PNG en la carpeta "salida_batch".
"""
import os
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # sin ventana grafica (la VM no tiene pantalla)
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RUTA = sys.argv[1] if len(sys.argv) > 1 else "yellow_tripdata_2025-01.parquet"
MES = sys.argv[2] if len(sys.argv) > 2 else "2025-01"
CARPETA = "salida_batch"
os.makedirs(CARPETA, exist_ok=True)

# Limites del periodo (mes completo)
inicio = datetime.strptime(MES + "-01", "%Y-%m-%d")
fin = datetime(inicio.year + (inicio.month == 12), inicio.month % 12 + 1, 1)

spark = (SparkSession.builder
         .appName("TaxiBatch")
         .config("spark.sql.shuffle.partitions", "8")  # pocas particiones: la VM es pequena
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------- 1. CARGA
print("\n=== 1. CARGA ===")
df = spark.read.parquet(RUTA)
total = df.count()
print(f"Registros cargados: {total:,}")
df.printSchema()

# -------------------------------------------------------------- 2. LIMPIEZA
print("\n=== 2. LIMPIEZA ===")
criticas = ["tpep_pickup_datetime", "tpep_dropoff_datetime", "trip_distance",
            "fare_amount", "total_amount", "PULocationID", "DOLocationID",
            "payment_type"]
df = df.dropDuplicates().dropna(subset=criticas)

df = df.withColumn(
    "duracion_min",
    (F.col("tpep_dropoff_datetime").cast("long")
     - F.col("tpep_pickup_datetime").cast("long")) / 60.0)

limpio = df.filter(
    (F.col("trip_distance") > 0) & (F.col("fare_amount") > 0) &
    (F.col("total_amount") > 0) &
    (F.col("duracion_min") > 0) & (F.col("duracion_min") < 360) &
    (F.col("tpep_pickup_datetime") >= F.lit(inicio)) &
    (F.col("tpep_pickup_datetime") < F.lit(fin))
).cache()

n_limpio = limpio.count()
print(f"Registros validos: {n_limpio:,}")
print(f"Descartados (nulos, duplicados o invalidos): {total - n_limpio:,} "
      f"({(total - n_limpio) / total * 100:.2f} %)")

# ----------------------------------------------------------- 3. TRANSFORMACION
print("\n=== 3. TRANSFORMACION ===")
limpio = (limpio
          .withColumn("hora", F.hour("tpep_pickup_datetime"))
          .withColumn("dia_num", F.dayofweek("tpep_pickup_datetime"))  # 1=domingo ... 7=sabado
          .withColumn("velocidad_mph",
                      F.col("trip_distance") / (F.col("duracion_min") / 60.0))
          .withColumn("pct_propina", F.col("tip_amount") / F.col("fare_amount") * 100))
print("Columnas derivadas creadas: hora, dia_num, duracion_min, velocidad_mph, pct_propina")

# -------------------------------------------------------------------- 4. EDA
print("\n=== 4. ANALISIS EXPLORATORIO ===")

print("\n-- Estadisticas descriptivas")
limpio.select("trip_distance", "duracion_min", "fare_amount",
              "tip_amount", "total_amount").describe().show()

print("-- Viajes por hora")
por_hora = limpio.groupBy("hora").count().orderBy("hora")
por_hora.show(24)

print("-- Viajes por dia de la semana (1=domingo ... 7=sabado)")
limpio.groupBy("dia_num").count().orderBy("dia_num").show()

print("-- Top 10 zonas de recogida")
top_pu = limpio.groupBy("PULocationID").count().orderBy(F.desc("count")).limit(10)
top_pu.show()

print("-- Top 10 zonas de destino")
limpio.groupBy("DOLocationID").count().orderBy(F.desc("count")).limit(10).show()

print("-- Correlaciones con la tarifa")
print("distancia vs tarifa :", round(limpio.stat.corr("trip_distance", "fare_amount"), 3))
print("duracion  vs tarifa :", round(limpio.stat.corr("duracion_min", "fare_amount"), 3))
print("distancia vs duracion:", round(limpio.stat.corr("trip_distance", "duracion_min"), 3))

print("\n-- Propina por tipo de pago (1=tarjeta, 2=efectivo, 3=sin cargo, 4=disputa)")
limpio.groupBy("payment_type").agg(
    F.count("*").alias("viajes"),
    F.round(F.avg("tip_amount"), 2).alias("propina_prom"),
    F.round(F.avg("pct_propina"), 2).alias("pct_propina_prom")
).orderBy("payment_type").show()

print("-- Propina promedio por hora (solo pagos con tarjeta)")
limpio.filter(F.col("payment_type") == 1).groupBy("hora").agg(
    F.round(F.avg("tip_amount"), 2).alias("propina_prom")
).orderBy("hora").show(24)

# ---------------------------------------------------------- 5. VISUALIZACION
print("\n=== 5. VISUALIZACION ===")

# 5.1 Viajes por hora
p = por_hora.toPandas()
plt.figure(figsize=(9, 4))
plt.bar(p["hora"], p["count"], color="#1f77b4")
plt.title(f"Viajes por hora de recogida ({MES})")
plt.xlabel("Hora del dia")
plt.ylabel("Numero de viajes")
plt.xticks(range(0, 24))
plt.tight_layout()
plt.savefig(f"{CARPETA}/1_viajes_por_hora.png", dpi=120)
plt.close()

# 5.2 Top zonas de recogida
p = top_pu.toPandas()
plt.figure(figsize=(9, 4))
plt.bar(p["PULocationID"].astype(str), p["count"], color="#2ca02c")
plt.title("10 zonas de recogida con mas viajes")
plt.xlabel("PULocationID")
plt.ylabel("Numero de viajes")
plt.tight_layout()
plt.savefig(f"{CARPETA}/2_top_zonas_recogida.png", dpi=120)
plt.close()

# 5.3 Mapa de calor dia x hora
p = (limpio.groupBy("dia_num", "hora").count().toPandas()
     .pivot(index="dia_num", columns="hora", values="count").fillna(0))
dias = {1: "Dom", 2: "Lun", 3: "Mar", 4: "Mie", 5: "Jue", 6: "Vie", 7: "Sab"}
plt.figure(figsize=(10, 4))
plt.imshow(p.values, aspect="auto", cmap="YlOrRd")
plt.colorbar(label="Numero de viajes")
plt.yticks(range(len(p.index)), [dias[i] for i in p.index])
plt.xticks(range(len(p.columns)), p.columns)
plt.title("Viajes por dia de la semana y hora")
plt.xlabel("Hora del dia")
plt.tight_layout()
plt.savefig(f"{CARPETA}/3_mapa_calor_dia_hora.png", dpi=120)
plt.close()

# 5.4 Histograma de tarifas (intervalos de 5 USD, hasta 100)
p = (limpio.filter(F.col("fare_amount") <= 100)
     .withColumn("intervalo", (F.floor(F.col("fare_amount") / 5) * 5))
     .groupBy("intervalo").count().orderBy("intervalo").toPandas())
plt.figure(figsize=(9, 4))
plt.bar(p["intervalo"], p["count"], width=4.5, color="#9467bd")
plt.title("Distribucion de la tarifa (fare_amount <= 100 USD)")
plt.xlabel("Tarifa en USD (intervalos de 5)")
plt.ylabel("Numero de viajes")
plt.tight_layout()
plt.savefig(f"{CARPETA}/4_histograma_tarifa.png", dpi=120)
plt.close()

print(f"Graficos guardados en la carpeta '{CARPETA}/'")
spark.stop()
