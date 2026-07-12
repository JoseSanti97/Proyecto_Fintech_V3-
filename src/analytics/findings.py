from pyspark.sql import SparkSession
from pathlib import Path

# Inicializar Spark
spark = SparkSession.builder.appName("LecturaResultados").getOrCreate()

# Ruta al resultado del modelo
ROOT = Path(__file__).resolve().parent
GOLD_ROOT = ROOT / "storage" / "gold" / "ml_credit_score"

# Leer y mostrar los hallazgos
try:
    df_resultados = spark.read.parquet(str(GOLD_ROOT.absolute()))
    print("=== HALLAZGOS DEL MODELO RANDOM FOREST ===")
    df_resultados.show(n=20, truncate=False) # Muestra los primeros 20 clientes
    
    print(f"Total de clientes evaluados: {df_resultados.count()}")
except Exception as e:
    print(f"No se pudieron leer los resultados: {e}")
finally:
    spark.stop()