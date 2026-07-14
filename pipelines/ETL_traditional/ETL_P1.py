import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, when, max as _max, ntile, expr
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
SILVER_ROOT = ROOT / "storage" / "silver"
GOLD_ROOT = ROOT / "storage" / "gold"

spark = SparkSession.builder.appName("ETL_I1").getOrCreate() # Sesión interna de Spark

    

def extract_parquet():
    logger.info("Iniciando extracción de archivos .parquet de la capa Silver")
    
    parquets = {
        "clientes": SILVER_ROOT / "dim_clientes",
        "cuentas": SILVER_ROOT / "dim_cuentas",
        "tarjetas": SILVER_ROOT / "dim_tarjetas",
        "prestamos": SILVER_ROOT / "dim_prestamo",
        "transacciones": SILVER_ROOT / "dim_fact_transacciones"
    }
    dataframes = {}
    try:
        for key, path in parquets.items():
            # Validar si la ruta existe
            if not path.exists():
                raise FileNotFoundError(f"No se encontró el archivo parquet en: {path}")
            
            logger.info(f"Cargando datos indexados en la capa silver: {key} desde {path}")
            dataframes[key] = spark.read.parquet(str(parquets.absolute()))
            logger.info("Extracción completada exitosamente")
        return dataframes

    except Exception as error:
        logger.error(f"Error durante la extracción: {error}", exc_info=True)
        raise error
    

def transform(dataframes_silver):
    logger.info("Iniciando transformación de los datos")
    try:
        df_clientes = dataframes_silver["clientes"]
        df_cuentas  = dataframes_silver["cuentas"]
        df_tarjetas = dataframes_silver["tarjetas"]
        df_transacciones = dataframes_silver["transacciones"]

        # Reducciones optimizadas
        df_clientes_red = df_clientes.select("id_cliente", "nombre", "apellido", "delegacion")
        df_cuentas_red = df_cuentas.select("no_cuenta", "tipo_cuenta", "saldo", "id_cliente")
        df_tarjetas_red = df_tarjetas.select("no_cuenta", "estado", "emisor")

        df_joined = (
            df_transacciones.select(
                "id_transaccion", "tipo_movimiento", "monto", "fecha_transaccion", "cuenta_origen", "no_cuenta"
            ).join(
                df_tarjetas_red, df_tarjetas_red.no_cuenta == df_transacciones.no_cuenta, how="left"
            ).join(
                df_cuentas_red, df_transacciones.cuenta_origen == df_cuentas_red.no_cuenta, how="left"
            ).join(
                df_clientes_red, on="id_cliente", how="left"
            ).select(
                "id_cliente", "id_transaccion", "cuenta_origen", "tipo_cuenta", "tipo_movimiento", "fecha_transaccion", "saldo", "monto", "estado", "emisor"
            )
        )
        
        window_spec = Window.orderBy("id_cliente")
        df_bloques = df_joined.withColumn("bloque", ntile(4).over(window_spec))

        small_dataframes = {}
        for i in range(1, 5):
            small_dataframes[f"bloque_{i}"] = df_bloques.filter(col("bloque") == i)
            
        return small_dataframes
    except Exception as error:
        logger.error(f"Error durante la transformación: {error}", exc_info=True)
        raise error


def validate_checkpoint(bloques_transformados, expect_col_count):
    logger.info("Inicializando Validación")
    try:
        todo_correcto = True
        for i in range(1, 5):
            df_bloque = bloques_transformados[f"bloque_{i}"]
            col_count = len(df_bloque.columns)
            if col_count != expect_col_count:
                logger.error(f"Fallo en bloque_{i}. Columnas esperadas: {expect_col_count}, reales: {col_count}")
                todo_correcto = False
        return todo_correcto
    except Exception as error:
        logger.error(f"Error en validación: {error}", exc_info=True)
        return False
    
def load(bloques_transformados):
    logger.info("Preparándose para cargar los datos a la capa Gold")
    try:
        for i in range(1, 5):
            nombre_bloque = f"bloque_{i}"
            df_bloque = bloques_transformados[nombre_bloque]
            csv_exit = GOLD_ROOT / f"dataset_{i}"
            
            (df_bloque.write.mode("overwrite").option("header", "true").csv(str(csv_exit)))
        logger.info("Carga a Gold finalizada exitosamente")
    except Exception as error:
        logger.error(f"Error en carga: {error}", exc_info=True)
        raise error

if __name__ == "__main__":
    try:
        # 1. Extraer
        dfs_silver = extract_parquet()
        
        # 2. Transformar
        bloques = transform(dfs_silver)
        
        # 3. Validar (Esperamos 10 columnas en el select final)
        if validate_checkpoint(bloques, expect_col_count=10):
            # 4. Cargar solo si la validación pasa
            load(bloques)
        else:
            logger.error("El pipeline se detuvo: Los datos no pasaron el control de calidad.")
            
    finally:
        spark.stop()
    




