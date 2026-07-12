import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, when, max as _max, ntile, expr
import logging

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
    

dataframes_silver = extract_parquet()

df_clientes = dataframes_silver["clientes"]
df_cuentas  = dataframes_silver["cuentas"]
df_tarjetas = dataframes_silver["tarjetas"]
df_prestamos = dataframes_silver["prestamos"]
df_transacciones = dataframes_silver["transacciones"]

df_clientes_red = df_clientes.select("id_cliente", "nombre", "apellido", "delegacion")
df_cuentas_red = df_cuentas.select("no_cuenta", "tipo_cuenta", "saldo", "id_cliente")
df_tarjetas_red = df_tarjetas.select("no_cuenta", "estado", "emisor")



def transform():
    logger.info(f"Iniciando transfomación de los datos")
    try:
        df_joined = (
                df_transacciones.select(
                "id_transaccion", "tipo_movimiento", "monto", "fecha_transaccion", "cuenta_origen"
                ).join(
                df_tarjetas, df_tarjetas.no_cuenta == df_transacciones.no_cuenta, how="left"
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

        for i in range(1,5):
            small_dataframes[f"bloque_{i}"] = df_bloques.filter(col("bloque")==i)
            logger.info(f"Bloque {i} registrado exitosamente")
        return small_dataframes
    
    except Exception as error:
        logger.error(f"Error durante la transformación: {error}", exc_info=True)
        raise error
    

def validate_checkpoint(bloques_transformados, expect_col_count, expect_row_count):
    logger.info("Inicializando Validación")
    
    try:
        todo_correcto = True
        
        for i in range(1, 5):
            nombre_bloque = f"bloque_{i}"
            df_bloque = bloques_transformados[nombre_bloque]
            
            
            row_count = df_bloque.count() 
            col_count = len(df_bloque.columns)
            
            
            logger.info(f"[{nombre_bloque.upper()}] Filas reales: {row_count} | Columnas reales: {col_count}")
            
            if col_count != expect_col_count:
                logger.error(f"FALLO DE INTEGRIDAD en {nombre_bloque}: Se esperaban {expect_col_count} columnas, pero se encontraron {col_count}")
                todo_correcto = False
                
            if row_count != expect_row_count:
                logger.warning(f"Alerta en {nombre_bloque}: Se esperaban {expect_row_count} filas, se encontraron {row_count}")
                todo_correcto = False
                
        if todo_correcto:
            logger.info("Validación completada con éxito.")
        else:
            logger.warning("El checkpoint detectó anomalías en algunos bloques. Revisar los logs anteriores.")        
        return todo_correcto

    except Exception as error:
        logger.error(f"Error crítico en la validación: {error}", exc_info=True)         
        raise error
    
def load(bloques_transformados):
    logger.info("Preparandose para cargar los datos a la capa gold")

    try:
        for i in range(1,5):
            nombre_bloque = f"bloque_{i}"
            df_bloque = bloques_transformados[nombre_bloque]

            csv_exit = GOLD_ROOT / f"dataset_{i}"

            logger.info(f"Guardando {nombre_bloque} en la ruta Gold: {csv_exit}")

        logger.info("Carga a la capa Gold completada exitosamente para todos los bloques.")
        df_bloque.coalesce(1).write.mode("overwrite").option("header", "true").csv(str(csv_exit))
    except Exception as error:
        logger.error(f"Error crítico durante la carga a la capa Gold: {error}", exc_info=True)
        raise error

bloques_transformados = transform()
load(bloques_transformados)




