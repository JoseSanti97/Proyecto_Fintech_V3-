import sys
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, when, max as _max, ntile, expr
from pipeline_lakehouse import _write_parquet
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
SILVER_ROOT = ROOT / "storage" / "silver"
GOLD_ROOT = ROOT / "storage" / "gold"

spark = SparkSession.builder.appName("ELT_P1").getOrCreate()

def extract_parquet():
    logger.info("Iniciando extracción de archivos .parquet de la capa Silver")
    
    parquets = {
        "clientes": SILVER_ROOT / "dim_clientes",
        "cuentas": SILVER_ROOT / "dim_cuentas",
        "tarjetas": SILVER_ROOT / "dim_tarjetas",
        "prestamos": SILVER_ROOT / "dim_prestamo",
        "transacciones": SILVER_ROOT / "dim_fact_transacciones",
        "beneficiarios": SILVER_ROOT / "dim_beneficiarios"
    }
    dataframes = {}
    try:
        for key, path in parquets.items():
            # Validar si la ruta existe
            if not path.exists():
                raise FileNotFoundError(f"No se encontró el archivo parquet en: {path}")
            
            logger.info(f"Cargando datos indexados en la capa silver: {key} desde {path}")
            dataframes[key] = spark.read.parquet(str(path.absolute()))
            logger.info("Extracción completada exitosamente")
        return dataframes

    except Exception as error:
        logger.error(f"Error durante la extracción: {error}", exc_info=True)
        raise error

def load(dataframes_dict):
    logger.info("Iniciando carga de parquets")

    try: 
       for table_name, df in dataframes_dict.items():
            #Ruta a la capa gold
            gold_path = GOLD_ROOT / f"fact_{table_name}"
            logger.info(f"Escribiendo DataFrame '{table_name}' en la capa Gold: {gold_path}")

            _write_parquet(df, str(gold_path.absolute()))
            logger.info(f"Carga de {table_name} completada exitosamente")

    except Exception as error:
        logger.error(f"Error durante la carga: {error}", exc_info=True)
        raise error
    

if __name__ == "__main__":
    
    # 1. Definimos las funciones necesarias para transformar, validar y cargar
    def transform(dfs_extraidos):
        logger.info("Inicializando transformación de los data frames")
        try:
            # Extraer las variables del hash
            df_clientes = dfs_extraidos["clientes"]
            df_cuentas  = dfs_extraidos["cuentas"]
            df_tarjetas = dfs_extraidos["tarjetas"]
            df_transacciones = dfs_extraidos["transacciones"]
            df_prestamos = dfs_extraidos["prestamos"]
            df_beneficiarios = dfs_extraidos["beneficiarios"]

            # DataFrames Reducidos
            df_clientes_red = df_clientes.select("id_cliente", "nombre", "apellido", "delegacion")
            df_cuentas_red = df_cuentas.select("no_cuenta", "tipo_cuenta", "saldo", "id_cliente")
            df_tarjetas_red = df_tarjetas.select("no_cuenta", col("estado").alias("estado_tarjeta"), "emisor")
            df_beneficiarios_red = df_beneficiarios.select("no_cuenta", "porcentaje_saldo", "parentesco")
            df_prestamos_red = df_prestamos.select(col("id_cliente").alias("id_cliente_prestamo"), col("monto").alias("monto_prestamo"), col("estado").alias("estado_prestamo"))

           
            df_joined = (
                df_transacciones.select(
                    "id_transaccion", "tipo_movimiento", col("monto").alias("monto_transaccion"), "fecha_transaccion", "cuenta_origen", "no_cuenta"
                )
                .join(df_tarjetas_red, on="no_cuenta", how="left")
                .join(df_cuentas_red, df_transacciones.cuenta_origen == df_cuentas_red.no_cuenta, how="left")
                .join(df_beneficiarios_red, df_cuentas_red.no_cuenta == df_beneficiarios_red.no_cuenta, how="left")
                .join(df_clientes_red, on="id_cliente", how="left")
                .join(df_prestamos_red, df_clientes_red.id_cliente == df_prestamos_red.id_cliente_prestamo, how="left")
                .select(
                    "id_cliente", 
                    "id_transaccion", 
                    "cuenta_origen", 
                    "tipo_cuenta", 
                    "tipo_movimiento", 
                    "fecha_transaccion", 
                    "saldo", 
                    "monto_transaccion", 
                    "estado_tarjeta", 
                    "emisor",
                    "estado_prestamo" 
                )
            )
            
            # Particionado en 4 bloques
            window_spec = Window.orderBy("id_cliente")
            df_bloques = df_joined.withColumn("bloque", ntile(4).over(window_spec))

            small_dataframes_gold = {}
            for i in range(1, 5):
                small_dataframes_gold[f"bloque_{i}"] = df_bloques.filter(col("bloque") == i)
        
            return small_dataframes_gold
        except Exception as error:
            logger.error(f"Error durante la transformación: {error}", exc_info=True)
            raise error
            
    def validate_checkpoint(bloques_transformados_gold, expect_col_count):
        logger.info("Inicializando Validación del Checkpoint")
        try:
            todo_correcto = True
            for i in range(1, 5):
                df_bloque = bloques_transformados_gold[f"bloque_{i}"]
                col_count = len(df_bloque.columns)
                if col_count != expect_col_count:
                    logger.error(f"Fallo en bloque_{i}. Columnas esperadas: {expect_col_count}, reales: {col_count}")
                    todo_correcto = False
            return todo_correcto
        except Exception as error:
            logger.error(f"Error en validación: {error}", exc_info=True)
            return False
            
    def load_gold(bloques_transformados_gold):
        logger.info("Preparándose para cargar los datos a la capa Gold")
        try:
            for i in range(1, 5):
                nombre_bloque = f"bloque_{i}"
                df_bloque = bloques_transformados_gold[nombre_bloque]
                csv_exit = GOLD_ROOT / f"golden_dataset_{i}"
                
                logger.info(f"Escribiendo bloque {i} en: {csv_exit}")
                df_bloque.write.mode("overwrite").option("header", "true").csv(str(csv_exit))
            logger.info("Carga a Gold finalizada exitosamente")
        except Exception as error:
            logger.error(f"Error en carga: {error}", exc_info=True)
            raise error

    try:
        # Paso 1: Extracción 
        dfs_extraidos = extract_parquet()
        
        # Paso 2: Transformación 
        bloques_gold = transform(dfs_extraidos)
        
        # Paso 3: Validación 
        if validate_checkpoint(bloques_gold, expect_col_count=11):
            # Paso 4: Carga 
            load_gold(bloques_gold)
        else:
            logger.error("Pipeline detenido: Los bloques no cumplieron los criterios de calidad.")
            
    except Exception as pipeline_error:
        logger.critical(f"Fallo crítico en la ejecución del pipeline: {pipeline_error}")
        
    finally:
        logger.info("Cerrando sesión de Spark de manera segura.")
        spark.stop()