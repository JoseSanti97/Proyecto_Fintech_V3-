from pathlib import Path
from urllib.parse import urlparse
import os 
os.environ["HADOOP_HOME"] = r"C:\hadoop-3.3.5"
os.environ["PATH"] = r"C:\hadoop-3.3.5\bin;" + os.environ.get("PATH", "")
os.environ["SPARK_LOCAL_DIRS"] = r"C:\tmp"
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    upper,
    regexp_replace,
    concat_ws,
    collect_list,
    col,
    date_format,
    sum as _sum,
    avg as _avg,
    count as _count,
)

from generador.conexion import DATABASE_URL

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
STORAGE_ROOT = ROOT / "storage"
BRONZE_ROOT = STORAGE_ROOT / "bronze"
SILVER_ROOT = STORAGE_ROOT / "silver"
GOLD_ROOT = STORAGE_ROOT / "gold"
JDBC_DRIVER_JAR = ROOT / "drivers" / "postgresql-42.6.0.jar"

for path in [BRONZE_ROOT, SILVER_ROOT, GOLD_ROOT]:
    path.mkdir(parents=True, exist_ok=True)

def _parse_database_url(database_url: str):
    """Toma la URL de SQLAlchemy y la convierte en un formato compatible para Spark JDBC."""
    parsed = urlparse(database_url)
    if not parsed.scheme or not parsed.hostname:
        raise ValueError("URL de base de datos inválida")
    
    jdbc_url = f"jdbc:postgresql://{parsed.hostname}:{parsed.port}{parsed.path}"
    properties = {
        "user": parsed.username or "",
        "password": parsed.password or "",
        "driver": "org.postgresql.Driver"
    }
    return jdbc_url, properties

def build_spark_session():
    """Inicializa la sesión de Spark con el driver JDBC de PostgreSQL y optimización de las particiones."""
    builder = SparkSession.builder.appName("FintechLakehousePipeline")

    if JDBC_DRIVER_JAR.exists():
        builder = builder.config("spark.jars", str(JDBC_DRIVER_JAR))
    else:
        logger.warning(f"No se encontró postgresql JDBC driver en {JDBC_DRIVER_JAR}. Asegúrese de que el archivo exista para la conectividad JDBC.")

    builder = (
        builder
        .config("spark.sql.shuffle.partitions", "2")  # Optimización para entornos de desarrollo locales
        .config("spark.ui.showConsoleProgress", "false")  # Desactiva la barra de progreso en la consola
        .config("spark.hadoop.fs.defaultFS", "file:///")
    )
    return builder.getOrCreate()

def _read_table(spark: SparkSession, jdbc_url: str, properties: dict, table_name: str):
    """Lee una tabla de PostgreSQL y la convierte en un DataFrame de Spark."""
    logger.info(f"[LAKEHOUSE] Leyendo tabla {table_name} desde PostgreSQL")
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .option("user", properties["user"])
        .option("password", properties["password"])
        .option("driver", properties["driver"])
        .load()
    )

def _write_parquet(df, path: Path):
    ruta_segura = str(path.absolute())
    df.write.mode("overwrite").parquet(ruta_segura)

def exec_pipeline_lakehouse():
    logger.info("=== INICIANDO PIPELINE DE DATA LAKEHOUSE ===")

    database_url = os.getenv("DATABASE_URL", DATABASE_URL)
    jdbc_url, properties = _parse_database_url(database_url)
    spark = build_spark_session()

    try:
        logger.info("[LAKEHOUSE] Extracción de datos desde PostgreSQL hacia la capa Bronze")
        
        bronze_cliente = _read_table(spark, jdbc_url, properties, "cliente")
        bronze_cliente_tel = _read_table(spark, jdbc_url, properties, "cliente_tel")
        bronze_beneficiario = _read_table(spark, jdbc_url, properties, "beneficiario")
        bronze_beneficiario_tel = _read_table(spark, jdbc_url, properties, "beneficiario_tel")
        bronze_cuenta = _read_table(spark, jdbc_url, properties, "cuenta")
        bronze_prestamo = _read_table(spark, jdbc_url, properties, "prestamo")
        bronze_tarjeta = _read_table(spark, jdbc_url, properties, "tarjeta")
        bronze_transaccion = _read_table(spark, jdbc_url, properties, "transaccion")
        bronze_biometria = _read_table(spark, jdbc_url, properties, "cliente_biometria")
        
        _write_parquet(bronze_cliente, BRONZE_ROOT / "cliente.parquet")
        _write_parquet(bronze_cliente_tel, BRONZE_ROOT / "cliente_tel.parquet")
        _write_parquet(bronze_beneficiario, BRONZE_ROOT / "beneficiario.parquet")
        _write_parquet(bronze_beneficiario_tel, BRONZE_ROOT / "beneficiario_tel.parquet")
        _write_parquet(bronze_cuenta, BRONZE_ROOT / "cuenta.parquet")
        _write_parquet(bronze_prestamo, BRONZE_ROOT / "prestamo.parquet")
        _write_parquet(bronze_tarjeta, BRONZE_ROOT / "tarjeta.parquet")
        _write_parquet(bronze_transaccion, BRONZE_ROOT / "transaccion.parquet")

   
        logger.info("Iniciando transformaciones y agregaciones para la capa Silver")
        
        b_cliente = spark.read.parquet(str((BRONZE_ROOT / "cliente.parquet").absolute()))
        b_cuenta = spark.read.parquet(str((BRONZE_ROOT / "cuenta.parquet").absolute()))
        b_transaccion = spark.read.parquet(str((BRONZE_ROOT / "transaccion.parquet").absolute()))
        b_prestamo = spark.read.parquet(str((BRONZE_ROOT / "prestamo.parquet").absolute()))

        # Reducciones optimizadas para el Join del modelo de Crédito
        clientes_red = b_cliente.select("id_cliente", "nombre", "apellido", "delegacion")
        cuentas_red = b_cuenta.select("no_cuenta", "tipo_cuenta", "saldo", "id_cliente")

        # Construcción del dataset desnormalizado para Random Forest
        dataset_pivot_ml = (
            b_transaccion.select(
                "id_transaccion", "tipo_movimiento", "monto", "fecha_transaccion", "cuenta_origen"
            ).join(
                cuentas_red, b_transaccion.cuenta_origen == cuentas_red.no_cuenta, how="left"
            ).join(
                clientes_red, on="id_cliente", how="left"
            ).select(
                "id_cliente", "id_transaccion", "cuenta_origen", "tipo_cuenta", "tipo_movimiento", "fecha_transaccion", "saldo", "monto"
            )
        )

        
        dim_clientes = (
            bronze_cliente.join(bronze_cliente_tel, on="id_cliente", how="left")
            .groupBy(
                "id_cliente",
                "nombre",
                "apellido",
                "correo",
                "delegacion",
                "estado_civil"
            ).agg(concat_ws(",", collect_list(col("tel"))).alias("telefonos"))
             .withColumn("nombre", upper(col("nombre")))
             .withColumn("apellido", upper(col("apellido")))
             .withColumn("correo", regexp_replace(col("correo"), r"^[^@]+", "xxxxxx"))
             .withColumn("telefonos", regexp_replace(col("telefonos"), r"[0-9]", "X"))
        )

        # Dimensión Beneficiarios
        dim_beneficiarios = (
            bronze_beneficiario
            .join(bronze_beneficiario_tel, on="id_beneficiario", how="left")
            .groupBy(
                "id_beneficiario",
                "parentesco",
                "nombre",
                "apellido",
                "porcentaje_saldo",
                "no_cuenta"
            )
            .agg(concat_ws(", ", collect_list(col("tel"))).alias("telefonos_beneficiario"))
            .withColumn("nombre", upper(col("nombre")))
            .withColumn("apellido", upper(col("apellido")))
            .withColumn("telefonos_beneficiario", regexp_replace(col("telefonos_beneficiario"), r"[0-9]", "X"))
        )

        dim_cuentas = bronze_cuenta.select("no_cuenta", "tipo_cuenta", "saldo", "id_cliente")
        dim_tarjetas = bronze_tarjeta.select("no_tarjeta", "cvv", "emisor", "estado", "no_cuenta")

        dim_fact_transacciones = (
            bronze_transaccion
            .join(
                bronze_cuenta.select("no_cuenta", "id_cliente"),
                bronze_transaccion.cuenta_origen == bronze_cuenta.no_cuenta,
                how="left",
            )
            .select(
                "id_transaccion",
                "no_tarjeta",
                "cuenta_origen",
                "cuenta_destino",
                "monto",
                "tipo_movimiento",
                "fecha_transaccion",
                "id_cliente",
            )
        )
        
        dim_fact_prestamos = bronze_prestamo.select("id_prestamo", "estado", "monto", "id_cliente")

        # Escritura estructurada de Capa Silver
        _write_parquet(dim_clientes, SILVER_ROOT / "dim_clientes")
        _write_parquet(dim_beneficiarios, SILVER_ROOT / "dim_beneficiarios")
        _write_parquet(dim_cuentas, SILVER_ROOT / "dim_cuentas")
        _write_parquet(dim_tarjetas, SILVER_ROOT / "dim_tarjetas")
        _write_parquet(dim_fact_transacciones, SILVER_ROOT / "dim_fact_transacciones")
        _write_parquet(dim_fact_prestamos, SILVER_ROOT / "dim_fact_prestamos")
        _write_parquet(dataset_pivot_ml, SILVER_ROOT / "pivot_model_credit")

        logger.info("Dataset para ML generado en Silver")


        
        
        logger.info("Procesando Capa GOLD")

        dim_fact_transacciones = spark.read.parquet(str((SILVER_ROOT / "dim_fact_transacciones").absolute()))
        dim_fact_prestamos = spark.read.parquet(str((SILVER_ROOT / "dim_fact_prestamos").absolute()))
        
        kpi_capital = (
            dim_cuentas.agg(
                _sum("saldo").alias("total_capital"),
                _avg("saldo").alias("saldo_promedio")
            ).withColumn("descripcion", col("total_capital"))
        )

        kpi_movimientos = (
            dim_fact_transacciones
            .withColumn("periodo", date_format(col("fecha_transaccion"), "yyyy-MM"))
            .groupBy("periodo", "tipo_movimiento")
            .agg(
                _count("id_transaccion").alias("movimientos"),
                _sum("monto").alias("monto_total"),
            )
            .orderBy("periodo", "tipo_movimiento")
        )
        
        kpi_prestamos = (
            dim_fact_prestamos
            .groupBy("estado")
            .agg(
                _count("id_prestamo").alias("total_creditos"),
                _sum("monto").alias("monto_total_colocado"),
                _avg("monto").alias("ticket_promedio_prestamo")
            )
        )

        # Escritura de Capa Gold
        _write_parquet(kpi_capital, GOLD_ROOT / "kpi_capital")
        _write_parquet(kpi_movimientos, GOLD_ROOT / "kpi_movimientos")
        _write_parquet(kpi_prestamos, GOLD_ROOT / "kpi_prestamos")
        
        logger.info("LAKEHOUSE completado y sincronizado")

    except Exception as e:
        logger.error(f"Error crítico en el procesamiento del Lakehouse: {e}", exc_info=True)
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
    exec_pipeline_lakehouse()