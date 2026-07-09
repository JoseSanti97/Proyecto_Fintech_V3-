import sys
from pathlib import Path
from pyspark.sql import SparkSession

def leer_parquet_dinamico():
    # 1. Validar que se pasaron los argumentos correctos por terminal
    if len(sys.argv) < 3:
        print("\n Error: Faltan argumentos.")
        print(" Uso correcto: python read_parquet.py [capa] [nombre_tabla]")
        print("  Ejemplos:")
        print("   -> python read_parquet.py silver dim_clientes")
        print("   -> python read_parquet.py gold kpi_capital")
        print("   -> python read_parquet.py gold ml_credit_score\n")
        return

    capa = sys.argv[1].lower()       # Ejemplo: 'silver' o 'gold'
    tabla = sys.argv[2]              # Ejemplo: 'dim_clientes', 'kpi_movimientos'

    # 2. Validar que la capa ingresada sea permitida
    if capa not in ["silver", "gold"]:
        print(f"Error: Capa '{capa}' inválida. Solo se permite 'silver' o 'gold'.")
        return

    # 3. Construir la ruta hacia el archivo usando Pathlib
    ROOT = Path(__file__).resolve().parent
    TARGET_PATH = ROOT / "storage" / capa / tabla

    # 4. Verificar físicamente si el Parquet existe en el disco duro
    if not TARGET_PATH.exists():
        print(f"Error: No se encontró la ruta: {TARGET_PATH.absolute()}")
        return

    # 5. Inicializar la sesión de Spark de forma silenciosa para que no ensucie la consola
    spark = (
        SparkSession.builder
        .appName(f"LecturaDinamica_{capa}_{tabla}")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Usamos str(PATH.absolute()) para evitar problemas con espacios en Windows
        df = spark.read.parquet(str(TARGET_PATH.absolute()))
        
        # 6. Mostrar los hallazgos en pantalla
        print("\n" + "="*60)
        print(f" CAPA: {capa.upper()} | TABLA: {tabla}")
        print("="*60)
        
        # Muestra las primeras 20 filas sin recortar las columnas largas
        df.show(n=20, truncate=False)
        
        print("="*60)
        print(f"Resumen: Total de registros encontrados = {df.count()}")
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"Error crítico leyendo el archivo Parquet: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    leer_parquet_dinamico()