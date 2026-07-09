import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, avg as _avg, count as _count, when, max as _max, expr, lit, date_sub
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent
SILVER_ROOT = ROOT / "storage" / "silver"
GOLD_ROOT = ROOT / "storage" / "gold"

def ejecutar_credit_score():
    logger.info("=== INICIANDO MODELO DE CREDIT SCORE ===")

    # Inicialización interna de la sesión de Spark
    spark = SparkSession.builder.appName("CreditScoreModel").getOrCreate()

    try:
        logger.info("Cargando datos indexados de la capa Silver")
        path_pivot = SILVER_ROOT / "pivot_model_credit"
        if not path_pivot.exists():
            raise FileNotFoundError(f"No se encontró el archivo parquet en: {path_pivot}")
        
        silver_transacciones = spark.read.parquet(str(path_pivot.absolute()))
        
       #INGENIERÍA DE CARACTERÍSTICAS
        logger.info("Calculando métricas temporales basadas en la ventana histórica de 365 días")
        
        # 1.- Obtenemos la fecha de la última transacción registrada en el datalake
        fecha_max_raw = silver_transacciones.select(_max("fecha_transaccion")).collect()[0][0]
        logger.info(f"Fecha de referencia analítica más reciente encontrada: {fecha_max_raw}")
        
        # 2.- Creamos el límite usando funciones nativas de Spark
        limite_fecha = date_sub(lit(fecha_max_raw), 90)
        
        # 3.- Creamos el DataFrame intermedio con la columna ya calculada de forma segura
        transacciones_preparadas = silver_transacciones.withColumn(
            "saldo_90_dias",
            when(col("fecha_transaccion") >= limite_fecha, col("saldo")).otherwise(None)
        )
        
        # Construcción de la matriz de entrenamiento por Cliente
        feature_cliente = (
            silver_transacciones.groupBy("id_cliente")
            .agg(
                # Variable 1: Saldo promedio filtrando transacciones dentro del intervalo de 90 días
                _avg(
                    when(col("fecha_transaccion") >= expr(f"cast('{fecha_max_raw}' as timestamp) - interval 90 days"), col("saldo"))
                    .otherwise(None)
                ).alias("saldo_promedio_ultimos_90_dias"),
                
                # Auxiliar de Ingresos: Suma acumulada en el año
                _sum(
                    when(col("tipo_movimiento") == "Deposito", col("monto")).otherwise(0)
                ).alias("total_ingresos"),
                
                # Auxiliar de Egresos: Suma de Retiros, Transferencias y Pagos en el año
                _sum(
                    when(col("tipo_movimiento") != "Deposito", col("monto")).otherwise(0)
                ).alias("total_egresos"),
                
                # Variable de Control: Frecuencia de uso anual
                _count("id_transaccion").alias("frecuencia_uso")
            )
            # Variable 2: Volumen neto financiero (Ingresos vs Egresos)
            .withColumn(
                "volumen_total_ingresos_vs_egresos", 
                col("total_ingresos") - col("total_egresos")
            )
            .na.fill(0.0)
        )

        # Clasificación binaria: 1 (Bajo Riesgo / Aprueba) si el balance ingresos vs egresos es positivo
        df_target = feature_cliente.withColumn(
            "label", 
            when(col("volumen_total_ingresos_vs_egresos") > 0, 1).otherwise(0)
        )

        # Vectorización 
        input_cols = [
            "saldo_promedio_ultimos_90_dias", 
            "volumen_total_ingresos_vs_egresos", 
            "frecuencia_uso"
        ]
        
        assembler = VectorAssembler(inputCols=input_cols, outputCol="features")
        df_vectorized = assembler.transform(df_target)


        # ENTRENAMIENTO DEL ALGORITMO CLASIFICADOR 
        logger.info("Entrenando algoritmo Random Forest Classifier...")
        rf = RandomForestClassifier(featuresCol="features", labelCol="label", numTrees=25, maxDepth=5, seed=42)
        rf_model = rf.fit(df_vectorized)

        # Evaluación mediante el Área Bajo la Curva ROC (AUC)
        evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        predictions = rf_model.transform(df_vectorized)
        auc = evaluator.evaluate(predictions)
        logger.info(f"Desempeño del modelo - Métrica AUC: {auc:.4f}")

        # === EXTRAER HALLAZGOS INTERNOS DEL MODELO ===
        logger.info("=== IMPORTANCIA DE LAS VARIABLES (HALLAZGOS) ===")
        importancias = rf_model.featureImportances.toArray()
        
        for i, col_name in enumerate(input_cols):
            logger.info(f"Variable: {col_name:<35} | Importancia: {importancias[i]:.4f}")

    
        # INFERENCIAS Y PERSISTENCIA EN CAPA GOLD
       
        logger.info("Generando asignaciones de líneas de crédito")  

        capa_gold = (
            predictions.withColumn(
                "línea_credito_potencial",
                when(col("prediction") == 1, col("saldo_promedio_ultimos_90_dias") * 1.2)
                .otherwise(col("saldo_promedio_ultimos_90_dias") * 1.01)
            )
            .select(
                col("id_cliente").alias("cliente_id"),
                col("prediction").alias("credit_aprobado"),
                col("línea_credito_potencial")
            )
        )

        gold_exit = GOLD_ROOT / "ml_credit_score"
        logger.info(f"Guardando resultados predictivos en la capa Gold Parquet: {gold_exit}")
        capa_gold.write.mode("overwrite").parquet(str(gold_exit))
        # Guardar en formato CSV usando Spark nativo
        csv_exit = GOLD_ROOT / "ml_credit_score_csv"
        capa_gold.write.mode("overwrite").option("header", "true").csv(str(csv_exit))
        
        logger.info("=== MODELO DE CREDIT SCORE FINALIZADO Y UNIFICADO CON ÉXITO ===")

    except Exception as error:
        logger.error(f"Error crítico durante la ejecución del pipeline: {error}", exc_info=True)
        raise error
    finally:
        spark.stop()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
    ejecutar_credit_score()
