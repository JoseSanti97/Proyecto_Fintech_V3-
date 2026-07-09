import sys
import logging
from generador.conexion import obtener_sesion, engine
from generador.poblador_masivo import poblar_sistema
from generador.modelos import Base

# Configuración centralizada de Logs 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(filename)s): %(message)s",
    handlers=[
        logging.FileHandler("fintech_pipeline.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]

)
logger = logging.getLogger(__name__)

def ejecutar_pipeline_fintech():
    logger.info("=== INICIANDO ECOSISTEMA FINTECH ===")

    logger.info("Verificando y mapeando estructuras relacionales de SQLAlchemy")
    Base.metadata.create_all(bind=engine)

    session = obtener_sesion()
    if not session:
        logger.error("Error fatal: No se pudo establecer la sesión con el servidor PostgreSQL.")
        sys.exit(1)
        
    try:
        # FASE 1: Ingesta transaccional optimizada (OLTP)
        logger.info("Iniciando inyección en bloques")
        poblar_sistema(session, total_clientes=10000, transacciones_por_cliente=5)
        logger.info("Fase Transaccional completada: Base de datos poblada exitosamente.")
        
        # FASE 2: Orquestación del Data Lakehouse (OLAP)
        logger.info("Ejecutando el pipeline del Data Lakehouse")
        import pipeline_lakehouse
        pipeline_lakehouse.exec_pipeline_lakehouse()
        # FASE 3: Inteligencia Artificial / Machine Learning
        logger.info("Ejecutando el modelo predictivo Random Forest")
        from modelo_ml import ejecutar_credit_score
        ejecutar_credit_score()
        
    except Exception as error:
        logger.error(f"Error crítico en el flujo de ejecución: {error}", exc_info=True)
        
    finally:
        session.close()
        logger.info("Conexión con el contenedor Docker cerrada de forma segura.")
        logger.info("=== FIN DEL PROCESO UNIFICADO ===")

if __name__ == "__main__":
    ejecutar_pipeline_fintech()