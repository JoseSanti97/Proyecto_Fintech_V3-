import sys
import logging
from sqlalchemy import text
from generador.conexion import obtener_sesion

logger = logging.getLogger(__name__)

def exec_test():
    logger.info("[DATA QUALITY] Iniciando auditoría de consistencia de datos")
    session = obtener_sesion()
    if not session:
        logger.critical("No se pudo establecer la sesión con el servidor PostgreSQL.")
        sys.exit(1)

    tests = {
        "1. Cuentas con saldos negativos (Debe ser 0)": 
            """SELECT COUNT(*) 
                FROM cuenta 
                WHERE saldo < 0;""",
        "2. Transacciones con monto cero o negativo (Debe ser 0)": 
            """SELECT COUNT(*) 
                FROM transaccion 
                WHERE monto <= 0;""",
        "3. Clientes huérfanos sin ninguna cuenta asignada (Debe ser 0)": 
             """SELECT COUNT(*) 
                FROM cliente c 
                LEFT JOIN cuenta cu ON c.id_cliente = cu.id_cliente 
                WHERE cu.no_cuenta IS NULL; """,
        "4. Beneficiarios cuyo porcentaje asignado NO sume 100% por cuenta (Debe ser 0)": 
            """
            SELECT COUNT(*) 
            FROM cuenta c 
            LEFT JOIN (
                SELECT no_cuenta, SUM(porcentaje_saldo) as total 
                FROM beneficiario 
                GROUP BY no_cuenta
            ) b ON c.no_cuenta = b.no_cuenta 
            WHERE COALESCE(b.total, 0) != 100.00;
            """
    }

    errors_found = 0
    
    try:  
        for name_test, query in tests.items():
            result = session.execute(text(query)).scalar()
            if result == 0:
                logger.info(f"Pasó exitosamente: {name_test}")
            else:
                # si hay registros corruptos, salta como WARNING en los logs
                logger.warning(f"FRACASO -> Se detectaron {result} registros corruptos en: {name_test}")
                errors_found += 1
                
        
        if errors_found > 0:
            logger.warning(f"[ALERTA] Se detectaron {errors_found} fallas estructurales en la calidad de datos.")
        else:
            logger.info("✅ Resumen de Calidad: 100% de los datos son consistentes con las reglas de negocio.")
            
    except Exception as e:
        logger.error(f"Error crítico al ejecutar las pruebas de calidad de datos: {e}", exc_info=True)
    finally:
        session.close()
        logger.info("[DATA QUALITY] Auditoría de calidad finalizada y recursos liberados.")

if __name__ == "__main__":
    # Configuración de pruebas por si corres este script de forma aislada en la terminal
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
    exec_test()