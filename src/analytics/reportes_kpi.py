from sqlalchemy import text
from generador.conexion import obtener_sesion

def fintech_kpi():
    print("[BI PIPELINE] Obteniendo indicadores clave de rendimiento (KPI)")
    session = obtener_sesion()
    if not session:
        return
    
    kpi = {
        "Total de Capital": 
            "SELECT TO_CHAR(SUM(saldo), '$999,999,999.00') FROM cuenta;",
        
        "Monto Promedio por Transacción Bancaria": 
            "SELECT TO_CHAR(AVG(monto), '$999,999.00') FROM transaccion;",
        
        "Top 3 Alcaldías de la CDMX con Mayor Número de Clientes": 
            "SELECT delegacion, COUNT(*) as total FROM cliente GROUP BY delegacion ORDER BY total DESC LIMIT 3;",
        
        "Distribución de Movimientos por Tipo de Operación": 
            "SELECT tipo_movimiento, COUNT(*) FROM transaccion GROUP BY tipo_movimiento ORDER BY COUNT(*) DESC;"
    }

    try:
        for titulo, query in kpi.items():
            print(f"\n  {titulo}:")
            result = session.execute(text(query))
            
            # Formateo dinámico dependiendo la estructura de la respuesta
            filas = result.fetchall()
            if len(filas) == 1 and len(filas[0]) == 1:
                print(f"   {filas[0][0]}")
            else:
                for fila in filas:
                    print(f"   • {fila[0]}: {fila[1]}")
                    
    except Exception as e:
        print(f"Error al procesar los KPI: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    fintech_kpi()