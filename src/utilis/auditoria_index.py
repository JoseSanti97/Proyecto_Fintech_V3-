from sqlalchemy import text
from generador.conexion import obtener_sesion

def rendimiento_index():
    print("[AUDITORÍA DE LA INFRAESTRUCTURA] Analizando uso de índices")

    session = obtener_sesion()
    if not session:
        print("No es posible conectarse a la base de datos")
        return
    
    query_postgres = text("""
        SELECT
            relname AS nombre_tabla,
            indexrelname AS nombre_index,
            idx_scan AS contar_usosd
        FROM
            pg_stat_user_indexes
        WHERE
            schemaname = 'public'
        ORDER BY
            idx_scan DESC;
    """)

    try: 
        result = session.execute(query_postgres)
        print(f"\n{'TABLA':<20} | {'ÍNDICE':<35} | {'VECES USADO':<12}")
        print("-" * 75)

        for fila in result.fetchall():
            print(f"{fila[0]:<20} | {fila[1]:<35} | {fila[2]:<12}")
            
    except Exception as e:
        print(f"Error al auditar los índices: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    rendimiento_index()