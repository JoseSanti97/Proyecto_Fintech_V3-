import logging
import subprocess
from datetime import datetime 
from dotenv import load_dotenv
import os

logger = logging.getLogger(__name__)

def respaldar_base_de_datos():
    logger.info("[INFRAESTRUCTURA] Ejecutando pg_dump automatizado dentro de Docker")
    db_user = os.getenv("DB_USER")
    db_name = os.getenv("DB_NAME")
    # Configuración dinámica del nombre de archivo basado en fecha y hora
    archivo_salida = f"backup_fintech_{datetime.now().strftime('%Y%m%d_%H%M%S')}.sql"
    
    # Comando para extraer los datos de tu contenedor de Postgres
    comando = ["docker", "exec", "-t", "bd_fintech", "pg_dump", "-U", db_user, "-d", db_name]   
    
    
    try:
        # 3. Intentamos ejecutar la orden en la terminal del sistema operativo
         with open(archivo_salida, "w") as f:
         subprocess.run(comando, stdout=f, check=True)
        logger.info(f"Backup generado con éxito y guardado localmente en: {archivo_salida}")
        
    except subprocess.CalledProcessError as e:
        # Si Docker está apagado o las credenciales fallan, se registra el error
        logger.error(f"Respaldo fallido. Docker inalcanzable o error de privilegios: {e}")
    except Exception as general_error:
        # Captura cualquier otro error imprevisto del sistema operativo
        logger.error(f"Ocurrió un fallo inesperado al procesar el backup: {general_error}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")
    respaldar_base_de_datos()