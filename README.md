# Ecosystem End-to-End: Data Lakehouse & Machine Learning para Plataforma Fintech

## Resumen Ejecutivo

Descripción de la Problemática: Se tiene la necesidad de una plataforma para gestionar cuentas digitales, tarjetas y préstamos. Es necesario poder llevar acabo eficientemente las transacciones y la gestión de la información perteneciente a los clientes. 

Este proyecto representa el escalamiento y evolución de una plataforma Fintech transaccional pura (OLTP) hacia un ecosistema analítico e híbrido de datos masivos. Se diseñó e implementó un **Data Lakehouse local** bajo la **Arquitectura Medallion**, acoplando la ingesta automatizada de datos, gobierno de datos financiero, procesamiento distribuido y un modelo predictivo de Machine Learning (*Credit Scoring*) para la asignación inteligente de líneas de crédito.

[Sistema Transaccional OLTP: Docker + PostgreSQL]
│
▼  (Ingesta mediante JDBC / PySpark)
[Data Lakehouse: Arquitectura Medallion (Storage/)]
├── Layer 1: Bronze (Ingesta Inmutable en Parquet)
├── Layer 2: Silver (Limpieza, Gobierno y Data Masking)
└── Layer 3: Gold   (Data Marts de KPI y Predicciones de ML)
│
▼
[Machine Learning: PySpark MLlib (Random Forest)] ──> Inferencia y Líneas de Crédito



### Pilares Fundamentales del Diseño:
1. **Procesamiento de Big Data con Spark:** Sustitución de queries relacionales tradicionales por transformaciones optimizadas distribuidas en **PySpark** sobre archivos de formato columnar **Apache Parquet**.
2. **Gobierno y Seguridad Financiera (Data Masking):** Mitigación de riesgos de ciberseguridad mediante la anonimización automática de Información Personal Identificable (PII) en la capa Silver. Los correos electrónicos se cifran parcialmente (`xxxxxx@email.com`) y los teléfonos se transforman en máscaras (`XXXXXX`) bajo normativas de cumplimiento regulatorio bancario.
3. **Orquestación Centralizada:** Automatización unificada a través de `main.py`, el cual levanta los modelos en SQLAlchemy, inyecta los lotes sintéticos controlados en memoria (evitando latencia de red) y activa secuencialmente las fases analíticas de Spark.

El diseño está optimizado bajo la Forma Normal de Boyce-Codd (BCNF) para evitar anomalías transaccionales, y cuenta con mecanismos de control internos (*Triggers* y *Procediminetos Almacenados*) encargados de la integridad financiera del sistema.

Tecnologías clave: 
* **Lenguaje Principal:** Python 3.13
* **Motor Analítico Distribuido:** Apache Spark 3.x (PySpark)
* **Entorno Operacional (OLTP):** PostgreSQL 15+ alojado en un contenedor Docker.
* **Mapeo Objeto-Relacional (ORM):** SQLAlchemy
* **Driver de Base de Datos:** `psycopg2-binary`
* **Generación de Datos Sintéticos:** `Faker`
* **Machine Learning:** PySpark MLlib (Clasificación e Inferencia distribuida)
* **Almacenamiento Optimizado:** Apache Parquet
* **Gobernanza Local:** Control de Accesos Basado en Roles (RBAC) simulado mediante políticas de permisos del sistema operativo sobre el directorio `storage/`.


-Requerimientos y Restricciones: 

El usuario debe poder registrar datos personales, no puede transferir más saldo del que tiene disponible y debe ser el saldo no negativo.
Un usuario no puede visualizar ni editar información a la que no tiene permiso.
Se debe poder registrar depoósitos, hacer transferencias y retiros.
Cada tarjeta debe estar vinculada a una única cuenta de usuario.

Enlace a la primera versión del proyecto: https://github.com/JoseSanti97/Proyects/tree/main/Proyecto_Fintech

Para garantizar el Gobierno de Datos y el Control de Accesos Basado en Roles (RBAC), decidí desacoplar la seguridad de la lógica de los scripts de PySpark. Localmente, la gobernanza se administra mediante políticas de permisos del sistema operativo sobre los directorios de storage/ de cada capa Medallion. Esto asegura que si un analista intenta leer la capa Bronze o Silver, el sistema bloqueará el acceso a nivel de infraestructura, "simulando" herramientas como Unity Catalog(de Databricks) en entornos Cloud.
---


---

## 📁 Estructura del Proyecto

```text
Proyecto_Fintech/
│
├── generador/
│   ├── conexion.py          # Módulo de enlace de red encapsulado para Docker
│   ├── modelos.py           # Definición de las relaciones y tipos de datos en SQLAlchemy
│   └── poblador_masivo.py   # Motor de simulación e inyección de datos (Faker)
│
├── scripts_sql/
│   ├── 01_schema.sql          # Definición de las relaciones con tipos de datos
│   ├── 02_constraints.sql     # Restricciones de integridad referencial, llaves y unicidad 
│   ├── 03_triggers.sql        # Triggers de seguridad (Garantiza saldos mínimos en transferencias)
│   ├── 04_procedures.sql      # Procedimientos almacenados para la lógica bancaria automatizada
│   ├── 05_metrics_queries.sql # Consultas de auditoría, analítica y métricas financieras de negocio
│   └── 06_escenarios.sql      # Implementación de Roles y Vistas
│
├── storage/                   # DIRECTORIO DEL DATA LAKEHOUSE [Acceso: Solo Data Engineers]
│   ├── bronze/                # Datos extraídos de Docker 
│   │   ├── cliente/             ↳ Guardado en formato .parquet
│   │   ├── cliente_tel/
│   │   ├── beneficiario/
│   │   ├── beneficiario_tel/
│   │   ├── cuenta/
│   │   ├── tarjeta/
│   │   ├── prestamo/
│   │   └── transaccion/
│   │
│   ├── silver/                   # Datos limpios, estructurados y anonimizados [Acceso: Data Engineers, Data Analyst y Data Scientists]
│   │   ├── dim_clientes/
│   │   ├── dim_beneficiarios/
│   │   ├── dim_cuentas/      
│   │   ├── dim_tarjetas/             
│   │   ├── pivot_model_credit/         
│   │   ├── dim_fact_prestamos/  
│   │   └── dim_fact_transacciones/ # Datos e información de operaciones financieras, cuentas
│   │
│   └── gold/                      # Agregaciones listas para reportes ejecutivos [Acceso: Analistas de Negocio, ML/IA Engineers y Data Scientists]
│       ├── kpi_capital            # ↳ Datos pre-calculados por Spark
│       ├── kpi_movimientos/       # Conexión con alguna herramienta de BI
│       ├── kpi_prestamos/
│       ├── ml_credit_score_csv/   
│       └── ml_credit_score/       # Asignación de crédito basada en el comportamiento
│
│
├── main.py                    # Coordinador del despliegue y automatización
├── pipeline_lakehouse.py      # Pipeline de PySpark para la Arquitectura Medallion
├── modelo_ml.py               # Random Forest Algorithm
├── auditoria_index.py         # Módulo de auditoría de índices en PostgreSQL
├── validar_calidad.py         # Pruebas de calidad y reglas de negocio
├── reportes_kpi.py            # Obtención de indicadores mediante SQL tradicional
├── backup_data.py             # Automatiza respaldos lógicos de la base de datos
├── requirements.txt           # Dependencias empaquetadas del proyecto
├── read_parquet.py            # Obervar .parquet de la capa / GOLD y / SILVER
├── parquet_to_csv.py          # Conversión de parquet a csv para posterior análisis
├── fintech_pipeline.log
└── README.md                  # Documentación

```

```


```

## Arquitectura y Tecnologías
* **Motor de Base de Datos:** PostgreSQL 15+ alojado en contenedor Docker.
* **Scripting & Automatización:** Python 3.13.
* **Librerías utilizadas:** `SQLAlchemy`, `psycopg2-binary`, `Faker` (Generador de datos sintéticos), `PySpark`.


## Instalación e Instrucciones

1. Inicializar la Base de Datos en Docker
Levanta el contenedor oficial de PostgreSQL configurando las variables de entorno correspondientes a tu configuración:

docker run -d --name bd_fintech -p 5432:5432 -e POSTGRES_USER=tu_usuario -e POSTGRES_DB=fintech_db -e POSTGRES_PASSWORD=tu_password postgres:latest

2. Configurar el Entorno Virtual de Python
Crea y activa tu entorno virtual aislado, e instala la suite de librerías del proyecto:

# Crear entorno virtual
python -m venv .venv

# Activar en Windows (PowerShell)
.venv\Scripts\Activate.ps1
# Activar en Linux / macOS / Git Bash
source .venv/bin/activate

# Actualizar gestor e instalar dependencias
pip install --upgrade pip
pip install -r requirements.txt (En caso de no funcionar, instala cada elemento de requirements uno por uno)

3. Descargar el Conector JDBC (Driver de Base de Datos)
Spark requiere el driver oficial de Java para conectarse a PostgreSQL. Descárgalo en el directorio raíz:

# Crear la carpeta de drivers
mkdir -p drivers

# En Windows (PowerShell)
Invoke-WebRequest -Uri "[https://jdbc.postgresql.org/download/postgresql-42.6.0.jar](https://jdbc.postgresql.org/download/postgresql-42.6.0.jar)" -OutFile "drivers/postgresql-42.6.0.jar"

# En Linux / macOS
curl -o drivers/postgresql-42.6.0.jar [https://jdbc.postgresql.org/download/postgresql-42.6.0.jar](https://jdbc.postgresql.org/download/postgresql-42.6.0.jar)

4. Ejecución del Flujo de Datos Completo
Ejecuta el pipeline unificado. Este purgará estados anteriores, compilará el esquema relacional DDL, inyectará datos sintéticos e iniciará las fases del Data Lakehouse y Machine Learning de Spark de manera secuencial:

python main.py

Módulos Auxiliares y Explotación de Datos
Una vez que el orquestador finalice con éxito, puedes auditar, leer y exportar la información del Lakehouse mediante la consola:

A. Inspección Dinámica de Archivos Parquet
Puedes leer cualquier tabla analítica de las capas Silver o Gold pasando la capa y el nombre como argumentos en la terminal:

# Inspeccionar dimensiones de clientes anonimizados en Silver
python read_parquet.py silver dim_clientes

# Inspeccionar el Data Mart consolidado de préstamos en Gold
python read_parquet.py gold kpi_prestamos

# Inspeccionar las predicciones financieras finales de Inteligencia Artificial
python read_parquet.py gold ml_credit_score

B. Exportación de Resultados a Formato CSV
Para análisis en herramientas externas como MS Excel, convierte cualquier tabla Parquet a un archivo .csv plano y unificado:

python parquet_to_csv.py gold ml_credit_score

C. Scripts de Calidad y Auditoría (OLTP):

python validar_calidad.py  # Control de calidad y consistencia lógica de saldos
python auditoria_index.py  # Análisis de eficiencia de índices en PostgreSQL
python backup_data.py      # Generación automática de respaldos lógicos (.sql)



##  Modelos de la Base de Datos
<details>

  <summary>Haz clic aquí para ver el Modelo Entidad-Relación (E-R)</summary>
  <br>
  <p align="center">
    <img src="./imagenes/modelo_er.png" alt="Modelo Entidad-Relación" width="90%">
  </p>
</details>

<details>
  <summary>Haz clic aquí para ver el Modelo Relacional</summary>
  <br>
  <p align="center">
    <img src="./imagenes/modelo_relacional.png" alt="Modelo Relacional" width="90%">
  </p>
</details>

---


## Normalización de la Base de Datos

Normalización del Modelo Relacional

El diseño de la base de datos operacional se estructuró bajo la Forma Normal de Boyce-Codd (BCNF) para erradicar anomalías de inserción, actualización y borrado:

1NF: Se resolvió la presencia de atributos multivalor creando las entidades atómicas CLIENTE_TEL y BENEFICIARIO_TEL.

2NF & 3NF: Se eliminaron las dependencias parciales y transitivas, aislando los datos del producto financiero en CUENTA y vinculándolos exclusivamente a través del identificador id_cliente como Llave Foránea.

BCNF: Cada determinante en las tablas principales (como id_cliente y CURP en la tabla CLIENTE) constituye estrictamente una superllave funcional.