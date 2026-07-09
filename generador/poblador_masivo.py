import random
from decimal import Decimal
from faker import Faker
from sqlalchemy import insert
from sqlalchemy.orm import Session
from generador.modelos import Cliente, Cliente_Tel, Cuenta, Tarjeta, Transaccion, Prestamo, Beneficiario, Beneficiario_Tel, Cliente_biometria

fake = Faker('es_MX')

TIPOS_CUENTA = ["Debito", "Ahorro", "Nomina", "Chequera"]
ESTADOS_TARJETA = ["Activa", "Bloqueada", "Vencida"]
TIPOS_MOVIMIENTO = ["Deposito", "Retiro", "Transferencia", "Pago de Servicio"]
PARENTESCOS = ["Hijo/a", "Esposo/a", "Madre", "Padre", "Hermano/a"]
ESTADOS_PRESTAMO = ["Activo", "Pagado", "Vencido", "En Mora"]
EMISORES_TARJETA = ["Visa", "Mastercard", "American Express"]
ESTADOS_CIVILES = ['casado/a', 'soltero/a', 'viudo/a']
BIOMETRIAS = ["Huella", "Iris", "Facial"]

DELEGACIONES_CDMX = [
    "Alvaro Obregon", "Azcapotzalco", "Benito Juarez", "Coyoacan", "Cuajimalpa",
    "Cuauhtemoc", "Gustavo A. Madero", "Iztacalco", "Iztapalapa", "Magdalena Contreras",
    "Miguel Hidalgo", "Milpa Alta", "Tlahuac", "Tlalpan", "Venustiano Carranza", "Xochimilco"
]

def poblar_sistema(session: Session, total_clientes: int = 500, transacciones_por_cliente: int = 5):
    print(f"[DATA PIPELINE] Iniciando registro en lotes de {total_clientes} clientes")

    tamaño_lote = 100

    try:
        
        for lote_incio in range(0, total_clientes, tamaño_lote):
            lote_fin = min(lote_incio + tamaño_lote, total_clientes) #límite superior del lote, asegurando que no exceda el total de clientes
            clientes_generados = lote_fin - lote_incio #clientes a generar en este lote específico

            print(f"Procesando lote: Clientes del {lote_incio + 1} al {lote_fin}")

       
           # GENERAR DICCIONARIOS (HASH MAPS) DE CLIENTES 
            lista_clientes_dict = []
            for i in range(clientes_generados):
                nombre_pax = fake.first_name()
                apellido_pax = fake.last_name()
                
                
                correo_unico = f"{nombre_pax.lower()}.{apellido_pax.lower()}.{lote_incio + i}@fintechmasiva.com"

                lista_clientes_dict.append({
                    "nombre": nombre_pax,
                    "apellido": apellido_pax,
                    "curp": fake.bothify(text='????######??????##').upper(),
                    "correo": correo_unico,
                    "delegacion": random.choice(DELEGACIONES_CDMX),
                    "estado_civil": random.choice(ESTADOS_CIVILES)
                })
        
            # Insertar clientes y regresar sus Id's
            stm_cliente = insert(Cliente).returning(Cliente.id_cliente)
            result_cliente = session.execute(stm_cliente, lista_clientes_dict)
            lista_id_clientes = [fila[0] for fila in result_cliente.fetchall()]

            
            lista_tel_clientes = []
            lista_cuentas = []
            lista_prestamos = []

            for id_cliente in lista_id_clientes:
                # Registro de teléfonos
                tel_cliente_unico = f"55{str(id_cliente).zfill(8)}"
                lista_tel_clientes.append({
                    "tel": tel_cliente_unico,
                    "id_cliente": id_cliente
                })

                # Registro de las cuentas del cliente
                no_cuenta_creada = fake.unique.bothify(text='############')
                lista_cuentas.append({
                    "no_cuenta": no_cuenta_creada,
                    "tipo_cuenta": random.choice(TIPOS_CUENTA),
                    "saldo": Decimal(round(random.uniform(1000.0, 1000000), 2)),
                    "id_cliente": id_cliente
                })

                # Préstamos
                if random.random() < 0.20:
                    lista_prestamos.append({
                        "estado": random.choice(ESTADOS_PRESTAMO),
                        "monto": Decimal(round(random.uniform(5000.0, 15000.0), 2)),
                        "id_cliente": id_cliente
                    })

            session.execute(insert(Cliente_Tel), lista_tel_clientes)
            
            lista_clientes_biometria = []
            for id_cliente in lista_id_clientes:
                lista_clientes_biometria.append({
                    "id_cliente": id_cliente,
                    "tipo_biometria": random.choice(BIOMETRIAS),
                    "hash_referencia": fake.sha256()
                })

            session.execute(insert(Cliente_biometria), lista_clientes_biometria)

            if lista_prestamos:
                session.execute(insert(Prestamo), lista_prestamos)
            
            stm_cuenta = insert(Cuenta).returning(Cuenta.no_cuenta)
            result_cuentas = session.execute(stm_cuenta, lista_cuentas)
            lista_no_cuenta = [fila[0] for fila in result_cuentas.fetchall()]
            
            # Hashmaps de beneficiarios y tarjetas
            lista_beneficiarios = []
            lista_tarjetas = []

            for no_cuenta in lista_no_cuenta:
                lista_beneficiarios.append({
                    "parentesco": random.choice(PARENTESCOS),
                    "nombre": fake.first_name(),
                    "apellido": fake.last_name(),
                    "porcentaje_saldo": Decimal('100.00'),
                    "no_cuenta": no_cuenta
                })

                lista_tarjetas.append({
                    "no_tarjeta": fake.unique.bothify(text='4152############'), 
                    "cvv": fake.bothify(text='###'),
                    "emisor": random.choice(EMISORES_TARJETA),
                    "estado": random.choice(ESTADOS_TARJETA),
                    "no_cuenta": no_cuenta
                })
            
            stm_beneficiario = insert(Beneficiario).returning(Beneficiario.id_beneficiario)
            result_beneficiario = session.execute(stm_beneficiario, lista_beneficiarios)
            lista_id_beneficiario = [fila[0] for fila in result_beneficiario.fetchall()]

            # Se registran tarjetas y se recupera el número 
            stm_tarjeta = insert(Tarjeta).returning(Tarjeta.no_tarjeta, Tarjeta.no_cuenta)
            result_tarjeta = session.execute(stm_tarjeta, lista_tarjetas)
            
            # Este hashmap le da estructura a las transacciones
            map_tarjeta_cuenta = {fila[0]: fila[1] for fila in result_tarjeta.fetchall()} # 0 es no_tarjeta, 1 es no_cuenta

            # Hashmaps finales
            lista_tel_beneficiarios = []
            lista_transacciones = []

            for id_beneficiario in lista_id_beneficiario:
                tel_beneficiario_unico = f"55{str(id_beneficiario).zfill(8)}"
                lista_tel_beneficiarios.append({
                    "tel": tel_beneficiario_unico,
                    "id_beneficiario": id_beneficiario
                })
            
            for no_tarjeta, no_cuenta in map_tarjeta_cuenta.items():
                for _ in range(transacciones_por_cliente): 
                    lista_transacciones.append({
                        "no_tarjeta": no_tarjeta,
                        "cuenta_origen": no_cuenta,
                        "cuenta_destino": fake.bothify(text='############'),
                        "monto": Decimal(round(random.uniform(50.0, 5000.0), 2)), 
                        "tipo_movimiento": random.choice(TIPOS_MOVIMIENTO),
                        "fecha_transaccion": fake.date_time_between(start_date='-365d', end_date='now')
                    })

            
            session.execute(insert(Beneficiario_Tel), lista_tel_beneficiarios)
            session.execute(insert(Transaccion), lista_transacciones)

            # Hacemos flush al final de cada lote de 100 
            session.flush()
            fake.unique.clear() 

        # Al terminar con éxito TODOS los lotes, consolidamos en un único COMMIT definitivo
        print("Guardando y confirmando cambios en Docker")
        session.commit()
        print("[PIPELINE EXITOSO] La base de datos de la Fintech ha sido poblada con rendimiento optimizado.")

    except Exception as e:
        print(f"[PIPELINE FALLIDO] Aplicando Rollback preventivo. Motivo: {e}")
        session.rollback()
        raise e