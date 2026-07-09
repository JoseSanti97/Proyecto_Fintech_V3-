-- 1.- Cuando un usuario nuevo se registra, el sistema debe garantizar que se cree su perfil de cliente y su primera cuenta
--Registro de un cliente y la creación de su cuenta bancaria
CREATE OR REPLACE PROCEDURE registrar_cliente_con_cuenta(
    p_nombre VARCHAR,
    p_apellido_p VARCHAR,
    p_apellido_m VARCHAR,
    p_curp VARCHAR,
    p_email VARCHAR,
    p_tipo_cuenta VARCHAR,
    p_saldo_inicial DECIMAL
)

LANGUAGE plpgsql
AS $$

DECLARE
    _id_cliente INT;
BEGIN
    -- Se toman los datos del cliente y los guardamos en la tabla
    INSERT INTO CLIENTE (Nombre, Apellido_Paterno, Apellido_Materno, CURP, Email)
    VALUES (p_nombre, p_apellido_p, p_apellido_m, p_curp, p_email)
    RETURNING ID_Cliente INTO _id_cliente;

    -- Creación de la cuenta bancaria asociada a _id_cliente
    INSERT INTO CUENTA (No_Cuenta, Tipo_Cuenta, Saldo, ID_Cliente)
    VALUES (
        substring(p_curp from 1 for 4) || floor(random() * 900000 + 100000)::text, -- Corte de las primeras 4 letras del CURP y se le concatena un número aleatorio de 6 dígitos
        p_tipo_cuenta, 
        p_saldo_inicial, 
        _id_cliente
    );
END;
$$;

-- 2.- Este procedimiento cambia el estado del préstamo y deposita automáticamente el dinero en la cuenta activa del cliente, reflejando su nuevo saldo
CREATE OR REPLACE PROCEDURE autorizar_prestamo(
    p_id_prestamo INT
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_id_cliente INT;
    v_monto DECIMAL(15,2);
    v_no_cuenta VARCHAR(20);
BEGIN
    -- a) Se busca el préstamo que esté 'Pendiente' y obtenemos la información
    SELECT ID_Cliente, Monto INTO v_id_cliente, v_monto
    FROM PRESTAMO
    WHERE ID_Prestamo = p_id_prestamo AND Estado = 'Pendiente';

    -- Si no se encuentra el préstamo con ese ID o ya no está pendiente, cancelamos
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Error: El préstamo con ID % no fue encontrado o ya no está pendiente.', p_id_prestamo;
    END IF;

    -- b) Se busca la primera cuenta activa que tenga el cliente para depositarle
    SELECT No_Cuenta INTO v_no_cuenta
    FROM CUENTA
    WHERE ID_Cliente = v_id_cliente
    LIMIT 1;

    -- Si el cliente no tiene ninguna cuenta abierta, se cancela la operación
    IF v_no_cuenta IS NULL THEN
        RAISE EXCEPTION 'Error: El cliente no cuenta con una cuenta digital activa para recibir el depósito.';
    END IF;

    -- c) Si todo está en orden, si hacen los cambios en la misma transacción:
    -- 1c) Cambiamos el estado del préstamo a Aprobado
    UPDATE PRESTAMO 
    SET Estado = 'Aprobado' 
    WHERE ID_Prestamo = p_id_prestamo;

    -- 2c): Le sumamos el dinero del préstamo al saldo de su cuenta
    UPDATE CUENTA 
    SET Saldo = Saldo + v_monto 
    WHERE No_Cuenta = v_no_cuenta;

END;
$$;

-- 3.- Este procedimiento recorre todas las cuenta de estado: 'Corriente'. 
--Si el saldo de este tipo de cuenta es positivo y menor o igual a $1000, entonces le cobra una comision de mantenimiento de $50
CREATE OR REPLACE PROCEDURE simular_corte_mes()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Cobro mediante una sola instrucción UPDATE 
    UPDATE CUENTA
    SET Saldo = Saldo - 50.00
    WHERE Tipo_Cuenta = 'Corriente' 
      AND Saldo < 1000.00 
      AND Saldo >= 50.00; -- Protección para no dejar la cuenta con saldo negativo menor a la comisión

    -- Uso de constraints (Saldo >= 0) de 02_constraints.sql, 
    -- Postgres bloquearía cualquier intento de cobro que intente dejar una cuenta en números negativos.
END;
$$;

