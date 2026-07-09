-- 1. Creamos la función que validará el saldo
CREATE OR REPLACE FUNCTION validar_saldo_transaccion()
RETURNS TRIGGER AS $$
DECLARE
    saldo_disponible DECIMAL(15,2);
BEGIN
    -- Obtenemos el saldo actual de la cuenta origen
    SELECT Saldo INTO saldo_disponible 
    FROM CUENTA 
    WHERE No_Cuenta = NEW.Cuenta_Origen;

    -- Si la cuenta origen no existe o no tiene saldo suficiente, cancelamos
    IF saldo_disponible IS NULL OR saldo_disponible < NEW.Monto THEN
        RAISE EXCEPTION 'Error Financiero: Saldo insuficiente en la cuenta de origen (%) para realizar este movimiento.', NEW.Cuenta_Origen;
    END IF;

    -- Si todo está bien, permitimos que continúe la inserción
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

--Creamos el trigger asociado a la tabla 
CREATE TRIGGER trg_validar_saldo
BEFORE INSERT ON TRANSACCION
FOR EACH ROW
EXECUTE FUNCTION validar_saldo_transaccion();


-- 2. Creamos la función que moverá el dinero entre las cuentas
CREATE OR REPLACE FUNCTION actualizar_saldos_cuentas()
RETURNS TRIGGER AS $$
BEGIN
    -- Restar el monto de la cuenta de origen
    UPDATE CUENTA 
    SET Saldo = Saldo - NEW.Monto 
    WHERE No_Cuenta = NEW.Cuenta_Origen;

    -- Sumar el monto a la cuenta de destino
    UPDATE CUENTA 
    SET Saldo = Saldo + NEW.Monto 
    WHERE No_Cuenta = NEW.Cuenta_Destino;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Creamos el trigger asociado a la tabla
CREATE TRIGGER trg_actualizar_saldos
AFTER INSERT ON TRANSACCION
FOR EACH ROW
EXECUTE FUNCTION actualizar_saldos_cuentas();





