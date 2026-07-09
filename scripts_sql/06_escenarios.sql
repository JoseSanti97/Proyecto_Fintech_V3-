-- 1.- Se necesita verificar los datos de los clientes, consultar saldos y registrar depósitos y/o retiros. Sin embargo por seguridad, tienen estrictamente prohibido:
-- 1A) Borrar registros de los clientes.
-- 1B) Eliminar cuentas bancarias.
-- 1C) Alterar los contratos de préstamos.

CREATE ROLE rol_cajero;
-- Se crea el rol
CREATE USER empleado_soporte1 WITH PASSWORD 'SoporteFintech2026';
-- Se crea el usuario
GRANT SELECT ON CLIENTE_TEL, TARJETA TO rol_cajero;

GRANT rol_cajero TO empleado_soporte1;
-- Asigna el rol al usuario epecificado

-- 2.- Un cliente que inicia sesión sólo debe ser capaz de ver sus propias cuentas, sus tarjetas y sus transacciones.
-- El cliente no puede tener acceso directo a las tablas globales porque podría ver la información financiera de otros usuarios

CREATE ROLE rol_cliente;

CREATE VIEW vista_mis_cuentas AS
SELECT c.No_Cuenta, c.Tipo_Cuenta, c.Saldo, cl.CURP
FROM CUENTA c
    JOIN CLIENTE cl ON c.ID_Cliente = cl.ID_Cliente
WHERE
    cl.CURP = CURRENT_USER;

-- Otorgar permiso al rol correcto
GRANT SELECT ON vista_mis_cuentas TO rol_cliente;

-- 3.- Se requiere revisar los movimientos para prevenir el lavado de dinero y analizar fraudes.
-- Es necesario ver todo el panorama de la base de datos, pero sin el permiso de realizar modificaciones a saldos ni permiso de realizar transacciones falsas.

CREATE ROLE rol_auditor;

CREATE USER auditor_externo WITH PASSWORD 'AuditoriaRiesgos2026.123';

GRANT SELECT ON ALL TABLES IN SCHEMA public TO rol_auditor;

GRANT rol_auditor TO auditor_externo;