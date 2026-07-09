-- El saldo de una cuenta debe ser mayor o igual a 0 (no negativo)
ALTER TABLE CUENTA
ADD CONSTRAINT ch_saldo_positivo CHECK (Saldo >= 0),
ADD CONSTRAINT ch_tipo_cuenta CHECK (
    Tipo_Cuenta IN (
        'Ahorro',
        'Corriente',
        'Nomina'
    )
);

ALTER TABLE TRANSACCION
ADD CONSTRAINT ch_monto_transaccion CHECK (Monto > 0);
--El porcentaje de saldo solo puede estar entre el 0% y 100%
ALTER TABLE beneficiario
ADD CONSTRAINT ch_porcentaje CHECK (
    Porcentaje_Saldo > 0
    AND Porcentaje_Saldo <= 100
);