from datetime import datetime
from decimal import Decimal
from sqlalchemy import String, Integer, ForeignKey, Numeric, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
class Base(DeclarativeBase):
    pass

class Cliente(Base):
    __tablename__ = "cliente"
    
    id_cliente: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    nombre: Mapped[str] = mapped_column(String(50), nullable=False)
    apellido: Mapped[str] = mapped_column(String(50), nullable=False)
    curp: Mapped[str] = mapped_column(String(18), unique=True, nullable=False)
    correo: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    ciudad: Mapped[str] = mapped_column(String(30), default="Ciudad de México", nullable=False)
    delegacion: Mapped[str] = mapped_column(String(50), nullable=False)
    estado_civil: Mapped[str] = mapped_column(String(20), nullable=False)

class Cliente_Tel(Base):
    __tablename__ = "cliente_tel"

    tel: Mapped[str] = mapped_column(String(15), primary_key=True, nullable=False)
    id_cliente: Mapped[int] = mapped_column(Integer, ForeignKey("cliente.id_cliente"), nullable=False)

class Cliente_biometria(Base):
    __tablename__ = "cliente_biometria"

    id_biometria: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    id_cliente: Mapped[int] = mapped_column(Integer, ForeignKey("cliente.id_cliente"), nullable=False)
    tipo_biometria: Mapped[str] = mapped_column(String(20), nullable=False)
    hash_referencia: Mapped[str] = mapped_column(String(64), nullable=False)
    

class Cuenta(Base):
    __tablename__= "cuenta"

    no_cuenta: Mapped[str] = mapped_column(String(20), primary_key=True)
    tipo_cuenta: Mapped[str] = mapped_column(String(20), nullable=False)
    saldo: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    id_cliente: Mapped[int] = mapped_column(Integer, ForeignKey("cliente.id_cliente"), nullable=False)

class Tarjeta(Base):
    __tablename__ = "tarjeta"

    no_tarjeta: Mapped[str] = mapped_column(String(16), primary_key=True, nullable=False)
    cvv: Mapped[str] = mapped_column(String(3), nullable=False)
    emisor: Mapped[str] = mapped_column(String(20), nullable=False)
    estado: Mapped[str] = mapped_column(String(20), nullable=False)
    no_cuenta: Mapped[str] = mapped_column(String(20), ForeignKey("cuenta.no_cuenta"), nullable=False)

class Transaccion(Base):
    __tablename__ = "transaccion"

    id_transaccion: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    no_tarjeta: Mapped[str] = mapped_column(String(16), ForeignKey("tarjeta.no_tarjeta"), nullable=False)
    cuenta_destino: Mapped[str] = mapped_column(String(20), nullable=False)
    cuenta_origen: Mapped[str] = mapped_column(String(20), nullable=False)
    monto: Mapped[Decimal] = mapped_column(Numeric(16, 2), nullable=False)
    tipo_movimiento: Mapped[str] = mapped_column(String(30), nullable=False)
    fecha_transaccion: Mapped[datetime] = mapped_column(DateTime, default=datetime.now, nullable=False)


class Prestamo(Base):
    __tablename__ = "prestamo"

    id_prestamo: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    estado: Mapped[str] = mapped_column(String(20), default="Activo", nullable=False)
    monto: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False )
    id_cliente: Mapped[int] = mapped_column(Integer, ForeignKey("cliente.id_cliente"), nullable=False)

class Beneficiario(Base):
    __tablename__ = "beneficiario"

    id_beneficiario: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    parentesco: Mapped[str] = mapped_column(String(30), nullable=False )
    nombre: Mapped[str] = mapped_column(String(50), nullable=False)
    apellido: Mapped[str] = mapped_column(String(50), nullable=False)
    porcentaje_saldo: Mapped[Decimal] = mapped_column(Numeric(5,2), nullable=False)
    no_cuenta: Mapped[str] = mapped_column(String(20), ForeignKey("cuenta.no_cuenta"), nullable=False)

class Beneficiario_Tel(Base):
    __tablename__ = "beneficiario_tel"

    tel: Mapped[str] = mapped_column(String(15),primary_key=True, nullable=False)
    id_beneficiario: Mapped[int] = mapped_column(Integer, ForeignKey("beneficiario.id_beneficiario"), nullable=False)
