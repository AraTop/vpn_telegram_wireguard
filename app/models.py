from __future__ import annotations
from datetime import datetime, timezone
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import BigInteger, ForeignKey, String, Boolean, Numeric, JSON, Text, DateTime, Integer
from app.database import Base

class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(primary_key=True)
    tg_id: Mapped[int] = mapped_column(BigInteger, unique=True, nullable=False)

    username: Mapped[str | None] = mapped_column(String(255))
    first_name: Mapped[str | None] = mapped_column(String(255))
    last_name: Mapped[str | None] = mapped_column(String(255))

    balance: Mapped[float] = mapped_column(Numeric(12, 2), default=0)

    is_admin: Mapped[bool] = mapped_column(Boolean, default=False)
    referral_code: Mapped[str | None] = mapped_column(String(64), unique=True)
    referred_by_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id", ondelete="SET NULL"))

    # БАЗОВАЯ ПОДПИСКА
    subscription_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # КВОТА ПО ТАРИФУ
    device_quota: Mapped[int] = mapped_column(Integer, default=0)

    # ДОП. УСТРОЙСТВА (помесячная подписка)
    # ВАЖНО: Эти 2 поля должны точно соответствовать колонкам в БД
    extra_devices_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    extra_devices_count: Mapped[int] = mapped_column(Integer, default=0)

    # СТАРОЕ ПОЛЕ (если оно ещё есть в схеме) — можно оставить, но не использовать:
    # extra_devices: Mapped[int] = mapped_column(Integer, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
    )

    devices: Mapped[list["Device"]] = relationship(back_populates="user")
    payments: Mapped[list["Payment"]] = relationship(back_populates="user")

    def has_base_active(self) -> bool:
        if not self.subscription_until:
            return False
        return self.subscription_until > datetime.now(timezone.utc)

    def has_extra_active(self) -> bool:
        if not self.extra_devices_until:
            return False
        return self.extra_devices_until > datetime.now(timezone.utc)

    def total_quota(self) -> int:
        base = self.device_quota or 0
        extra = self.extra_devices_count if self.has_extra_active() else 0
        return base + extra

class Tariff(Base):
    __tablename__ = "tariffs"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100))
    days: Mapped[int] = mapped_column(Integer)
    price: Mapped[float] = mapped_column(Numeric(12,2))
    max_devices: Mapped[int] = mapped_column(Integer)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))

    wg_client_id: Mapped[str] = mapped_column(String(255))
    wg_client_name: Mapped[str] = mapped_column(String(255))

    # новое поле: это «доп» устройство (идёт из доп-квоты)?
    is_extra: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    node_id: Mapped[int] = mapped_column(ForeignKey("nodes.id", ondelete="SET NULL"))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)

    user: Mapped["User"] = relationship(back_populates="devices")

class Payment(Base):
    __tablename__ = "payments"
    id: Mapped[int] = mapped_column(primary_key=True)
    yk_payment_id: Mapped[str | None] = mapped_column(String(255), unique=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"))
    status: Mapped[str] = mapped_column(String(50))    # pending, succeeded, canceled
    purpose: Mapped[str] = mapped_column(String(50))   # TARIFF, TOPUP, EXTRA_DEVICE
    amount: Mapped[float] = mapped_column(Numeric(12,2))
    currency: Mapped[str] = mapped_column(String(10), default="RUB")
    tariff_id: Mapped[int | None] = mapped_column(ForeignKey("tariffs.id", ondelete="SET NULL"))
    confirmation_url: Mapped[str | None] = mapped_column(Text)
    meta: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=datetime.utcnow, onupdate=datetime.utcnow)

    user: Mapped[User] = relationship(back_populates="payments")

class Node(Base):
    __tablename__ = "nodes"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True)  # название сервера
    api_url: Mapped[str] = mapped_column(String(255))  # URL WG-Easy API
    api_password: Mapped[str] = mapped_column(String(255))  # пароль для API
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)  # активен ли сервер
    load: Mapped[int] = mapped_column(Integer, default=0)  # сколько устройств на сервере
    max_capacity: Mapped[int] = mapped_column(Integer, default=100)  # максимум клиентов