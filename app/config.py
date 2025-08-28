from pydantic import BaseModel
from dotenv import load_dotenv
import os
from typing import List
from pathlib import Path

# Грузим .env из корня проекта, даже если стартуешь бот из другой папки
ENV_PATH = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=ENV_PATH)

class Settings(BaseModel):
    ref_trial_days: int = int(os.getenv("REF_TRIAL_DAYS", "0"))
    ref_referrer_fixed_rub: int = int(os.getenv("REF_REFERRER_FIXED_RUB", "0"))
    telegram_token: str = os.getenv("TELEGRAM_BOT_TOKEN", "")
    admin_ids: List[int] = [int(x) for x in os.getenv("TELEGRAM_ADMIN_IDS", "").replace(" ", "").split(",") if x]
    database_url: str = os.getenv("DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5432/vpn_bot")
    currency: str = os.getenv("CURRENCY", "RUB")

    # WG-Easy
    wg_url: str = os.getenv("WGEASY_URL", "http://localhost:51821").rstrip("/")
    wg_password: str = os.getenv("WGEASY_PASSWORD", "")

    # YooKassa
    yk_shop_id: str = os.getenv("YOOKASSA_SHOP_ID", "")
    yk_secret_key: str = os.getenv("YOOKASSA_SECRET_KEY", "")
    yk_return_url: str = os.getenv("YOOKASSA_RETURN_URL", "https://t.me/YourBot")

    # Business rules
    referral_bonus_percent: int = int(os.getenv("REFERRAL_BONUS_PERCENT", "10"))
    device_extra_price: float = float(os.getenv("DEVICE_EXTRA_PRICE", "100.00"))

settings = Settings()
