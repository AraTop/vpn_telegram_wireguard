# bot.py — чистый async запуск PTB v22
import asyncio
import signal
from typing import Optional
from sqlalchemy import select
from telegram.ext import Application
from app.database import async_session
from app.config import settings
from app.database import init_db
from app.handlers import register_handlers, poll_pending_payments
import app.models as M
from app.handlers import enforce_user_devices
from app.config import settings
from app.wg_api import WGEasyClient
from datetime import datetime, timezone

wg_client = WGEasyClient(settings.wg_url, settings.wg_password)

async def sync_access_loop():
    while True:
        try:
            async with async_session() as session:
                res = await session.execute(select(M.User))
                users = res.scalars().all()  # ← вытаскиваем список, а не печатаем res
                #print(f"[sync_access_loop] {datetime.now(timezone.utc).isoformat()} users={len(users)}")
                for u in users:
                    #print(
                    #    f"[sync_access_loop] enforce uid={u.id} "
                    #    f"base_until={u.subscription_until} "
                    #    f"extra_until={getattr(u,'extra_devices_until',None)} "
                    #    f"extra_cnt={getattr(u,'extra_devices_count',0)}"
                    #)
                    await enforce_user_devices(session, wg_client, u)
        except Exception as e:
            print(f"[sync_access_loop] error: {e}")
        await asyncio.sleep(15)
        #await asyncio.sleep(1800)  # каждые 30 минут

async def _start_payments_scheduler(app: Application) -> Optional[asyncio.Task]:
    """
    Если доступен JobQueue (установлен extra [job-queue]) — пользуемся им.
    Иначе — поднимем фоновую asyncio-таску, которая раз в 60 сек дергает poll_pending_payments().
    Возвращаем Task, чтобы при выключении можно было её отменить.
    """
    if app.job_queue:
        # JobQueue есть — планируем повторяющуюся задачу
        app.job_queue.run_repeating(poll_pending_payments, interval=60, first=10)
        return None

    # Фоллбек без JobQueue: собственный цикл
    async def _loop():
        # Дадим приложению стартовать
        await asyncio.sleep(10)
        while True:
            try:
                await poll_pending_payments(app)
            except Exception as e:
                print("[payments] error:", e)
            await asyncio.sleep(60)

    return asyncio.create_task(_loop(), name="payments-poll-loop")


def _setup_signal_handlers(loop: asyncio.AbstractEventLoop, stop_event: asyncio.Event) -> None:
    """
    Навесим обработчики SIGINT/SIGTERM, чтобы корректно гасить приложение.
    На Windows внутри Docker (Linux) сигналы работают штатно.
    """
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            # На редких платформах сигналы могут быть не поддержаны в event loop
            pass


async def main() -> None:
    # 1) Инициализируем БД ДО старта поллинга
    await init_db()

    # 2) Собираем приложение PTB
    app = Application.builder().token(settings.telegram_token).build()

    # 3) Регистрируем хендлеры
    register_handlers(app)

    asyncio.get_running_loop().create_task(sync_access_loop())

    # 4) Планировщик платежей (JobQueue или наш фоллбек)
    fallback_task = await _start_payments_scheduler(app)

    # 5) Запускаем PTB вручную в async-режиме
    await app.initialize()
    await app.start()
    await app.updater.start_polling()  # ВАЖНО: тут именно await, а не просто вызов
    print("Bot is running...")

    # 6) Ждём сигналов завершения
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    _setup_signal_handlers(loop, stop_event)

    try:
        await stop_event.wait()
    finally:
        # 7) Акуратная остановка
        if fallback_task:
            fallback_task.cancel()
            try:
                await fallback_task
            except asyncio.CancelledError:
                pass

        await app.updater.stop()
        await app.stop()
        await app.shutdown()
        print("Bot stopped.")


if __name__ == "__main__":
    asyncio.run(main())
