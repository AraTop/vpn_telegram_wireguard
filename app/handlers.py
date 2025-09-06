import asyncio
import io
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Optional, List

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputFile,
)
from telegram.ext import (
    ContextTypes,
    CommandHandler,
    CallbackQueryHandler,
    Application,
)
from sqlalchemy import asc, select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.database import async_session
import app.models as M
from app.models import Node, User, Tariff, Device, Payment
from app.utils import rub, gen_ref_code
from app.wg_api import WGEasyClient
from app.payments import YooKassaClient
from app.wg_api import WGEasyError
from zoneinfo import ZoneInfo
# === singletons ===
wg_client = WGEasyClient(settings.wg_url, settings.wg_password)
yk_client = YooKassaClient(settings.yk_shop_id, settings.yk_secret_key)
user_payment_tasks = {}
BOT_BROADCAST_HEADER = "📣 Сообщение от VPN-сервиса\n\n"  # шапка, чтобы было видно «от бота»

# ---------------------------
# Small helpers (no stack)
# ---------------------------

async def pick_best_node(session: AsyncSession) -> M.Node | None:
    result = await session.execute(
        select(M.Node)
        .where(
            and_(
                M.Node.is_active == True,
                M.Node.load < M.Node.max_capacity  # <-- проверка лимита
            )
        )
        .order_by(asc(M.Node.load))
    )
    return result.scalars().first()

def _extra_active(u: M.User) -> bool:
    return bool(u.extra_devices_until and u.extra_devices_until > datetime.now(timezone.utc))

def _base_quota(u: M.User) -> int:
    return int(u.device_quota or 0)

def _extra_quota(u: M.User) -> int:
    return int(u.extra_devices_count or 0) if _extra_active(u) else 0

def _has_extra(u: User) -> bool:
    return bool(
        getattr(u, "extra_devices_until", None)
        and u.extra_devices_until > datetime.now(timezone.utc)
        and (getattr(u, "extra_devices_count", 0) or 0) > 0
    )

def safe_username(u):
    return u.username if getattr(u, "username", None) else "—"

import html

# Функция отмены
async def cancel_user_payment_check(user_id):
    if user_id in user_payment_tasks:
        user_payment_tasks[user_id].cancel()
        try:
            await user_payment_tasks[user_id]
        except asyncio.CancelledError:
            pass

async def auto_check_payment(application, payment_id: str, user_id: int, yk_client):
    start_time = asyncio.get_event_loop().time()
    timeout = 600  # 10 минут в секундах
    
    print(f'🎯 Запуск проверки для payment {payment_id}, user {user_id}')
    print(f'📊 Активные задачи: {list(user_payment_tasks.keys())}')

    try:
        while True:
            # Проверяем таймаут
            current_time = asyncio.get_event_loop().time()
            if current_time - start_time > timeout:
                # Время вышло, отменяем платеж
                async with async_session() as session:
                    p = (await session.execute(
                        select(Payment).where(Payment.yk_payment_id == payment_id)
                    )).scalar_one_or_none()
                    
                    # Отправляем сообщение пользователю
                    try:
                        await application.edit_message_text(
                            "⏰ Время оплаты истекло. Платеж отменен.\n\n"
                            "💡 Если вы хотели оплатить, создайте новый платеж.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🏠 В меню", callback_data="menu:main")]
                            ])
                        )
                    except Exception:
                        pass
                return

            async with async_session() as session:
                p = (await session.execute(
                    select(Payment).where(Payment.yk_payment_id == payment_id)
                )).scalar_one_or_none()

                if not p:
                    return  # Платёж удалён или не найден

                try:
                    info = await yk_client.get_payment(payment_id)
                except Exception as e:
                    print(f"⚠️ Ошибка получения статуса платежа {payment_id}: {e}")
                    await asyncio.sleep(15)
                    continue

                status = info.get("status", "pending")
                p.status = status
                p.updated_at = datetime.now(timezone.utc)
                
                # Выводим время до автоотмены
                time_left = int(timeout - (current_time - start_time))
                print(f'🔄 Проверка платежа {payment_id}. До отмены: {time_left} сек.')
                
                if status == "succeeded":
                    print('✅ Оплата прошла успешно')
                    await _apply_successful_payment(session, p)
                    await session.commit()

                    try:
                        await application.edit_message_text(
                            "✅ Оплата прошла успешно!",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🏠 Открыть меню", callback_data="menu:main")]
                            ])
                        )
                    except Exception:
                        pass
                    return

                elif status == "canceled":
                    print('❌ Оплата отменена.')
                    await session.commit()
                    try:
                        await application.edit_message_text(
                            "❌ Оплата отменена.",
                            reply_markup=InlineKeyboardMarkup([
                                [InlineKeyboardButton("🔄 Попробовать снова", callback_data="menu:tariffs")],
                                [InlineKeyboardButton("🏠 В меню", callback_data="menu:main")]
                            ])
                        )
                    except Exception:
                        pass
                    return

            await asyncio.sleep(10)
            
    except asyncio.CancelledError:
        # Задача была отменена вручную
        print(f"⏹️ Проверка платежа {payment_id} отменена вручную")
    except Exception as e:
        print(f"❌ Критическая ошибка в auto_check_payment: {e}")
    finally:
        # Очищаем только если это текущая активная задача
        if user_id in user_payment_tasks and user_payment_tasks[user_id] == asyncio.current_task():
            del user_payment_tasks[user_id]
            print(f"🧹 Задача для user {user_id} очищена")
        print(f'📋 Осталось задач: {list(user_payment_tasks.keys())}')

async def _render_admin_payments(query, tg_user_id: int, kind: str = "today"):
    # права
    async with async_session() as session:
        admin = (await session.execute(select(User).where(User.tg_id == tg_user_id))).scalar_one_or_none()
        if not admin or not admin.is_admin:
            await query.edit_message_text("❌ Недостаточно прав.", reply_markup=InlineKeyboardMarkup([back_to_admin()]))
            return

        start, end = _period_bounds(kind)
        conds = [Payment.status == "succeeded", Payment.created_at < end]
        if start is not None:
            conds.append(Payment.created_at >= start)

        total_count = (await session.execute(
            select(func.count(Payment.id)).where(and_(*conds))
        )).scalar_one()

        total_sum = (await session.execute(
            select(func.coalesce(func.sum(Payment.amount), 0)).where(and_(*conds))
        )).scalar_one()

        breakdown = (await session.execute(
            select(Payment.purpose,
                   func.count(Payment.id),
                   func.coalesce(func.sum(Payment.amount), 0))
            .where(and_(*conds))
            .group_by(Payment.purpose)
        )).all()

    # нормальные названия
    purpose_labels = {
        "TARIFF": "🧾 Подписки",
        "EXTRA_DEVICE": "🧩 Доп. устройства"
    }

    title = _payments_period_title(kind)

    lines = [
        f"💳 <b>Платежи {html.escape(title)}</b>",
        "",
        f"Всего покупок: <b>{int(total_count)}</b>",
        f"На сумму: <b>{float(total_sum):.2f} ₽</b>",
    ]
    if breakdown:
        lines.append("")
        lines.append("📊 По категориям:")
        for purpose, cnt, summ in breakdown:
            label = purpose_labels.get(purpose, f"• {purpose}")
            lines.append(f"{label}: <b>{int(cnt)}</b> шт. / <b>{float(summ):.2f} ₽</b>")

    await query.edit_message_text("\n".join(lines), reply_markup=_payments_kbd(), parse_mode="HTML")

def _has_base(u: User) -> bool:
    return bool(u.subscription_until and u.subscription_until > datetime.now(timezone.utc))

async def _delete_peer_safe(wg_client: WGEasyClient, client_id: str | None):
    if not client_id:
        return
    try:
        await wg_client.delete_client(client_id)
    except Exception:
        # уже удалено через UI — норм
        pass

# handlers.py (или где у тебя эта функция)
from sqlalchemy import and_
from datetime import datetime, timezone

async def enforce_user_devices(session, wg_client: WGEasyClient, user: M.User):
    now = datetime.now(timezone.utc)

    base_active = bool(user.subscription_until and user.subscription_until > now)
    base_quota  = (user.device_quota or 0) if base_active else 0
    extra_active_count = (
        user.extra_devices_count or 0
        if (user.extra_devices_until and user.extra_devices_until > now)
        else 0
    )

    #print(f"[enforce] uid={user.id} base_active={base_active} base_quota={base_quota} extra_active_count={extra_active_count}")

    # --- базовые
    res = await session.execute(
        select(M.Device).where(and_(M.Device.user_id == user.id, M.Device.is_extra == False)).order_by(M.Device.created_at.asc())
    )
    base_devices = res.scalars().all()

    if not base_active:
        for d in base_devices:
            #print(f"[enforce] delete BASE device id={d.id} wg_id={d.wg_client_id}")
            await _delete_peer_safe(wg_client, d.wg_client_id)
            await session.delete(d)
        await session.commit()
    else:
        if len(base_devices) > base_quota:
            to_remove = base_devices[base_quota:]
            for d in to_remove:
                #print(f"[enforce] trim BASE device id={d.id} wg_id={d.wg_client_id}")
                await _delete_peer_safe(wg_client, d.wg_client_id)
                await session.delete(d)
            await session.commit()

    # --- доп
    res = await session.execute(
        select(M.Device).where(and_(M.Device.user_id == user.id, M.Device.is_extra == True)).order_by(M.Device.created_at.asc())
    )
    extra_devices = res.scalars().all()

    if extra_active_count <= 0:
        for d in extra_devices:
            #print(f"[enforce] delete EXTRA device id={d.id} wg_id={d.wg_client_id}")
            await _delete_peer_safe(wg_client, d.wg_client_id)
            await session.delete(d)
        await session.commit()
    else:
        if len(extra_devices) > extra_active_count:
            to_remove = extra_devices[extra_active_count:]
            for d in to_remove:
                #print(f"[enforce] trim EXTRA device id={d.id} wg_id={d.wg_client_id}")
                await _delete_peer_safe(wg_client, d.wg_client_id)
                await session.delete(d)
            await session.commit()

def fmt_human(dt, tz_name: str = "Europe/Moscow") -> str:
    """
    Превращает datetime в вид 'DD.MM.YYYY HH:MM (TZ)'.
    Поддерживает naive/aware. Если dt = None — вернёт '—'.
    """
    if not dt:
        return "—"
    if dt.tzinfo is None:
        # считаем, что в базе время хранится в UTC (если иначе — поправь тут)
        dt = dt.replace(tzinfo=timezone.utc)
    tz = ZoneInfo(tz_name)
    local = dt.astimezone(tz)
    return local.strftime("%d.%m.%Y %H:%M")

def kb(rows: list[list[InlineKeyboardButton]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(rows)

def back_to_main() -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton("⬅️ Назад", callback_data="menu:main")]

def back_to_admin() -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton("⬅️ Назад", callback_data="menu:admin")]

def _period_bounds(kind: str) -> tuple[datetime | None, datetime]:
    now = datetime.now(timezone.utc)
    if kind == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0), now
    if kind == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0), now
    if kind == "year":
        return now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0), now
    return None, now  # all

def _payments_period_title(kind: str) -> str:
    return {
        "today": "сегодня",
        "month": "за месяц",
        "year": "за год",
        "all": "за всё время",
    }.get(kind, "за всё время")

def _payments_kbd() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📅 Сегодня", callback_data="admin:payments:period:today"),
            InlineKeyboardButton("📅 Месяц", callback_data="admin:payments:period:month"),
        ],
        [
            InlineKeyboardButton("📆 Год", callback_data="admin:payments:period:year"),
            InlineKeyboardButton("📅 Всё время", callback_data="admin:payments:period:all"),
        ],
        [InlineKeyboardButton("⬅️ Назад", callback_data="admin:payments_list")],
    ])

async def require_admin(update, session) -> bool:
    me = (await session.execute(select(User).where(User.tg_id == update.effective_user.id))).scalar_one_or_none()
    return bool(me and me.is_admin)

async def get_user_by_id(session, uid: int) -> User | None:
    return (await session.execute(select(User).where(User.id == uid))).scalar_one_or_none()

async def build_user_card(uid: int, show_devices: bool):
    async with async_session() as session:
        u = await get_user_by_id(session, uid)
        if not u:
            return "Пользователь не найден.", InlineKeyboardMarkup([back_to_admin()])

        now = datetime.now(timezone.utc)
        total_quota = max(0, int(u.total_quota() or 0))

        used = (await session.execute(
            select(func.count(Device.id)).where(Device.user_id == u.id)
        )).scalar_one() or 0

        free = max(0, total_quota - int(used))

        base_active  = bool(u.subscription_until and u.subscription_until > now)
        extra_active = bool(u.extra_devices_until and u.extra_devices_until > now)
        cnt = int(u.extra_devices_count or 0)

        title = f"{(u.first_name or '').strip()} {(u.last_name or '').strip()}".strip() or "Пользователь"
        handle = f"@{u.username}" if u.username else "—"

        text = (
            f"👤 *{title}*\n"
            f"ID: `{u.tg_id}`  |  {handle}\n\n"
            f"💳 *Подписка:* {'активна до ' + fmt_human(u.subscription_until) if base_active else 'не активна'}\n"
            f"➕ *Доп. слоты:* "
            f"{('активны до ' + fmt_human(u.extra_devices_until) + f' (x{cnt})') if extra_active and cnt>0 else 'нет'}\n\n"
            f"🖥 *Устройства:* {used} использовано / {total_quota} всего • свободно: {free}\n"
            f"📅 *Регистрация:* {fmt_human(getattr(u, 'created_at', None))}\n"
        )

        if show_devices:
            devices = (await session.execute(
                select(Device).where(Device.user_id == u.id).order_by(Device.created_at.desc()).limit(10)
            )).scalars().all()
            if devices:
                lines = []
                for d in devices:
                    status = "✅" if d.enabled else "🚫"
                    kind = "➕доп" if d.is_extra else "💳база"
                    lines.append(f"• `{d.wg_client_name}` {status} • {kind} • {fmt_human(d.created_at)}")
                text += "\n*Последние устройства:*\n" + "\n".join(lines)
            else:
                text += "\n*Последние устройства:* —"

        s = "1" if show_devices else "0"
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("➕ +7",  callback_data=f"admin:card:add_days:{uid}:7:{s}"),
             InlineKeyboardButton("➕ +30", callback_data=f"admin:card:add_days:{uid}:30:{s}"),
             InlineKeyboardButton("➕ +90", callback_data=f"admin:card:add_days:{uid}:90:{s}"),
             InlineKeyboardButton("➕ +365",callback_data=f"admin:card:add_days:{uid}:365:{s}")],
            [InlineKeyboardButton("Квота 1", callback_data=f"admin:card:set_quota:{uid}:1:{s}"),
             InlineKeyboardButton("2",       callback_data=f"admin:card:set_quota:{uid}:2:{s}"),
             InlineKeyboardButton("3",       callback_data=f"admin:card:set_quota:{uid}:3:{s}"),
             InlineKeyboardButton("5",       callback_data=f"admin:card:set_quota:{uid}:5:{s}")],
            [InlineKeyboardButton("❌ Отключить подписку", callback_data=f"admin:card:deactivate:{uid}:{s}")],
            [InlineKeyboardButton("➕ +1 доп", callback_data=f"admin:card:addons_inc:{uid}:{s}"),
             InlineKeyboardButton("➖ −1",     callback_data=f"admin:card:addons_dec:{uid}:{s}")],
            [InlineKeyboardButton("📆 +30 дней доп", callback_data=f"admin:card:addons_extend:{uid}:{s}"),
             InlineKeyboardButton("🧹 Сбросить доп", callback_data=f"admin:card:addons_deact:{uid}:{s}")],
            [InlineKeyboardButton("📋 Показать устройства" if s=="0" else "🔽 Скрыть устройства",
                                  callback_data=f"admin:card:toggle_devices:{uid}:{s}")],
            [InlineKeyboardButton("💬 Написать", url=f"tg://user?id={u.tg_id}")],
            back_to_admin()
        ])
        return text, kb

async def render_user_card_view(query, uid: int, show_devices: bool):
    text, kb = await build_user_card(uid, show_devices)
    await query.edit_message_text(text, reply_markup=kb)

async def render_user_card_message(chat_id: int, u: User, update: Update, context: ContextTypes.DEFAULT_TYPE):
    text, kb = await build_user_card(u.id, show_devices=False)
    await context.bot.send_message(chat_id=chat_id, text=text, reply_markup=kb)

from telegram.constants import ParseMode

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    text_in = (msg.text or "").strip()

    # 1) ждём текст для рассылки?
    if context.user_data.get("await_notify_text"):
        if not text_in:
            await msg.reply_text("Сообщение пустое. Отправьте текст уведомления.")
            return

        scope = context.user_data.get("notify_scope")
        if not scope:
            await msg.reply_text("Не выбрана аудитория. Откройте: 📣 Уведомления.")
            context.user_data.pop("await_notify_text", None)
            return

        # Сохраняем текст и показываем предпросмотр
        context.user_data["notify_text"] = text_in
        scope_h = (
            "активным пользователям" if scope == "active"
            else "неактивным пользователям" if scope == "inactive"
            else "всем пользователям"
        )
        preview = (
            "👀 *Предпросмотр*\n\n"
            f"Будет отправлено *{scope_h}*.\n\n"
            f"{BOT_BROADCAST_HEADER}{text_in}"
        )
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Отправить", callback_data="admin:notify:confirm:send")],
            [InlineKeyboardButton("❌ Отмена",    callback_data="admin:notify:confirm:cancel")],
            [InlineKeyboardButton("⬅️ Назад",     callback_data="admin:notify")]
        ])
        await msg.reply_text(preview, reply_markup=kb, parse_mode=ParseMode.MARKDOWN)
        return

    # 2) иначе — строгий поиск пользователя (@username или ID)
    if context.user_data.get("await_user_search_exact"):
        if not text_in:
            await msg.reply_text("Отправьте @username или ID.")
            return

        is_username = text_in.startswith("@")
        is_id = text_in.isdigit()
        if not (is_username or is_id):
            await msg.reply_text("Нужно отправить *точный* @username (с @) или *числовой* ID.", parse_mode=ParseMode.MARKDOWN)
            return

        async with async_session() as session:
            me = (await session.execute(select(User).where(User.tg_id == update.effective_user.id))).scalar_one_or_none()
            if not me or not me.is_admin:
                await msg.reply_text("❌ Недостаточно прав.")
                return

            if is_username:
                term = text_in[1:].lower()
                q = select(User).where(func.lower(User.username) == term).limit(1)
            else:
                q = select(User).where(User.tg_id == int(text_in)).limit(1)

            u = (await session.execute(q)).scalar_one_or_none()

        if not u:
            await msg.reply_text("Пользователь не найден. Проверьте @username или ID.")
            return

        await render_user_card_message(update.effective_chat.id, u, update, context)
        context.user_data["await_user_search_exact"] = False
        return

from typing import Optional, List, Tuple
async def ensure_user(
    session: AsyncSession,
    tg_id: int,
    username: Optional[str],
    first: Optional[str],
    last: Optional[str],
    ref_code: Optional[str] = None,
) -> Tuple[User, List[Tuple[int, str]]]:
    """
    Создаёт пользователя при первом заходе. Возвращает (user, notices),
    где notices — список уведомлений [(chat_id, text), ...] для рассылки.

    Логика:
    - Если пользователь уже существует:
        * Если он пришёл со ссылкой (ref_code указан), отправим ему вежливое сообщение,
          что рефералка работает только для новых пользователей.
        * Никаких бонусов не начисляем.
    - Если пользователя нет:
        * Создаём, назначаем referral_code.
        * Если ref_code валиден (и не self-ref):
            - Новому пользователю: пробный период settings.ref_trial_days дней,
              + минимум 1 базовый слот (device_quota >= 1).
            - Рефереру: фикс на баланс settings.ref_referrer_fixed_rub рублей.
            - В notices добавляем два сообщения (новому и рефереру).
    Сообщения — plain text (без Markdown/HTML), чтобы 100% дошли.
    """
    notices: List[Tuple[int, str]] = []

    # 1) Ищем пользователя
    res = await session.execute(select(User).where(User.tg_id == tg_id))
    user = res.scalar_one_or_none()

    # --- УЖЕ СУЩЕСТВУЕТ ---
    if user is not None:
        # пришёл по реф-коду, но он уже есть в системе
        if ref_code:
            # Найдём владельца кода (чисто для корректности, но бонусов не будет)
            r = await session.execute(select(User).where(User.referral_code == ref_code))
            owner = r.scalar_one_or_none()
            # Сообщим пользователю, что рефералка только для новых аккаунтов
            # (не важно, чей код — просто вежливо уведомим)
            notices.append((
                tg_id,
                (
                    "ℹ️ Реферальная ссылка\n\n"
                    "Увы, реферальная программа работает только для новых пользователей.\n"
                    "Но вы уже с нами — спасибо! 😊"
                )
            ))
        return user, notices

    # --- НОВЫЙ ПОЛЬЗОВАТЕЛЬ ---
    user = User(
        tg_id=tg_id,
        username=username,
        first_name=first,
        last_name=last,
        is_admin=(tg_id in settings.admin_ids),
        referral_code=gen_ref_code(),
    )
    session.add(user)
    await session.flush()  # нужен user.id

    ref_owner = None
    if ref_code:
        r = await session.execute(select(User).where(User.referral_code == ref_code))
        ref_owner = r.scalar_one_or_none()
        # Защита от self-ref: если код его же, не применять рефералку
        if ref_owner and ref_owner.id == user.id:
            ref_owner = None

    # Если есть валидный владелец кода — применяем бонусы
    if ref_owner:
        # 1) Пробный период другу
        trial_days = int(getattr(settings, "ref_trial_days", 0) or 0)
        if trial_days > 0:
            now = datetime.now(timezone.utc)
            base_from = user.subscription_until or now
            if base_from < now:
                base_from = now
            user.subscription_until = base_from + timedelta(days=trial_days)
            # минимум 1 базовый слот на время триала
            if (user.device_quota or 0) < 1:
                user.device_quota = 1

            until_txt = fmt_human(user.subscription_until) if user.subscription_until else "—"
            # Уведомление новому пользователю
            notices.append((
                tg_id,
                (
                    "🎉 Пробный период активирован!\n"
                    f"Вы получили доступ на {trial_days} дн.\n"
                    f"Действует до: {until_txt}\n\n"
                    "🖥 Доступен 1 бесплатный слот на время пробного периода.\n"
                    "Добавьте устройство в меню «Устройства»."
                )
            ))

        # 2) Фикс на баланс рефереру
        ref_fix = getattr(settings, "ref_referrer_fixed_rub", 0) or 0
        try:
            ref_fix_dec = Decimal(ref_fix)
        except Exception:
            ref_fix_dec = Decimal(0)
        if ref_fix_dec > 0:
            # аккуратно суммируем Decimal
            ref_owner.balance = (Decimal(ref_owner.balance) + ref_fix_dec)
            # красивое имя пришедшего
            who = f"@{username}" if username else (first or "пользователь")
            # Уведомление рефереру
            notices.append((
                ref_owner.tg_id,
                (
                    "💸 Реферальное начисление\n"
                    f"По вашей ссылке зарегистрировался {who}.\n"
                    f"Начислено на баланс: {int(ref_fix_dec)} ₽.\n"
                    "Спасибо, что делитесь сервисом! 🙌"
                )
            ))

        # связка
        user.referred_by_user_id = ref_owner.id

    await session.commit()
    return user, notices

# ---------------------------
# Menus (static parents)
# ---------------------------

def main_menu(user: User) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("💰 Подписки", callback_data="menu:tariffs")],
        [InlineKeyboardButton("🖥 Мои устройства", callback_data="menu:devices")],
        [InlineKeyboardButton("🔗 Реферальная программа", callback_data="menu:ref")],
        [InlineKeyboardButton("❓ Помощь", callback_data="menu:help")]
    ]
    if user.is_admin:
        rows.append([InlineKeyboardButton("⚙️ Админ-панель", callback_data="menu:admin")])
    return kb(rows)

async def list_recipient_ids(session, scope: str) -> list[int]:
    now = datetime.now(timezone.utc)
    if scope == "all":
        q = select(User.tg_id)
    elif scope == "active":
        q = select(User.tg_id).where(active_clause(now))
    else:  # inactive
        q = select(User.tg_id).where(~active_clause(now))
    return [row[0] for row in (await session.execute(q)).all()]

async def count_recipients(session, scope: str) -> int:
    now = datetime.now(timezone.utc)
    if scope == "all":
        q = select(func.count(User.id))
    elif scope == "active":
        q = select(func.count(User.id)).where(active_clause(now))
    elif scope == "inactive":
        q = select(func.count(User.id)).where(~active_clause(now))
    else:
        return 0
    return (await session.execute(q)).scalar_one()

def active_clause(now):
    return or_(
        and_(User.subscription_until.is_not(None), User.subscription_until > now),
        and_(User.extra_devices_until.is_not(None), User.extra_devices_until > now),
    )

def notify_scope_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🟢 Активным",   callback_data="admin:notify:scope:active")],
        [InlineKeyboardButton("⚪️ Неактивным", callback_data="admin:notify:scope:inactive")],
        [InlineKeyboardButton("👥 Всем",       callback_data="admin:notify:scope:all")],
        back_to_admin()
    ])

def admin_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⚙️ Настройки", callback_data="admin:settings")],
        [InlineKeyboardButton("📣 Уведомления", callback_data="admin:notify")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin:users_list")],
        [InlineKeyboardButton("💳 Платежи", callback_data="admin:payments_list")],
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu:main")],
    ]
    return kb(rows)

# ---------------------------
# Commands
# ---------------------------
async def _render_main_menu(query_or_message, tg_user):
    """
    Красивое главное меню со статусом подписки, платными слотами и счётчиками устройств.
    Работает и из callback (edit), и из /start (reply).
    """
    # 1) Берём пользователя и считаем использованные устройства
    async with async_session() as session:
        u = (await session.execute(
            select(User).where(User.tg_id == tg_user.id)
        )).scalar_one()

        used = (await session.execute(
            select(func.count(Device.id)).where(Device.user_id == u.id)
        )).scalar_one()

    # 2) Квоты
    base_q = int(u.device_quota or 0) if _has_base(u) else 0
    extra_q = int(getattr(u, "extra_devices_count", 0) or 0) if _has_extra(u) else 0
    total_q = max(0, base_q + extra_q)

    # 3) Статусы
    sub_line = f"✅ Активна до {fmt_human(u.subscription_until)}" if _has_base(u) else "❌ Нет активной подписки"
    extra_line = (
        f"💳 Платные устройства: {extra_q} (до {fmt_human(getattr(u, 'extra_devices_until', None))})"
        if extra_q > 0 else
        "💳 Платные устройства: нет"
    )
    devices_line = f"🖥 Устройства: {used}/{total_q}  ·  🆓 {base_q}  ·  💳 {extra_q}"

    # 4) Текст
    text = (
        f"👋 Привет, {tg_user.first_name}!\n\n"
        f"📦 Подписка: {sub_line}\n"
        f"{extra_line}\n"
        f"{devices_line}\n"
        f"💰 Баланс: {rub(u.balance)}"
    )

    # 5) Кнопки
    kbd = main_menu(u)

    # 6) Отправляем/редактируем
    if hasattr(query_or_message, "edit_message_text"):  # CallbackQuery
        await query_or_message.edit_message_text(text, reply_markup=kbd)
    else:  # Message
        await query_or_message.reply_text(text, reply_markup=kbd)

async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ref = None
    if context.args and len(context.args) == 1:
        ref = context.args[0]

    async with async_session() as session:
        user, notices = await ensure_user(
            session,
            tg_id=update.effective_user.id,
            username=update.effective_user.username,
            first=update.effective_user.first_name,
            last=update.effective_user.last_name,
            ref_code=ref,
        )

    # разошлём уведомления (если есть)
    for chat_id, text in notices:
        try:
            await context.bot.send_message(chat_id=chat_id, text=text, disable_web_page_preview=True)
        except Exception as e:
            print(f"[referral notice] failed to send to {chat_id}: {e}")

    # рендер главного меню как раньше
    await _render_main_menu(update.effective_message, update.effective_user)

async def admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with async_session() as session:
        res = await session.execute(select(User).where(User.tg_id == update.effective_user.id))
        user = res.scalar_one_or_none()
    if not user or not user.is_admin:
        await update.effective_message.reply_text("Недостаточно прав.")
        return
    await update.effective_message.reply_text("Админ-панель", reply_markup=admin_menu())

# ---------------------------
# Callbacks (hard tree)
# ---------------------------

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data or ""

    # ---- help ----
    if data == "menu:help":
        text = (
            "❓ *Помощь*\n\n"
            "Добро пожаловать в центр поддержки!\n\n"
            "Здесь ты найдёшь:\n"
            "• ответы на частые вопросы,\n"
            "• инструкции по подключению и настройке,\n"
            "• информацию о тарифах и устройствах.\n\n"
            "Выбирай нужный раздел ниже и получай подсказки 👇"
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("📡 Как подключить VPN", callback_data="help:how")],
            [InlineKeyboardButton("🧰 VPN не работает", callback_data="help:troubleshoot")],
            [InlineKeyboardButton("📱 Устройства и лимиты", callback_data="help:devices")],
            [InlineKeyboardButton("➕ Доп. устройства", callback_data="help:addons")],
            [InlineKeyboardButton("💬 Чат поддержки", callback_data="help:support")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu:main")]
        ]), parse_mode="Markdown")
        return

    if data == "help:how":
        text = (
            "📡 *Как подключить VPN*\n\n"
            "1) 💰 *Оформи подписку* — выбери подходящий срок и оплати.\n\n"
            "2) 🖥 *Зайди в «Мои устройства»* — открой раздел в боте.\n\n"
            "3) ➕ *Нажми «Добавить устройство»* — бот создаст конфиг (1 конфиг = 1 устройство).\n"
            "   • Если конфиг *пришёл сообщением* — просто скачай его.\n"
            "   • Если конфиг *не пришёл автоматически* — открой созданное устройство и нажми:\n"
            "     📥 *Скачать конфиг* — файл *.conf* для импорта\n"
            "     🗑 *Удалить* — если устройство больше не нужно\n\n"
            "4) ⚙️ *Установи WireGuard* на своё устройство (iOS/Android/Windows/macOS/Linux).\n\n"
            "5) 📲 *Импортируй конфиг* в WireGuard:\n"
            "   • через файл *.conf* (📥 Импорт из файла),\n"
            "6) 🔌 *Включи туннель* в WireGuard — готово! Интернет пойдёт через VPN.\n\n"
            "ℹ️ Подсказки:\n"
            "• Подписка даёт базовый лимит устройств; доп. устройства покупаются отдельно.\n"
            "• Нужно освободить слот? Удали лишний конфиг и создай новый.\n"
            "• Что-то не работает — смотри раздел «🧰 VPN не работает»."
        )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="menu:help")]]
            ),
            parse_mode="Markdown"
        )
        return

    if data == "help:troubleshoot":
        text = (
            "🧰 *VPN не работает*\n\n"
            "Действуем по шагам — обычно этого достаточно:\n\n"
            "1) 🔐 *Проверь подписку.* Если она не активна, доступ к VPN закрыт.\n"
            "2) 📄 *Обнови конфиг.* Зайди в *🖥 Мои устройства* → выбери устройство → "
            "нажми 📥 *Скачать конфиг* (или 🗑 *Удалить* и ➕ *Добавить устройство* заново).\n"
            "3) 🔁 *Перезапусти VPN и устройство.* Выключи/включи профиль в приложении WireGuard, "
            "затем перезагрузи телефон/компьютер.\n\n"
            "🚫 *Не подключается*\n"
            "• 🌍 Попробуй *другую сеть*: мобильный интернет вместо Wi-Fi или наоборот — "
            "иногда сеть блокирует VPN.\n"
            "• ⏱ Убедись, что на устройстве *включено авто-время и авто-часовой пояс* — "
            "сбитые часы мешают соединению.\n"
            "• 📴 Отключи другие VPN/прокси/блокировщики трафика, если они включены.\n\n"
            "🐢 *Медленно или обрывы*\n"
            "• Переключись между Wi-Fi и мобильной сетью, закрой тяжёлые загрузки и попробуй ещё раз.\n\n"
            "Если проблема осталась — напиши нам в поддержку, мы быстро поможем 💬"
        )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="menu:help")]]
            ),
            parse_mode="Markdown"
        )
        return

    if data == "help:devices":
        text = (
            "📱 *Устройства и лимиты*\n\n"
            "• Лимит «включённых в подписку» устройств зависит от тарифа (1/2/3/5).\n"
            "• Каждый конфиг = 1 устройство.\n"
            "• Конфиги из подписки можно удалять и перевыпускать.\n"
            "• Если *основная подписка заканчивается*, то *все устройства, выданные по подписке, удаляются*.\n"
            "• *Доп. устройства* (купленные отдельно) при этом продолжают работать *до окончания их оплаченного срока*.\n"
            "• Купить новые доп. устройства *нельзя*, если подписка не активна."
        )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="menu:help")]]
            ),
            parse_mode="Markdown"
        )
        return

    if data == "help:addons":
        text = (
            "➕ *Доп. устройства*\n\n"
            "• Стоимость: *100 ₽/мес* за 1 устройство.\n"
            "• Купить можно *только при активной* основной подписке.\n"
            "• Все доп. устройства синхронизированы по сроку: первая покупка задаёт «якорь»,\n"
            "  и *все* последующие допы закончатся в *один день* — через ~1 месяц от якоря.\n"
            "• Если основная подписка закончилась, уже купленные доп. устройства *продолжают работать*\n"
            "  до конца своего оплаченного срока, затем *удаляются*.\n"
            "• Пока подписка не активна, *докупать* доп. устройства нельзя."
        )
        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Назад", callback_data="menu:help")]]
            ),
            parse_mode="Markdown"
        )
        return

    if data == "help:support":
        handle = "@AraTop4k"
        text = (
            "💬 *Чат поддержки*\n\n"
            "Нужна помощь или остались вопросы? Мы рядом и ответим максимально быстро.\n\n"
            "👤 *Кому писать:* {handle}\n"
            "✍️ *Что указать в первом сообщении:*\n"
            "• ваш тариф (7/30/90/365)\n"
            "• коротко проблему/вопрос\n"
            "• при необходимости — скрин/ошибку\n\n"
            "Нажмите кнопку ниже, чтобы открыть чат и написать нам прямо сейчас."
        ).format(handle=handle)

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🗨️ Открыть чат", url="https://t.me/AraTop4k")],
                [InlineKeyboardButton("⬅️ Назад", callback_data="menu:help")]
            ])
        )
        return

    # ---- MAIN ----
    if data == "menu:main":
        await _render_main_menu(update.callback_query, update.effective_user)
        return

    # ---- TARIFFS ----

    if data.startswith("menu:tariffs"):
        async with async_session() as session:
            res = await session.execute(
                select(Tariff).where(Tariff.is_active == True).order_by(Tariff.days)
            )
            tariffs = res.scalars().all()

        rows = [[InlineKeyboardButton(
                    f"{t.name} — {rub(t.price)} ({t.max_devices} устр.)",
                    callback_data=f"tariff:buy:{t.id}"
                )] for t in tariffs]
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:main")])

        await query.edit_message_text("🛒 Выберите тариф:", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("tariff:buy:"):
        _, _, sid = data.split(":")
        tariff_id = int(sid)

        async with async_session() as session:
            t = await session.get(Tariff, tariff_id)
            if not t or not t.is_active:
                await query.edit_message_text("❌ Тариф не найден или отключён.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return

            u = (await session.execute(select(User).where(User.tg_id == update.effective_user.id))).scalar_one()

            # Проверка активной подписки
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            if u.subscription_until and u.subscription_until > now:
                until = fmt_human(u.subscription_until)
                await query.edit_message_text(
                    f"❌ У вас уже есть активная подписка до {until}.\n💰 Покупка новой подписки доступна после окончания текущей.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="menu:tariffs")]]),
                )
                return

            try:
                pay = await yk_client.create_payment(
                    float(t.price),
                    settings.currency,
                    f"Оплата тарифа {t.name} ({t.days} дней)",
                    settings.yk_return_url,
                    metadata={"tg_id": update.effective_user.id, "purpose": "TARIFF", "tariff_id": t.id},
                )
            except Exception as e:
                await query.edit_message_text(
                    f"❌ Не удалось создать платёж: {e}",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="menu:tariffs")], back_to_main()]),
                )
                return

            confirmation_url = (pay.get("confirmation") or {}).get("confirmation_url")
            if not confirmation_url:
                await query.edit_message_text(
                    "❌ Платёж создан, но платёжная ссылка не пришла. Попробуйте ещё раз позже.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="menu:tariffs")], back_to_main()]),
                )
                return

            # Создаем платеж в БД
            p = Payment(
                yk_payment_id=pay["id"],
                user_id=u.id,
                status=pay.get("status", "pending"),
                purpose="TARIFF",
                amount=float(t.price),
                currency=settings.currency,
                tariff_id=t.id,
                confirmation_url=confirmation_url,
                meta=pay.get("metadata") or {},
            )
            session.add(p)
            await session.commit()

            from datetime import datetime, timedelta
            end_date = datetime.now() + timedelta(days=t.days)
            formatted_date = end_date.strftime("%d.%m.%Y")
            # Красивый чек-лист
            check_list_text = f"""
🎯 ДЕТАЛИ ВАШЕГО ЗАКАЗА

✨ Тариф: {t.name}
⏳ Срок действия: {t.days} дней (до {formatted_date})
📱 Устройства: до {t.max_devices} шт.

💎 Преимущества тарифа:
• 🚀 Высокоскоростные VPN-серверы
• 🛡️ 100% защита данных и анонимность
• 📶 Стабильное соединение без разрывов
• 🔒 Сквозное шифрование трафика
• 🚫 Блокировка рекламы и трекеров
• 🆘 Круглосуточная поддержка

💰 Стоимость: {rub(t.price)}
⏰ Счет действителен: 10 минут

📝 Условия:
• Оплата через безопасный шлюз
• Мгновенная активация после оплаты

После успешной оплаты подписка активируется автоматически 🎉

💫 Спасибо, что выбираете нас!
"""

            # Кнопки
            rows = []
            rows.append([InlineKeyboardButton("💳 Оплатить картой", url=confirmation_url)])
            
            if float(u.balance) >= t.price:
                rows.append([InlineKeyboardButton("💳 Оплатить балансом", callback_data=f"paybalance:TARIFF:{t.id}")])
            
            rows.append([InlineKeyboardButton("⬅️ К тарифам", callback_data="menu:tariffs")])
            rows.append([InlineKeyboardButton("🏠 Главное меню", callback_data="menu:main")])

            if u.id in user_payment_tasks:
                old_task = user_payment_tasks[u.id]
                old_task.cancel()  # Отменяем старую задачу
                try:
                    await old_task  # Ждем завершения
                except asyncio.CancelledError:
                    print(f"⏹️ Предыдущая задача для user {u.id} отменена")
                except Exception as e:
                    print(f"⚠️ Ошибка при отмене предыдущей задачи: {e}")
            # Запускаем проверку платежа
            user_payment_tasks[u.id] = asyncio.create_task(auto_check_payment(query, pay["id"], u.id, yk_client))

            await query.edit_message_text(
                check_list_text,
                reply_markup=InlineKeyboardMarkup(rows),
                parse_mode="Markdown"
            )
            return

    async def _render_devices_menu(query, user_id: int):
        # 1) Достаём пользователя и его устройства
        async with async_session() as session:
            u = (await session.execute(
                select(M.User).where(M.User.tg_id == user_id)
            )).scalar_one()

            res = await session.execute(
                select(M.Device).where(M.Device.user_id == u.id)
            )
            devices = list(res.scalars().all())

        # 2) Считаем квоты и использованные слоты
        base_q = _base_quota(u)
        extra_q = _extra_quota(u)
        total_q = base_q + extra_q

        # стабильный порядок — по дате создания
        devices.sort(key=lambda d: d.created_at or datetime.min.replace(tzinfo=timezone.utc))

        used_total = len(devices)
        used_base = min(used_total, base_q)
        used_paid = max(0, used_total - used_base)

        # 3) Текстовая шапка без подписок — только про устройства
        if used_total == 0:
            header = (
                "🖥 Устройства\n\n"
                f"🆓 Бесплатные занято: 0/{base_q}\n"
                f"💳 Платные занято: 0/{extra_q}\n"
                f"📈 Всего: 0/{total_q}\n\n"
                "Пока устройств нет."
            )
        else:
            header = (
                "🖥 Устройства\n\n"
                f"🆓 Бесплатные занято: {used_base}/{base_q}\n"
                f"💳 Платные занято: {used_paid}/{extra_q}\n"
                f"📈 Всего: {used_total}/{total_q}"
            )

        # 4) Кнопки устройств — каждую явно помечаем
        rows: list[list[InlineKeyboardButton]] = []
        for idx, d in enumerate(devices, start=1):
            is_paid = idx > base_q  # всё, что выходит за базовую квоту — платное
            icon = "💳" if is_paid else "🆓"
            title = f"{icon} {d.wg_client_name}"
            rows.append([InlineKeyboardButton(title, callback_data=f"device:view:{d.id}")])

        # 5) Действия
        rows.append([InlineKeyboardButton("➕ Добавить устройство", callback_data="device:add")])
        rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:main")])

        await query.edit_message_text(header, reply_markup=InlineKeyboardMarkup(rows))


    if data == "menu:devices":
        await _render_devices_menu(query, update.effective_user.id)
        return

    if data == "device:add":
        async with async_session() as session:
            u: M.User = (await session.execute(select(M.User).where(M.User.tg_id == update.effective_user.id))).scalar_one()

            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            base_active = bool(u.subscription_until and u.subscription_until > now)
            if not base_active:
                await query.edit_message_text(
                    "❌ Сначала оформите подписку. \n🖥 Доп. устройства можно покупать только при активной подписке.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("💰 К подпискам", callback_data="menu:tariffs")],
                                    [InlineKeyboardButton("🏠 Меню", callback_data="menu:main")]]),
                )
                return

            # посчитаем текущее кол-во базовых и доп.
            base_count = (await session.execute(
                select(func.count(M.Device.id)).where(M.Device.user_id == u.id, M.Device.is_extra == False)
            )).scalar_one()
            extra_count = (await session.execute(
                select(func.count(M.Device.id)).where(M.Device.user_id == u.id, M.Device.is_extra == True)
            )).scalar_one()

            base_quota = u.device_quota or 0
            extra_active_count = (u.extra_devices_count or 0) if (u.extra_devices_until and u.extra_devices_until > now) else 0

            # Решаем, куда будет относиться новое устройство
            if base_count < base_quota:
                # создаём базовое устройство
                node = await pick_best_node(session)
                if not node:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Нет доступных серверов для создания устройства. \n Обратитесь в поддержку")
                    return

                name = f"user{u.id}-{base_count+1}"
                peer = await wg_client.create_client(name=name)

                d = M.Device(
                    user_id=u.id,
                    wg_client_id=peer.get("id"),
                    wg_client_name=name,
                    is_extra=False,
                    node_id=node.id
                )
                session.add(d)
                node.load += 1
                await session.commit()

                # отсылаем конфиг
                try:
                    cfg = await wg_client.get_config(d.wg_client_id)
                    bio = io.BytesIO(cfg.encode("utf-8"))
                    bio.name = f"{d.wg_client_name}.conf"
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(bio))
                except Exception:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Устройство создано, но конфиг не получен. Откройте WG-Easy UI.")

                await _render_devices_menu(query, update.effective_user.id)
                return

            # базовая квота забита -> можно ли создать доп?
            if extra_count < extra_active_count:
                # есть оплаченный слот доп. устройства -> создаём доп
                name = f"user{u.id}-extra{extra_count+1}"
                peer = await wg_client.create_client(name=name)

                d = M.Device(
                    user_id=u.id,
                    wg_client_id=str(peer.get("id") or peer.get("clientId") or peer.get("_id")),
                    wg_client_name=peer.get("name", name),
                    is_extra=True,
                )
                session.add(d)
                await session.commit()

                try:
                    cfg = await wg_client.get_config(d.wg_client_id)
                    bio = io.BytesIO(cfg.encode("utf-8"))
                    bio.name = f"{d.wg_client_name}.conf"
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(bio))
                except Exception:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text="⚠️ Устройство создано, но конфиг не получен. Откройте WG-Easy UI.")

                await _render_devices_menu(query, update.effective_user.id)
                return

            # нет свободных слотов доп. устройств — предлагаем купить
            from decimal import Decimal
            price = Decimal(settings.device_extra_price)
            pay = await yk_client.create_payment(
                float(price), settings.currency, "Покупка доп. устройства (1 мес.)", settings.yk_return_url,
                metadata={"tg_id": update.effective_user.id, "purpose": "EXTRA_DEVICE"}
            )
            p = M.Payment(
                yk_payment_id=pay["id"],
                user_id=u.id,
                status=pay.get("status", "pending"),
                purpose="EXTRA_DEVICE",
                amount=float(price),
                currency=settings.currency,
                confirmation_url=pay.get("confirmation", {}).get("confirmation_url", ""),
            )
            session.add(p)
            await session.commit()

            rows = []
            # Если хватает баланса — добавляем кнопку "Оплатить балансом"
            # Кнопка YooKassa (основная)
            rows.append([InlineKeyboardButton("💳 Оплатить", url=p.confirmation_url)])

            if Decimal(u.balance) >= price:
                rows.append([InlineKeyboardButton("💳 Оплатить балансом", callback_data="paybalance:EXTRA_DEVICE:-")])

            if u.id in user_payment_tasks:
                old_task = user_payment_tasks[u.id]
                old_task.cancel()  # Отменяем старую задачу
                try:
                    await old_task  # Ждем завершения
                except asyncio.CancelledError:
                    print(f"⏹️ Предыдущая задача для user {u.id} отменена")
                except Exception as e:
                    print(f"⚠️ Ошибка при отмене предыдущей задачи: {e}")

            # Остальные кнопки
            user_payment_tasks[u.id] = asyncio.create_task(auto_check_payment(query, pay["id"], u.id, yk_client))
            rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:devices")])
            rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:main")])

            await query.edit_message_text(
                (
                    "🔓 Дополнительных слотов нет.\n"
                    f"➕ Купите новый слот для 1 устройства за {rub(price)}.\n\n"
                    "⏳ Срок действия: 30 дней.\n"
                    "⏰ Счет действителен: 10 минут\n"
                    "💡 Важно: все купленные доп-слоты имеют _общий_ срок действия. "
                    "Даже если вы купите несколько слотов в разные дни, они истекут одновременно — "
                    "по единой дате «платных слотов» в профиле.\n\n"
                    "⚠️ Если подписка закончится, устройства будут удалены, "
                    "а срок доп-слотов продолжит идти."
                ),
                reply_markup=InlineKeyboardMarkup(rows),
                parse_mode="Markdown",
            )
            return

    if data.startswith("device:view:"):
        try:
            await query.answer()
        except Exception:
            pass
        _, _, sid = data.split(":")
        dev_id = int(sid)
        async with async_session() as session:
            d = await session.get(M.Device, dev_id)
        if not d:
            await query.edit_message_text("❌ Устройство не найдено.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
            return
        rows = [
            [InlineKeyboardButton("📥 Скачать конфиг", callback_data=f"device:cfg:{dev_id}")],
            [InlineKeyboardButton("🗑 Удалить", callback_data=f"device:del:{dev_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu:devices")],
        ]
        await query.edit_message_text(f"🖥 Устройство: {d.wg_client_name}", reply_markup=InlineKeyboardMarkup(rows))
        return

    if data.startswith("device:cfg:"):
        _, _, sid = data.split(":")
        dev_id = int(sid)
        async with async_session() as session:
            d = await session.get(M.Device, dev_id)
            if not d:
                await query.edit_message_text("Устройство не найдено.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return
            try:
                cfg = await wg_client.get_config(d.wg_client_id)
                bio = io.BytesIO(cfg.encode("utf-8")); bio.name = f"{d.wg_client_name}.conf"
                await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(bio))
            except WGEasyError as e:
                # Если в тексте есть 404 — значит peer уже удалён в UI
                if " 404:" in str(e) or "Cannot find" in str(e):
                    await context.bot.send_message(chat_id=update.effective_chat.id,
                                                text="Пир отсутствует в WG-Easy. Удаляю запись из базы…")
                    await session.delete(d)
                    await session.commit()
                    # Вернёмся к списку устройств
                    await _render_devices_menu(query, update.effective_user.id)
                    return
                # Любая другая ошибка
                await context.bot.send_message(chat_id=update.effective_chat.id, text=f"❌ Не удалось получить конфиг: {e}")
            # остаёмся на экране устройства или обновим меню
            await _render_devices_menu(query, update.effective_user.id)
            return

    if data.startswith("device:del:"):
        try:
            await query.answer()
        except Exception:
            pass

        _, _, sid = data.split(":")
        dev_id = int(sid)

        async with async_session() as session:
            d = await session.get(M.Device, dev_id)
            if not d:
                await query.edit_message_text(
                    "❌ Устройство не найдено.",
                    reply_markup=InlineKeyboardMarkup([back_to_main()])
                )
                return

            # Берем ноду устройства
            if d.node_id:
                node = await session.get(M.Node, d.node_id)
            else:
                node = None

            if d.wg_client_id and node:
                node_client = WGEasyClient(node.api_url, node.api_password)
                try:
                    await node_client.delete_client(d.wg_client_id)
                    # уменьшаем нагрузку на сервер
                    node.load = max(0, node.load - 1)
                except Exception as e:
                    await query.edit_message_text(
                        f"WG API ошибка при удалении: {e}",
                        reply_markup=InlineKeyboardMarkup([back_to_main()])
                    )
                    return
                finally:
                    # обязательно закрываем сессию
                    await node_client.close()

            await session.delete(d)
            await session.commit()

        await query.edit_message_text(
            "✅ Устройство удалено.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🖥 К устройствам", callback_data="menu:devices")],
                back_to_main()
            ]),
        )
        return
    # ---- PAY BY BALANCE ----
    if data.startswith("paybalance:"):
        # форматы: paybalance:TARIFF:<tariff_id>  или  paybalance:EXTRA_DEVICE:-
        _, purpose, sid = data.split(":")
        async with async_session() as session:
            u = (await session.execute(
                select(User).where(User.tg_id == update.effective_user.id)
            )).scalar_one()

            from decimal import Decimal
            from datetime import datetime, timezone

            if purpose == "TARIFF":
                tariff_id = int(sid)
                t = await session.get(Tariff, tariff_id)
                if not t:
                    await query.edit_message_text("❌ Тариф не найден.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                    return
                price = Decimal(str(t.price))
                if Decimal(u.balance) < price:
                    await query.edit_message_text("❌ Недостаточно средств на балансе.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                    return

                # списываем
                u.balance = (Decimal(u.balance) - price)

                # создаём внутренний платеж (succeeded)
                p = Payment(
                    yk_payment_id=None,
                    user_id=u.id,
                    status="succeeded",
                    purpose="TARIFF",
                    amount=float(price),
                    currency=settings.currency,
                    tariff_id=t.id,
                    confirmation_url=None,
                    meta={"paid_by_balance": True, "used_balance": float(price)},
                )
                session.add(p)
                await session.commit()

                # применяем право
                await cancel_user_payment_check(u.id)
                await _apply_successful_payment(session, p)
                await session.commit()

                await query.edit_message_text(
                    "✅ Подписка активирована (оплачено балансом)",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 В меню", callback_data="menu:main")]])
                )
                return

            if purpose == "EXTRA_DEVICE":
                price = Decimal(str(settings.device_extra_price))
                if Decimal(u.balance) < price:
                    await query.edit_message_text("❌ Недостаточно средств на балансе.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                    return

                # списываем
                u.balance = (Decimal(u.balance) - price)

                p = Payment(
                    yk_payment_id=None,
                    user_id=u.id,
                    status="succeeded",
                    purpose="EXTRA_DEVICE",
                    amount=float(price),
                    currency=settings.currency,
                    tariff_id=None,
                    confirmation_url=None,
                    meta={"paid_by_balance": True, "used_balance": float(price)},
                )
                session.add(p)
                await session.commit()
                
                # В обработчике оплаты балансом
                await cancel_user_payment_check(u.id)
                await _apply_successful_payment(session, p)
                await session.commit()

                await query.edit_message_text(
                    "✅ Доп. слот активирован (оплачено балансом)",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥 К устройствам", callback_data="menu:devices")]])
                )
                return
    # ---- REF ----
    if data == "menu:ref":
        async with async_session() as session:
            u = (await session.execute(select(User).where(User.tg_id == update.effective_user.id))).scalar_one()
        me = await context.bot.get_me()
        deep = f"https://t.me/{me.username}?start={u.referral_code}"

        trial = settings.ref_trial_days
        ref_fix = settings.ref_referrer_fixed_rub

        txt = (
            "🎁 Реферальная программа\n\n"
            f"• Дай другу ссылку: {deep}\n"
            f"• Новый пользователь получает пробный доступ на {trial} дн. (авто-активация)\n"
            f"• Ты получаешь {ref_fix} ₽ на баланс сразу\n\n"
            "Баланс можно использовать для оплаты подписки и доп-устройств.\n"
            "Если баланса достаточно — появится кнопка «Оплатить балансом»."
        )
        await query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup([back_to_main()]))
        return

    # ---- ADMIN ----

    if data == "menu:admin":
        # проверим права
        async with async_session() as session:
            res = await session.execute(select(User).where(User.tg_id == update.effective_user.id))
            user = res.scalar_one_or_none()
        if not user or not user.is_admin:
            await query.edit_message_text("❌ Недостаточно прав.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
            return
        await query.edit_message_text("🛠 Админ-панель", reply_markup=admin_menu())
        return

    if data == "admin:notify":
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.edit_message_text("❌ Недостаточно прав.", reply_markup=InlineKeyboardMarkup([back_to_admin()]))
                return
            # сброс состояния
            context.user_data.pop("notify_scope", None)
            context.user_data.pop("await_notify_text", None)
            context.user_data.pop("notify_text", None)

        text = (
            "📣 Уведомления\n\n"
            "Выберите аудиторию ниже."
        )
        await query.edit_message_text(text, reply_markup=notify_scope_kb())
        return

    # 2) Выбор аудитории
    if data.startswith("admin:notify:scope:"):
        scope = data.split(":")[3]  # active | inactive | all
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            n = await count_recipients(session, scope)

        context.user_data["notify_scope"] = scope
        context.user_data["await_notify_text"] = True
        scope_h = "Активные" if scope == "active" else ("Неактивные" if scope == "inactive" else "Все пользователи")
        text = (
            "✍️ Текст уведомления\n\n"
            f"Аудитория: {scope_h} (получателей: {n})\n\n"
            "Отправьте сообщение одним текстом (Markdown разрешён). "
            "Шапка «📣 Сообщение от VPN-сервиса» будет добавлена автоматически."
        )
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="admin:notify")]]))
        return

    # 3) Подтверждение отправки (после предпросмотра)
    if data.startswith("admin:notify:confirm:"):
        # admin:notify:confirm:<send|cancel>
        action = data.split(":")[3]
        scope = context.user_data.get("notify_scope")
        notify_text = context.user_data.get("notify_text")
        if action == "cancel":
            # сброс
            context.user_data.pop("await_notify_text", None)
            context.user_data.pop("notify_text", None)
            text = "🚫 Отправка отменена."
            await query.edit_message_text(text, reply_markup=notify_scope_kb())
            return

        if action == "send":
            if not (scope and notify_text):
                await query.answer("Нет данных для отправки.", show_alert=True); return

            # берём список получателей и шлём
            async with async_session() as session:
                if not await require_admin(update, session):
                    await query.answer("Нет прав", show_alert=True); return
                ids = await list_recipient_ids(session, scope)

            sent = 0
            failed = 0
            header = BOT_BROADCAST_HEADER
            full_text = f"{header}{notify_text}"

            # аккуратно шлём, уважая rate-limit
            for tg_id in ids:
                try:
                    await context.bot.send_message(tg_id, full_text)
                    sent += 1
                except Exception:
                    failed += 1
                await asyncio.sleep(0.05)  # лёгкий троттлинг

            # сброс состояния
            context.user_data.pop("await_notify_text", None)
            context.user_data.pop("notify_text", None)

            result = (
                "✅ *Рассылка завершена*\n\n"
                f"Отправлено: *{sent}*\n"
                f"Не доставлено: *{failed}*"
            )
            await query.edit_message_text(result, reply_markup=notify_scope_kb())
            return

    if data == "admin:users_list":
        # кнопки действий
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Поиск пользователя", callback_data="admin:users")],
            back_to_admin()
        ])

        await query.edit_message_text("👥 Пользователи", reply_markup=kb)
        return

    if data == "admin:users":
        async with async_session() as session:
            res = await session.execute(select(User).where(User.tg_id == update.effective_user.id))
            user = res.scalar_one_or_none()
            if not user or not user.is_admin:
                await query.edit_message_text("❌ Недостаточно прав.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return

        text = (
            "👥 *Пользователи*\n\n"
            "Отправьте *точный* `@username` (с @) *или* числовой *ID* пользователя.\n"
            "Примеры: `@vasya` или `123456789`."
        )
        context.user_data["await_user_search_exact"] = True
        await query.edit_message_text(
            text
        )
        return

    # открыть карточку профиля из любого места
    if data.startswith("admin:user:"):
        uid = int(data.split(":")[2])
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.edit_message_text("❌ Недостаточно прав.", reply_markup=InlineKeyboardMarkup([back_to_admin()]))
                return
        await render_user_card_view(query, uid, show_devices=False)
        return

    # показать/скрыть список устройств в самой карточке
    if data.startswith("admin:card:toggle_devices:"):
        _, _, _, uid, state = data.split(":")
        await render_user_card_view(query, int(uid), show_devices=(state == "0"))
        return

    # продлить базовую подписку
    if data.startswith("admin:card:add_days:"):
        _, _, _, uid, days, state = data.split(":")
        uid, days, show = int(uid), int(days), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc)
            start = u.subscription_until if (u.subscription_until and u.subscription_until > now) else now
            u.subscription_until = start + timedelta(days=days)
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # установить квоту
    if data.startswith("admin:card:set_quota:"):
        _, _, _, uid, quota, state = data.split(":")
        uid, quota, show = int(uid), int(quota), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            u.device_quota = max(0, quota)
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # отключить базовую подписку
    if data.startswith("admin:card:deactivate:"):
        _, _, _, uid, state = data.split(":")
        uid, show = int(uid), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            u.subscription_until = None
            u.device_quota = 0
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # +1 доп-слот (только при активной базе)
    if data.startswith("admin:card:addons_inc:"):
        _, _, _, uid, state = data.split(":")
        uid, show = int(uid), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc)
            base_active = bool(u.subscription_until and u.subscription_until > now)
            if not base_active:
                await query.answer("База не активна — доп. слоты нельзя выдать.", show_alert=True)
                await render_user_card_view(query, uid, show_devices=show)
                return
            u.extra_devices_count = max(0, int(u.extra_devices_count or 0) + 1)
            if not (u.extra_devices_until and u.extra_devices_until > now):
                u.extra_devices_until = now + timedelta(days=30)
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # -1 доп-слот
    if data.startswith("admin:card:addons_dec:"):
        _, _, _, uid, state = data.split(":")
        uid, show = int(uid), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            u.extra_devices_count = max(0, int(u.extra_devices_count or 0) - 1)
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # продлить доп-слоты на 30 дней
    if data.startswith("admin:card:addons_extend:"):
        _, _, _, uid, state = data.split(":")
        uid, show = int(uid), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            from datetime import datetime, timedelta, timezone
            now = datetime.now(timezone.utc)
            start = u.extra_devices_until if (u.extra_devices_until and u.extra_devices_until > now) else now
            u.extra_devices_until = start + timedelta(days=30)
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # сбросить доп-слоты
    if data.startswith("admin:card:addons_deact:"):
        _, _, _, uid, state = data.split(":")
        uid, show = int(uid), (state == "1")
        async with async_session() as session:
            if not await require_admin(update, session):
                await query.answer("Нет прав", show_alert=True); return
            u = await get_user_by_id(session, uid)
            if not u:
                await query.answer("Пользователь не найден", show_alert=True); return
            u.extra_devices_count = 0
            u.extra_devices_until = None
            await session.commit()
        await render_user_card_view(query, uid, show_devices=show)
        return

    # Переключение периода
    if data.startswith("admin:payments:period:"):
        _, _, _, kind = data.split(":")  # today|month|year|all
        await _render_admin_payments(update.callback_query, update.effective_user.id, kind)
        return

    if data == "admin:payments_list":
        # кнопки действий
        kb = InlineKeyboardMarkup([
            [InlineKeyboardButton("💰 Статистика платежей", callback_data="admin:payments")],
            back_to_admin()
        ])

        await query.edit_message_text("💳 Платежи", reply_markup=kb)
        return

    if data == "admin:payments":
        await _render_admin_payments(update.callback_query, update.effective_user.id, "today")
        return

    if data == "admin:stats":
        # права проверим как и в 'menu:admin'
        async with async_session() as session:
            res = await session.execute(select(User).where(User.tg_id == update.effective_user.id))
            admin = res.scalar_one_or_none()
        if not admin or not admin.is_admin:
            await query.edit_message_text("❌ Недостаточно прав.", reply_markup=InlineKeyboardMarkup([back_to_admin()]))
            return
        from datetime import datetime, timezone
        # сразу покажем сводку + кнопки
        async with async_session() as session:
            now = datetime.now(timezone.utc)

            # все пользователи
            all_users = (await session.execute(select(func.count(User.id)))).scalar_one()

            # активные пользователи: есть базовая подписка ИЛИ активны доп. слоты
            users_active = (await session.execute(
                select(func.count(User.id)).where(
                    or_(
                        and_(User.subscription_until.is_not(None), User.subscription_until > now),
                        and_(User.extra_devices_until.is_not(None), User.extra_devices_until > now),
                    )
                )
            )).scalar_one()

            users_inactive = all_users - users_active

            # активные устройства (enabled = true)
            active_devices = (await session.execute(
                select(func.count(Device.id)).where(Device.enabled.is_(True))
            )).scalar_one()

        text = (
            "📊 *Статистика*\n\n"
            f"👥 Всего пользователей: *{all_users}*\n"
            f"🟢 Активных пользователей: *{users_active}*\n"
            f"⚪️ Неактивных пользователей: *{users_inactive}*\n"
            f"🖥 Активных устройств: *{active_devices}*"
        )

        kb = InlineKeyboardMarkup([
            back_to_admin()
        ])
        await query.edit_message_text(text, reply_markup=kb, parse_mode="Markdown")
        return

    # catch-all
    await query.edit_message_text("Неизвестное действие.", reply_markup=InlineKeyboardMarkup([back_to_main()]))

# ---------------------------
# Background polling (optional)
# ---------------------------

async def _apply_successful_payment(session: AsyncSession, p: Payment) -> None:
    """
    Обновляет состояние пользователя по успешному платежу.
    - TARIFF: продлеваем базовую подписку, выставляем квоту устройств по тарифу.
    - TOPUP: пополняем баланс (если решишь опять использовать).
    - EXTRA_DEVICE: увеличиваем подписку на доп. устройства (помесячно), наращиваем счётчик.
    """
    u = await session.get(User, p.user_id)

    if p.purpose == "TARIFF" and p.tariff_id:
        t = await session.get(Tariff, p.tariff_id)
        now = datetime.now(timezone.utc)

        # продлеваем/включаем базовую подписку
        sub_until = u.subscription_until or now
        if sub_until < now:
            sub_until = now
        sub_until = sub_until + timedelta(days=t.days)
        u.subscription_until = sub_until

        # квота устройств по тарифу
        u.device_quota = t.max_devices

        # реферальный бонус (если используешь)
        if u.referred_by_user_id:
            bonus = (Decimal(p.amount) * Decimal(     # p.amount — float -> Decimal
                getattr(__import__("app.config", fromlist=["settings"]).config.settings, "referral_bonus_percent", 0)
            ) / Decimal(100)).quantize(Decimal("0.01"))
            ref_user = await session.get(User, u.referred_by_user_id)
            ref_user.balance = (Decimal(ref_user.balance or 0) + bonus)

    elif p.purpose == "TOPUP":
        u.balance = (Decimal(u.balance or 0) + Decimal(p.amount))

    elif p.purpose == "EXTRA_DEVICE":
        now = datetime.now(timezone.utc)

        # активный период доп. устройств: +30 дней от текущего конца (или от сейчас, если не активно)
        current_until = u.extra_devices_until or now
        if current_until < now:
            current_until = now
        u.extra_devices_until = current_until + timedelta(days=30)

        # увеличиваем количество оплаченных доп. устройств
        u.extra_devices_count = (u.extra_devices_count or 0) + 1

async def poll_pending_payments(context: ContextTypes.DEFAULT_TYPE):
    async with async_session() as session:
        res = await session.execute(select(Payment).where(Payment.status == "pending").order_by(Payment.created_at).limit(20))
        pending = res.scalars().all()
        for p in pending:
            try:
                info = await yk_client.get_payment(p.yk_payment_id)
            except Exception:
                continue
            status = info.get("status", "pending")
            if status != p.status:
                p.status = status
                p.updated_at = datetime.now(timezone.utc)
                if status == "succeeded":
                    await _apply_successful_payment(session, p)
        await session.commit()

# ---------------------------
# Registration
# ---------------------------
from telegram.ext import MessageHandler, filters
def register_handlers(app: Application):
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(CallbackQueryHandler(on_callback))
