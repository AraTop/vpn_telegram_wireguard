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
from sqlalchemy import select, func, and_, or_
from sqlalchemy.ext.asyncio import AsyncSession
from app.config import settings
from app.database import async_session
import app.models as M
from app.models import User, Tariff, Device, Payment
from app.utils import rub, gen_ref_code
from app.wg_api import WGEasyClient
from app.payments import YooKassaClient
from app.wg_api import WGEasyError
from zoneinfo import ZoneInfo
# === singletons ===
wg_client = WGEasyClient(settings.wg_url, settings.wg_password)
yk_client = YooKassaClient(settings.yk_shop_id, settings.yk_secret_key)

# ---------------------------
# Small helpers (no stack)
# ---------------------------

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

import html

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
        "EXTRA_DEVICE": "🧩 Доп. устройства",
        "TOPUP": "💳 Пополнения",
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
        [InlineKeyboardButton("⬅️ Назад", callback_data="menu:admin")],
    ])

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
        [InlineKeyboardButton("🔗 Реферальная программа", callback_data="menu:ref")]
    ]
    if user.is_admin:
        rows.append([InlineKeyboardButton("⚙️ Админ-панель", callback_data="menu:admin")])
    return kb(rows)

def admin_menu() -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("⚙️ Настройки", callback_data="admin:settings")],
        [InlineKeyboardButton("📊 Статистика", callback_data="admin:stats")],
        [InlineKeyboardButton("👥 Пользователи", callback_data="admin:users")],
        [InlineKeyboardButton("💳 Платежи", callback_data="admin:payments")],
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
        f"💳 Платные слоты: {extra_q} (до {fmt_human(getattr(u, 'extra_devices_until', None))})"
        if extra_q > 0 else
        "💳 Платные слоты: нет"
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

async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.effective_message.reply_text(
        "Команды:\n"
        "/start — главное меню\n"
        "/help — помощь\n"
        "/admin — админка (для администраторов)",
        reply_markup=kb([[InlineKeyboardButton("Открыть меню", callback_data="menu:main")]]),
    )

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

        # 1) достаём тариф и пользователя
        async with async_session() as session:
            t = await session.get(Tariff, tariff_id)
            if not t or not t.is_active:
                await query.edit_message_text("❌ Тариф не найден или отключён.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return

            u = (await session.execute(select(User).where(User.tg_id == update.effective_user.id))).scalar_one()

            # 2) запрет повторной покупки, пока действующая подписка не кончилась
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)
            if u.subscription_until and u.subscription_until > now:
                until= fmt_human(u.subscription_until)
                #until = u.subscription_until.astimezone(timezone.utc).strftime("%d-%m-%Y %H:%M")
                await query.edit_message_text(
                    f"❌ У вас уже есть активная подписка до {until}. \n💰 Покупка новой подписки доступна после окончания текущей.",
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
                    "Платёж создан, но платёжная ссылка не пришла. Попробуйте ещё раз позже.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Назад", callback_data="menu:tariffs")], back_to_main()]),
                )
                return

            # 4) пишем платёж в БД
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
            price = float(t.price)
            rows = [
                [InlineKeyboardButton("💳 Оплатить", url=confirmation_url)],
            ]
            if float(u.balance) >= price:
                rows.append([InlineKeyboardButton("💳 Оплатить балансом", callback_data=f"paybalance:TARIFF:{t.id}")])

            rows.append([InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"paycheck:{pay['id']}")])
            rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:tariffs")])
        # 5) показываем кнопки только после успешного создания платежа
        await query.edit_message_text(
            f"🧾 Счёт создан на {rub(t.price)}.\n💳 Нажмите кнопку для оплаты.",
            reply_markup=InlineKeyboardMarkup(rows),
        )
        return

    if data.startswith("paycheck:"):
        _, payment_id = data.split(":")
        async with async_session() as session:
            p = (await session.execute(select(Payment).where(Payment.yk_payment_id == payment_id))).scalar_one_or_none()
            if not p:
                await query.edit_message_text("❌ Платёж не найден.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return

            try:
                info = await yk_client.get_payment(payment_id)
            except Exception as e:
                await query.edit_message_text(f"❌ Ошибка запроса статуса: {e}", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return

            status = info.get("status", "pending")
            p.status = status
            from datetime import datetime, timezone
            p.updated_at = datetime.now(timezone.utc)

            if status == "succeeded":
                # применяем покупку
                await _apply_successful_payment(session, p)
                await session.commit()
                await query.edit_message_text(
                    "✅ Оплата прошла успешно ",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Открыть меню", callback_data="menu:main")]]),
                )
                return

            await session.commit()

        # если ещё не подтвердилось
        rows = [
            [InlineKeyboardButton("🔄 Проверить ещё раз", callback_data=f"paycheck:{payment_id}")],
            [InlineKeyboardButton("⬅️ Назад", callback_data="menu:devices")],
        ]
        await query.edit_message_text("❌ Платёж пока не подтверждён. Попробуйте позже.", reply_markup=InlineKeyboardMarkup(rows))
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
                name = f"user{u.id}-{base_count+1}"
                peer = await wg_client.create_client(name=name)

                d = M.Device(
                    user_id=u.id,
                    wg_client_id=str(peer.get("id") or peer.get("clientId") or peer.get("_id")),
                    wg_client_name=peer.get("name", name),
                    is_extra=False,
                )
                session.add(d)
                await session.commit()

                # отсылаем конфиг
                try:
                    cfg = await wg_client.get_config(d.wg_client_id)
                    bio = io.BytesIO(cfg.encode("utf-8"))
                    bio.name = f"{d.wg_client_name}.conf"
                    await context.bot.send_document(chat_id=update.effective_chat.id, document=InputFile(bio))
                except Exception:
                    await context.bot.send_message(chat_id=update.effective_chat.id, text="Устройство создано, но конфиг не получен. Откройте WG-Easy UI.")

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
                    await context.bot.send_message(chat_id=update.effective_chat.id, text="Устройство создано, но конфиг не получен. Откройте WG-Easy UI.")

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

            # Остальные кнопки
            rows.append([InlineKeyboardButton("🔄 Проверить оплату", callback_data=f"paycheck:{p.yk_payment_id}")])
            rows.append([InlineKeyboardButton("⬅️ Назад", callback_data="menu:devices")])
            rows.append([InlineKeyboardButton("🏠 Меню", callback_data="menu:main")])

            await query.edit_message_text(
                (
                    "🔓 Дополнительных слотов нет.\n"
                    f"➕ Купите новый слот для 1 устройства за {rub(price)}.\n\n"
                    "⏳ *Срок действия:* 30 дней.\n"
                    "💡 *Важно:* все купленные доп-слоты имеют _общий_ срок действия. "
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
                await query.edit_message_text("❌ Устройство не найдено.", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                return
            if d.wg_client_id:
                try:
                    await wg_client.delete_client(d.wg_client_id)
                except Exception as e:
                    await query.edit_message_text(f"WG API ошибка: {e}", reply_markup=InlineKeyboardMarkup([back_to_main()]))
                    return
            await session.delete(d)
            await session.commit()

        await query.edit_message_text(
            "✅ Устройство удалено.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🖥 К устройствам", callback_data="menu:devices")], back_to_main()]),
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

                await _apply_successful_payment(session, p)
                await session.commit()

                await query.edit_message_text(
                    "✅ Доп. слот активирован (оплачено балансом)",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("К устройствам", callback_data="menu:devices")]])
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

    # Переключение периода
    if data.startswith("admin:payments:period:"):
        _, _, _, kind = data.split(":")  # today|month|year|all
        await _render_admin_payments(update.callback_query, update.effective_user.id, kind)
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
        await query.edit_message_text(text, reply_markup=InlineKeyboardMarkup, parse_mode="Markdown")
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

def register_handlers(app: Application):
    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("admin", admin_cmd))
    app.add_handler(CallbackQueryHandler(on_callback))
