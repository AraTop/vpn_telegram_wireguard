from __future__ import annotations
from decimal import Decimal
from typing import Optional
from uuid import uuid4

from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import ContextTypes

def rub(amount: float | Decimal) -> str:
    return f"{Decimal(amount):.2f} ₽"

MAIN_MSG_KEY = "main_msg_id"
STACK_KEY = "menu_stack"

async def send_or_edit(update: Update, context: ContextTypes.DEFAULT_TYPE, text: str, keyboard: list[list[InlineKeyboardButton]]):
    chat_id = update.effective_chat.id
    msg_id = context.chat_data.get(MAIN_MSG_KEY)
    markup = InlineKeyboardMarkup(keyboard)
    if msg_id:
        try:
            await context.bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text, reply_markup=markup, disable_web_page_preview=True)
            return
        except Exception:
            # fall back to sending
            pass
    msg = await update.effective_message.reply_text(text, reply_markup=markup, disable_web_page_preview=True)
    context.chat_data[MAIN_MSG_KEY] = msg.message_id

def push_stack(context: ContextTypes.DEFAULT_TYPE, key: str):
    stack = context.chat_data.get(STACK_KEY, [])
    stack.append(key)
    context.chat_data[STACK_KEY] = stack

def pop_stack(context: ContextTypes.DEFAULT_TYPE) -> Optional[str]:
    stack = context.chat_data.get(STACK_KEY, [])
    if stack:
        stack.pop()
    context.chat_data[STACK_KEY] = stack
    return stack[-1] if stack else None

def gen_ref_code() -> str:
    return uuid4().hex[:8]
