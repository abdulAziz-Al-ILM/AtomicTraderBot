import asyncio
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", 5432)
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Banklar va bank.uz sahifalari (scraping)
BANKS = {
    "InfinBank": "https://bank.uz/uz/currency/bank/invest-finance-bank",
    "KapitalBank": "https://bank.uz/uz/currency/bank/kapitalbank",
    "Ipoteka Bank": "https://bank.uz/uz/currency/bank/ipoteka-bank",
    "Trastbank": "https://bank.uz/uz/currency/bank/trastbank",
    "TBC Bank": "https://bank.uz/uz/currency/bank/tbc",
    "Xalq Banki": "https://bank.uz/uz/currency/bank/xalqbank",
    "Asaka Bank": "https://bank.uz/uz/currency/bank/asaka",
    "Orient Finans Bank": "https://bank.uz/uz/currency/bank/orient-finans"
}

CHECK_INTERVAL = 60 * 10  # 10 daqiqa

# PostgreSQL ulanish
def get_conn():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

# DB yaratish
def init_db():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS rates (
            id SERIAL PRIMARY KEY,
            bank VARCHAR(50),
            sell NUMERIC,
            buy NUMERIC,
            timestamp TIMESTAMP
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

# Bank kursini scraping
async def get_bank_rate(session, url):
    try:
        async with session.get(url) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, "lxml")
            usd_row = soup.find("tr", {"data-currency-code": "USD"})
            if usd_row:
                sell = float(usd_row.find("td", class_="sell").text.strip())
                buy = float(usd_row.find("td", class_="buy").text.strip())
                return sell, buy
    except:
        pass
    return None, None

# Barcha bank kurslarini olish
async def fetch_all_rates():
    rates = {}
    async with aiohttp.ClientSession() as session:
        for bank, url in BANKS.items():
            sell, buy = await get_bank_rate(session, url)
            if sell and buy:
                rates[bank] = {"sell": sell, "buy": buy}
    return rates

# DB ga yozish
def save_rates(rates):
    conn = get_conn()
    cur = conn.cursor()
    for bank, data in rates.items():
        cur.execute(
            "INSERT INTO rates (bank, sell, buy, timestamp) VALUES (%s, %s, %s, %s)",
            (bank, data["sell"], data["buy"], datetime.now())
        )
    conn.commit()
    cur.close()
    conn.close()

# Trend va signal tahlil
def analyse_and_signal(rates):
    # Past sell bank
    cheapest_bank = min(rates, key=lambda b: rates[b]['sell'])
    min_sell = rates[cheapest_bank]['sell']

    # Yuqori buy bank
    expensive_bank = max(rates, key=lambda b: rates[b]['buy'])
    max_buy = rates[expensive_bank]['buy']

    # Trend prognoz (oddiy: oxirgi 3 kun narxi)
    trend_msg = ""
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM rates WHERE timestamp >= NOW() - INTERVAL '3 days'", conn)
    conn.close()
    if not df.empty:
        last_avg = df.groupby('bank')['sell'].mean()
        trend_msg = "📈 Oxirgi 3 kunning o‘rtacha sell qiymatlari:\n"
        for bank in last_avg.index:
            trend_msg += f"{bank}: {last_avg[bank]:.2f} so‘m\n"

    diff = max_buy - min_sell
    return cheapest_bank, min_sell, expensive_bank, max_buy, diff, trend_msg

# Tugmalar
def get_keyboard():
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Hozir olish mumkinmi?", callback_data="check_buy")],
            [InlineKeyboardButton(text="Statistika (30 kun)", callback_data="get_stats")]
        ]
    )
    return kb

@dp.message(Command("start"))
async def start(msg: types.Message):
    await msg.answer("💵 Valyuta kuzatuvchi bot ishga tushdi.\nTugmalardan foydalaning.", reply_markup=get_keyboard())

@dp.callback_query(lambda c: c.data == "check_buy")
async def check_buy(call: types.CallbackQuery):
    rates = await fetch_all_rates()
    save_rates(rates)
    cheapest_bank, min_sell, expensive_bank, max_buy, diff, trend_msg = analyse_and_signal(rates)
    msg_text = f"📊 Dollar hozir olish uchun eng qulay bank: {cheapest_bank} (sell: {min_sell})\n"
    msg_text += f"💰 Sotish uchun eng yuqori bank: {expensive_bank} (buy: {max_buy})\n"
    if diff > 0:
        msg_text += f"⚡ Foyda imkoniyati: {diff:.2f} so‘m\n"
    else:
        msg_text += "❌ Hozircha foydali vaqt emas\n"
    msg_text += trend_msg
    await call.message.answer(msg_text)

@dp.callback_query(lambda c: c.data == "get_stats")
async def get_stats(call: types.CallbackQuery):
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM rates WHERE timestamp >= NOW() - INTERVAL '30 days'", conn)
    conn.close()
    filename = f"rates_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    df.to_excel(filename, index=False)
    await call.message.answer_document(open(filename, "rb"))

# Periodic task
async def periodic_check():
    while True:
        rates = await fetch_all_rates()
        save_rates(rates)
        await asyncio.sleep(CHECK_INTERVAL)

async def main():
    init_db()
    asyncio.create_task(periodic_check())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())    return rates

# DB ga yozish
def save_rates(rates):
    conn = get_conn()
    cur = conn.cursor()
    for bank, data in rates.items():
        cur.execute(
            "INSERT INTO rates (bank, sell, buy, timestamp) VALUES (%s, %s, %s, %s)",
            (bank, data["sell"], data["buy"], datetime.now())
        )
    conn.commit()
    cur.close()
    conn.close()

# Analiz va signal
def analyse_and_signal(rates):
    # Past sell bank
    cheapest_bank = min(rates, key=lambda b: rates[b]['sell'])
    max_sell = rates[cheapest_bank]['sell']
    # Yuqori buy bank
    expensive_bank = max(rates, key=lambda b: rates[b]['buy'])
    max_buy = rates[expensive_bank]['buy']

    diff = max_buy - max_sell
    return cheapest_bank, max_sell, expensive_bank, max_buy, diff

# Tugmalar
def get_keyboard():
    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Hozir olish mumkinmi?", callback_data="check_buy")],
            [InlineKeyboardButton(text="Statistika (Excel)", callback_data="get_stats")]
        ]
    )
    return kb

@dp.message(Command("start"))
async def start(msg: types.Message):
    await msg.answer("💵 Valyuta kuzatuvchi bot ishga tushdi.\n"
                     "Kurslarni tekshirish uchun tugmalardan foydalaning.", reply_markup=get_keyboard())

# Tugma ishlashi
@dp.callback_query(lambda c: c.data == "check_buy")
async def check_buy(call: types.CallbackQuery):
    rates = await fetch_all_rates()
    cheapest_bank, sell, expensive_bank, buy, diff = analyse_and_signal(rates)
    msg_text = f"📊 Dollar hozir olish uchun eng qulay bank: {cheapest_bank} (sell: {sell})\n"
    msg_text += f"💰 Sotish uchun eng yuqori bank: {expensive_bank} (buy: {buy})\n"
    if diff > 0:
        msg_text += f"⚡ Foyda imkoniyati: {diff} so‘m\n"
    else:
        msg_text += "❌ Hozircha foydali vaqt emas"
    await call.message.answer(msg_text)

@dp.callback_query(lambda c: c.data == "get_stats")
async def get_stats(call: types.CallbackQuery):
    conn = get_conn()
    df = pd.read_sql("SELECT * FROM rates WHERE timestamp >= NOW() - INTERVAL '30 days'", conn)
    conn.close()
    filename = f"rates_{datetime.now().strftime('%Y%m%d%H%M%S')}.xlsx"
    df.to_excel(filename, index=False)
    await call.message.answer_document(open(filename, "rb"))

# Asosiy loop
async def periodic_check():
    while True:
        rates = await fetch_all_rates()
        save_rates(rates)
        await asyncio.sleep(CHECK_INTERVAL)

async def main():
    init_db()
    asyncio.create_task(periodic_check())
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
