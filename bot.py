"""
GoldScalper Telegram Trading Bot
- Connects to Deriv via WebSocket API
- EMA crossover auto-trading strategy
- Telegram control & alerts
- Deploy free on Railway / Render
"""

import os
import asyncio
import json
import logging
from datetime import datetime
from collections import deque

import websockets
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes
)

# ── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO
)
log = logging.getLogger(__name__)

# ── Config (set these in Railway/Render environment variables) ─────────────
TELEGRAM_TOKEN  = os.environ["TELEGRAM_TOKEN"]       # Your bot token
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]    # Your chat/user ID
DERIV_API_TOKEN = os.environ["DERIV_API_TOKEN"]       # Deriv API token
DERIV_APP_ID    = os.environ.get("DERIV_APP_ID", "1089")  # Deriv app ID

# ── Strategy settings (tweak freely) ──────────────────────────────────────
SYMBOL          = "frxXAUUSD"   # Gold on Deriv
GRANULARITY     = 60            # 1-minute candles (seconds)
FAST_EMA_PERIOD = 9
SLOW_EMA_PERIOD = 21
STAKE_AMOUNT    = 1.0           # USD per trade
CONTRACT_TYPE   = "MULTUP"      # MULTUP = buy, MULTDOWN = sell (Multiplier)
MULTIPLIER      = 10            # Leverage multiplier
MAX_OPEN_TRADES = 1

# ── State ──────────────────────────────────────────────────────────────────
class BotState:
    def __init__(self):
        self.running       = False   # Auto-trading on/off
        self.open_contracts = []     # List of open contract IDs
        self.balance       = 0.0
        self.currency      = "USD"
        self.price_history = deque(maxlen=100)  # Recent close prices
        self.ws            = None    # Active WebSocket connection
        self.req_id        = 0       # Request ID counter

state = BotState()

# ── EMA calculation ────────────────────────────────────────────────────────
def calculate_ema(prices: list, period: int) -> list:
    if len(prices) < period:
        return []
    k = 2 / (period + 1)
    emas = [sum(prices[:period]) / period]
    for price in prices[period:]:
        emas.append(price * k + emas[-1] * (1 - k))
    return emas

def get_signal() -> str | None:
    """Returns 'BUY', 'SELL', or None based on EMA crossover."""
    prices = list(state.price_history)
    if len(prices) < SLOW_EMA_PERIOD + 2:
        return None

    fast = calculate_ema(prices, FAST_EMA_PERIOD)
    slow = calculate_ema(prices, SLOW_EMA_PERIOD)

    if len(fast) < 2 or len(slow) < 2:
        return None

    # Align lists (slow is shorter)
    offset = len(fast) - len(slow)
    f_prev, f_curr = fast[-(2 + offset)], fast[-(1 + offset)]
    s_prev, s_curr = slow[-2], slow[-1]

    if f_prev <= s_prev and f_curr > s_curr:
        return "BUY"
    if f_prev >= s_prev and f_curr < s_curr:
        return "SELL"
    return None

# ── Deriv WebSocket helpers ────────────────────────────────────────────────
DERIV_WS_URL = f"wss://ws.binaryws.com/websockets/v3?app_id={DERIV_APP_ID}"

async def deriv_send(ws, payload: dict) -> dict:
    state.req_id += 1
    payload["req_id"] = state.req_id
    await ws.send(json.dumps(payload))
    while True:
        raw = await ws.recv()
        resp = json.loads(raw)
        if resp.get("req_id") == state.req_id:
            return resp

async def deriv_authorize(ws) -> bool:
    resp = await deriv_send(ws, {"authorize": DERIV_API_TOKEN})
    if resp.get("error"):
        log.error("Deriv auth failed: %s", resp["error"]["message"])
        return False
    state.balance  = resp["authorize"]["balance"]
    state.currency = resp["authorize"]["currency"]
    log.info("Authorized. Balance: %s %s", state.balance, state.currency)
    return True

async def get_balance(ws) -> float:
    resp = await deriv_send(ws, {"balance": 1, "account": "current"})
    if not resp.get("error"):
        state.balance = resp["balance"]["balance"]
    return state.balance

async def open_trade(ws, direction: str) -> dict | None:
    contract = "MULTUP" if direction == "BUY" else "MULTDOWN"
    payload = {
        "buy": 1,
        "price": STAKE_AMOUNT,
        "parameters": {
            "contract_type": contract,
            "symbol": SYMBOL,
            "amount": STAKE_AMOUNT,
            "currency": "USD",
            "multiplier": MULTIPLIER,
            "limit_order": {
                "stop_loss": {"order_type": "stop_loss", "order_amount": STAKE_AMOUNT * 0.8}
            }
        }
    }
    resp = await deriv_send(ws, payload)
    if resp.get("error"):
        log.error("Trade open failed: %s", resp["error"]["message"])
        return None
    contract_id = resp["buy"]["contract_id"]
    state.open_contracts.append(contract_id)
    log.info("Trade opened: %s | Contract: %s", direction, contract_id)
    return resp["buy"]

async def close_all_trades(ws) -> int:
    closed = 0
    for cid in list(state.open_contracts):
        resp = await deriv_send(ws, {"sell": cid, "price": 0})
        if not resp.get("error"):
            state.open_contracts.remove(cid)
            closed += 1
    return closed

async def get_open_positions(ws) -> list:
    resp = await deriv_send(ws, {"portfolio": 1})
    if resp.get("error"):
        return []
    return resp.get("portfolio", {}).get("contracts", [])

# ── Auto-trading loop ──────────────────────────────────────────────────────
async def trading_loop(app: Application):
    """Main auto-trading coroutine. Runs while state.running is True."""
    await send_alert(app, "🟢 *Auto-trading started* on XAUUSD\nStrategy: EMA 9/21 crossover")

    async with websockets.connect(DERIV_WS_URL) as ws:
        state.ws = ws
        if not await deriv_authorize(ws):
            await send_alert(app, "❌ Deriv authorization failed. Check your API token.")
            state.running = False
            return

        # Subscribe to candle stream
        await ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "end": "latest",
            "start": 1,
            "count": 100,
            "style": "candles",
            "granularity": GRANULARITY,
            "subscribe": 1,
            "req_id": 0
        }))

        async for raw in ws:
            if not state.running:
                break

            msg = json.loads(raw)

            # Populate history from initial response
            if msg.get("msg_type") == "candles":
                for candle in msg["candles"]:
                    state.price_history.append(float(candle["close"]))
                log.info("Loaded %d candles", len(state.price_history))
                continue

            # Update price on new candle tick
            if msg.get("msg_type") == "ohlc":
                ohlc = msg["ohlc"]
                if int(ohlc.get("open_time", 0)) != int(ohlc.get("epoch", 0)):
                    # New closed candle
                    state.price_history.append(float(ohlc["close"]))
                    signal = get_signal()

                    if signal and len(state.open_contracts) < MAX_OPEN_TRADES:
                        await get_balance(ws)
                        trade_info = await open_trade(ws, signal)
                        if trade_info:
                            emoji = "📈" if signal == "BUY" else "📉"
                            await send_alert(
                                app,
                                f"{emoji} *Trade Opened*\n"
                                f"Direction: `{signal}`\n"
                                f"Symbol: `{SYMBOL}`\n"
                                f"Stake: `${STAKE_AMOUNT}`\n"
                                f"Contract: `{trade_info['contract_id']}`\n"
                                f"Balance: `${state.balance:.2f}`"
                            )

    state.ws = None
    if state.running:
        state.running = False
        await send_alert(app, "⚠️ Auto-trading stopped (connection lost). Use /start\\_trading to restart.")

# ── Telegram helpers ───────────────────────────────────────────────────────
async def send_alert(app: Application, text: str):
    try:
        await app.bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text=text,
            parse_mode="Markdown"
        )
    except Exception as e:
        log.error("Telegram send error: %s", e)

def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("▶️ Start Auto-Trading", callback_data="start_trading"),
         InlineKeyboardButton("⏹ Stop Trading",       callback_data="stop_trading")],
        [InlineKeyboardButton("💰 Balance",            callback_data="balance"),
         InlineKeyboardButton("📊 Open Positions",     callback_data="positions")],
        [InlineKeyboardButton("❌ Close All Trades",   callback_data="close_all")]
    ])

# ── Command handlers ───────────────────────────────────────────────────────
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "👋 *GoldScalper Bot* is online!\n\n"
        "Use the buttons below to control your trading:",
        parse_mode="Markdown",
        reply_markup=main_menu_keyboard()
    )

async def cmd_status(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    status = "🟢 Running" if state.running else "🔴 Stopped"
    text = (
        f"*Bot Status*\n"
        f"Auto-trading: {status}\n"
        f"Open trades: {len(state.open_contracts)}\n"
        f"Balance: ${state.balance:.2f} {state.currency}\n"
        f"Candles loaded: {len(state.price_history)}"
    )
    await update.message.reply_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

async def cmd_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "*Available Commands*\n\n"
        "/start — Show main menu\n"
        "/status — Bot & account status\n"
        "/help — This message\n\n"
        "Use the inline buttons to control trading.",
        parse_mode="Markdown"
    )

# ── Inline button handlers ─────────────────────────────────────────────────
async def button_handler(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    action = query.data

    if action == "start_trading":
        if state.running:
            await query.edit_message_text("⚠️ Auto-trading is already running.", reply_markup=main_menu_keyboard())
            return
        state.running = True
        asyncio.create_task(trading_loop(ctx.application))
        await query.edit_message_text("✅ Auto-trading *started*!", parse_mode="Markdown", reply_markup=main_menu_keyboard())

    elif action == "stop_trading":
        if not state.running:
            await query.edit_message_text("ℹ️ Auto-trading is not running.", reply_markup=main_menu_keyboard())
            return
        state.running = False
        await query.edit_message_text("⏹ Auto-trading *stopped*.", parse_mode="Markdown", reply_markup=main_menu_keyboard())

    elif action == "balance":
        async with websockets.connect(DERIV_WS_URL) as ws:
            await deriv_authorize(ws)
            bal = await get_balance(ws)
        await query.edit_message_text(
            f"💰 *Balance*: `${bal:.2f} {state.currency}`",
            parse_mode="Markdown", reply_markup=main_menu_keyboard()
        )

    elif action == "positions":
        async with websockets.connect(DERIV_WS_URL) as ws:
            await deriv_authorize(ws)
            positions = await get_open_positions(ws)
        if not positions:
            text = "📊 No open positions."
        else:
            lines = ["📊 *Open Positions*\n"]
            for p in positions:
                lines.append(f"• `{p.get('contract_type','?')}` | ID: `{p['contract_id']}` | P&L: `{p.get('profit','?')}`")
            text = "\n".join(lines)
        await query.edit_message_text(text, parse_mode="Markdown", reply_markup=main_menu_keyboard())

    elif action == "close_all":
        async with websockets.connect(DERIV_WS_URL) as ws:
            await deriv_authorize(ws)
            closed = await close_all_trades(ws)
        await query.edit_message_text(
            f"❌ Closed *{closed}* trade(s).",
            parse_mode="Markdown", reply_markup=main_menu_keyboard()
        )

# ── Entry point ────────────────────────────────────────────────────────────
def main():
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start",  cmd_start))
    app.add_handler(CommandHandler("status", cmd_status))
    app.add_handler(CommandHandler("help",   cmd_help))
    app.add_handler(CallbackQueryHandler(button_handler))

    log.info("Bot started. Listening for commands...")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()
