"""
main.py
Ponto de entrada do microsserviço "Quant Terminal Sniper" (FastAPI + APScheduler + Polars + CCXT).
Suporta Modo SPOT (padrão para contas Binance Brasil, comprando LONG e posicionando OCO TP/SL)
e Modo FUTURES.
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from dotenv import load_dotenv
from fastapi import Body, FastAPI
from fastapi.responses import FileResponse, HTMLResponse, Response
from pydantic import BaseModel

from app.engine_polars import (
    ROUND_TRIP_COST_PCT,
    calibrate_symbol_champion,
    evaluate_closed_candle_signal,
)
from app.executor_ccxt import BinanceFuturesExecutor, VISION_BASE_URL

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("quant_sniper.main")

DASHBOARD_HTML_PATH = Path(__file__).parent / "dashboard.html"


class ToggleRequest(BaseModel):
    enabled: Optional[bool] = None
    max_concurrent_positions: Optional[int] = None
    max_sniper_positions: Optional[int] = None
    max_scalp_positions: Optional[int] = None
    max_rocket_positions: Optional[int] = None
    max_session_positions: Optional[int] = None
    session_special_enabled: Optional[bool] = None
    session_margin_usdt: Optional[float] = None
    allow_scalp_borrow_idle_capital: Optional[bool] = None
    order_margin_usdt: Optional[float] = None
    scalp_margin_usdt: Optional[float] = None
    rocket_margin_usdt: Optional[float] = None
    default_leverage: Optional[int] = None
    min_profit_factor: Optional[float] = None
    lookback_days: Optional[int] = None
    timeframe_filter: Optional[str] = None
    orderbook_scalp_auto: Optional[bool] = None
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    testnet: Optional[bool] = None
    market_type: Optional[str] = None
    proxy_url: Optional[str] = None


class ScalpExecuteRequest(BaseModel):
    symbol: str
    entry_price: float
    stop_price: float
    take_profit_price: float


class ScalpConfigRequest(BaseModel):
    scalp_tp_pct: Optional[float] = None
    scalp_sl_pct: Optional[float] = None
    scalp_target_mode: Optional[str] = None
    scalp_margin_usdt: Optional[float] = None
    max_scalp_positions: Optional[int] = None
    orderbook_scalp_auto: Optional[bool] = None
    auto_momentum_exit: Optional[bool] = None
    max_hold_minutes: Optional[int] = None
    scalp_rotation_enabled: Optional[bool] = None
    scalp_rotation_min_pnl: Optional[float] = None
    apply_to_open: Optional[bool] = True


class ClosePositionRequest(BaseModel):
    symbol: str


class SniperRuntimeState:
    """Estado global em memória do Quant Terminal Sniper."""

    def __init__(self) -> None:
        self.enabled: bool = os.getenv("BOT_ENABLED", "true").lower() in ("true", "1", "yes")
        # Vagas dedicadas e isoladas para cada modalidade (Scalp Book L2, Sniper Swing, Rocket Memes e Sessão Especial)
        self.max_scalp_positions: int = int(os.getenv("MAX_SCALP_POSITIONS", "1"))
        self.max_sniper_positions: int = int(os.getenv("MAX_SNIPER_POSITIONS", "1"))
        self.max_rocket_positions: int = int(os.getenv("MAX_ROCKET_POSITIONS", "1"))
        self.max_session_positions: int = int(os.getenv("MAX_SESSION_POSITIONS", "1"))
        self.max_concurrent_positions: int = (
            self.max_scalp_positions + self.max_sniper_positions + self.max_rocket_positions
        )
        self.top_symbols_limit: int = int(os.getenv("TOP_SYMBOLS_LIMIT", "35"))
        self.min_volume_24h_usd: float = float(os.getenv("MIN_VOLUME_24H_USD", "50000000"))
        self.min_profit_factor: float = float(os.getenv("MIN_PROFIT_FACTOR", "1.35"))
        self.lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "15"))
        self.timeframe_filter: str = os.getenv("TIMEFRAME_FILTER", "ALL").upper()
        self.timeframes: List[str] = ["5m", "15m", "30m", "1h"]

        # Motor de Sessões Globais (Virada da China 00h/08h UTC e Almoço nos EUA 16h-18h UTC)
        self.session_special_enabled: bool = os.getenv("SESSION_SPECIAL_ENABLED", "true").lower() in ("true", "1", "yes")
        self.session_margin_usdt: float = float(os.getenv("SESSION_MARGIN_USDT", "5.25"))
        self.last_session_scan_utc: Optional[str] = None

        # Configuração dedicada e separada do Motor de Scalp (Livro de Ofertas L2)
        self.scalp_tp_pct: float = float(os.getenv("SCALP_TP_PCT", "0.50"))
        self.scalp_sl_pct: float = float(os.getenv("SCALP_SL_PCT", "0.35"))
        self.scalp_target_mode: str = os.getenv("SCALP_TARGET_MODE", "FIXED").upper()
        self.scalp_margin_usdt: float = float(os.getenv("SCALP_MARGIN_USDT", "5.25"))
        # Pooling / Empréstimo Dinâmico: permite que o Scalp aproveite o saldo e vagas paradas de Sniper/Memes
        self.allow_scalp_borrow_idle_capital: bool = os.getenv("ALLOW_SCALP_BORROW_IDLE", "true").lower() in ("true", "1", "yes")
        self.orderbook_scalp_auto: bool = False
        self.orderbook_cache: List[Dict[str, Any]] = []
        self.orderbook_last_ts: float = 0.0

        # Configuração do Motor Rocket Memes (Alta Volatilidade / Alvos 10-15%+ com Trailing Stop)
        self.rocket_margin_usdt: float = float(os.getenv("ROCKET_MARGIN_USDT", "5.25"))
        self.rocket_tp_target_pct: float = float(os.getenv("ROCKET_TP_TARGET_PCT", "15.0"))
        self.rocket_sl_pct: float = float(os.getenv("ROCKET_SL_PCT", "2.5"))
        self.rocket_auto_enabled: bool = True

        self.wallet_balance: Dict[str, Any] = {
            "total_usdt": 0.0,
            "free_usdt": 0.0,
            "used_usdt": 0.0,
            "authenticated": False,
            "error": None,
        }
        self.open_positions: List[Dict[str, Any]] = []
        self.champions_map: Dict[str, Dict[str, Any]] = {}
        self.champions_by_tf: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self.top_options_ranking: List[Dict[str, Any]] = []
        self.ohlcv_cache: Dict[str, Dict[str, List[List[float]]]] = {}
        self.symbols_meta: Dict[str, Dict[str, Any]] = {}
        self.trades_history: Deque[Dict[str, Any]] = deque(maxlen=100)
        self.execution_logs: Deque[Dict[str, Any]] = deque(maxlen=150)

        self.is_calibrating: bool = False
        self.last_calibration_utc: Optional[str] = None
        self.last_sniper_run_utc: Optional[str] = None
        self._dispatched_candles: Set[str] = set()
        self._calibration_task: Optional[asyncio.Task[Any]] = None

    def append_log(
        self,
        event_type: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        entry = {
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": event_type,
            "message": message,
            "details": details or {},
        }
        self.execution_logs.appendleft(entry)
        logger.info(f"[{event_type}] {message}")


state = SniperRuntimeState()
executor = BinanceFuturesExecutor(
    market_type=os.getenv("BINANCE_MARKET_TYPE", "spot"),
    default_leverage=int(os.getenv("DEFAULT_LEVERAGE", "1")),
    order_margin_usdt=float(os.getenv("ORDER_MARGIN_USDT", "5.25")),
)
scheduler = AsyncIOScheduler(timezone="UTC")


def _determine_closed_timeframes(now_utc: datetime) -> List[str]:
    minute = now_utc.minute
    closed = ["5m"]
    if minute % 15 == 0:
        closed.append("15m")
    if minute in (0, 30):
        closed.append("30m")
    if minute == 0:
        closed.append("1h")
    return closed


SESSION_SYMBOLS: List[str] = [
    "BTC/USDT",
    "ETH/USDT",
    "SOL/USDT",
    "BNB/USDT",
    "XRP/USDT",
    "DOGE/USDT",
    "SUI/USDT",
    "ADA/USDT",
    "AVAX/USDT",
    "LINK/USDT",
    "NEAR/USDT",
    "APT/USDT",
]


def get_market_session_info(now_utc: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Identifica as janelas institucionais do mercado financeiro global e crypto:
    1. Virada da China #1 (23:30 - 01:30 UTC): Abertura Ásia / Virada Diária / Funding Rate 00:00 UTC.
    2. Virada da China #2 (07:30 - 09:00 UTC): Fechamento Ásia & Abertura Londres / Funding Rate 08:00 UTC.
    3. Almoço nos EUA (16:00 - 18:00 UTC): Wall Street Lunch Lull (12h-14h NY / 13h-15h BRT).
    """
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)
    hour = now_utc.hour
    minute = now_utc.minute
    total_minutes = hour * 60 + minute

    # 1. Virada da China #1: 23:30 a 01:30 UTC (Abertura Ásia / Funding Rate 00:00 UTC)
    is_china_1 = (total_minutes >= 1410) or (total_minutes <= 90)

    # 2. Virada da China #2: 07:30 a 09:00 UTC (Fechamento Ásia & Abertura Londres / Funding Rate 08:00 UTC)
    is_china_2 = (450 <= total_minutes <= 540)

    # 3. Almoço nos EUA: 16:00 a 18:00 UTC (12h-14h Nova York / 13h-15h BRT / Wall Street Lunch Lull)
    is_us_lunch = (960 <= total_minutes <= 1080)

    if is_china_1:
        session_id = "CHINA_VIRADA_1"
        session_name = "🌏 Virada da China #1 (Abertura Ásia & Funding 00:00 UTC)"
        session_badge = "🌏 VIRADA DA CHINA #1 (00h UTC / 21h BRT)"
        formula_id = "rompimento-1m"
        formula_name = "Rompimento de Expansão & Agressão China #1"
        formula_type = "BREAKOUT"
        formula_rule = "close > sma_21 && range > 1.75 * mean_range(30) && volume > 1.8 * mean_vol(30) && is_green == 1"
        stop_rule = "-0.75% (Abaixo da mínima do candle)"
        target_rule = "+1.50% (1:2 R:R)"
        timeframe = "5m"
        is_special = True
        desc = "Abertura oficial dos pregões de Xangai/Hong Kong e virada diária do mercado cripto. Forte expansão de range direcional impulsionada por agressão compradora."
    elif is_china_2:
        session_id = "CHINA_VIRADA_2"
        session_name = "🌏 Virada da China #2 (Fechamento Ásia & Abertura Londres 08:00 UTC)"
        session_badge = "🌏 VIRADA DA CHINA #2 (08h UTC / 05h BRT)"
        formula_id = "breakout-5m"
        formula_name = "Rompimento & Handover Institucional China #2"
        formula_type = "BREAKOUT"
        formula_rule = "close > sma_21 && range > 1.75 * mean_range(30) && volume > 1.8 * mean_vol(30) && is_green == 1"
        stop_rule = "-0.75% (Abaixo da mínima do candle)"
        target_rule = "+1.50% (1:2 R:R)"
        timeframe = "5m"
        is_special = True
        desc = "Fechamento dos bancos asiáticos e abertura das mesas financeiras de Londres/Europa. Volume institucional de handover e 2º reset de funding rate."
    elif is_us_lunch:
        session_id = "US_LUNCH"
        session_name = "🥪 Almoço nos EUA (Wall Street Lunch Lull 16h-18h UTC)"
        session_badge = "🥪 ALMOÇO EUA (16h-18h UTC / 13h-15h BRT)"
        formula_id = "exaustao-vwap"
        formula_name = "F1: Exaustão & Reversão à VWAP (Afastamento >1.5% + Clímax Trades)"
        formula_type = "VWAP_REVERSION"
        formula_rule = "abs(close - vwap_24) / vwap_24 > 0.014 && vol_ratio >= 1.4 && lower_wick >= 0.25 * range"
        stop_rule = "-0.75% fixo"
        target_rule = "+1.50% (Retorno até a VWAP central)"
        timeframe = "5m"
        is_special = True
        desc = "Pausa para almoço das mesas institucionais de Nova York (12h-14h NY). A liquidez direcional estagna e falsos rompimentos revertem rapidamente em direção à VWAP."
    else:
        if 8 <= hour < 16:
            base_s = "🏛️ Londres / Europa (08h-16h UTC)"
        elif 13 <= hour < 21:
            base_s = "🗽 Nova York / EUA (13h-21h UTC)"
        else:
            base_s = "🌏 Ásia / Pacífico (00h-08h UTC)"

        session_id = "REGULAR"
        session_name = f"📈 {base_s}"
        session_badge = f"{base_s}"
        formula_id = "standard-quant"
        formula_name = "Sniper Quant Multi-Timeframe + Scalp Book L2"
        formula_type = "STANDARD"
        formula_rule = "Walk-Forward Polars 5m, 15m, 30m, 1h + Livro L2 Depth"
        stop_rule = "Dinâmico ATR"
        target_rule = "1:2.0 a 1:2.5 R:R"
        timeframe = "Multi"
        is_special = False
        desc = "Operação normal 24/7 com proteção de capital e varredura de velas fechadas."

    special_windows = [
        (450, "Virada da China #2 (07:30 UTC / 04:30 BRT)"),
        (960, "Almoço nos EUA (16:00 UTC / 13:00 BRT)"),
        (1410, "Virada da China #1 (23:30 UTC / 20:30 BRT)"),
    ]
    next_window = ""
    minutes_left = 0
    if not is_special:
        for w_min, w_name in special_windows:
            if total_minutes < w_min:
                diff = w_min - total_minutes
                h = diff // 60
                m = diff % 60
                next_window = f"{w_name} em {h}h {m:02d}m"
                minutes_left = diff
                break
        if not next_window:
            diff = (1440 - total_minutes) + 450
            h = diff // 60
            m = diff % 60
            next_window = f"Virada da China #2 (07:30 UTC) em {h}h {m:02d}m"
            minutes_left = diff
    else:
        next_window = "🔥 JANELA ESPECIAL ATIVA AGORA"

    utc_str = now_utc.strftime("%H:%M:%S UTC")
    brt_time = now_utc - timedelta(hours=3)
    brt_str = brt_time.strftime("%H:%M:%S BRT")
    beijing_time = now_utc + timedelta(hours=8)
    beijing_str = beijing_time.strftime("%H:%M:%S CST (Pequim)")
    ny_time = now_utc - timedelta(hours=5)
    ny_str = ny_time.strftime("%H:%M:%S EST (Nova York)")

    return {
        "session_id": session_id,
        "session_name": session_name,
        "session_badge": session_badge,
        "formula_id": formula_id,
        "formula_name": formula_name,
        "formula_type": formula_type,
        "formula_rule": formula_rule,
        "stop_rule": stop_rule,
        "target_rule": target_rule,
        "timeframe": timeframe,
        "is_special": is_special,
        "description": desc,
        "next_special_window": next_window,
        "minutes_until_special": minutes_left,
        "clock_utc": utc_str,
        "clock_brt": brt_str,
        "clock_beijing": beijing_str,
        "clock_ny": ny_str,
    }


# ============================================================================
# LOOP 1: CALIBRADOR WALK-FORWARD & RECOMPUTAÇÃO INSTANTÂNEA DE INTERVALOS
# ============================================================================

def _get_active_timeframes() -> List[str]:
    tf_filter = state.timeframe_filter.lower()
    if tf_filter in ("5m", "15m", "30m", "1h"):
        return [tf_filter]
    return ["5m", "15m", "30m", "1h"]


def _recompute_champions_sync() -> Dict[str, Dict[str, Any]]:
    """
    Recalcula instantaneamente (< 0.4s) as melhores estratégias no intervalo escolhido
    (`lookback_days`: 30D, 15D, 7D, 3D, 1D e `timeframe_filter`: ALL, 1h, 30m, 15m)
    a partir do cache em memória `state.ohlcv_cache`.
    """
    allowed_dirs = ["LONG"] if executor.market_type == "spot" else ["LONG", "SHORT"]
    target_tfs = _get_active_timeframes()
    new_champions: Dict[str, Dict[str, Any]] = {}
    by_tf: Dict[str, Dict[str, Dict[str, Any]]] = {tf: {} for tf in state.timeframes}
    all_options: List[Dict[str, Any]] = []

    # Se o usuário filtrou um timeframe único ou janela curta, usa PF efetivo flexível se necessário
    for symbol, ohlcv_by_tf in state.ohlcv_cache.items():
        mkt = state.symbols_meta.get(symbol, {})
        # Também indexa a campeã específica de cada timeframe (5m, 15m, 30m, 1h) para varreduras Auto
        for single_tf in state.timeframes:
            if single_tf in ohlcv_by_tf:
                tf_champ = calibrate_symbol_champion(
                    symbol=symbol,
                    ohlcv_by_tf=ohlcv_by_tf,
                    min_pf=state.min_profit_factor,
                    allowed_directions=allowed_dirs,
                    lookback_days=state.lookback_days,
                    target_timeframes=[single_tf],
                )
                if tf_champ is not None:
                    tf_champ["quote_volume_24h"] = mkt.get("quote_volume_24h", 0.0)
                    tf_champ["min_notional_usdt"] = mkt.get("min_notional_usdt", 5.0)
                    by_tf[single_tf][symbol] = tf_champ

        champion = calibrate_symbol_champion(
            symbol=symbol,
            ohlcv_by_tf=ohlcv_by_tf,
            min_pf=state.min_profit_factor,
            allowed_directions=allowed_dirs,
            lookback_days=state.lookback_days,
            target_timeframes=target_tfs,
        )
        # Se no filtro específico não bateu o PF estrito, busca a melhor opção positiva >= 1.12 para ranking
        if champion is None and (state.timeframe_filter != "ALL" or state.lookback_days < 30):
            champion = calibrate_symbol_champion(
                symbol=symbol,
                ohlcv_by_tf=ohlcv_by_tf,
                min_pf=1.12,
                allowed_directions=allowed_dirs,
                lookback_days=state.lookback_days,
                target_timeframes=target_tfs,
            )

        if champion is not None:
            champion["quote_volume_24h"] = mkt.get("quote_volume_24h", 0.0)
            champion["min_notional_usdt"] = mkt.get("min_notional_usdt", 5.0)
            champion["calibrated_at_utc"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
            new_champions[symbol] = champion
            for alt in champion.get("top_alternatives", [champion]):
                alt_copy = dict(alt)
                alt_copy["symbol"] = symbol
                alt_copy["quote_volume_24h"] = mkt.get("quote_volume_24h", 0.0)
                all_options.append(alt_copy)

    all_options.sort(key=lambda x: x.get("quant_score", 0.0), reverse=True)
    state.top_options_ranking = all_options[:30]
    state.champions_map = new_champions
    state.champions_by_tf = by_tf
    state.last_calibration_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    return new_champions


async def run_walk_forward_calibration() -> None:
    if state.is_calibrating:
        logger.warning("Calibração Walk-Forward já está em andamento. Ignorando chamada duplicada.")
        return

    state.is_calibrating = True
    t0 = datetime.now(timezone.utc)
    allowed_dirs = ["LONG"] if executor.market_type == "spot" else ["LONG", "SHORT"]
    state.append_log(
        "CALIBRATION_START",
        f"Iniciando Calibração Walk-Forward {state.lookback_days}D [{executor.market_type.upper()} | TF: {state.timeframe_filter} | Direções: {allowed_dirs}] "
        f"(Top {state.top_symbols_limit} ativos | PF >= {state.min_profit_factor}).",
    )

    try:
        top_markets = await executor.fetch_top_liquid_symbols(
            limit=state.top_symbols_limit,
            min_quote_vol_usd=state.min_volume_24h_usd,
        )

        for mkt in top_markets:
            symbol = mkt["symbol"]
            state.symbols_meta[symbol] = mkt
            try:
                ohlcv_by_tf: Dict[str, List[List[float]]] = {}
                for tf in state.timeframes:
                    fetch_days = 15 if tf == "5m" else 30
                    candles = await executor.fetch_ohlcv_days(symbol=symbol, timeframe=tf, days=fetch_days)
                    ohlcv_by_tf[tf] = candles
                state.ohlcv_cache[symbol] = ohlcv_by_tf
            except Exception as sym_exc:
                logger.warning(f"[{symbol}] Erro ao baixar histórico de 30D: {sym_exc}")

        new_champions = await asyncio.to_thread(_recompute_champions_sync)
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()

        state.append_log(
            "CALIBRATION_DONE",
            f"Calibração {executor.market_type.upper()} ({state.lookback_days}D | TF: {state.timeframe_filter}) concluída em {elapsed:.1f}s: "
            f"{len(new_champions)}/{len(top_markets)} moedas aprovadas (Fricção {ROUND_TRIP_COST_PCT}%).",
            {"approved_symbols": list(new_champions.keys()), "elapsed_sec": round(elapsed, 2)},
        )
    except Exception as exc:
        state.append_log("CALIBRATION_ERROR", f"Falha na calibração Walk-Forward: {exc}")
    finally:
        state.is_calibrating = False


# ============================================================================
# LOOP 2: SNIPER DE FECHAMENTO (:00:01, :15:01, :30:01, :45:01 UTC)
# ============================================================================

async def run_candle_close_sniper() -> None:
    now_utc = datetime.now(timezone.utc)
    state.last_sniper_run_utc = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    closed_tfs = _determine_closed_timeframes(now_utc)

    if not state.enabled:
        state.append_log(
            "SNIPER_SKIPPED",
            f"Kill Switch desativado (enabled=False). Ignorando varredura dos timeframes {closed_tfs}.",
        )
        return

    if not state.champions_map:
        state.append_log(
            "SNIPER_SKIPPED",
            "Mapa de campeãs ainda vazio (aguardando conclusão da Calibração Walk-Forward).",
        )
        return

    open_positions = await executor.get_open_positions()
    state.open_positions = open_positions
    state.wallet_balance = await executor.get_wallet_balance()

    open_symbols = {pos["symbol"] for pos in open_positions}
    sniper_positions = [p for p in open_positions if p.get("mode") in ("SNIPER_CANDLE", "SNIPER")]
    available_slots = max(0, state.max_sniper_positions - len(sniper_positions))

    if available_slots <= 0:
        state.append_log(
            "ANTI_CORRELATION_LOCK",
            f"Vagas dedicadas do Sniper atingidas ({len(sniper_positions)}/{state.max_sniper_positions} abertas). Novos disparos de velas aguardando encerramento.",
            {"open_symbols": [p["symbol"] for p in sniper_positions]},
        )
        return

    current_notional = executor.order_margin_usdt * executor.default_leverage

    eligible_targets: List[ Any ] = []
    seen_pairs: Set[str] = set()
    if state.timeframe_filter == "ALL" and state.champions_by_tf:
        for tf in closed_tfs:
            for sym, champ in state.champions_by_tf.get(tf, {}).items():
                if (
                    sym not in open_symbols
                    and f"{sym}:{tf}" not in seen_pairs
                    and float(champ.get("min_notional_usdt", 5.0)) <= (current_notional * 1.25)
                ):
                    seen_pairs.add(f"{sym}:{tf}")
                    eligible_targets.append((sym, champ))
    else:
        eligible_targets = [
            (sym, champ)
            for sym, champ in state.champions_map.items()
            if champ.get("timeframe") in closed_tfs
            and sym not in open_symbols
            and float(champ.get("min_notional_usdt", 5.0)) <= (current_notional * 1.25)
        ]

    if not eligible_targets:
        return

    async def _check_symbol(sym: str, champ: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tf = champ["timeframe"]
        try:
            recent_candles = await executor.fetch_recent_candles(symbol=sym, timeframe=tf, limit=120)
            eval_res = await asyncio.to_thread(evaluate_closed_candle_signal, recent_candles, champ)
            if eval_res.get("signal"):
                eval_res["quant_score"] = champ.get("quant_score", 0.0)
                eval_res["profit_factor"] = champ.get("profit_factor", 0.0)
                return eval_res
        except Exception as err:
            logger.warning(f"[{sym}] Falha ao checar vela fechada ({tf}): {err}")
        return None

    results = await asyncio.gather(*(_check_symbol(s, c) for s, c in eligible_targets))
    approved_signals = [r for r in results if r is not None]

    if not approved_signals:
        state.append_log(
            "SNIPER_SCAN_COMPLETE",
            f"Varredura {closed_tfs} ({executor.market_type.upper()}) concluída em {len(eligible_targets)} ativos: nenhum gatilho na vela fechada [-2].",
        )
        return

    approved_signals.sort(key=lambda x: x.get("quant_score", 0.0), reverse=True)

    for sig in approved_signals[:available_slots]:
        sym = sig["symbol"]
        tf = sig["timeframe"]
        candle_key = f"{sym}:{tf}:{sig['closed_candle_ts']}"

        if candle_key in state._dispatched_candles:
            continue

        state._dispatched_candles.add(candle_key)
        try:
            free_usdt = float(state.wallet_balance.get("free_usdt") or 0.0)
            needed_margin = float(executor.order_margin_usdt)
            if free_usdt < (needed_margin * 0.95):
                # Se o saldo estiver tomado por um scalp, recicla o scalp no lucro para dar prioridade ao Sniper
                scalp_to_recycle = next(
                    (p for p in state.open_positions if p.get("mode") in ("SCALP_BOOK", "SCALP") and float(p.get("unrealized_pnl", 0.0)) >= 0.0),
                    None
                )
                if scalp_to_recycle:
                    state.append_log(
                        "CAPITAL_PRIORITY_RECYCLE",
                        f"Reciclando scalp {scalp_to_recycle['symbol']} no lucro para liberar margem para o disparo Sniper de {sym} [{tf}]!",
                    )
                    await executor.close_position(scalp_to_recycle["symbol"], reason="PRIORIDADE_SNIPER")
                    await asyncio.sleep(0.5)
                    state.wallet_balance = await executor.get_wallet_balance()

            order_receipt = await executor.execute_atomic_order(
                symbol=sym,
                direction=sig["direction"],
                entry_price=sig["entry_price"],
                stop_price=sig["stop_price"],
                take_profit_price=sig["take_profit_price"],
                mode="SNIPER_CANDLE",
                timeframe=tf,
                strategy_name=sig["strategy_name"],
            )
            trade_record = {
                "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                "symbol": sym,
                "timeframe": tf,
                "strategy_name": sig["strategy_name"],
                "direction": sig["direction"],
                "entry_price": order_receipt["entry_price"],
                "stop_price": order_receipt["stop_price"],
                "take_profit_price": order_receipt["take_profit_price"],
                "margin_usdt": order_receipt["margin_usdt"],
                "leverage": order_receipt["leverage"],
                "notional_usdt": order_receipt["notional_usdt"],
                "entry_order_id": order_receipt["entry_order_id"],
            }
            state.trades_history.appendleft(trade_record)
            state.append_log(
                "ATOMIC_ORDER_FIRED",
                (
                    f"ORDEM {executor.market_type.upper()} DISPARADA: {sym} [{tf}] {sig['direction']} "
                    f"(${order_receipt['notional_usdt']} @ {order_receipt['entry_price']}) | "
                    f"SL: {order_receipt['stop_price']} | TP: {order_receipt['take_profit_price']}"
                ),
                {"signal": sig, "order_receipt": order_receipt},
            )
        except Exception as order_exc:
            state.append_log(
                "ATOMIC_ORDER_FAILED",
                f"Aviso ao executar ordem para {sym} [{tf}]: {order_exc}",
                {"signal": sig},
            )

    state.open_positions = await executor.get_open_positions()
    state.wallet_balance = await executor.get_wallet_balance()


async def run_scalp_position_guardian() -> None:
    """
    Guardião 24/7 (a cada 6 segundos) que monitora o preço a mercado e o momento do Livro/Tape
    das posições abertas (Scalp ou Sniper) e executa a venda automática caso atinjam o Take Profit,
    Stop Loss ou se o momento passar (Proteção de Topo, Virada de Fluxo ou Expiração de Tempo).
    """
    try:
        current_pos = await executor.get_open_positions()
        state.open_positions = current_pos

        if executor.last_closed_events:
            state.wallet_balance = await executor.get_wallet_balance()
            while executor.last_closed_events:
                ev = executor.last_closed_events.pop(0)
                if ev.get("reason") == "MANUAL_CLICK":
                    continue
                state.append_log(
                    "POSITION_CLOSED",
                    f"AUTO-VENDA EXECUTADA [{ev.get('reason')}]: {ev.get('symbol')} vendido por ${ev.get('received_usdt', 0):.2f} USDT @ {ev.get('exit_price', 0)} (PnL: ${ev.get('realized_pnl', 0):+.2f} USDT)",
                    ev,
                )
    except Exception as exc:
        logger.debug(f"Erro no guardião de posições: {exc}")

    try:
        if state.enabled and state.orderbook_scalp_auto and state.orderbook_cache:
            await check_auto_scalp_and_rotation()
    except Exception as rot_err:
        logger.debug(f"Aviso no ciclo de auto-scalp/rotação do guardião: {rot_err}")


# ============================================================================
# CICLO DE VIDA (LIFESPAN) & ENDPOINTS FASTAPI
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await executor.initialize()

    scheduler.add_job(
        run_walk_forward_calibration,
        trigger=CronTrigger(hour="0,6,12,18", minute="0", second="0", timezone="UTC"),
        id="walk_forward_calibrator",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        run_candle_close_sniper,
        trigger=CronTrigger(minute="0,5,10,15,20,25,30,35,40,45,50,55", second="1", timezone="UTC"),
        id="candle_close_sniper",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        run_scalp_position_guardian,
        trigger=IntervalTrigger(seconds=6),
        id="scalp_position_guardian",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        check_rocket_meme_breakouts,
        trigger=IntervalTrigger(seconds=20),
        id="rocket_meme_scanner",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        check_session_special_trades,
        trigger=IntervalTrigger(seconds=30),
        id="session_special_scanner",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    state.append_log(
        "SYSTEM_STARTUP",
        f"APScheduler iniciado [{executor.market_type.upper()}] | Valor por Trade: ${executor.order_margin_usdt:.2f} USDT | Max Posições: {state.max_concurrent_positions}.",
    )

    if os.getenv("AUTO_CALIBRATE_ON_STARTUP", "true").lower() in ("true", "1", "yes"):
        state._calibration_task = asyncio.create_task(run_walk_forward_calibration())

    yield

    scheduler.shutdown(wait=False)
    await executor.close()


app = FastAPI(
    title="Quant Terminal Sniper Microservice",
    description="Microsserviço 24/7 de calibração Walk-Forward em Polars e execução atômica Spot/Futures na Binance via CCXT.",
    version="1.2.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> HTMLResponse:
    if DASHBOARD_HTML_PATH.exists():
        return HTMLResponse(content=DASHBOARD_HTML_PATH.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Quant Terminal Sniper Running</h1>")


@app.get("/manifest.json")
async def serve_manifest() -> Response:
    p = Path(__file__).resolve().parent / "manifest.json"
    if p.exists():
        return Response(content=p.read_text(encoding="utf-8"), media_type="application/manifest+json")
    return Response(content="{}", media_type="application/manifest+json")


@app.get("/sw.js")
async def serve_sw() -> Response:
    p = Path(__file__).resolve().parent / "sw.js"
    if p.exists():
        return Response(content=p.read_text(encoding="utf-8"), media_type="application/javascript")
    return Response(content="// SW", media_type="application/javascript")


@app.get("/icon.svg")
async def serve_icon_svg() -> Response:
    p = Path(__file__).resolve().parent / "icon.svg"
    if p.exists():
        return Response(content=p.read_text(encoding="utf-8"), media_type="image/svg+xml")
    return Response(content="", media_type="image/svg+xml")


@app.get("/icon-192.png")
async def serve_icon_192():
    p = Path(__file__).resolve().parent / "icon-192.png"
    if p.exists():
        return FileResponse(p, media_type="image/png")
    return await serve_icon_svg()


@app.get("/icon-512.png")
async def serve_icon_512():
    p = Path(__file__).resolve().parent / "icon-512.png"
    if p.exists():
        return FileResponse(p, media_type="image/png")
    return await serve_icon_svg()


@app.get("/api/status")
async def get_status() -> Dict[str, Any]:
    state.wallet_balance = await executor.get_wallet_balance()
    state.open_positions = await executor.get_open_positions()

    return {
        "enabled": state.enabled,
        "market_type": executor.market_type,
        "testnet": executor.testnet,
        "authenticated": bool(state.wallet_balance.get("authenticated", True)),
        "auth_error": executor.last_auth_error,
        "order_margin_usdt": executor.order_margin_usdt,
        "default_leverage": executor.default_leverage,
        "notional_per_trade_usdt": round(executor.order_margin_usdt * executor.default_leverage, 2),
        "max_concurrent_positions": state.max_concurrent_positions,
        "max_scalp_positions": state.max_scalp_positions,
        "max_sniper_positions": state.max_sniper_positions,
        "max_rocket_positions": state.max_rocket_positions,
        "max_session_positions": state.max_session_positions,
        "scalp_positions_count": len([p for p in state.open_positions if p.get("mode") in ("SCALP_BOOK", "SCALP")]),
        "sniper_positions_count": len([p for p in state.open_positions if p.get("mode") in ("SNIPER_CANDLE", "SNIPER")]),
        "rocket_positions_count": len([p for p in state.open_positions if p.get("mode") in ("ROCKET_MEME", "ROCKET")]),
        "session_positions_count": len([p for p in state.open_positions if "SESSION" in p.get("mode", "")]),
        "rocket_margin_usdt": state.rocket_margin_usdt,
        "rocket_tp_target_pct": state.rocket_tp_target_pct,
        "rocket_sl_pct": state.rocket_sl_pct,
        "rocket_auto_enabled": state.rocket_auto_enabled,
        "session_info": get_market_session_info(),
        "session_special_enabled": state.session_special_enabled,
        "session_margin_usdt": state.session_margin_usdt,
        "last_session_scan_utc": state.last_session_scan_utc,
        "closed_trades": executor.closed_trades,
        "equity_history": executor.equity_history,
        "wallet_balance": state.wallet_balance,
        "open_positions_count": len(state.open_positions),
        "open_positions": state.open_positions,
        "trades_history": list(state.trades_history),
        "is_calibrating": state.is_calibrating,
        "last_calibration_utc": state.last_calibration_utc,
        "last_sniper_run_utc": state.last_sniper_run_utc,
        "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
        "min_profit_factor": state.min_profit_factor,
        "lookback_days": state.lookback_days,
        "timeframe_filter": state.timeframe_filter,
        "orderbook_scalp_auto": state.orderbook_scalp_auto,
        "scalp_tp_pct": state.scalp_tp_pct,
        "scalp_sl_pct": state.scalp_sl_pct,
        "scalp_target_mode": state.scalp_target_mode,
        "scalp_margin_usdt": state.scalp_margin_usdt,
        "allow_scalp_borrow_idle_capital": state.allow_scalp_borrow_idle_capital,
        "auto_momentum_exit": executor.auto_momentum_exit,
        "max_hold_minutes": executor.max_hold_minutes,
        "scalp_rotation_enabled": executor.scalp_rotation_enabled,
        "scalp_rotation_min_pnl": executor.scalp_rotation_min_pnl,
        "champions_count": len(state.champions_map),
        "champions_map": state.champions_map,
        "top_options_ranking": state.top_options_ranking,
        "execution_logs": list(state.execution_logs)[:50],
    }


@app.get("/api/reports/download")
async def download_report(period: str = "all") -> Response:
    """
    Gera e exporta relatório financeiro completo em formato CSV compatível com Excel (UTF-8 BOM).
    Suporta filtros por período:
      - 'day': Hoje / Últimas 24 horas
      - 'week': Semanal / Últimos 7 dias
      - 'month': Mensal / Últimos 30 dias
      - 'all': Histórico completo
    Inclui:
      - Resumo consolidado (depósito/base, saldo atual, lucro líquido, win rate)
      - Distribuição de PnL por modalidade (Scalp, Sniper, Memes)
      - Posições abertas no momento
      - Histórico linha a linha de compras e vendas com duração, alvos e motivos de saída
    """
    import io
    import csv

    now_utc = datetime.now(timezone.utc)
    now_ts = now_utc.timestamp()

    period_lower = period.lower().strip()
    if period_lower == "day":
        cutoff_ts = now_ts - 86400.0
        period_label = "Hoje / Últimas 24 Horas"
    elif period_lower == "week":
        cutoff_ts = now_ts - (7 * 86400.0)
        period_label = "Últimos 7 Dias (Semanal)"
    elif period_lower == "month":
        cutoff_ts = now_ts - (30 * 86400.0)
        period_label = "Últimos 30 Dias (Mensal)"
    else:
        cutoff_ts = 0.0
        period_label = "Histórico Completo"

    all_closed = list(executor.closed_trades)
    filtered_trades = [
        t for t in all_closed
        if float(t.get("timestamp_ts", 0.0)) >= cutoff_ts
    ]

    total_trades = len(filtered_trades)
    wins = [t for t in filtered_trades if float(t.get("realized_pnl", 0.0)) > 0]
    breakeven_or_loss = [t for t in filtered_trades if float(t.get("realized_pnl", 0.0)) <= 0]
    win_rate = (len(wins) / total_trades * 100.0) if total_trades > 0 else 0.0
    net_pnl = sum(float(t.get("realized_pnl", 0.0)) for t in filtered_trades)
    total_volume_traded = sum(float(t.get("received_usdt", 0.0)) for t in filtered_trades)

    scalp_trades = [t for t in filtered_trades if "SCALP" in str(t.get("mode", "")).upper()]
    scalp_pnl = sum(float(t.get("realized_pnl", 0.0)) for t in scalp_trades)

    sniper_trades = [t for t in filtered_trades if "SNIPER" in str(t.get("mode", "")).upper()]
    sniper_pnl = sum(float(t.get("realized_pnl", 0.0)) for t in sniper_trades)

    rocket_trades = [t for t in filtered_trades if "ROCKET" in str(t.get("mode", "")).upper()]
    rocket_pnl = sum(float(t.get("realized_pnl", 0.0)) for t in rocket_trades)

    wallet = await executor.get_wallet_balance()
    current_balance = float(wallet.get("total_usdt") or 0.0)
    free_balance = float(wallet.get("free_usdt") or 0.0)
    open_positions = await executor.get_open_positions()
    in_positions_val = sum(float(p.get("notional") or 0.0) for p in open_positions)

    estimated_start_balance = current_balance - net_pnl
    return_on_portfolio = (net_pnl / estimated_start_balance * 100.0) if estimated_start_balance > 0 else 0.0

    output = io.StringIO()
    writer = csv.writer(output, delimiter=";")

    writer.writerow(["RELATÓRIO FINANCEIRO & AUDITORIA DE OPERAÇÕES - QUANT SNIPER BINANCE"])
    writer.writerow(["Gerado em (UTC)", now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")])
    writer.writerow(["Período Selecionado", period_label])
    writer.writerow(["Modo de Operação", f"{executor.market_type.upper()} ({'Testnet' if executor.testnet else 'Binance Oficial Ao Vivo'})"])
    writer.writerow([])

    writer.writerow(["--- RESUMO CONSOLIDADO DO PERÍODO ---"])
    writer.writerow(["Métrica", "Valor"])
    writer.writerow(["Saldo Total Atual da Carteira", f"${current_balance:.2f} USDT"])
    writer.writerow(["Saldo Livre para Novas Operações", f"${free_balance:.2f} USDT"])
    writer.writerow(["Capital Alocado em Posições Abertas", f"${in_positions_val:.2f} USDT"])
    writer.writerow(["Saldo Inicial / Base Estimada do Período", f"${estimated_start_balance:.2f} USDT"])
    writer.writerow(["Lucro Líquido Realizado no Período", f"{'+' if net_pnl >= 0 else ''}${net_pnl:.4f} USDT"])
    writer.writerow(["Retorno Percentual sobre a Carteira", f"{'+' if return_on_portfolio >= 0 else ''}{return_on_portfolio:.2f}%"])
    writer.writerow(["Total de Operações Encerradas", total_trades])
    writer.writerow(["Operações Lucrativas (Green)", len(wins)])
    writer.writerow(["Operações 0-0 ou Stop (Loss/BE)", len(breakeven_or_loss)])
    writer.writerow(["Taxa de Acerto (Win Rate)", f"{win_rate:.1f}%"])
    writer.writerow(["Volume Total Negociado (Notional)", f"${total_volume_traded:.2f} USDT"])
    writer.writerow(["Lucro Scalp Book L2", f"{'+' if scalp_pnl >= 0 else ''}${scalp_pnl:.4f} USDT ({len(scalp_trades)} trades)"])
    writer.writerow(["Lucro Sniper Swing Trades", f"{'+' if sniper_pnl >= 0 else ''}${sniper_pnl:.4f} USDT ({len(sniper_trades)} trades)"])
    writer.writerow(["Lucro Rocket Memes", f"{'+' if rocket_pnl >= 0 else ''}${rocket_pnl:.4f} USDT ({len(rocket_trades)} trades)"])
    writer.writerow([])

    writer.writerow(["--- POSIÇÕES EM ABERTO NO MOMENTO ---"])
    if open_positions:
        writer.writerow([
            "Ativo",
            "Modalidade",
            "Prazo / Horizonte",
            "Direção",
            "Preço Entrada",
            "Preço Atual",
            "Stop Loss",
            "Take Profit",
            "Notional (USDT)",
            "PnL Não Realizado ($)",
            "PnL Flutuante (%)",
        ])
        for p in open_positions:
            writer.writerow([
                p.get("symbol", ""),
                p.get("mode", ""),
                p.get("horizon", "Swing"),
                p.get("direction", "LONG"),
                f"{float(p.get('entry_price', 0)):.6f}",
                f"{float(p.get('current_price', 0)):.6f}",
                f"{float(p.get('stop_price', 0)):.6f}",
                f"{float(p.get('take_profit_price', 0)):.6f}",
                f"${float(p.get('notional', 0)):.2f}",
                f"${float(p.get('unrealized_pnl', 0)):.4f}",
                f"{float(p.get('pnl_percentage', 0)):.2f}%",
            ])
    else:
        writer.writerow(["Nenhuma posição em aberto no momento."])
    writer.writerow([])

    writer.writerow(["--- DETALHAMENTO DE OPERAÇÕES FECHADAS (COMPRA E VENDA) ---"])
    writer.writerow([
        "ID Operação",
        "Data/Hora Encerramento (UTC)",
        "Ativo",
        "Modalidade",
        "Prazo / Horizonte",
        "Lado",
        "Preço Entrada",
        "Preço Saída",
        "Quantidade Vendida",
        "Volume Notional (USDT)",
        "Lucro/Prejuízo Líquido (USDT)",
        "Resultado (%)",
        "Duração (Minutos)",
        "Gatilho de Saída / Motivo",
    ])

    for t in filtered_trades:
        writer.writerow([
            t.get("id", ""),
            t.get("timestamp_utc", ""),
            t.get("symbol", ""),
            t.get("mode", ""),
            t.get("horizon", "Minutos"),
            t.get("direction", "LONG"),
            f"{float(t.get('entry_price', 0)):.6f}",
            f"{float(t.get('exit_price', 0)):.6f}",
            f"{float(t.get('sold_qty', 0)):.6f}",
            f"${float(t.get('received_usdt', 0)):.2f}",
            f"{'+' if float(t.get('realized_pnl', 0)) >= 0 else ''}${float(t.get('realized_pnl', 0)):.4f}",
            f"{'+' if float(t.get('pnl_pct', 0)) >= 0 else ''}{float(t.get('pnl_pct', 0)):.2f}%",
            f"{float(t.get('duration_minutes', 0)):.1f}",
            t.get("reason", ""),
        ])

    csv_data = "\ufeff" + output.getvalue()
    filename = f"relatorio_quant_sniper_{period_lower}_{now_utc.strftime('%Y%m%d_%H%M%S')}.csv"
    return Response(
        content=csv_data.encode("utf-8"),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@app.post("/api/toggle")
async def toggle_bot(
    payload: Optional[ToggleRequest] = Body(default=None),
) -> Dict[str, Any]:
    changes: List[str] = []
    market_type_changed = False
    recompute_needed = False

    if payload is None:
        state.enabled = not state.enabled
        changes.append(f"enabled={state.enabled}")
    else:
        if payload.enabled is not None:
            state.enabled = bool(payload.enabled)
            changes.append(f"enabled={state.enabled}")

        if payload.market_type is not None and payload.market_type.lower() in ("spot", "future"):
            new_mt = payload.market_type.lower()
            if new_mt != executor.market_type:
                executor.market_type = new_mt
                if new_mt == "spot":
                    executor.default_leverage = 1
                market_type_changed = True
                recompute_needed = True
                changes.append(f"Modo={new_mt.upper()}")

        if payload.order_margin_usdt is not None:
            executor.order_margin_usdt = max(1.0, float(payload.order_margin_usdt))
            changes.append(f"Margem=${executor.order_margin_usdt:.2f}")

        if payload.default_leverage is not None:
            raw_lev = int(payload.default_leverage)
            if executor.market_type == "spot" and raw_lev > 1:
                executor.default_leverage = 1
                changes.append("Alavancagem mantida em 1x (Modo SPOT direto não suporta alavancagem; use FUTURES para 2x+)")
            else:
                executor.default_leverage = 1 if executor.market_type == "spot" else max(1, min(50, raw_lev))
                changes.append(f"Alavancagem={executor.default_leverage}x")

        if payload.scalp_margin_usdt is not None:
            state.scalp_margin_usdt = max(1.0, float(payload.scalp_margin_usdt))
            changes.append(f"MargemScalp=${state.scalp_margin_usdt:.2f}")

        if payload.rocket_margin_usdt is not None:
            state.rocket_margin_usdt = max(1.0, float(payload.rocket_margin_usdt))
            changes.append(f"MargemRocket=${state.rocket_margin_usdt:.2f}")

        if payload.session_margin_usdt is not None:
            state.session_margin_usdt = max(1.0, float(payload.session_margin_usdt))
            changes.append(f"MargemSessao=${state.session_margin_usdt:.2f}")

        if payload.max_rocket_positions is not None:
            state.max_rocket_positions = max(0, min(10, int(payload.max_rocket_positions)))
            changes.append(f"VagasRocket={state.max_rocket_positions}")

        if payload.max_session_positions is not None:
            state.max_session_positions = max(0, min(10, int(payload.max_session_positions)))
            changes.append(f"VagasSessao={state.max_session_positions}")

        if payload.max_sniper_positions is not None:
            state.max_sniper_positions = max(0, min(10, int(payload.max_sniper_positions)))
            changes.append(f"VagasSniper={state.max_sniper_positions}")

        if payload.max_scalp_positions is not None:
            state.max_scalp_positions = max(0, min(10, int(payload.max_scalp_positions)))
            changes.append(f"VagasScalp={state.max_scalp_positions}")

        if payload.max_concurrent_positions is not None:
            state.max_concurrent_positions = max(1, int(payload.max_concurrent_positions))
            changes.append(f"MaxPos={state.max_concurrent_positions}")
        else:
            state.max_concurrent_positions = (
                state.max_scalp_positions + state.max_sniper_positions + state.max_rocket_positions
            )

        if payload.session_special_enabled is not None:
            state.session_special_enabled = bool(payload.session_special_enabled)
            changes.append(f"SessoesEspeciais={'ON' if state.session_special_enabled else 'OFF'}")

        if payload.min_profit_factor is not None:
            new_pf = max(1.05, float(payload.min_profit_factor))
            if abs(new_pf - state.min_profit_factor) > 0.001:
                state.min_profit_factor = new_pf
                recompute_needed = True
                changes.append(f"MinPF={state.min_profit_factor:.2f}")

        if payload.lookback_days is not None:
            new_days = max(1, min(90, int(payload.lookback_days)))
            if new_days != state.lookback_days:
                state.lookback_days = new_days
                recompute_needed = True
                changes.append(f"Janela={state.lookback_days}D")

        if payload.timeframe_filter is not None:
            tf_raw = payload.timeframe_filter.strip()
            new_tf = "ALL" if tf_raw.upper() == "ALL" else tf_raw.lower()
            if new_tf in ("ALL", "5m", "15m", "30m", "1h") and new_tf != state.timeframe_filter:
                state.timeframe_filter = new_tf
                recompute_needed = True
                changes.append(f"Timeframe={state.timeframe_filter}")

        if payload.orderbook_scalp_auto is not None:
            state.orderbook_scalp_auto = bool(payload.orderbook_scalp_auto)
            changes.append(f"AutoScalpBook={'ON' if state.orderbook_scalp_auto else 'OFF'}")

        if payload.allow_scalp_borrow_idle_capital is not None:
            state.allow_scalp_borrow_idle_capital = bool(payload.allow_scalp_borrow_idle_capital)
            changes.append(f"EmprestarSaldoParado={'ON' if state.allow_scalp_borrow_idle_capital else 'OFF'}")

        if payload.proxy_url is not None:
            executor.proxy_url = payload.proxy_url.strip()
            changes.append("Proxy atualizado")

        if payload.binance_api_key is not None and payload.binance_api_secret is not None:
            new_testnet = payload.testnet if payload.testnet is not None else executor.testnet
            new_mt = payload.market_type.lower() if payload.market_type else executor.market_type
            await executor.update_credentials(
                api_key=payload.binance_api_key,
                api_secret=payload.binance_api_secret,
                testnet=new_testnet,
                market_type=new_mt,
            )
            try:
                env_path = Path(__file__).parent.parent / ".env"
                env_content = (
                    f"BINANCE_API_KEY={executor.api_key}\n"
                    f"BINANCE_API_SECRET={executor.api_secret}\n"
                    f"BINANCE_TESTNET={'true' if executor.testnet else 'false'}\n"
                    f"BINANCE_MARKET_TYPE={executor.market_type}\n"
                )
                env_path.write_text(env_content, encoding="utf-8")
            except Exception:
                pass
            state.wallet_balance = await executor.get_wallet_balance()
            state.open_positions = await executor.get_open_positions()
            if state.wallet_balance.get("error"):
                state.append_log("AUTH_WARNING", state.wallet_balance["error"])
            else:
                changes.append(
                    f"Credenciais Binance {executor.market_type.upper()} ativas (Saldo: ${state.wallet_balance.get('total_usdt', 0):.2f} USDT)"
                )

    if recompute_needed:
        if state.ohlcv_cache:
            await asyncio.to_thread(_recompute_champions_sync)
            changes.append(f"{len(state.champions_map)} opções recalibradas")
        elif not state.is_calibrating:
            state._calibration_task = asyncio.create_task(run_walk_forward_calibration())

    status_label = "ARMADO" if state.enabled else "PAUSADO (KILL SWITCH)"
    summary = ", ".join(changes) if changes else status_label
    state.append_log(
        "RUNTIME_CONFIG_UPDATED",
        f"Configuração [{executor.market_type.upper()} | {state.lookback_days}D | TF:{state.timeframe_filter}]: {summary}",
    )

    return {
        "status": "ok",
        "enabled": state.enabled,
        "market_type": executor.market_type,
        "order_margin_usdt": executor.order_margin_usdt,
        "default_leverage": executor.default_leverage,
        "notional_per_trade_usdt": round(executor.order_margin_usdt * executor.default_leverage, 2),
        "max_concurrent_positions": state.max_concurrent_positions,
        "max_scalp_positions": state.max_scalp_positions,
        "max_sniper_positions": state.max_sniper_positions,
        "max_rocket_positions": state.max_rocket_positions,
        "scalp_margin_usdt": state.scalp_margin_usdt,
        "rocket_margin_usdt": state.rocket_margin_usdt,
        "rocket_tp_target_pct": state.rocket_tp_target_pct,
        "rocket_sl_pct": state.rocket_sl_pct,
        "rocket_auto_enabled": state.rocket_auto_enabled,
        "scalp_positions_count": len([p for p in state.open_positions if p.get("mode") in ("SCALP_BOOK", "SCALP")]),
        "sniper_positions_count": len([p for p in state.open_positions if p.get("mode") in ("SNIPER_CANDLE", "SNIPER")]),
        "rocket_positions_count": len([p for p in state.open_positions if p.get("mode") in ("ROCKET_MEME", "ROCKET")]),
        "min_profit_factor": state.min_profit_factor,
        "lookback_days": state.lookback_days,
        "timeframe_filter": state.timeframe_filter,
        "orderbook_scalp_auto": state.orderbook_scalp_auto,
        "allow_scalp_borrow_idle_capital": state.allow_scalp_borrow_idle_capital,
        "auto_momentum_exit": executor.auto_momentum_exit,
        "scalp_rotation_enabled": executor.scalp_rotation_enabled,
        "scalp_rotation_min_pnl": executor.scalp_rotation_min_pnl,
        "champions_count": len(state.champions_map),
        "champions_map": state.champions_map,
        "top_options_ranking": state.top_options_ranking,
        "authenticated": bool(state.wallet_balance.get("authenticated", True)),
        "auth_error": executor.last_auth_error,
        "testnet": executor.testnet,
        "wallet_balance": state.wallet_balance,
    }


@app.post("/api/calibrate")
async def trigger_calibration() -> Dict[str, Any]:
    if state.is_calibrating:
        return {"status": "busy", "message": "Calibração Walk-Forward já está em execução."}

    state._calibration_task = asyncio.create_task(run_walk_forward_calibration())
    return {"status": "started", "message": "Calibração Walk-Forward iniciada em background."}


@app.get("/api/orderbook-scalp")
async def get_orderbook_scalp() -> Dict[str, Any]:
    """
    Escaneia em tempo real o Livro de Ofertas (Order Book Nível 2) + Fluxo de Agressão (Taker Buy)
    das principais moedas líquidas e dispara automaticamente se `orderbook_scalp_auto` estiver ativo.
    """
    import time as _t

    now_sec = _t.time()
    if (now_sec - state.orderbook_last_ts) >= 3.5 or not state.orderbook_cache:
        target_syms = list(state.symbols_meta.keys())[:16]
        if not target_syms:
            target_syms = [
                "BTC/USDT",
                "ETH/USDT",
                "SOL/USDT",
                "BNB/USDT",
                "XRP/USDT",
                "DOGE/USDT",
                "PEPE/USDT",
                "ZEC/USDT",
                "NEAR/USDT",
                "UNI/USDT",
                "XAUT/USDT",
            ]
        state.orderbook_cache = await executor.scan_orderbook_scalp(
            target_syms,
            scalp_tp_pct=state.scalp_tp_pct,
            scalp_sl_pct=state.scalp_sl_pct,
            target_mode=state.scalp_target_mode,
        )
        state.orderbook_last_ts = now_sec

        # Se Auto-Scalp do Livro de Ofertas estiver ligado, avalia entradas e rotação inteligente
        if state.enabled and state.orderbook_scalp_auto:
            await check_auto_scalp_and_rotation()

    return {
        "status": "ok",
        "auto_enabled": state.orderbook_scalp_auto,
        "auto_momentum_exit": executor.auto_momentum_exit,
        "max_hold_minutes": executor.max_hold_minutes,
        "scalp_rotation_enabled": executor.scalp_rotation_enabled,
        "scalp_rotation_min_pnl": executor.scalp_rotation_min_pnl,
        "scalp_tp_pct": state.scalp_tp_pct,
        "scalp_sl_pct": state.scalp_sl_pct,
        "scalp_target_mode": state.scalp_target_mode,
        "scalp_margin_usdt": state.scalp_margin_usdt,
        "updated_utc": datetime.now(timezone.utc).strftime("%H:%M:%S UTC"),
        "opportunities": state.orderbook_cache,
    }


@app.post("/api/scalp/config")
async def update_scalp_config(req: ScalpConfigRequest) -> Dict[str, Any]:
    """
    Atualiza os parâmetros dedicados do Terminal de Scalp Separado (Alvo TP %, Stop Loss %,
    Modo Fixo vs Paredão, Mão por Trade, Auto-Scalp e Auto-Venda por Perda de Momento)
    e opcionalmente recalcula o preço de venda das posições abertas imediatamente.
    """
    changes: List[str] = []
    if req.scalp_tp_pct is not None:
        state.scalp_tp_pct = max(0.25, min(5.0, float(req.scalp_tp_pct)))
        changes.append(f"TP=+{state.scalp_tp_pct:.2f}%")
    if req.scalp_sl_pct is not None:
        state.scalp_sl_pct = max(0.20, min(3.0, float(req.scalp_sl_pct)))
        changes.append(f"SL=-{state.scalp_sl_pct:.2f}%")
    if req.scalp_target_mode is not None and req.scalp_target_mode.upper() in ("FIXED", "WALL"):
        state.scalp_target_mode = req.scalp_target_mode.upper()
        changes.append(f"ModoAlvo={state.scalp_target_mode}")
    if req.scalp_margin_usdt is not None:
        state.scalp_margin_usdt = max(5.0, float(req.scalp_margin_usdt))
        changes.append(f"MãoScalp=${state.scalp_margin_usdt:.2f}")
    if req.orderbook_scalp_auto is not None:
        state.orderbook_scalp_auto = bool(req.orderbook_scalp_auto)
        changes.append(f"AutoScalp={'ON' if state.orderbook_scalp_auto else 'OFF'}")
    if req.auto_momentum_exit is not None:
        executor.auto_momentum_exit = bool(req.auto_momentum_exit)
        changes.append(f"AutoVendaMomento={'ON' if executor.auto_momentum_exit else 'OFF'}")
    if req.max_hold_minutes is not None:
        executor.max_hold_minutes = max(3, min(120, int(req.max_hold_minutes)))
        changes.append(f"TempoMáx={executor.max_hold_minutes}m")
    if req.scalp_rotation_enabled is not None:
        executor.scalp_rotation_enabled = bool(req.scalp_rotation_enabled)
        changes.append(f"Rotação={'ON' if executor.scalp_rotation_enabled else 'OFF'}")
    if req.scalp_rotation_min_pnl is not None:
        executor.scalp_rotation_min_pnl = max(0.15, min(2.0, float(req.scalp_rotation_min_pnl)))
        changes.append(f"MinPnLRot=+{executor.scalp_rotation_min_pnl:.2f}%")
    if req.max_scalp_positions is not None:
        state.max_scalp_positions = max(1, min(10, int(req.max_scalp_positions)))
        state.max_concurrent_positions = state.max_scalp_positions + state.max_sniper_positions
        changes.append(f"VagasScalp={state.max_scalp_positions}")

    updated_open: List[str] = []
    if req.apply_to_open:
        for sym, bkt in list(executor.real_spot_brackets.items()):
            entry_p = float(bkt.get("entry_price") or 0.0)
            if entry_p > 0:
                bkt["take_profit_price"] = round(entry_p * (1.0 + (state.scalp_tp_pct / 100.0)), 6)
                bkt["stop_price"] = round(entry_p * (1.0 - (state.scalp_sl_pct / 100.0)), 6)
                bkt["mode"] = "SCALP_BOOK"
                updated_open.append(f"{sym} (TP={bkt['take_profit_price']} | SL={bkt['stop_price']})")
        executor.save_brackets()

    state.orderbook_last_ts = 0.0
    state.open_positions = await executor.get_open_positions()
    msg_open = f" | Aplicado em: {', '.join(updated_open)}" if updated_open else ""
    state.append_log(
        "SCALP_CONFIG_UPDATED",
        f"Configuração do Scalp Separado atualizada ({', '.join(changes)}){msg_open}",
    )
    return {
        "status": "ok",
        "scalp_tp_pct": state.scalp_tp_pct,
        "scalp_sl_pct": state.scalp_sl_pct,
        "scalp_target_mode": state.scalp_target_mode,
        "scalp_margin_usdt": state.scalp_margin_usdt,
        "orderbook_scalp_auto": state.orderbook_scalp_auto,
        "allow_scalp_borrow_idle_capital": state.allow_scalp_borrow_idle_capital,
        "auto_momentum_exit": executor.auto_momentum_exit,
        "max_hold_minutes": executor.max_hold_minutes,
        "scalp_rotation_enabled": executor.scalp_rotation_enabled,
        "scalp_rotation_min_pnl": executor.scalp_rotation_min_pnl,
        "open_positions": state.open_positions,
    }


@app.post("/api/positions/close")
async def close_open_position(req: ClosePositionRequest) -> Dict[str, Any]:
    """Vende imediatamente a mercado 100% da posição Spot aberta para USDT."""
    try:
        res = await executor.close_spot_position(req.symbol, reason="MANUAL_CLICK")
        state.open_positions = await executor.get_open_positions()
        state.wallet_balance = await executor.get_wallet_balance()
        state.append_log(
            "POSITION_CLOSED",
            f"VENDA MANUAL A MERCADO EXECUTADA: {res['symbol']} vendido por ${res.get('received_usdt', 0):.2f} USDT @ {res.get('exit_price', 0)} (PnL: ${res.get('realized_pnl', 0):+.2f} USDT)",
            res,
        )
        return {"status": "ok", "receipt": res}
    except Exception as exc:
        state.append_log("ORDER_ERROR", f"Falha ao vender posição {req.symbol}: {exc}")
        return {"status": "error", "message": str(exc)}


@app.post("/api/positions/close-all")
async def close_all_open_positions() -> Dict[str, Any]:
    """Vende imediatamente a mercado TODAS as posições abertas na conta para USDT."""
    open_pos = await executor.get_open_positions()
    receipts: List[Dict[str, Any]] = []
    for p in open_pos:
        sym = p["symbol"]
        try:
            r = await executor.close_spot_position(sym, reason="MANUAL_CLOSE_ALL")
            receipts.append(r)
            state.append_log(
                "POSITION_CLOSED",
                f"VENDA TOTAL A MERCADO: {r['symbol']} vendido por ${r.get('received_usdt', 0):.2f} USDT @ {r.get('exit_price', 0)} (PnL: ${r.get('realized_pnl', 0):+.2f} USDT)",
                r,
            )
        except Exception as exc:
            state.append_log("ORDER_ERROR", f"Erro ao zerar {sym}: {exc}")
    state.open_positions = await executor.get_open_positions()
    state.wallet_balance = await executor.get_wallet_balance()
    return {"status": "ok", "closed_count": len(receipts), "receipts": receipts}


@app.post("/api/orderbook-scalp/execute")
async def execute_orderbook_scalp(req: ScalpExecuteRequest) -> Dict[str, Any]:
    """
    Dispara uma ordem atômica de Scalp baseada nas paredes de liquidez do Livro de Ofertas.
    Suporta Empréstimo Dinâmico de Capital Parado: se as vagas dedicadas de Scalp estiverem cheias,
    mas houver vagas ociosas do portfólio (Sniper/Memes sem posição) e saldo livre na Binance,
    o robô permite usar o dinheiro parado para executar a oportunidade!
    """
    open_positions = await executor.get_open_positions()
    state.open_positions = open_positions
    state.wallet_balance = await executor.get_wallet_balance()

    scalp_positions = [p for p in open_positions if p.get("mode") in ("SCALP_BOOK", "SCALP")]
    total_positions = len(open_positions)
    free_usdt = float(state.wallet_balance.get("free_usdt") or 0.0)

    is_borrowed_slot = False
    if len(scalp_positions) >= state.max_scalp_positions:
        if not state.allow_scalp_borrow_idle_capital:
            return {
                "status": "blocked",
                "message": f"Limite de vagas dedicadas de Scalp atingido ({len(scalp_positions)}/{state.max_scalp_positions}). Ative 'Usar Saldo Parado' para emprestar vagas ociosas.",
            }

        # Verifica se o portfólio global já atingiu o limite de vagas
        if total_positions >= state.max_concurrent_positions:
            return {
                "status": "blocked",
                "message": f"Todas as {state.max_concurrent_positions} vagas da carteira estão ocupadas ({len(scalp_positions)} Scalps, {total_positions - len(scalp_positions)} Trades). Aguarde a finalização de uma operação.",
            }

        if free_usdt < (state.scalp_margin_usdt * 0.95):
            return {
                "status": "blocked",
                "message": f"Saldo livre insuficiente na Binance (${free_usdt:.2f} USDT livre; necessário ${state.scalp_margin_usdt:.2f} USDT).",
            }

        is_borrowed_slot = True

    order_receipt = await executor.execute_atomic_order(
        symbol=req.symbol,
        direction="LONG",
        entry_price=req.entry_price,
        stop_price=req.stop_price,
        take_profit_price=req.take_profit_price,
        margin_usdt=state.scalp_margin_usdt,
        mode="SCALP_BOOK",
        timeframe="BOOK-L2",
        strategy_name=f"Order Book Scalp ({state.scalp_target_mode} +{state.scalp_tp_pct:.2f}%){' [POOLING]' if is_borrowed_slot else ''}",
        horizon="Minutos (~1 a 12m)",
    )
    trade_record = {
        "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "symbol": req.symbol,
        "timeframe": "BOOK-L2",
        "strategy_name": f"Order Book Scalp ({state.scalp_target_mode} +{state.scalp_tp_pct:.2f}%){' [POOLING]' if is_borrowed_slot else ''}",
        "direction": "LONG",
        "entry_price": order_receipt["entry_price"],
        "stop_price": order_receipt["stop_price"],
        "take_profit_price": order_receipt["take_profit_price"],
        "margin_usdt": order_receipt["margin_usdt"],
        "leverage": order_receipt["leverage"],
        "notional_usdt": order_receipt["notional_usdt"],
        "entry_order_id": order_receipt["entry_order_id"],
        "borrowed_idle_capital": is_borrowed_slot,
    }
    state.trades_history.appendleft(trade_record)

    log_tag = "SCALP_IDLE_BORROW_FIRED" if is_borrowed_slot else "ATOMIC_ORDER_FIRED"
    borrow_note = " • (Vaga Emprestada do Saldo Parado!)" if is_borrowed_slot else ""
    state.append_log(
        log_tag,
        f"SCALP LIVRO DE OFERTAS DISPARADO{borrow_note}: {req.symbol} [BOOK-L2] LONG (${order_receipt['notional_usdt']} @ {order_receipt['entry_price']}) | SL: {order_receipt['stop_price']} | TP: {order_receipt['take_profit_price']}",
        {"order_receipt": order_receipt, "borrowed_idle_capital": is_borrowed_slot},
    )
    state.open_positions = await executor.get_open_positions()
    state.wallet_balance = await executor.get_wallet_balance()
    return {"status": "ok", "order_receipt": order_receipt, "borrowed_idle_capital": is_borrowed_slot}


async def check_auto_scalp_and_rotation() -> None:
    """
    Avalia a execução automática do Scalp e a Rotação Ativa de Capital:
    1. Se houver vagas livres dedicadas ou vagas ociosas emprestadas do saldo parado:
       dispara na melhor oportunidade qualificada (Antecipação no Fundo ou Demanda Dominante).
    2. Se as vagas estiverem cheias e a Rotação Ativa estiver ON:
       - Detecta se há uma candidata explosiva no radar.
       - Só rotaciona se a posição atual já cobriu todas as taxas (PnL >= +0.22%) e momento estagnou.
    """
    if not (state.enabled and state.orderbook_scalp_auto and state.orderbook_cache):
        return

    open_positions = state.open_positions
    open_syms = {p["symbol"] for p in open_positions}
    scalp_positions = [p for p in open_positions if p.get("mode") in ("SCALP_BOOK", "SCALP")]
    dedicated_slots = max(0, state.max_scalp_positions - len(scalp_positions))

    total_open = len(open_positions)
    portfolio_idle_slots = max(0, state.max_concurrent_positions - total_open)
    free_usdt = float(state.wallet_balance.get("free_usdt") or 0.0)

    # Determina vagas disponíveis para Scalp (Dedicadas + Empréstimo de Saldo Parado)
    if dedicated_slots > 0:
        available_slots = dedicated_slots
    elif state.allow_scalp_borrow_idle_capital and portfolio_idle_slots > 0 and free_usdt >= (state.scalp_margin_usdt * 0.95):
        available_slots = portfolio_idle_slots
    else:
        available_slots = 0

    if available_slots > 0:
        for item in state.orderbook_cache:
            if item.get("symbol") in open_syms:
                continue

            # Qualifica se tem sinal de Antecipação no Suporte OU Demanda Dominante (Bloqueia Clímax de Topo)
            is_promising = bool(
                item.get("signal_ready")
                and not item.get("is_climax_top")
                and (
                    item.get("is_anticipation")
                    or (item.get("buy_pressure_pct", 0) >= 59.0 and item.get("taker_buy_pct", 0) >= 52.0 and item.get("range_position_pct", 50) <= 68.0)
                )
            )

            if is_promising:
                await execute_orderbook_scalp(
                    ScalpExecuteRequest(
                        symbol=item["symbol"],
                        entry_price=item["entry_price"],
                        stop_price=item["stop_price"],
                        take_profit_price=item["take_profit_price"],
                    )
                )
                break
    elif executor.scalp_rotation_enabled and len(scalp_positions) >= state.max_scalp_positions:
        candidate = None
        for item in state.orderbook_cache:
            if (
                item.get("signal_ready")
                and item.get("scalp_score", 0) >= executor.scalp_rotation_min_score
                and item.get("buy_pressure_pct", 0) >= 60.0
                and item.get("taker_buy_pct", 0) >= 53.0
                and item["symbol"] not in open_syms
            ):
                candidate = item
                break

        if candidate:
            rotatable = []
            for p in scalp_positions:
                p_pnl = float(p.get("pnl_pct") or 0.0)
                p_age = float(p.get("age_minutes") or 0.0)
                p_mom = float(p.get("live_buy_pressure_pct") or 50.0)
                p_tak = float(p.get("live_taker_buy_pct") or 50.0)

                # Critério de proteção: PnL >= min_pnl (+0.22% cobre round-trip fee + spread), idade >= 2.0 min, e momento mais fraco
                if p_pnl >= executor.scalp_rotation_min_pnl and p_age >= 2.0:
                    delta_score = candidate["scalp_score"] - ((p_mom * 0.58) + (p_tak * 0.42))
                    if delta_score >= 12.0 or p_mom <= 48.0 or p_tak <= 48.0:
                        rotatable.append((delta_score, p_age, p))

            if rotatable:
                rotatable.sort(key=lambda x: (x[0], x[1]), reverse=True)
                victim = rotatable[0][2]
                v_sym = victim["symbol"]
                v_pnl = float(victim.get("pnl_pct") or 0.0)

                reason = f"ROTACAO_OPORTUNIDADE (Saindo de {v_sym} em +{v_pnl:.2f}% [Taxas Cobertas] -> Gira para {candidate['symbol']} Score {candidate['scalp_score']})"
                logger.info(f"🔄 Executando rotação de capital: {reason}")
                try:
                    await executor.close_spot_position(v_sym, reason=reason)
                    await asyncio.sleep(0.4)
                    await execute_orderbook_scalp(
                        ScalpExecuteRequest(
                            symbol=candidate["symbol"],
                            entry_price=candidate["entry_price"],
                            stop_price=candidate["stop_price"],
                            take_profit_price=candidate["take_profit_price"],
                        )
                    )
                    state.append_log(
                        "CAPITAL_ROTATION",
                        f"🔄 ROTAÇÃO ATIVA: Fechado {v_sym} (+{v_pnl:.2f}%) e capital girado imediatamente para {candidate['symbol']} (Score {candidate['scalp_score']}).",
                        {"closed_symbol": v_sym, "target_symbol": candidate["symbol"], "candidate_score": candidate["scalp_score"]},
                    )
                except Exception as rot_err:
                    logger.warning(f"Erro ao processar rotação de capital: {rot_err}")


MEME_SYMBOLS: List[str] = [
    "PEPE/USDT",
    "DOGE/USDT",
    "SHIB/USDT",
    "WIF/USDT",
    "BONK/USDT",
    "FLOKI/USDT",
    "NEIRO/USDT",
]


async def check_rocket_meme_breakouts() -> None:
    """
    Radar de Ignição em Tempo Real para Memecoins / Alta Volatilidade (10-15%+ com Trailing Stop).
    Executa a cada 20 segundos se houver vaga livre de Rocket Memes.
    """
    if not (state.enabled and state.rocket_auto_enabled):
        return
    if executor.market_type != "spot":
        return

    rocket_positions = [p for p in state.open_positions if p.get("mode") in ("ROCKET_MEME", "ROCKET")]
    if len(rocket_positions) >= state.max_rocket_positions:
        return

    free_usdt = float(state.wallet_balance.get("free_usdt") or 0.0)
    if free_usdt < 5.20:
        return

    open_symbols = {p.get("symbol") for p in state.open_positions}

    for sym in MEME_SYMBOLS:
        if sym in open_symbols:
            continue

        raw_id = sym.replace("/", "")
        try:
            assert executor.http_client is not None
            resp = await executor.http_client.get(
                f"{VISION_BASE_URL}/klines",
                params={"symbol": raw_id, "interval": "15m", "limit": 25},
            )
            if resp.status_code != 200:
                continue
            klines = resp.json()
            if len(klines) < 22:
                continue

            last_c = klines[-1]
            prev_klines = klines[:-1]
            close_p = float(last_c[4])
            open_p = float(last_c[1])
            high_p = float(last_c[2])
            vol_cur = float(last_c[5])

            avg_vol = sum(float(k[5]) for k in prev_klines[-20:]) / 20.0
            vol_ratio = (vol_cur / avg_vol) if avg_vol > 0 else 1.0
            donchian_high = max(float(k[2]) for k in prev_klines[-20:])

            # Condição de Ignição Rocket: Volume 15m >= 1.6x E fechamento próximo da máxima
            is_igniting = (
                (vol_ratio >= 1.6 and close_p > open_p and close_p >= (high_p * 0.992))
                or (close_p >= donchian_high and vol_ratio >= 1.30 and close_p > open_p)
            )
            if not is_igniting:
                continue

            mom = await executor._check_position_momentum(raw_id, close_p)
            if mom["buy_pressure_pct"] < 50.0:
                continue

            entry_p = close_p
            stop_p = round(entry_p * (1.0 - (state.rocket_sl_pct / 100.0)), 6)
            tp_p = round(entry_p * (1.0 + (state.rocket_tp_target_pct / 100.0)), 6)

            order_receipt = await executor.execute_atomic_order(
                symbol=sym,
                direction="LONG",
                entry_price=entry_p,
                stop_price=stop_p,
                take_profit_price=tp_p,
                margin_usdt=state.rocket_margin_usdt,
                mode="ROCKET_MEME",
                timeframe="15m",
                strategy_name="Rocket Meme Breakout + Trailing",
                horizon="Minutos a Horas (~15m a 6h)",
            )

            state.append_log(
                "ROCKET_MEME_FIRED",
                (
                    f"🚀 ROCKET MEME DISPARADO: {sym} [15m] LONG "
                    f"(${order_receipt['notional_usdt']} @ {order_receipt['entry_price']}) | "
                    f"Alvo: +{state.rocket_tp_target_pct:.1f}% (${order_receipt['take_profit_price']}) • "
                    f"SL: -{state.rocket_sl_pct:.1f}% • Trailing Escadinha Ativo • Vol: {vol_ratio:.1f}x"
                ),
                {"order_receipt": order_receipt, "vol_ratio": vol_ratio},
            )
            state.open_positions = await executor.get_open_positions()
            state.wallet_balance = await executor.get_wallet_balance()
            break
        except Exception as exc:
            logger.debug(f"Erro ao avaliar ignição Rocket {sym}: {exc}")


# ============================================================================
# MOTOR DE SESSÕES INSTITUCIONAIS: VIRADA DA CHINA (00h/08h UTC) & ALMOÇO EUA (16h-18h UTC)
# ============================================================================

async def check_session_special_trades(force_scan: bool = False) -> Dict[str, Any]:
    """
    Motor 24/7 de Janelas Especiais Institucionais:
    - Virada da China #1 (23:30 - 01:30 UTC): Rompimento de Expansão & Agressão (Range 1.8x + Volume 2x).
    - Virada da China #2 (07:30 - 09:00 UTC): Rompimento & Handover Londres (Range 1.8x + Volume 2x).
    - Almoço nos EUA (16:00 - 18:00 UTC): F1 Exaustão & Reversão à VWAP (Afastamento >1.5% + Clímax Trades).
    """
    sess = get_market_session_info()
    state.last_session_scan_utc = sess["clock_utc"]

    if not force_scan and not (state.enabled and state.session_special_enabled):
        return {"status": "SKIPPED", "reason": "Bot ou Sessões Especiais desativados.", "session": sess}

    if not force_scan and not sess["is_special"]:
        return {"status": "STANDBY", "reason": "Fora de janelas institucionais especiais.", "session": sess}

    if executor.market_type != "spot":
        return {"status": "SKIPPED", "reason": "Modo spot ativo.", "session": sess}

    session_positions = [p for p in state.open_positions if "SESSION" in p.get("mode", "")]
    if len(session_positions) >= state.max_session_positions and not force_scan:
        return {"status": "FULL", "reason": f"Vagas de Sessão atingidas ({len(session_positions)}/{state.max_session_positions}).", "session": sess}

    free_usdt = float(state.wallet_balance.get("free_usdt") or 0.0)
    if free_usdt < 5.20 and not force_scan:
        return {"status": "INSUFFICIENT_FUNDS", "reason": f"Saldo livre insuficiente (${free_usdt:.2f} < $5.20).", "session": sess}

    open_symbols = {p.get("symbol") for p in state.open_positions}
    scanned_results: List[Dict[str, Any]] = []
    fired_trade: Optional[Dict[str, Any]] = None

    for sym in SESSION_SYMBOLS:
        if sym in open_symbols and not force_scan:
            scanned_results.append({"symbol": sym, "evaluated": False, "reason": "Posição já aberta neste ativo."})
            continue

        raw_id = sym.replace("/", "")
        try:
            assert executor.http_client is not None
            resp = await executor.http_client.get(
                f"{VISION_BASE_URL}/klines",
                params={"symbol": raw_id, "interval": "5m", "limit": 40},
            )
            if resp.status_code != 200:
                scanned_results.append({"symbol": sym, "evaluated": False, "reason": f"HTTP {resp.status_code}"})
                continue

            klines = resp.json()
            if len(klines) < 32:
                scanned_results.append({"symbol": sym, "evaluated": False, "reason": "Histórico insuficiente de 5m."})
                continue

            # Anti-repainting: avalia o candle fechado [-2]
            closed_c = klines[-2]
            prev_klines = klines[:-2]

            c_open = float(closed_c[1])
            c_high = float(closed_c[2])
            c_low = float(closed_c[3])
            c_close = float(closed_c[4])
            c_vol = float(closed_c[5])
            c_trades = int(closed_c[8]) if len(closed_c) > 8 else 0

            # Médias e volatilidade
            prev_closes = [float(k[4]) for k in prev_klines[-21:]]
            sma_21 = sum(prev_closes) / len(prev_closes) if prev_closes else c_close

            ranges = [(float(k[2]) - float(k[3])) for k in prev_klines[-30:]]
            avg_range_30 = sum(ranges) / len(ranges) if ranges else 1e-6
            cur_range = c_high - c_low
            range_ratio = (cur_range / avg_range_30) if avg_range_30 > 0 else 1.0

            vols = [float(k[5]) for k in prev_klines[-30:]]
            avg_vol_30 = sum(vols) / len(vols) if vols else 1.0
            vol_ratio = (c_vol / avg_vol_30) if avg_vol_30 > 0 else 1.0

            # Cálculo da VWAP móvel de 24 períodos
            recent_24 = prev_klines[-24:] + [closed_c]
            sum_pv = sum(((float(k[2]) + float(k[3]) + float(k[4])) / 3.0) * float(k[5]) for k in recent_24)
            sum_v = sum(float(k[5]) for k in recent_24)
            vwap_24 = (sum_pv / sum_v) if sum_v > 0 else c_close
            dist_vwap_pct = ((c_close - vwap_24) / vwap_24) * 100.0

            lower_wick = (min(c_open, c_close) - c_low)
            lower_wick_ratio = (lower_wick / cur_range) if cur_range > 0 else 0.0

            is_green = (c_close > c_open)
            body_ratio = (abs(c_close - c_open) / cur_range) if cur_range > 0 else 0.0

            signal_fired = False
            trigger_reason = ""

            if sess["session_id"] in ("CHINA_VIRADA_1", "CHINA_VIRADA_2"):
                # Fórmula Rompimento & Agressão China:
                # close > sma_21 && range > 1.70 * avg_range && volume > 1.70 * avg_vol && is_green == 1
                cond_trend = (c_close > sma_21)
                cond_range = (range_ratio >= 1.70)
                cond_vol = (vol_ratio >= 1.70)
                cond_green = is_green and (body_ratio >= 0.45)

                if cond_trend and cond_range and cond_vol and cond_green:
                    signal_fired = True
                    trigger_reason = f"Gatilho Rompimento China atendido (Range {range_ratio:.2f}x | Vol {vol_ratio:.2f}x | Body {body_ratio:.2f})"
                else:
                    trigger_reason = f"Aguardando (Range: {range_ratio:.2f}x/1.7x | Vol: {vol_ratio:.2f}x/1.7x | SMA21: {'OK' if cond_trend else 'Abaixo'})"

            elif sess["session_id"] == "US_LUNCH":
                # Fórmula F1 Exaustão & Reversão à VWAP:
                # Preço esticou para baixo (> 1.35% abaixo da VWAP), volume de clímax e cauda inferior compradora
                cond_vwap = (dist_vwap_pct <= -1.35)
                cond_climax = (vol_ratio >= 1.35)
                cond_rejection = (lower_wick_ratio >= 0.25) or is_green

                if cond_vwap and cond_climax and cond_rejection:
                    signal_fired = True
                    trigger_reason = f"Gatilho Exaustão VWAP atendido (Dist VWAP: {dist_vwap_pct:.2f}% | Vol Clímax: {vol_ratio:.2f}x | Cauda: {lower_wick_ratio:.2f})"
                else:
                    trigger_reason = f"Aguardando (Dist VWAP: {dist_vwap_pct:.2f}% / <= -1.35% | Vol: {vol_ratio:.2f}x)"
            else:
                trigger_reason = "Sessão regular: varredura em standby para janelas especiais."

            candle_ts = int(closed_c[0])
            candle_key = f"SESSION:{sym}:{sess['session_id']}:{candle_ts}"

            diag = {
                "symbol": sym,
                "price": c_close,
                "sma_21": round(sma_21, 6),
                "range_ratio": round(range_ratio, 2),
                "vol_ratio": round(vol_ratio, 2),
                "dist_vwap_pct": round(dist_vwap_pct, 2),
                "lower_wick_ratio": round(lower_wick_ratio, 2),
                "body_ratio": round(body_ratio, 2),
                "is_green": is_green,
                "signal_fired": signal_fired,
                "reason": trigger_reason,
            }
            scanned_results.append(diag)

            if signal_fired and not force_scan and candle_key not in state._dispatched_candles:
                state._dispatched_candles.add(candle_key)
                entry_p = c_close
                stop_p = round(entry_p * 0.9925, 6)  # -0.75%
                tp_p = round(entry_p * 1.015, 6)    # +1.50% (1:2 R:R)
                mode_label = f"SNIPER_SESSION_{'CHINA' if 'CHINA' in sess['session_id'] else 'US_LUNCH'}"

                order_receipt = await executor.execute_atomic_order(
                    symbol=sym,
                    direction="LONG",
                    entry_price=entry_p,
                    stop_price=stop_p,
                    take_profit_price=tp_p,
                    margin_usdt=state.session_margin_usdt,
                    mode=mode_label,
                    timeframe="5m",
                    strategy_name=sess["formula_name"],
                    horizon="15 a 45 minutos",
                )

                fired_trade = {
                    "symbol": sym,
                    "session": sess["session_name"],
                    "order_receipt": order_receipt,
                }

                state.append_log(
                    "SESSION_ORDER_FIRED",
                    (
                        f"🌏 DISPARO DE SESSÃO INSTITUCIONAL: {sym} [5m] LONG "
                        f"(${order_receipt['notional_usdt']} @ {order_receipt['entry_price']}) | "
                        f"Sessão: {sess['session_name']} | Alvo: +1.50% (${order_receipt['take_profit_price']}) | "
                        f"SL: -0.75% (${order_receipt['stop_price']})"
                    ),
                    {"session_info": sess, "order_receipt": order_receipt, "diagnostics": diag},
                )

                state.open_positions = await executor.get_open_positions()
                state.wallet_balance = await executor.get_wallet_balance()
                break

        except Exception as sym_err:
            scanned_results.append({"symbol": sym, "evaluated": False, "reason": str(sym_err)[:100]})

    return {
        "status": "COMPLETED",
        "session": sess,
        "scanned_count": len(scanned_results),
        "diagnostics": scanned_results,
        "fired_trade": fired_trade,
    }


@app.post("/api/session/trigger")
async def trigger_session_scan() -> Dict[str, Any]:
    """Dispara imediatamente a varredura das fórmulas institucionais e retorna o diagnóstico auditado."""
    return await check_session_special_trades(force_scan=True)
