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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import Body, FastAPI
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from app.engine_polars import (
    ROUND_TRIP_COST_PCT,
    calibrate_symbol_champion,
    evaluate_closed_candle_signal,
)
from app.executor_ccxt import BinanceFuturesExecutor

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
    order_margin_usdt: Optional[float] = None
    default_leverage: Optional[int] = None
    min_profit_factor: Optional[float] = None
    lookback_days: Optional[int] = None
    timeframe_filter: Optional[str] = None
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    testnet: Optional[bool] = None
    market_type: Optional[str] = None
    proxy_url: Optional[str] = None


class SniperRuntimeState:
    """Estado global em memória do Quant Terminal Sniper."""

    def __init__(self) -> None:
        self.enabled: bool = os.getenv("BOT_ENABLED", "true").lower() in ("true", "1", "yes")
        self.max_concurrent_positions: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "2"))
        self.top_symbols_limit: int = int(os.getenv("TOP_SYMBOLS_LIMIT", "35"))
        self.min_volume_24h_usd: float = float(os.getenv("MIN_VOLUME_24H_USD", "50000000"))
        self.min_profit_factor: float = float(os.getenv("MIN_PROFIT_FACTOR", "1.35"))
        self.lookback_days: int = int(os.getenv("LOOKBACK_DAYS", "15"))
        self.timeframe_filter: str = os.getenv("TIMEFRAME_FILTER", "ALL").upper()
        self.timeframes: List[str] = ["5m", "15m", "30m", "1h"]

        self.wallet_balance: Dict[str, Any] = {
            "total_usdt": 0.0,
            "free_usdt": 0.0,
            "used_usdt": 0.0,
            "authenticated": False,
            "error": None,
        }
        self.open_positions: List[Dict[str, Any]] = []
        self.champions_map: Dict[str, Dict[str, Any]] = {}
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
    all_options: List[Dict[str, Any]] = []

    # Se o usuário filtrou um timeframe único ou janela curta, usa PF efetivo flexível se necessário
    for symbol, ohlcv_by_tf in state.ohlcv_cache.items():
        mkt = state.symbols_meta.get(symbol, {})
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
    available_slots = max(0, state.max_concurrent_positions - len(open_positions))

    if available_slots <= 0:
        state.append_log(
            "ANTI_CORRELATION_LOCK",
            f"Limite máximo atingido ({len(open_positions)}/{state.max_concurrent_positions} posições abertas). Novos disparos bloqueados.",
            {"open_symbols": list(open_symbols)},
        )
        return

    current_notional = executor.order_margin_usdt * executor.default_leverage

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
            order_receipt = await executor.execute_atomic_order(
                symbol=sym,
                direction=sig["direction"],
                entry_price=sig["entry_price"],
                stop_price=sig["stop_price"],
                take_profit_price=sig["take_profit_price"],
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
        "champions_count": len(state.champions_map),
        "champions_map": state.champions_map,
        "top_options_ranking": state.top_options_ranking,
        "execution_logs": list(state.execution_logs)[:50],
    }


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
            executor.default_leverage = (
                1 if executor.market_type == "spot" else max(1, min(50, int(payload.default_leverage)))
            )
            changes.append(f"Alavancagem={executor.default_leverage}x")

        if payload.max_concurrent_positions is not None:
            state.max_concurrent_positions = max(1, int(payload.max_concurrent_positions))
            changes.append(f"MaxPos={state.max_concurrent_positions}")

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
        "min_profit_factor": state.min_profit_factor,
        "lookback_days": state.lookback_days,
        "timeframe_filter": state.timeframe_filter,
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
