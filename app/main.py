"""
main.py
Ponto de entrada do microsserviço "Quant Terminal Sniper" (FastAPI + APScheduler + Polars + CCXT).
Projetado para execução 24/7 em container Docker no Coolify, com Dashboard Web integrado em GET /.
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
    binance_api_key: Optional[str] = None
    binance_api_secret: Optional[str] = None
    testnet: Optional[bool] = None


class SniperRuntimeState:
    """Estado global em memória do Quant Terminal Sniper (configurado por padrão para banca de $10 USDT)."""

    def __init__(self) -> None:
        self.enabled: bool = os.getenv("BOT_ENABLED", "true").lower() in ("true", "1", "yes")
        self.max_concurrent_positions: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "2"))
        self.top_symbols_limit: int = int(os.getenv("TOP_SYMBOLS_LIMIT", "35"))
        self.min_volume_24h_usd: float = float(os.getenv("MIN_VOLUME_24H_USD", "100000000"))
        self.min_profit_factor: float = float(os.getenv("MIN_PROFIT_FACTOR", "1.35"))
        self.timeframes: List[str] = ["15m", "30m", "1h"]

        self.wallet_balance: Dict[str, Any] = {
            "total_usdt": 0.0,
            "free_usdt": 0.0,
            "used_usdt": 0.0,
            "authenticated": False,
        }
        self.open_positions: List[Dict[str, Any]] = []
        self.champions_map: Dict[str, Dict[str, Any]] = {}
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
    default_leverage=int(os.getenv("DEFAULT_LEVERAGE", "3")),
    order_margin_usdt=float(os.getenv("ORDER_MARGIN_USDT", "2.5")),
)
scheduler = AsyncIOScheduler(timezone="UTC")


def _determine_closed_timeframes(now_utc: datetime) -> List[str]:
    minute = now_utc.minute
    closed = ["15m"]
    if minute in (0, 30):
        closed.append("30m")
    if minute == 0:
        closed.append("1h")
    return closed


# ============================================================================
# LOOP 1: CALIBRADOR WALK-FORWARD (00:00, 06:00, 12:00, 18:00 UTC)
# ============================================================================

async def run_walk_forward_calibration() -> None:
    if state.is_calibrating:
        logger.warning("Calibração Walk-Forward já está em andamento. Ignorando chamada duplicada.")
        return

    state.is_calibrating = True
    t0 = datetime.now(timezone.utc)
    state.append_log(
        "CALIBRATION_START",
        f"Iniciando Calibração Walk-Forward 30D (Top {state.top_symbols_limit} ativos > ${state.min_volume_24h_usd:,.0f} Vol 24h | PF >= {state.min_profit_factor}).",
    )

    try:
        top_markets = await executor.fetch_top_liquid_symbols(
            limit=state.top_symbols_limit,
            min_quote_vol_usd=state.min_volume_24h_usd,
        )
        new_champions: Dict[str, Dict[str, Any]] = {}

        for mkt in top_markets:
            symbol = mkt["symbol"]
            try:
                ohlcv_by_tf: Dict[str, List[List[float]]] = {}
                for tf in state.timeframes:
                    candles = await executor.fetch_ohlcv_days(symbol=symbol, timeframe=tf, days=30)
                    ohlcv_by_tf[tf] = candles

                champion = await asyncio.to_thread(
                    calibrate_symbol_champion,
                    symbol,
                    ohlcv_by_tf,
                    state.min_profit_factor,
                )

                if champion is not None:
                    champion["quote_volume_24h"] = mkt["quote_volume_24h"]
                    champion["min_notional_usdt"] = mkt.get("min_notional_usdt", 5.0)
                    champion["calibrated_at_utc"] = datetime.now(timezone.utc).strftime(
                        "%Y-%m-%d %H:%M:%S UTC"
                    )
                    new_champions[symbol] = champion
            except Exception as sym_exc:
                logger.warning(f"[{symbol}] Erro ao calibrar histórico de 30D: {sym_exc}")

        state.champions_map = new_champions
        state.last_calibration_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        elapsed = (datetime.now(timezone.utc) - t0).total_seconds()

        state.append_log(
            "CALIBRATION_DONE",
            f"Calibração concluída em {elapsed:.1f}s: {len(new_champions)}/{len(top_markets)} moedas aprovadas (PF >= {state.min_profit_factor}, Fricção {ROUND_TRIP_COST_PCT}%).",
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

    # 1. Trava Anti-Correlação: consulta posições reais abertas na Binance Futures
    try:
        open_positions = await executor.get_open_positions()
        state.open_positions = open_positions
        state.wallet_balance = await executor.get_wallet_balance()
    except Exception as exc:
        state.append_log("POSITION_CHECK_ERROR", f"Erro ao consultar posições abertas: {exc}")
        return

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

    # 2. Filtra moedas do timeframe que fechou e cujo lote mínimo é compatível com a mão configurada
    eligible_targets = [
        (sym, champ)
        for sym, champ in state.champions_map.items()
        if champ.get("timeframe") in closed_tfs
        and sym not in open_symbols
        and float(champ.get("min_notional_usdt", 5.0)) <= (current_notional * 1.25)
    ]

    if not eligible_targets:
        return

    # 3. Avalia em paralelo o sinal na vela fechada [-2] (No-Repainting)
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
            f"Varredura {closed_tfs} concluída em {len(eligible_targets)} ativos compatíveis: nenhum gatilho na vela fechada [-2].",
        )
        return

    approved_signals.sort(key=lambda x: x.get("quant_score", 0.0), reverse=True)

    # 4. Dispara Ordens Atômicas respeitando as vagas disponíveis
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
                    f"ORDEM ATÔMICA DISPARADA: {sym} [{tf}] {sig['direction']} "
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

    try:
        state.open_positions = await executor.get_open_positions()
        state.wallet_balance = await executor.get_wallet_balance()
    except Exception:
        pass


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
        trigger=CronTrigger(minute="0,15,30,45", second="1", timezone="UTC"),
        id="candle_close_sniper",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.start()
    state.append_log(
        "SYSTEM_STARTUP",
        f"APScheduler iniciado | Perfil Padrão: Margem ${executor.order_margin_usdt:.2f} × {executor.default_leverage}x (${executor.order_margin_usdt * executor.default_leverage:.2f}/trade) | Max Posições: {state.max_concurrent_positions}.",
    )

    if os.getenv("AUTO_CALIBRATE_ON_STARTUP", "true").lower() in ("true", "1", "yes"):
        state._calibration_task = asyncio.create_task(run_walk_forward_calibration())

    yield

    scheduler.shutdown(wait=False)
    await executor.close()


app = FastAPI(
    title="Quant Terminal Sniper Microservice",
    description="Microsserviço 24/7 de calibração Walk-Forward em Polars e execução atômica na Binance Futures via CCXT.",
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/", response_class=HTMLResponse)
async def serve_dashboard() -> HTMLResponse:
    """Renderiza o Painel Web de Controle & Monitoramento do Quant Terminal Sniper."""
    if DASHBOARD_HTML_PATH.exists():
        return HTMLResponse(content=DASHBOARD_HTML_PATH.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Quant Terminal Sniper Running</h1>")


@app.get("/api/status")
async def get_status() -> Dict[str, Any]:
    """Retorna o estado operacional completo do robô, saldo da carteira, posições e histórico."""
    if executor.api_key and executor.api_secret:
        try:
            state.open_positions = await executor.get_open_positions()
            state.wallet_balance = await executor.get_wallet_balance()
        except Exception:
            pass

    return {
        "enabled": state.enabled,
        "testnet": executor.testnet,
        "authenticated": bool(executor.api_key and executor.api_secret),
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
        "champions_count": len(state.champions_map),
        "champions_map": state.champions_map,
        "execution_logs": list(state.execution_logs)[:50],
    }


@app.post("/api/toggle")
async def toggle_bot(
    payload: Optional[ToggleRequest] = Body(default=None),
) -> Dict[str, Any]:
    """
    Kill Switch e atualização em tempo de execução (sem reiniciar o container):
    - `enabled`: liga/pausa o robô
    - `order_margin_usdt`: altera a margem em dólares por operação (ex: $2.50)
    - `default_leverage`: altera a alavancagem isolada (ex: 3x)
    - `max_concurrent_positions`: altera o limite de posições simultâneas (ex: 2)
    - `min_profit_factor`: altera o Profit Factor mínimo de filtro
    - `binance_api_key` / `binance_api_secret` / `testnet`: atualiza credenciais em tempo real
    """
    changes: List[str] = []

    if payload is None:
        state.enabled = not state.enabled
        changes.append(f"enabled={state.enabled}")
    else:
        if payload.enabled is not None:
            state.enabled = bool(payload.enabled)
            changes.append(f"enabled={state.enabled}")

        if payload.order_margin_usdt is not None:
            executor.order_margin_usdt = max(1.0, float(payload.order_margin_usdt))
            changes.append(f"Margem=${executor.order_margin_usdt:.2f}")

        if payload.default_leverage is not None:
            executor.default_leverage = max(1, min(50, int(payload.default_leverage)))
            changes.append(f"Alavancagem={executor.default_leverage}x")

        if payload.max_concurrent_positions is not None:
            state.max_concurrent_positions = max(1, int(payload.max_concurrent_positions))
            changes.append(f"MaxPos={state.max_concurrent_positions}")

        if payload.min_profit_factor is not None:
            state.min_profit_factor = max(1.05, float(payload.min_profit_factor))
            changes.append(f"MinPF={state.min_profit_factor:.2f}")

        if payload.binance_api_key is not None and payload.binance_api_secret is not None:
            new_testnet = payload.testnet if payload.testnet is not None else executor.testnet
            await executor.update_credentials(
                api_key=payload.binance_api_key,
                api_secret=payload.binance_api_secret,
                testnet=new_testnet,
            )
            state.wallet_balance = await executor.get_wallet_balance()
            state.open_positions = await executor.get_open_positions()
            changes.append(
                f"Credenciais Binance atualizadas ({'TESTNET' if new_testnet else 'CONTA REAL'})"
            )

    status_label = "ARMADO" if state.enabled else "PAUSADO (KILL SWITCH)"
    summary = ", ".join(changes) if changes else status_label
    state.append_log(
        "RUNTIME_CONFIG_UPDATED",
        f"Configuração atualizada em tempo real: {summary} | Mão Nominal: ${executor.order_margin_usdt * executor.default_leverage:.2f} USDT.",
    )

    return {
        "status": "ok",
        "enabled": state.enabled,
        "order_margin_usdt": executor.order_margin_usdt,
        "default_leverage": executor.default_leverage,
        "notional_per_trade_usdt": round(executor.order_margin_usdt * executor.default_leverage, 2),
        "max_concurrent_positions": state.max_concurrent_positions,
        "min_profit_factor": state.min_profit_factor,
        "authenticated": bool(executor.api_key and executor.api_secret),
        "testnet": executor.testnet,
        "wallet_balance": state.wallet_balance,
    }


@app.post("/api/calibrate")
async def trigger_calibration() -> Dict[str, Any]:
    """Aciona manualmente uma recalibração Walk-Forward de 30 dias em background."""
    if state.is_calibrating:
        return {"status": "busy", "message": "Calibração Walk-Forward já está em execução."}

    state._calibration_task = asyncio.create_task(run_walk_forward_calibration())
    return {"status": "started", "message": "Calibração Walk-Forward iniciada em background."}
