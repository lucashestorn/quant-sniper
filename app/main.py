"""
main.py
Ponto de entrada do microsserviço "Quant Terminal Sniper" (FastAPI + APScheduler + Polars + CCXT).
Projetado para execução 24/7 em container Docker no Coolify.

Loops em Background (UTC):
- Loop 1 (Calibrador Walk-Forward): 00:00, 06:00, 12:00, 18:00 UTC -> Top 35 moedas (Vol 24h > $100M),
  últimos 30 dias de candles (15m, 30m, 1h), atualiza champions_map (PF >= 1.35 e fricção 0.11%).
- Loop 2 (Sniper de Fechamento): :00:01, :15:01, :30:01, :45:01 UTC -> Avalia estritamente a penúltima vela [-2]
  (df.tail(2).head(1)) das moedas mapeadas nos timeframes que fecharam e dispara ordens atômicas respeitando
  a trava anti-correlação (máximo de posições simultâneas).
"""

from __future__ import annotations

import asyncio
import logging
import os
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Set

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv
from fastapi import Body, FastAPI
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


class ToggleRequest(BaseModel):
    enabled: Optional[bool] = None
    max_concurrent_positions: Optional[int] = None


class SniperRuntimeState:
    """Estado global em memória do Quant Terminal Sniper."""

    def __init__(self) -> None:
        self.enabled: bool = os.getenv("BOT_ENABLED", "true").lower() in ("true", "1", "yes")
        self.max_concurrent_positions: int = int(os.getenv("MAX_CONCURRENT_POSITIONS", "4"))
        self.top_symbols_limit: int = int(os.getenv("TOP_SYMBOLS_LIMIT", "35"))
        self.min_volume_24h_usd: float = float(os.getenv("MIN_VOLUME_24H_USD", "100000000"))
        self.min_profit_factor: float = float(os.getenv("MIN_PROFIT_FACTOR", "1.35"))
        self.timeframes: List[str] = ["15m", "30m", "1h"]

        self.open_positions: List[Dict[str, Any]] = []
        self.champions_map: Dict[str, Dict[str, Any]] = {}
        self.execution_logs: Deque[Dict[str, Any]] = deque(maxlen=150)

        self.is_calibrating: bool = False
        self.last_calibration_utc: Optional[str] = None
        self.last_sniper_run_utc: Optional[str] = None
        self._dispatched_candles: Set[str] = set()
        self._calibration_task: Optional[asyncio.Task[Any]] = None

    def append_log(self, event_type: str, message: str, details: Optional[Dict[str, Any]] = None) -> None:
        entry = {
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "event_type": event_type,
            "message": message,
            "details": details or {},
        }
        self.execution_logs.appendleft(entry)
        logger.info(f"[{event_type}] {message}")


state = SniperRuntimeState()
executor = BinanceFuturesExecutor()
scheduler = AsyncIOScheduler(timezone="UTC")


def _determine_closed_timeframes(now_utc: datetime) -> List[str]:
    """
    Identifica quais timeframes acabaram de fechar no minuto atual:
    - :00 -> fecharam 15m, 30m e 1h
    - :15 -> fechou 15m
    - :30 -> fecharam 15m e 30m
    - :45 -> fechou 15m
    """
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
    """
    Varre as Top 35 moedas perpétuas com Volume 24h > $100M, baixa os últimos 30 dias
    de candles (15m, 30m, 1h) e atualiza o dicionário em memória `champions_map`
    com a Estratégia Campeã de cada ativo (PF >= 1.35 e desconto obrigatório de 0.11%).
    """
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

                # Executa a simulação vetorial Polars em thread separada para não bloquear o loop assíncrono
                champion = await asyncio.to_thread(
                    calibrate_symbol_champion,
                    symbol,
                    ohlcv_by_tf,
                    state.min_profit_factor,
                )

                if champion is not None:
                    champion["quote_volume_24h"] = mkt["quote_volume_24h"]
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
    """
    Executado no segundo :01 de cada quarto de hora (:00:01, :15:01, :30:01, :45:01 UTC).
    1. Identifica quais timeframes fecharam naquele instante.
    2. Verifica Kill Switch (`state.enabled`) e consulta posições abertas na corretora (Trava Anti-Correlação).
    3. Avalia estritamente a penúltima linha `df.tail(2).head(1)` (índice [-2], vela 100% fechada).
    4. Dispara ordens atômicas (Market Isolado + STOP_MARKET + TAKE_PROFIT_MARKET) nas vagas disponíveis.
    """
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

    # 2. Filtra moedas cuja estratégia campeã pertence a um timeframe que acabou de fechar
    eligible_targets = [
        (sym, champ)
        for sym, champ in state.champions_map.items()
        if champ.get("timeframe") in closed_tfs and sym not in open_symbols
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
            f"Varredura {closed_tfs} concluída em {len(eligible_targets)} ativos: nenhum gatilho na vela fechada [-2].",
        )
        return

    # Prioriza os sinais com maior quant_score caso haja mais sinais do que vagas
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
            state.append_log(
                "ATOMIC_ORDER_FIRED",
                (
                    f"ORDEM ATÔMICA DISPARADA: {sym} [{tf}] {sig['direction']} "
                    f"({sig['strategy_name']}) | Entrada: {sig['entry_price']} | "
                    f"SL: {order_receipt['stop_price']} | TP: {order_receipt['take_profit_price']}"
                ),
                {"signal": sig, "order_receipt": order_receipt},
            )
        except Exception as order_exc:
            state.append_log(
                "ATOMIC_ORDER_FAILED",
                f"Falha ao executar ordem atômica para {sym} [{tf}]: {order_exc}",
                {"signal": sig},
            )

    # Atualiza cache de posições abertas após disparos
    try:
        state.open_positions = await executor.get_open_positions()
    except Exception:
        pass


# ============================================================================
# CICLO DE VIDA (LIFESPAN) & ENDPOINTS FASTAPI
# ============================================================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    await executor.initialize()

    # Loop 1: Calibrador Walk-Forward a cada 6 horas (00:00, 06:00, 12:00, 18:00 UTC)
    scheduler.add_job(
        run_walk_forward_calibration,
        trigger=CronTrigger(hour="0,6,12,18", minute="0", second="0", timezone="UTC"),
        id="walk_forward_calibrator",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Loop 2: Sniper de Fechamento no segundo 01 de cada quarto de hora (:00:01, :15:01, :30:01, :45:01 UTC)
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
        "APScheduler iniciado (Loop 1: 00/06/12/18h UTC | Loop 2: :00:01, :15:01, :30:01, :45:01 UTC).",
    )

    # Dispara uma calibração inicial em background caso AUTO_CALIBRATE_ON_STARTUP=true
    if os.getenv("AUTO_CALIBRATE_ON_STARTUP", "true").lower() in ("true", "1", "yes"):
        state._calibration_task = asyncio.create_task(run_walk_forward_calibration())

    yield

    scheduler.shutdown(wait=False)
    await executor.close()


app = FastAPI(
    title="Quant Terminal Sniper Microservice",
    description="Microsserviço 24/7 de calibração Walk-Forward em Polars e execução atômica na Binance Futures via CCXT.",
    version="1.0.0",
    lifespan=lifespan,
)


@app.get("/api/status")
async def get_status() -> Dict[str, Any]:
    """
    Retorna o estado operacional completo do robô:
    - `enabled`: status do Kill Switch
    - `max_concurrent_positions`: trava anti-correlação
    - `open_positions`: posições ativas na corretora
    - `champions_map`: estratégias campeãs calibradas nos últimos 30D (PF >= 1.35)
    - `execution_logs`: últimos logs de disparo e varredura
    """
    try:
        state.open_positions = await executor.get_open_positions()
    except Exception:
        pass

    return {
        "enabled": state.enabled,
        "testnet": executor.testnet,
        "authenticated": bool(executor.api_key and executor.api_secret),
        "max_concurrent_positions": state.max_concurrent_positions,
        "open_positions_count": len(state.open_positions),
        "open_positions": state.open_positions,
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
    Kill Switch em tempo de execução para ligar/desligar o robô
    e/ou atualizar o limite de posições simultâneas.
    """
    if payload is not None and payload.enabled is not None:
        state.enabled = bool(payload.enabled)
    else:
        state.enabled = not state.enabled

    if payload is not None and payload.max_concurrent_positions is not None:
        state.max_concurrent_positions = max(1, int(payload.max_concurrent_positions))

    status_label = "ATIVADO (ARMED)" if state.enabled else "PAUSADO (KILL SWITCH ON)"
    state.append_log(
        "KILL_SWITCH_TOGGLE",
        f"Estado do robô alterado manualmente para: {status_label} (Max Posições: {state.max_concurrent_positions}).",
    )

    return {
        "status": "ok",
        "enabled": state.enabled,
        "max_concurrent_positions": state.max_concurrent_positions,
        "message": f"Robô {status_label}",
    }


@app.post("/api/calibrate")
async def trigger_calibration() -> Dict[str, Any]:
    """Permite acionar manualmente uma recalibração Walk-Forward de 30 dias em background."""
    if state.is_calibrating:
        return {"status": "busy", "message": "Calibração Walk-Forward já está em execução."}

    state._calibration_task = asyncio.create_task(run_walk_forward_calibration())
    return {"status": "started", "message": "Calibração Walk-Forward iniciada em background."}
