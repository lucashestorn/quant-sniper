"""
engine_polars.py
Motor Vetorial em Polars para:
1. Calibração Walk-Forward de 30 Dias (com Fricção Obrigatória de 0.11% e PF >= 1.35).
2. Checagem de Sinal Anti-Repintura (avaliando estritamente a penúltima linha [-2] / df.tail(2).head(1)).
"""

from __future__ import annotations

import logging
import math
from typing import Any, Callable, Dict, List, Optional

import numpy as np
import polars as pl

logger = logging.getLogger("quant_sniper.engine_polars")

# Regra de Ouro #1: Fricção Obrigatória (0.08% Taker Fee Entrada+Saída + 0.03% Slippage)
ROUND_TRIP_COST_PCT: float = 0.11


def ohlcv_list_to_polars(ohlcv: List[List[float]]) -> pl.DataFrame:
    """
    Converte matriz OHLCV padrão do CCXT [[timestamp_ms, open, high, low, close, volume], ...]
    em um DataFrame Polars tipado e ordenado cronologicamente sem duplicatas.
    """
    if not ohlcv:
        return pl.DataFrame()

    df = pl.DataFrame(
        ohlcv,
        schema={
            "timestamp_ms": pl.Int64,
            "open": pl.Float64,
            "high": pl.Float64,
            "low": pl.Float64,
            "close": pl.Float64,
            "volume": pl.Float64,
        },
        orient="row",
    )

    df = (
        df.unique(subset=["timestamp_ms"], keep="last")
        .sort("timestamp_ms")
        .with_columns(
            pl.from_epoch(pl.col("timestamp_ms"), time_unit="ms")
            .dt.replace_time_zone("UTC")
            .alias("datetime")
        )
    )
    return df


def compute_indicators_polars(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calcula todos os indicadores técnicos de forma 100% vetorizada no Polars.
    Nenhum dado futuro vaza para o passado (canais de Donchian usam .shift(1)).
    """
    if df.is_empty() or df.height < 55:
        return df

    # 1. Médias Móveis e Range Básico
    df = df.with_columns(
        [
            pl.col("close").rolling_mean(window_size=21).alias("sma_21"),
            pl.col("close").rolling_mean(window_size=50).alias("sma_50"),
            pl.col("close").ewm_mean(span=9, adjust=False).alias("ema_9"),
            pl.col("close").ewm_mean(span=21, adjust=False).alias("ema_21"),
            pl.col("volume").rolling_mean(window_size=20).alias("vol_sma_20"),
            pl.col("close").shift(1).alias("prev_close"),
            (pl.col("high") - pl.col("low")).alias("candle_range"),
            (pl.col("close") - pl.col("open")).abs().alias("body_size"),
            (pl.col("close") > pl.col("open")).alias("is_green"),
            (pl.col("close") < pl.col("open")).alias("is_red"),
            (pl.col("close") * pl.col("volume")).alias("dollar_vol"),
        ]
    )

    # 2. True Range & ATR (14) + RSI (14) + Bollinger Z-Score + Donchian (shift 1)
    delta = pl.col("close").diff()
    gain = pl.when(delta > 0).then(delta).otherwise(0.0)
    loss = pl.when(delta < 0).then(-delta).otherwise(0.0)

    avg_gain = gain.ewm_mean(alpha=1.0 / 14.0, adjust=False)
    avg_loss = loss.ewm_mean(alpha=1.0 / 14.0, adjust=False)
    rs = avg_gain / pl.when(avg_loss == 0).then(1e-10).otherwise(avg_loss)

    tr = pl.max_horizontal(
        pl.col("high") - pl.col("low"),
        (pl.col("high") - pl.col("prev_close")).abs(),
        (pl.col("low") - pl.col("prev_close")).abs(),
    )

    bb_mean = pl.col("close").rolling_mean(window_size=20)
    bb_std = pl.col("close").rolling_std(window_size=20)

    upper_wick = pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))
    lower_wick = pl.min_horizontal(pl.col("open"), pl.col("close")) - pl.col("low")
    safe_range = pl.when(pl.col("candle_range") <= 0).then(1e-10).otherwise(pl.col("candle_range"))

    df = df.with_columns(
        [
            tr.ewm_mean(alpha=1.0 / 14.0, adjust=False).alias("atr_14"),
            (100.0 - (100.0 / (1.0 + rs))).alias("rsi_14"),
            (
                (pl.col("close") - bb_mean)
                / pl.when(bb_std <= 0).then(1e-10).otherwise(bb_std)
            ).alias("bb_zscore"),
            (
                pl.col("volume")
                / pl.when(pl.col("vol_sma_20") <= 0).then(1e-10).otherwise(pl.col("vol_sma_20"))
            ).alias("vol_ratio"),
            # Canais de Donchian deslocados em 1 vela (sem viés de antecipação)
            pl.col("high").rolling_max(window_size=20).shift(1).alias("donchian_high_20"),
            pl.col("low").rolling_min(window_size=20).shift(1).alias("donchian_low_20"),
            pl.col("high").rolling_max(window_size=50).shift(1).alias("donchian_high_50"),
            pl.col("low").rolling_min(window_size=50).shift(1).alias("donchian_low_50"),
            # VWAP Móvel de 24 barras
            (
                pl.col("dollar_vol").rolling_sum(window_size=24)
                / pl.when(pl.col("volume").rolling_sum(window_size=24) <= 0)
                .then(1e-10)
                .otherwise(pl.col("volume").rolling_sum(window_size=24))
            ).alias("vwap_24"),
            (upper_wick / safe_range).alias("upper_wick_pct"),
            (lower_wick / safe_range).alias("lower_wick_pct"),
            (pl.col("body_size") / safe_range).alias("body_pct"),
            (
                pl.col("is_green")
                & pl.col("is_green").shift(1).fill_null(False)
                & pl.col("is_green").shift(2).fill_null(False)
            ).alias("streak_3_green"),
            (
                pl.col("is_red")
                & pl.col("is_red").shift(1).fill_null(False)
                & pl.col("is_red").shift(2).fill_null(False)
            ).alias("streak_3_red"),
        ]
    )

    df = df.with_columns(
        [
            ((pl.col("atr_14") / pl.col("close")) * 100.0).alias("atr_pct"),
            (((pl.col("close") - pl.col("vwap_24")) / pl.col("vwap_24")) * 100.0).alias(
                "dist_vwap_pct"
            ),
        ]
    )

    return df


# ============================================================================
# CATÁLOGO SIMÉTRICO DE ESTRATÉGIAS QUANTITATIVAS AUDITADAS (LONG & SHORT)
# ============================================================================

def _build_strategies_catalog() -> List[Dict[str, Any]]:
    return [
        # 1. Tendência Institucional
        {
            "id": "trend_inst_long",
            "name": "Tendência Institucional Bullish (SMA 21 > 50)",
            "direction": "LONG",
            "atr_stop_mult": 0.5,
            "rr_ratio": 2.0,
            "max_bars": 24,
            "expr": (
                (pl.col("close") > pl.col("sma_21"))
                & (pl.col("sma_21") > pl.col("sma_50"))
                & (pl.col("vol_ratio") >= 1.35)
                & pl.col("is_green")
                & (pl.col("rsi_14") >= 52.0)
                & (pl.col("rsi_14") <= 72.0)
            ),
        },
        {
            "id": "trend_inst_short",
            "name": "Tendência Institucional Bearish (SMA 21 < 50)",
            "direction": "SHORT",
            "atr_stop_mult": 0.5,
            "rr_ratio": 2.0,
            "max_bars": 24,
            "expr": (
                (pl.col("close") < pl.col("sma_21"))
                & (pl.col("sma_21") < pl.col("sma_50"))
                & (pl.col("vol_ratio") >= 1.35)
                & pl.col("is_red")
                & (pl.col("rsi_14") <= 48.0)
                & (pl.col("rsi_14") >= 28.0)
            ),
        },
        # 2. Donchian Breakout / Breakdown 20p
        {
            "id": "donchian_20_long",
            "name": "Donchian Breakout Topo 20p + Volume",
            "direction": "LONG",
            "atr_stop_mult": 0.6,
            "rr_ratio": 2.2,
            "max_bars": 24,
            "expr": (
                (pl.col("close") > pl.col("donchian_high_20"))
                & (pl.col("vol_ratio") >= 1.5)
                & pl.col("is_green")
                & (pl.col("body_pct") >= 0.50)
            ),
        },
        {
            "id": "donchian_20_short",
            "name": "Donchian Breakdown Fundo 20p + Volume",
            "direction": "SHORT",
            "atr_stop_mult": 0.6,
            "rr_ratio": 2.2,
            "max_bars": 24,
            "expr": (
                (pl.col("close") < pl.col("donchian_low_20"))
                & (pl.col("vol_ratio") >= 1.5)
                & pl.col("is_red")
                & (pl.col("body_pct") >= 0.50)
            ),
        },
        # 3. Momentum & Agressão de Volume
        {
            "id": "momentum_vol_long",
            "name": "Breakout Momentum & Agressão Compradora",
            "direction": "LONG",
            "atr_stop_mult": 0.4,
            "rr_ratio": 2.0,
            "max_bars": 20,
            "expr": (
                (pl.col("close") > pl.col("ema_9"))
                & (pl.col("ema_9") > pl.col("ema_21"))
                & (pl.col("vol_ratio") >= 1.8)
                & (pl.col("body_pct") >= 0.60)
                & pl.col("is_green")
            ),
        },
        {
            "id": "momentum_vol_short",
            "name": "Breakdown Momentum & Agressão Vendedora",
            "direction": "SHORT",
            "atr_stop_mult": 0.4,
            "rr_ratio": 2.0,
            "max_bars": 20,
            "expr": (
                (pl.col("close") < pl.col("ema_9"))
                & (pl.col("ema_9") < pl.col("ema_21"))
                & (pl.col("vol_ratio") >= 1.8)
                & (pl.col("body_pct") >= 0.60)
                & pl.col("is_red")
            ),
        },
        # 4. Exaustão / Reversão VWAP & Bollinger Z-Score
        {
            "id": "vwap_rev_long",
            "name": "Exaustão / Reversão VWAP Bullish",
            "direction": "LONG",
            "atr_stop_mult": 0.5,
            "rr_ratio": 1.8,
            "max_bars": 18,
            "expr": (
                (pl.col("bb_zscore") <= -1.85)
                & (pl.col("rsi_14") <= 34.0)
                & (pl.col("lower_wick_pct") >= 0.30)
                & (pl.col("vol_ratio") >= 1.2)
            ),
        },
        {
            "id": "vwap_rev_short",
            "name": "Exaustão / Reversão VWAP Bearish",
            "direction": "SHORT",
            "atr_stop_mult": 0.5,
            "rr_ratio": 1.8,
            "max_bars": 18,
            "expr": (
                (pl.col("bb_zscore") >= 1.85)
                & (pl.col("rsi_14") >= 66.0)
                & (pl.col("upper_wick_pct") >= 0.30)
                & (pl.col("vol_ratio") >= 1.2)
            ),
        },
        # 5. Pullback Institucional na Média de 21
        {
            "id": "pullback_sma21_long",
            "name": "Pullback Institucional na SMA 21 (Long)",
            "direction": "LONG",
            "atr_stop_mult": 0.5,
            "rr_ratio": 2.0,
            "max_bars": 20,
            "expr": (
                (pl.col("sma_21") > pl.col("sma_50"))
                & (pl.col("low") <= pl.col("sma_21"))
                & (pl.col("close") > pl.col("sma_21"))
                & pl.col("is_green")
                & (pl.col("lower_wick_pct") >= 0.35)
            ),
        },
        {
            "id": "pullback_sma21_short",
            "name": "Pullback Institucional na SMA 21 (Short)",
            "direction": "SHORT",
            "atr_stop_mult": 0.5,
            "rr_ratio": 2.0,
            "max_bars": 20,
            "expr": (
                (pl.col("sma_21") < pl.col("sma_50"))
                & (pl.col("high") >= pl.col("sma_21"))
                & (pl.col("close") < pl.col("sma_21"))
                & pl.col("is_red")
                & (pl.col("upper_wick_pct") >= 0.35)
            ),
        },
        # 6. Sequência de Fluxo Contínuo (3 Candles + Volume)
        {
            "id": "streak_3_long",
            "name": "Impulso 3 Soldados Verdes + Expansão ATR",
            "direction": "LONG",
            "atr_stop_mult": 0.6,
            "rr_ratio": 2.0,
            "max_bars": 18,
            "expr": (
                pl.col("streak_3_green")
                & (pl.col("close") > pl.col("sma_21"))
                & (pl.col("vol_ratio") >= 1.3)
                & (pl.col("rsi_14") < 74.0)
            ),
        },
        {
            "id": "streak_3_short",
            "name": "Impulso 3 Corvos Vermelhos + Expansão ATR",
            "direction": "SHORT",
            "atr_stop_mult": 0.6,
            "rr_ratio": 2.0,
            "max_bars": 18,
            "expr": (
                pl.col("streak_3_red")
                & (pl.col("close") < pl.col("sma_21"))
                & (pl.col("vol_ratio") >= 1.3)
                & (pl.col("rsi_14") > 26.0)
            ),
        },
    ]


STRATEGIES_CATALOG: List[Dict[str, Any]] = _build_strategies_catalog()
STRATEGIES_BY_ID: Dict[str, Dict[str, Any]] = {s["id"]: s for s in STRATEGIES_CATALOG}


def _compute_bracket_prices(
    direction: str,
    entry_price: float,
    candle_high: float,
    candle_low: float,
    atr_val: float,
    atr_stop_mult: float,
    rr_ratio: float,
) -> Dict[str, float]:
    """
    Calcula os preços exatos de Stop Loss e Take Profit para a ordem atômica,
    garantindo distância mínima de segurança (0.30%) e teto máximo (4.50%).
    """
    safe_atr = atr_val if (atr_val and not math.isnan(atr_val) and atr_val > 0) else entry_price * 0.008

    if direction == "LONG":
        raw_stop = candle_low - (atr_stop_mult * safe_atr)
        risk_dist = entry_price - raw_stop
    else:
        raw_stop = candle_high + (atr_stop_mult * safe_atr)
        risk_dist = raw_stop - entry_price

    # Limites de segurança sobre a distância do Stop (0.30% a 4.50% do preço de entrada)
    min_dist = entry_price * 0.0030
    max_dist = entry_price * 0.0450
    risk_dist = max(min_dist, min(risk_dist, max_dist))

    if direction == "LONG":
        stop_price = entry_price - risk_dist
        tp_price = entry_price + (risk_dist * rr_ratio)
    else:
        stop_price = entry_price + risk_dist
        tp_price = entry_price - (risk_dist * rr_ratio)

    return {
        "entry_price": float(entry_price),
        "stop_price": float(stop_price),
        "take_profit_price": float(tp_price),
        "risk_pct": round((risk_dist / entry_price) * 100.0, 4),
        "reward_pct": round(((risk_dist * rr_ratio) / entry_price) * 100.0, 4),
    }


def simulate_strategy_30d(
    df_enriched: pl.DataFrame,
    strategy: Dict[str, Any],
    timeframe: str,
    min_pf: float = 1.35,
) -> Optional[Dict[str, Any]]:
    """
    Executa a simulação Walk-Forward nos candles históricos (30 dias),
    descontando obrigatoriamente ROUND_TRIP_COST_PCT = 0.11% em cada trade
    e aplicando as Hard Constraints determinísticas.
    """
    # Ignora a última vela (em formação) durante o backtest também
    if df_enriched.height < 60:
        return None

    df_closed = df_enriched.slice(0, df_enriched.height - 1)
    df_sig = df_closed.with_columns(strategy["expr"].fill_null(False).alias("signal"))

    signals = df_sig["signal"].to_numpy()
    if not np.any(signals):
        return None

    closes = df_sig["close"].to_numpy()
    highs = df_sig["high"].to_numpy()
    lows = df_sig["low"].to_numpy()
    atrs = df_sig["atr_14"].to_numpy()
    n = len(closes)

    direction = strategy["direction"]
    atr_mult = float(strategy["atr_stop_mult"])
    rr_ratio = float(strategy["rr_ratio"])
    max_bars = int(strategy["max_bars"])

    min_trades_required = 10 if timeframe == "1h" else 12

    net_returns: List[float] = []
    gross_returns: List[float] = []

    i = 50  # Começa após warmup das médias de 50 períodos
    while i < n - 1:
        if not signals[i]:
            i += 1
            continue

        entry_p = closes[i]
        if entry_p <= 0 or np.isnan(entry_p):
            i += 1
            continue

        brackets = _compute_bracket_prices(
            direction=direction,
            entry_price=entry_p,
            candle_high=highs[i],
            candle_low=lows[i],
            atr_val=atrs[i],
            atr_stop_mult=atr_mult,
            rr_ratio=rr_ratio,
        )
        stop_p = brackets["stop_price"]
        tp_p = brackets["take_profit_price"]

        exit_p = closes[min(i + max_bars, n - 1)]
        exit_idx = min(i + max_bars, n - 1)

        for j in range(i + 1, min(i + max_bars + 1, n)):
            if direction == "LONG":
                # Conservador: testa Stop Loss antes do Take Profit na mesma barra
                if lows[j] <= stop_p:
                    exit_p = stop_p
                    exit_idx = j
                    break
                if highs[j] >= tp_p:
                    exit_p = tp_p
                    exit_idx = j
                    break
            else:
                if highs[j] >= stop_p:
                    exit_p = stop_p
                    exit_idx = j
                    break
                if lows[j] <= tp_p:
                    exit_p = tp_p
                    exit_idx = j
                    break

        if direction == "LONG":
            gross_pct = ((exit_p - entry_p) / entry_p) * 100.0
        else:
            gross_pct = ((entry_p - exit_p) / entry_p) * 100.0

        # Desconto Obrigatório de Fricção (0.11%)
        net_pct = gross_pct - ROUND_TRIP_COST_PCT

        gross_returns.append(gross_pct)
        net_returns.append(net_pct)

        # Não sobrepõe operações no mesmo ativo
        i = exit_idx + 1

    total_trades = len(net_returns)
    if total_trades < min_trades_required:
        return None

    net_arr = np.array(net_returns, dtype=np.float64)
    gross_profit_pct = float(np.sum(gross_returns))
    net_profit_real = float(np.sum(net_arr))
    edge_per_trade = net_profit_real / total_trades

    # Hard Constraint #2 e #4: Lucro Líquido Real > 0 e Edge >= 0.15%
    if net_profit_real <= 0.0 or edge_per_trade < 0.15:
        return None

    wins = net_arr[net_arr > 0]
    losses = net_arr[net_arr <= 0]
    sum_wins = float(np.sum(wins)) if len(wins) > 0 else 0.0
    sum_losses = float(np.abs(np.sum(losses))) if len(losses) > 0 else 0.0

    profit_factor = (sum_wins / sum_losses) if sum_losses > 0 else (9.99 if sum_wins > 0 else 0.0)

    # Hard Constraint #3: Profit Factor >= min_pf (1.35 na calibração Walk-Forward)
    if profit_factor < min_pf:
        return None

    win_rate_pct = (len(wins) / total_trades) * 100.0

    # Cálculo do Max Drawdown Real (base 100%)
    equity = 100.0 + np.cumsum(net_arr)
    peaks = np.maximum.accumulate(np.insert(equity, 0, 100.0))[1:]
    drawdowns = np.maximum(0.0, peaks - equity)
    max_drawdown_pct = float(np.max(drawdowns)) if len(drawdowns) > 0 else 0.0

    # Fórmula de Ranking Determinístico (quant_score)
    quant_score = (
        (net_profit_real / max(max_drawdown_pct, 1.0))
        * math.log(total_trades)
        * (win_rate_pct / 100.0)
    )

    return {
        "strategy_id": strategy["id"],
        "strategy_name": strategy["name"],
        "timeframe": timeframe,
        "direction": direction,
        "atr_stop_mult": atr_mult,
        "rr_ratio": rr_ratio,
        "max_bars": max_bars,
        "total_trades": total_trades,
        "win_rate_pct": round(win_rate_pct, 2),
        "gross_profit_pct": round(gross_profit_pct, 2),
        "net_profit_real": round(net_profit_real, 2),
        "edge_per_trade": round(edge_per_trade, 3),
        "profit_factor": round(profit_factor, 2),
        "max_drawdown_pct": round(max_drawdown_pct, 2),
        "quant_score": round(quant_score, 4),
        "round_trip_cost_pct": ROUND_TRIP_COST_PCT,
    }


def calibrate_symbol_champion(
    symbol: str,
    ohlcv_by_tf: Dict[str, List[List[float]]],
    min_pf: float = 1.35,
) -> Optional[Dict[str, Any]]:
    """
    Varre todos os timeframes ('15m', '30m', '1h') e todo o catálogo de estratégias
    para um ativo, retornando a Estratégia Campeã Absoluta (maior quant_score)
    que cumpra estritamente todas as travas (incluindo PF >= 1.35).
    """
    candidates: List[Dict[str, Any]] = []

    for tf, raw_candles in ohlcv_by_tf.items():
        if not raw_candles or len(raw_candles) < 65:
            continue

        df_raw = ohlcv_list_to_polars(raw_candles)
        df_enriched = compute_indicators_polars(df_raw)

        for strat in STRATEGIES_CATALOG:
            res = simulate_strategy_30d(
                df_enriched=df_enriched,
                strategy=strat,
                timeframe=tf,
                min_pf=min_pf,
            )
            if res is not None:
                res["symbol"] = symbol
                candidates.append(res)

    if not candidates:
        return None

    # Ordena de forma decrescente pelo quant_score determinístico
    candidates.sort(key=lambda x: x["quant_score"], reverse=True)
    return candidates[0]


def evaluate_closed_candle_signal(
    raw_candles: List[List[float]],
    champion_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Regra de Ouro #2: Anti-Repintura (No-Repainting).
    Avalia ESTRITAMENTE a penúltima linha do DataFrame:
      `closed_candle = df_eval.tail(2).head(1)` (índice [-2]),
    que é a vela que acabou de fechar 100%, ignorando a vela atual em formação (índice [-1]).
    """
    strategy_id = champion_cfg.get("strategy_id", "")
    strategy = STRATEGIES_BY_ID.get(strategy_id)
    if not strategy:
        return {"signal": False, "reason": f"Estratégia '{strategy_id}' não encontrada no catálogo."}

    df_raw = ohlcv_list_to_polars(raw_candles)
    if df_raw.height < 60:
        return {"signal": False, "reason": "Histórico insuficiente de candles para cálculo."}

    df_enriched = compute_indicators_polars(df_raw)
    df_eval = df_enriched.with_columns(strategy["expr"].fill_null(False).alias("signal_triggered"))

    # EXTRAÇÃO ANTI-REPINTURA: Penúltima vela (df.tail(2).head(1) == índice [-2])
    closed_candle_df = df_eval.tail(2).head(1)
    forming_candle_df = df_eval.tail(1)

    closed_row = closed_candle_df.to_dicts()[0]
    forming_row = forming_candle_df.to_dicts()[0]

    signal_fired = bool(closed_row.get("signal_triggered", False))
    entry_price = float(closed_row["close"])
    candle_high = float(closed_row["high"])
    candle_low = float(closed_row["low"])
    atr_val = float(closed_row.get("atr_14") or (entry_price * 0.008))

    brackets = _compute_bracket_prices(
        direction=strategy["direction"],
        entry_price=entry_price,
        candle_high=candle_high,
        candle_low=candle_low,
        atr_val=atr_val,
        atr_stop_mult=float(strategy["atr_stop_mult"]),
        rr_ratio=float(strategy["rr_ratio"]),
    )

    return {
        "signal": signal_fired,
        "symbol": champion_cfg.get("symbol"),
        "timeframe": champion_cfg.get("timeframe"),
        "strategy_id": strategy["id"],
        "strategy_name": strategy["name"],
        "direction": strategy["direction"],
        "closed_candle_ts": int(closed_row["timestamp_ms"]),
        "closed_candle_utc": str(closed_row["datetime"]),
        "forming_candle_ts": int(forming_row["timestamp_ms"]),
        "entry_price": brackets["entry_price"],
        "stop_price": brackets["stop_price"],
        "take_profit_price": brackets["take_profit_price"],
        "risk_pct": brackets["risk_pct"],
        "reward_pct": brackets["reward_pct"],
        "rsi_14": round(float(closed_row.get("rsi_14") or 0.0), 2),
        "vol_ratio": round(float(closed_row.get("vol_ratio") or 0.0), 2),
    }
