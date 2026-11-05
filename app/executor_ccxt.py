"""
executor_ccxt.py
Conector Assíncrono com a Binance Futures (USDⓈ-M) via CCXT.
Implementa:
- Seleção das Top 35 moedas perpétuas com Volume 24h > $100M.
- Download paginado de 30 dias de candles (15m, 30m, 1h).
- Consulta de posições abertas em tempo real (Trava Anti-Correlação).
- Disparo de Ordem Atômica: Market (Isolado, Alavancado) + STOP_MARKET + TAKE_PROFIT_MARKET
  com reduceOnly=True e workingType='MARK_PRICE' (com rollback de emergência caso SL/TP falhe).
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

import ccxt.async_support as ccxt

logger = logging.getLogger("quant_sniper.executor_ccxt")

TIMEFRAME_MS: Dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}


class BinanceFuturesExecutor:
    """Gerencia a conexão assíncrona com a Binance USDⓈ-M Futures (Testnet ou Produção)."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
        default_leverage: int = 3,
        order_margin_usdt: float = 50.0,
    ) -> None:
        self.api_key = (api_key if api_key is not None else os.getenv("BINANCE_API_KEY", "")).strip()
        self.api_secret = (
            api_secret if api_secret is not None else os.getenv("BINANCE_API_SECRET", "")
        ).strip()

        if testnet is None:
            self.testnet = os.getenv("BINANCE_TESTNET", "true").lower() in ("true", "1", "yes")
        else:
            self.testnet = testnet

        self.default_leverage = int(os.getenv("DEFAULT_LEVERAGE", str(default_leverage)))
        self.order_margin_usdt = float(os.getenv("ORDER_MARGIN_USDT", str(order_margin_usdt)))

        self.exchange: Optional[ccxt.binanceusdm] = None
        self._markets_loaded: bool = False
        self._sem = asyncio.Semaphore(8)  # Limita concorrência de chamadas REST para evitar HTTP 429

    async def initialize(self) -> None:
        """Inicializa a sessão assíncrona do CCXT e carrega os mercados da Binance Futures."""
        if self.exchange is not None:
            return

        config: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {
                "defaultType": "future",
                "adjustForTimeDifference": True,
            },
        }
        if self.api_key and self.api_secret:
            config["apiKey"] = self.api_key
            config["secret"] = self.api_secret

        self.exchange = ccxt.binanceusdm(config)
        if self.testnet:
            self.exchange.set_sandbox_mode(True)

        try:
            await self.exchange.load_markets()
            self._markets_loaded = True
            mode_label = "TESTNET (Sandbox)" if self.testnet else "LIVE (Produção)"
            auth_label = "AUTENTICADO" if (self.api_key and self.api_secret) else "SOMENTE LEITURA (Sem API Key)"
            logger.info(f"Conector CCXT Binance Futures inicializado [{mode_label} | {auth_label}]")
        except Exception as exc:
            logger.warning(f"Aviso ao carregar mercados no startup ({exc}). Tentará novamente sob demanda.")

    async def close(self) -> None:
        """Encerra a sessão HTTP assíncrona do CCXT."""
        if self.exchange is not None:
            await self.exchange.close()
            self.exchange = None
            self._markets_loaded = False

    async def _ensure_markets(self) -> None:
        if self.exchange is None:
            await self.initialize()
        if self.exchange is not None and not self._markets_loaded:
            await self.exchange.load_markets()
            self._markets_loaded = True

    async def fetch_top_liquid_symbols(
        self,
        limit: int = 35,
        min_quote_vol_usd: float = 100_000_000.0,
    ) -> List[Dict[str, Any]]:
        """
        Busca os contratos perpétuos lineares USDT-M ativos na Binance Futures
        com Volume 24h > $100M USD, retornando o Top `limit` ordenado por liquidez.
        """
        await self._ensure_markets()
        assert self.exchange is not None

        tickers = await self.exchange.fetch_tickers()
        eligible: List[Dict[str, Any]] = []

        for sym, mkt in self.exchange.markets.items():
            # Filtra estritamente contratos perpétuos lineares cotados em USDT
            if not (
                mkt.get("swap")
                and mkt.get("linear")
                and mkt.get("quote") == "USDT"
                and mkt.get("active", True)
            ):
                continue

            ticker = tickers.get(sym)
            if not ticker:
                continue

            quote_vol = float(ticker.get("quoteVolume") or 0.0)
            last_price = float(ticker.get("last") or 0.0)
            if quote_vol >= min_quote_vol_usd and last_price > 0:
                eligible.append(
                    {
                        "symbol": sym,
                        "id": mkt.get("id", sym),
                        "quote_volume_24h": round(quote_vol, 2),
                        "last_price": last_price,
                    }
                )

        # Em ambiente Testnet o volume reportado nos tickers pode ser menor que $100M;
        # caso retorne vazio no Testnet, ordena pelos maiores volumes disponíveis.
        if not eligible and self.testnet:
            for sym, mkt in self.exchange.markets.items():
                if mkt.get("swap") and mkt.get("linear") and mkt.get("quote") == "USDT" and mkt.get("active", True):
                    ticker = tickers.get(sym) or {}
                    eligible.append(
                        {
                            "symbol": sym,
                            "id": mkt.get("id", sym),
                            "quote_volume_24h": float(ticker.get("quoteVolume") or 0.0),
                            "last_price": float(ticker.get("last") or 0.0),
                        }
                    )

        eligible.sort(key=lambda x: x["quote_volume_24h"], reverse=True)
        return eligible[:limit]

    async def fetch_ohlcv_days(
        self,
        symbol: str,
        timeframe: str = "15m",
        days: int = 30,
    ) -> List[List[float]]:
        """
        Baixa o histórico completo dos últimos `days` dias de candles para um par e timeframe,
        paginando chamadas assíncronas com semáforo de rate-limit.
        """
        await self._ensure_markets()
        assert self.exchange is not None

        tf_ms = TIMEFRAME_MS.get(timeframe, 900_000)
        now_ms = int(time.time() * 1000)
        since_ms = now_ms - (days * 86_400_000)

        all_candles: List[List[float]] = []
        cursor = since_ms
        max_batch = 1500

        async with self._sem:
            while cursor < now_ms - tf_ms:
                batch = await self.exchange.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    since=cursor,
                    limit=max_batch,
                )
                if not batch:
                    break

                all_candles.extend(batch)
                last_ts = int(batch[-1][0])
                if last_ts <= cursor:
                    break
                cursor = last_ts + tf_ms

                if len(batch) < max_batch:
                    break
                await asyncio.sleep(0.08)

        return all_candles

    async def fetch_recent_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 120,
    ) -> List[List[float]]:
        """
        Busca rápida das últimas `limit` velas para avaliação instantânea no Sniper de Fechamento.
        """
        await self._ensure_markets()
        assert self.exchange is not None

        async with self._sem:
            return await self.exchange.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        Regra de Ouro #4 (Trava Anti-Correlação):
        Consulta as posições abertas na Binance Futures em tempo real.
        Retorna apenas posições com exposição ativa (`contracts > 0`).
        """
        await self._ensure_markets()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            return []

        raw_positions = await self.exchange.fetch_positions()
        active_positions: List[Dict[str, Any]] = []

        for pos in raw_positions:
            contracts = abs(float(pos.get("contracts") or 0.0))
            if contracts <= 0:
                continue

            active_positions.append(
                {
                    "symbol": pos.get("symbol"),
                    "side": str(pos.get("side") or "").upper(),
                    "contracts": contracts,
                    "notional_usd": round(abs(float(pos.get("notional") or 0.0)), 2),
                    "entry_price": float(pos.get("entryPrice") or 0.0),
                    "mark_price": float(pos.get("markPrice") or 0.0),
                    "unrealized_pnl": round(float(pos.get("unrealizedPnl") or 0.0), 2),
                    "leverage": int(pos.get("leverage") or self.default_leverage),
                    "margin_mode": pos.get("marginMode") or "isolated",
                }
            )

        return active_positions

    async def configure_isolated_and_leverage(self, symbol: str, leverage: int) -> None:
        """
        Configura modo de margem ISOLATED e alavancagem para o par antes do envio da ordem.
        """
        await self._ensure_markets()
        assert self.exchange is not None

        try:
            await self.exchange.set_margin_mode("isolated", symbol)
        except Exception as exc:
            # A Binance retorna erro benigno (-4046) quando o ativo já está em modo ISOLATED
            if "-4046" not in str(exc) and "No need to change margin type" not in str(exc):
                logger.debug(f"[{symbol}] Aviso ao ajustar margem isolada: {exc}")

        try:
            await self.exchange.set_leverage(leverage, symbol)
        except Exception as exc:
            logger.warning(f"[{symbol}] Aviso ao ajustar alavancagem {leverage}x: {exc}")

    async def execute_atomic_order(
        self,
        symbol: str,
        direction: str,
        entry_price: float,
        stop_price: float,
        take_profit_price: float,
        leverage: Optional[int] = None,
        margin_usdt: Optional[float] = None,
    ) -> Dict[str, Any]:
        """
        Regra de Ouro #3: Ordem Atômica na Corretora.
        Envia sequencialmente:
        1. Ordem principal a mercado (`market`) em modo `isolated` com alavancagem configurada.
        2. Ordem de Stop Loss (`STOP_MARKET`) com `reduceOnly=True` e `workingType='MARK_PRICE'`.
        3. Ordem de Take Profit (`TAKE_PROFIT_MARKET`) com `reduceOnly=True` e `workingType='MARK_PRICE'`.
        Caso o posicionamento do SL ou TP falhe na corretora, executa fechamento imediato a mercado
        para jamais deixar uma posição descoberta.
        """
        await self._ensure_markets()
        assert self.exchange is not None

        lev = leverage or self.default_leverage
        margin = margin_usdt or self.order_margin_usdt
        notional_usdt = margin * lev

        is_long = direction.upper() == "LONG"
        entry_side = "buy" if is_long else "sell"
        exit_side = "sell" if is_long else "buy"

        raw_amount = notional_usdt / entry_price
        amount_str = self.exchange.amount_to_precision(symbol, raw_amount)
        stop_str = self.exchange.price_to_precision(symbol, stop_price)
        tp_str = self.exchange.price_to_precision(symbol, take_profit_price)

        amount_float = float(amount_str)
        stop_float = float(stop_str)
        tp_float = float(tp_str)

        if not (self.api_key and self.api_secret):
            raise RuntimeError(
                f"[{symbol}] Credenciais BINANCE_API_KEY / BINANCE_API_SECRET não configuradas para envio de ordem real."
            )

        # 1. Configura Margem Isolada + Alavancagem
        await self.configure_isolated_and_leverage(symbol=symbol, leverage=lev)

        # 2. Dispara Ordem Principal a Mercado
        logger.info(
            f"[{symbol}] Disparando entrada {direction} MARKET | Qtd: {amount_float} | "
            f"Alavancagem: {lev}x (Isolado) | SL: {stop_float} | TP: {tp_float}"
        )
        main_order = await self.exchange.create_order(
            symbol=symbol,
            type="market",
            side=entry_side,
            amount=amount_float,
        )

        # 3. Dispara Stop Loss (STOP_MARKET) e Take Profit (TAKE_PROFIT_MARKET) Atômicos
        sl_order: Optional[Dict[str, Any]] = None
        tp_order: Optional[Dict[str, Any]] = None

        try:
            sl_order = await self.exchange.create_order(
                symbol=symbol,
                type="STOP_MARKET",
                side=exit_side,
                amount=amount_float,
                price=None,
                params={
                    "stopPrice": stop_float,
                    "reduceOnly": True,
                    "workingType": "MARK_PRICE",
                },
            )

            tp_order = await self.exchange.create_order(
                symbol=symbol,
                type="TAKE_PROFIT_MARKET",
                side=exit_side,
                amount=amount_float,
                price=None,
                params={
                    "stopPrice": tp_float,
                    "reduceOnly": True,
                    "workingType": "MARK_PRICE",
                },
            )
        except Exception as bracket_exc:
            logger.error(
                f"[{symbol}] FALHA CRÍTICA ao posicionar SL/TP ({bracket_exc}). "
                f"Acionando Rollback Atômico (fechamento imediato reduceOnly)!"
            )
            try:
                await self.exchange.cancel_all_orders(symbol)
            except Exception:
                pass
            try:
                await self.exchange.create_order(
                    symbol=symbol,
                    type="market",
                    side=exit_side,
                    amount=amount_float,
                    params={"reduceOnly": True},
                )
            except Exception as close_exc:
                logger.critical(f"[{symbol}] Erro no fechamento de emergência: {close_exc}")
            raise RuntimeError(
                f"[{symbol}] Ordem abortada e revertida por falha na criação de SL/TP: {bracket_exc}"
            ) from bracket_exc

        return {
            "symbol": symbol,
            "direction": direction.upper(),
            "leverage": lev,
            "margin_mode": "isolated",
            "amount": amount_float,
            "notional_usdt": round(notional_usdt, 2),
            "entry_order_id": main_order.get("id"),
            "stop_order_id": sl_order.get("id") if sl_order else None,
            "tp_order_id": tp_order.get("id") if tp_order else None,
            "stop_price": stop_float,
            "take_profit_price": tp_float,
            "working_type": "MARK_PRICE",
            "reduce_only": True,
        }
