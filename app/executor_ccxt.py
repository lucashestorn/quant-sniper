"""
executor_ccxt.py
Conector Assíncrono Híbrido (Binance SPOT & FUTURES) via CCXT.
Implementa:
- Modo SPOT (padrão para contas no Brasil):
  * Usa `data-api.binance.vision` para dados públicos (imune a bloqueio HTTP 451 em servidores nos EUA).
  * Opera estratégias LONG comprando a mercado no Spot e posicionando saída atômica OCO (Take Profit + Stop Loss).
  * Monitora saldo de USDT e criptomoedas em carteira como posições abertas.
- Modo FUTURES (opcional para Testnet ou contas internacionais).
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

STABLECOINS_IGNORE = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "BRL", "DAI"}


class BinanceFuturesExecutor:
    """Gerencia a conexão assíncrona com a Binance (Modo SPOT ou FUTURES, Live ou Testnet)."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        testnet: Optional[bool] = None,
        market_type: Optional[str] = None,
        default_leverage: int = 1,
        order_margin_usdt: float = 5.5,
    ) -> None:
        self.api_key = (api_key if api_key is not None else os.getenv("BINANCE_API_KEY", "")).strip()
        self.api_secret = (
            api_secret if api_secret is not None else os.getenv("BINANCE_API_SECRET", "")
        ).strip()

        if testnet is None:
            self.testnet = os.getenv("BINANCE_TESTNET", "false").lower() in ("true", "1", "yes")
        else:
            self.testnet = testnet

        self.market_type = (
            market_type if market_type is not None else os.getenv("BINANCE_MARKET_TYPE", "spot")
        ).lower().strip()
        if self.market_type not in ("spot", "future"):
            self.market_type = "spot"

        self.default_leverage = (
            1 if self.market_type == "spot" else int(os.getenv("DEFAULT_LEVERAGE", str(default_leverage)))
        )
        self.order_margin_usdt = float(os.getenv("ORDER_MARGIN_USDT", str(order_margin_usdt)))
        self.proxy_url = os.getenv("BINANCE_PROXY_URL", "").strip()

        self.exchange: Optional[ccxt.Exchange] = None
        self.public_exchange: Optional[ccxt.binance] = None
        self._markets_loaded: bool = False
        self.last_auth_error: Optional[str] = None
        self._sem = asyncio.Semaphore(8)

    async def initialize(self) -> None:
        """Inicializa o cliente público (data-api.binance.vision) e o cliente autenticado (Spot/Futures)."""
        if self.exchange is not None and self.public_exchange is not None:
            return

        # 1. Cliente público dedicado via data-api.binance.vision (nunca sofre bloqueio HTTP 451 nos EUA)
        pub_config: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {
                "defaultType": "spot",
                "fetchCurrencies": False,
            },
            "urls": {
                "api": {
                    "public": "https://data-api.binance.vision/api/v3",
                }
            },
        }
        self.public_exchange = ccxt.binance(pub_config)

        # 2. Cliente principal (Autenticado para ordens e saldo)
        auth_config: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {
                "defaultType": self.market_type,
                "adjustForTimeDifference": True,
                "fetchCurrencies": False,
            },
        }
        if self.api_key and self.api_secret:
            auth_config["apiKey"] = self.api_key
            auth_config["secret"] = self.api_secret

        if self.proxy_url:
            auth_config["aiohttp_proxy"] = self.proxy_url

        if self.market_type == "spot" and not self.testnet:
            # Usa data-api.binance.vision para chamadas públicas também no cliente Spot principal
            auth_config["urls"] = {
                "api": {
                    "public": "https://data-api.binance.vision/api/v3",
                }
            }
            self.exchange = ccxt.binance(auth_config)
        elif self.market_type == "spot" and self.testnet:
            self.exchange = ccxt.binance(auth_config)
            self.exchange.set_sandbox_mode(True)
        else:
            self.exchange = ccxt.binanceusdm(auth_config)
            if self.testnet:
                self.exchange.set_sandbox_mode(True)

        try:
            await self.public_exchange.load_markets()
            if self.market_type == "spot" and not self.testnet:
                self.exchange.markets = self.public_exchange.markets
                self.exchange.markets_by_id = self.public_exchange.markets_by_id
                self._markets_loaded = True
            else:
                await self.exchange.load_markets()
                self._markets_loaded = True

            logger.info(
                f"Conector CCXT Binance inicializado [Modo: {self.market_type.upper()} | "
                f"{'TESTNET' if self.testnet else 'LIVE'} | "
                f"{'AUTENTICADO' if (self.api_key and self.api_secret) else 'LEITURA'}]"
            )
        except Exception as exc:
            logger.warning(f"Aviso ao carregar mercados ({exc}).")

    async def update_credentials(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool,
        market_type: Optional[str] = None,
    ) -> None:
        """Atualiza API Key, Secret, modo Testnet/Live e Spot/Futures sem derrubar o servidor."""
        await self.close()
        self.api_key = api_key.strip()
        self.api_secret = api_secret.strip()
        self.testnet = bool(testnet)
        if market_type in ("spot", "future"):
            self.market_type = market_type
        if self.market_type == "spot":
            self.default_leverage = 1
        self.last_auth_error = None
        await self.initialize()

    async def close(self) -> None:
        if self.exchange is not None:
            try:
                await self.exchange.close()
            except Exception:
                pass
            self.exchange = None
        if self.public_exchange is not None:
            try:
                await self.public_exchange.close()
            except Exception:
                pass
            self.public_exchange = None
        self._markets_loaded = False

    async def _ensure_markets(self) -> None:
        if self.exchange is None or self.public_exchange is None:
            await self.initialize()
        if not self._markets_loaded and self.public_exchange is not None:
            await self.public_exchange.load_markets()
            if self.exchange is not None and self.market_type == "spot" and not self.testnet:
                self.exchange.markets = self.public_exchange.markets
                self.exchange.markets_by_id = self.public_exchange.markets_by_id
            elif self.exchange is not None:
                await self.exchange.load_markets()
            self._markets_loaded = True

    def get_symbol_min_notional(self, symbol: str, ref_price: float = 0.0) -> float:
        mkt_source = (
            self.public_exchange.markets
            if (self.public_exchange and self.public_exchange.markets)
            else (self.exchange.markets if self.exchange else {})
        )
        if not mkt_source or symbol not in mkt_source:
            return 5.0

        mkt = mkt_source[symbol]
        limits = mkt.get("limits") or {}
        cost_min = float((limits.get("cost") or {}).get("min") or 5.0)
        amount_min = float((limits.get("amount") or {}).get("min") or 0.0)

        if ref_price > 0 and amount_min > 0:
            cost_from_amount = amount_min * ref_price
            return round(max(cost_min, cost_from_amount), 2)
        return round(cost_min, 2)

    async def get_wallet_balance(self) -> Dict[str, Any]:
        """Consulta o saldo em tempo real da carteira (Spot ou Futures) sem lançar erro 500."""
        await self._ensure_markets()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            return {
                "total_usdt": 0.0,
                "free_usdt": 0.0,
                "used_usdt": 0.0,
                "authenticated": False,
                "error": None,
            }

        try:
            bal = await self.exchange.fetch_balance()
            usdt_info = bal.get("USDT") or {}
            total_val = float(usdt_info.get("total") or 0.0)
            free_val = float(usdt_info.get("free") or 0.0)
            used_val = float(usdt_info.get("used") or 0.0)
            self.last_auth_error = None
            return {
                "total_usdt": round(total_val, 2),
                "free_usdt": round(free_val, 2),
                "used_usdt": round(used_val, 2),
                "authenticated": True,
                "error": None,
            }
        except Exception as exc:
            err_msg = str(exc)
            if "451" in err_msg or "restricted location" in err_msg.lower():
                friendly = (
                    "Bloqueio Geográfico HTTP 451: A Binance bloqueia chamadas autenticadas vindas de IPs dos EUA "
                    "(o servidor Coolify 2.25.215.205 fica em Boston/EUA). Veja instruções no painel."
                )
            elif "-2015" in err_msg or "Invalid API-key, IP, or permissions" in err_msg:
                friendly = (
                    "Recusado pela Binance (-2015): Verifique se o IP 2.25.215.205 está liberado na API Key e se 'Habilitar Trading Spot' está marcado."
                )
            else:
                friendly = err_msg[:220]

            self.last_auth_error = friendly
            logger.warning(f"Aviso ao consultar saldo USDT: {friendly}")
            return {
                "total_usdt": 0.0,
                "free_usdt": 0.0,
                "used_usdt": 0.0,
                "authenticated": True,
                "error": friendly,
            }

    async def fetch_top_liquid_symbols(
        self,
        limit: int = 35,
        min_quote_vol_usd: float = 50_000_000.0,
    ) -> List[Dict[str, Any]]:
        """
        Busca as Top 35 moedas mais líquidas cotadas em USDT.
        No modo SPOT, utiliza `public_exchange` (`data-api.binance.vision`), funcionando em qualquer região.
        """
        await self._ensure_markets()
        client = (
            self.public_exchange
            if (self.market_type == "spot" and not self.testnet and self.public_exchange)
            else self.exchange
        )
        assert client is not None

        tickers = await client.fetch_tickers()
        eligible: List[Dict[str, Any]] = []

        for sym, mkt in client.markets.items():
            if not mkt.get("active", True) or mkt.get("quote") != "USDT":
                continue

            base_coin = mkt.get("base", "")
            if base_coin in STABLECOINS_IGNORE:
                continue

            if self.market_type == "spot":
                if not mkt.get("spot"):
                    continue
            else:
                if not (mkt.get("swap") and mkt.get("linear")):
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
                        "min_notional_usdt": self.get_symbol_min_notional(sym, last_price),
                    }
                )

        if not eligible:
            for sym, mkt in client.markets.items():
                if mkt.get("active", True) and mkt.get("quote") == "USDT":
                    if self.market_type == "spot" and not mkt.get("spot"):
                        continue
                    ticker = tickers.get(sym) or {}
                    last_price = float(ticker.get("last") or 0.0)
                    if last_price > 0:
                        eligible.append(
                            {
                                "symbol": sym,
                                "id": mkt.get("id", sym),
                                "quote_volume_24h": float(ticker.get("quoteVolume") or 0.0),
                                "last_price": last_price,
                                "min_notional_usdt": self.get_symbol_min_notional(sym, last_price),
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
        """Baixa o histórico de 30 dias usando data-api.binance.vision quando em modo Spot."""
        await self._ensure_markets()
        client = (
            self.public_exchange
            if (self.market_type == "spot" and not self.testnet and self.public_exchange)
            else self.exchange
        )
        assert client is not None

        tf_ms = TIMEFRAME_MS.get(timeframe, 900_000)
        now_ms = int(time.time() * 1000)
        since_ms = now_ms - (days * 86_400_000)

        all_candles: List[List[float]] = []
        cursor = since_ms
        max_batch = 1000

        async with self._sem:
            while cursor < now_ms - tf_ms:
                batch = await client.fetch_ohlcv(
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
                await asyncio.sleep(0.06)

        return all_candles

    async def fetch_recent_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 120,
    ) -> List[List[float]]:
        await self._ensure_markets()
        client = (
            self.public_exchange
            if (self.market_type == "spot" and not self.testnet and self.public_exchange)
            else self.exchange
        )
        assert client is not None

        async with self._sem:
            return await client.fetch_ohlcv(symbol=symbol, timeframe=timeframe, limit=limit)

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        """
        Consulta posições abertas de forma segura (sem lançar exceção não tratada):
        - No modo FUTURES: consulta `fetch_positions()`.
        - No modo SPOT: verifica criptomoedas em carteira com valor equivalente >= $4.00 USDT.
        """
        await self._ensure_markets()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            return []

        try:
            if self.market_type == "spot":
                bal = await self.exchange.fetch_balance()
                totals = bal.get("total") or {}
                non_zero_coins = [
                    (coin, float(qty))
                    for coin, qty in totals.items()
                    if coin not in STABLECOINS_IGNORE and float(qty or 0.0) > 0
                ]
                if not non_zero_coins:
                    return []

                client = self.public_exchange or self.exchange
                tickers = await client.fetch_tickers()
                spot_positions: List[Dict[str, Any]] = []

                for coin, qty in non_zero_coins:
                    pair = f"{coin}/USDT"
                    ticker = tickers.get(pair)
                    if not ticker:
                        continue
                    mark_p = float(ticker.get("last") or 0.0)
                    notional = qty * mark_p
                    if notional >= 4.0:
                        spot_positions.append(
                            {
                                "symbol": pair,
                                "side": "SPOT LONG",
                                "contracts": qty,
                                "notional_usd": round(notional, 2),
                                "entry_price": mark_p,
                                "mark_price": mark_p,
                                "unrealized_pnl": 0.0,
                                "leverage": 1,
                                "margin_mode": "spot",
                            }
                        )
                return spot_positions

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
        except Exception as exc:
            logger.warning(f"Aviso ao consultar posições abertas ({self.market_type}): {exc}")
            return []

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
        Executa Ordem Atômica:
        - Modo SPOT: Compra LONG a mercado + posiciona ordem OCO (Take Profit + Stop Loss Limit).
        - Modo FUTURES: Entrada Market Isolada + STOP_MARKET + TAKE_PROFIT_MARKET (reduceOnly).
        """
        await self._ensure_markets()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            raise RuntimeError(f"[{symbol}] API Key e Secret não configuradas.")

        if self.market_type == "spot":
            if direction.upper() != "LONG":
                raise RuntimeError(f"[{symbol}] Modo SPOT opera exclusivamente sinais LONG.")

            notional_usdt = float(margin_usdt if margin_usdt is not None else self.order_margin_usdt)
            min_notional = self.get_symbol_min_notional(symbol, entry_price)
            if notional_usdt < min_notional:
                if min_notional <= notional_usdt * 1.25:
                    notional_usdt = min_notional * 1.02
                else:
                    raise RuntimeError(
                        f"[{symbol}] Valor por trade (${notional_usdt:.2f}) menor que o mínimo Spot da Binance (${min_notional:.2f})."
                    )

            raw_amount = notional_usdt / entry_price
            amount_float = float(self.exchange.amount_to_precision(symbol, raw_amount))

            logger.info(
                f"[{symbol}] [SPOT] Comprando a mercado | Qtd: {amount_float} (~${notional_usdt:.2f} USDT) | "
                f"SL: {stop_price} | TP: {take_profit_price}"
            )
            buy_order = await self.exchange.create_order(
                symbol=symbol,
                type="market",
                side="buy",
                amount=amount_float,
            )

            filled_qty = float(buy_order.get("filled") or amount_float)
            # Desconta 0.15% de margem de segurança para taxa Spot caso o usuário não tenha BNB para taxa
            sell_qty = float(self.exchange.amount_to_precision(symbol, filled_qty * 0.9985))
            tp_str = self.exchange.price_to_precision(symbol, take_profit_price)
            stop_trigger_str = self.exchange.price_to_precision(symbol, stop_price)
            stop_limit_str = self.exchange.price_to_precision(symbol, stop_price * 0.996)

            mkt = self.exchange.market(symbol)
            oco_order_id = None
            try:
                oco_res = await self.exchange.private_post_order_oco(
                    {
                        "symbol": mkt["id"],
                        "side": "SELL",
                        "quantity": str(sell_qty),
                        "price": str(tp_str),
                        "stopPrice": str(stop_trigger_str),
                        "stopLimitPrice": str(stop_limit_str),
                        "stopLimitTimeInForce": "GTC",
                    }
                )
                oco_order_id = oco_res.get("orderListId")
            except Exception as oco_exc:
                logger.warning(f"[{symbol}] Aviso ao criar OCO no Spot ({oco_exc}). Posicionando TP Limit.")
                tp_ord = await self.exchange.create_order(
                    symbol=symbol,
                    type="limit",
                    side="sell",
                    amount=sell_qty,
                    price=float(tp_str),
                )
                oco_order_id = tp_ord.get("id")

            return {
                "symbol": symbol,
                "direction": "LONG",
                "leverage": 1,
                "margin_usdt": round(notional_usdt, 2),
                "margin_mode": "spot",
                "amount": sell_qty,
                "notional_usdt": round(notional_usdt, 2),
                "entry_order_id": buy_order.get("id"),
                "stop_order_id": oco_order_id,
                "tp_order_id": oco_order_id,
                "entry_price": float(buy_order.get("average") or entry_price),
                "stop_price": float(stop_trigger_str),
                "take_profit_price": float(tp_str),
                "working_type": "SPOT_OCO",
                "reduce_only": True,
            }

        # Fluxo FUTURES
        lev = int(leverage if leverage is not None else self.default_leverage)
        margin = float(margin_usdt if margin_usdt is not None else self.order_margin_usdt)
        notional_usdt = margin * lev

        min_notional = self.get_symbol_min_notional(symbol, entry_price)
        if notional_usdt < min_notional:
            if min_notional <= notional_usdt * 1.25:
                notional_usdt = min_notional * 1.02
            else:
                raise RuntimeError(
                    f"[{symbol}] Posição nominal (${notional_usdt:.2f}) menor que o lote mínimo (${min_notional:.2f})."
                )

        is_long = direction.upper() == "LONG"
        entry_side = "buy" if is_long else "sell"
        exit_side = "sell" if is_long else "buy"

        raw_amount = notional_usdt / entry_price
        amount_float = float(self.exchange.amount_to_precision(symbol, raw_amount))
        stop_float = float(self.exchange.price_to_precision(symbol, stop_price))
        tp_float = float(self.exchange.price_to_precision(symbol, take_profit_price))

        try:
            await self.exchange.set_margin_mode("isolated", symbol)
        except Exception:
            pass
        try:
            await self.exchange.set_leverage(lev, symbol)
        except Exception:
            pass

        main_order = await self.exchange.create_order(
            symbol=symbol,
            type="market",
            side=entry_side,
            amount=amount_float,
        )

        sl_order = await self.exchange.create_order(
            symbol=symbol,
            type="STOP_MARKET",
            side=exit_side,
            amount=amount_float,
            price=None,
            params={"stopPrice": stop_float, "reduceOnly": True, "workingType": "MARK_PRICE"},
        )
        tp_order = await self.exchange.create_order(
            symbol=symbol,
            type="TAKE_PROFIT_MARKET",
            side=exit_side,
            amount=amount_float,
            price=None,
            params={"stopPrice": tp_float, "reduceOnly": True, "workingType": "MARK_PRICE"},
        )

        return {
            "symbol": symbol,
            "direction": direction.upper(),
            "leverage": lev,
            "margin_usdt": round(margin, 2),
            "margin_mode": "isolated",
            "amount": amount_float,
            "notional_usdt": round(notional_usdt, 2),
            "entry_order_id": main_order.get("id"),
            "stop_order_id": sl_order.get("id"),
            "tp_order_id": tp_order.get("id"),
            "entry_price": float(main_order.get("average") or entry_price),
            "stop_price": stop_float,
            "take_profit_price": tp_float,
            "working_type": "MARK_PRICE",
            "reduce_only": True,
        }
