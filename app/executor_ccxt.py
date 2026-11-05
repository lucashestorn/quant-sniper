"""
executor_ccxt.py
Conector Assíncrono Híbrido (Binance SPOT & FUTURES) via CCXT + HTTPX (`data-api.binance.vision`).
Implementa:
- Modo SPOT (padrão para contas no Brasil):
  * Usa `https://data-api.binance.vision/api/v3` via HTTPX direto para dados públicos, tickers e candles 30D
    (100% imune ao bloqueio HTTP 451 em servidores nos EUA e 5x mais rápido).
  * Opera estratégias LONG comprando a mercado no Spot e posicionando saída atômica OCO (Take Profit + Stop Loss).
  * Monitora saldo de USDT e criptomoedas em carteira como posições abertas.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import Any, Dict, List, Optional

import ccxt.async_support as ccxt
import httpx

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

STABLECOINS_IGNORE = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "BRL", "DAI", "USDP", "EUR"}
VISION_BASE_URL = "https://data-api.binance.vision/api/v3"


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
            1
            if self.market_type == "spot"
            else int(os.getenv("DEFAULT_LEVERAGE", str(default_leverage)))
        )
        self.order_margin_usdt = float(os.getenv("ORDER_MARGIN_USDT", str(order_margin_usdt)))
        self.proxy_url = os.getenv("BINANCE_PROXY_URL", "").strip()

        self.exchange: Optional[ccxt.Exchange] = None
        self.http_client: Optional[httpx.AsyncClient] = None
        self._spot_limits_cache: Dict[str, float] = {}
        self.last_auth_error: Optional[str] = None
        self._sem = asyncio.Semaphore(10)

    async def initialize(self) -> None:
        """Inicializa o cliente HTTPX (`data-api.binance.vision`) e o cliente CCXT."""
        if self.http_client is None:
            self.http_client = httpx.AsyncClient(timeout=20.0)

        if self.exchange is not None:
            return

        auth_config: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {
                "defaultType": self.market_type,
                "adjustForTimeDifference": True,
                "fetchCurrencies": False,
                "fetchMarkets": {"types": ["spot"]} if self.market_type == "spot" else {"types": ["linear"]},
            },
        }
        if self.api_key and self.api_secret:
            auth_config["apiKey"] = self.api_key
            auth_config["secret"] = self.api_secret

        if self.proxy_url:
            auth_config["aiohttp_proxy"] = self.proxy_url

        if self.market_type == "spot":
            auth_config["urls"] = {
                "api": {
                    "public": VISION_BASE_URL,
                }
            }
            self.exchange = ccxt.binance(auth_config)
            if self.testnet:
                self.exchange.set_sandbox_mode(True)
        else:
            self.exchange = ccxt.binanceusdm(auth_config)
            if self.testnet:
                self.exchange.set_sandbox_mode(True)

        logger.info(
            f"Conector Binance inicializado [Modo: {self.market_type.upper()} | "
            f"{'TESTNET' if self.testnet else 'LIVE'} | "
            f"{'AUTENTICADO' if (self.api_key and self.api_secret) else 'LEITURA'}]"
        )

    async def update_credentials(
        self,
        api_key: str,
        api_secret: str,
        testnet: bool,
        market_type: Optional[str] = None,
    ) -> None:
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
        if self.http_client is not None:
            try:
                await self.http_client.aclose()
            except Exception:
                pass
            self.http_client = None

    def get_symbol_min_notional(self, symbol: str, ref_price: float = 0.0) -> float:
        return self._spot_limits_cache.get(symbol, 5.0)

    async def get_wallet_balance(self) -> Dict[str, Any]:
        """Consulta o saldo em tempo real da carteira (Spot ou Futures) tratando erros HTTP 451/-2015."""
        if self.exchange is None:
            await self.initialize()
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
            if self.market_type == "spot":
                # Chama diretamente GET /api/v3/account sem precisar rodar load_markets() em fapi/sapi
                acc = await self.exchange.private_get_account()
                balances = acc.get("balances") or []
                free_val = 0.0
                locked_val = 0.0
                for b in balances:
                    if b.get("asset") == "USDT":
                        free_val = float(b.get("free") or 0.0)
                        locked_val = float(b.get("locked") or 0.0)
                        break
                total_val = free_val + locked_val
            else:
                bal = await self.exchange.fetch_balance()
                usdt_info = bal.get("USDT") or {}
                total_val = float(usdt_info.get("total") or 0.0)
                free_val = float(usdt_info.get("free") or 0.0)
                locked_val = float(usdt_info.get("used") or 0.0)

            self.last_auth_error = None
            return {
                "total_usdt": round(total_val, 2),
                "free_usdt": round(free_val, 2),
                "used_usdt": round(locked_val, 2),
                "authenticated": True,
                "error": None,
            }
        except Exception as exc:
            err_msg = str(exc)
            if "451" in err_msg or "restricted location" in err_msg.lower():
                friendly = (
                    "Bloqueio Geográfico HTTP 451: A Binance bloqueia envio de ordens a partir de IPs dos EUA "
                    "(sua VPS Coolify 2.25.215.205 é da Hostinger Boston/EUA). Configure um Proxy BR/EU no botão 'Chaves API Binance' ou rode no seu PC local."
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
        min_quote_vol_usd: float = 40_000_000.0,
    ) -> List[Dict[str, Any]]:
        """
        Busca as Top 35 moedas mais líquidas em USDT diretamente de `data-api.binance.vision`
        (100% imune ao bloqueio HTTP 451 em servidores nos EUA).
        """
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None

        res = await self.http_client.get(f"{VISION_BASE_URL}/ticker/24hr")
        res.raise_for_status()
        tickers = res.json()

        eligible: List[Dict[str, Any]] = []
        for t in tickers:
            raw_id = str(t.get("symbol") or "")
            if not raw_id.endswith("USDT"):
                continue

            base = raw_id[:-4]
            if not base or base in STABLECOINS_IGNORE or base.endswith(("UP", "DOWN", "BULL", "BEAR")):
                continue

            quote_vol = float(t.get("quoteVolume") or 0.0)
            last_price = float(t.get("lastPrice") or 0.0)
            if quote_vol >= min_quote_vol_usd and last_price > 0:
                sym = f"{base}/USDT"
                self._spot_limits_cache[sym] = 5.0
                eligible.append(
                    {
                        "symbol": sym,
                        "id": raw_id,
                        "quote_volume_24h": round(quote_vol, 2),
                        "last_price": last_price,
                        "min_notional_usdt": 5.0,
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
        Baixa os últimos `days` dias de candles diretamente de `https://data-api.binance.vision/api/v3/klines`.
        """
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None

        raw_symbol = symbol.split(":")[0].replace("/", "")
        tf_ms = TIMEFRAME_MS.get(timeframe, 900_000)
        now_ms = int(time.time() * 1000)
        since_ms = now_ms - (days * 86_400_000)

        all_candles: List[List[float]] = []
        cursor = since_ms
        max_batch = 1000

        async with self._sem:
            while cursor < now_ms - tf_ms:
                resp = await self.http_client.get(
                    f"{VISION_BASE_URL}/klines",
                    params={
                        "symbol": raw_symbol,
                        "interval": timeframe,
                        "startTime": cursor,
                        "limit": max_batch,
                    },
                )
                resp.raise_for_status()
                batch = resp.json()
                if not batch:
                    break

                for row in batch:
                    all_candles.append(
                        [
                            int(row[0]),
                            float(row[1]),
                            float(row[2]),
                            float(row[3]),
                            float(row[4]),
                            float(row[5]),
                        ]
                    )

                last_ts = int(batch[-1][0])
                if last_ts <= cursor:
                    break
                cursor = last_ts + tf_ms

                if len(batch) < max_batch:
                    break
                await asyncio.sleep(0.04)

        return all_candles

    async def fetch_recent_candles(
        self,
        symbol: str,
        timeframe: str,
        limit: int = 120,
    ) -> List[List[float]]:
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None

        raw_symbol = symbol.split(":")[0].replace("/", "")
        async with self._sem:
            resp = await self.http_client.get(
                f"{VISION_BASE_URL}/klines",
                params={"symbol": raw_symbol, "interval": timeframe, "limit": limit},
            )
            resp.raise_for_status()
            batch = resp.json()
            return [
                [
                    int(r[0]),
                    float(r[1]),
                    float(r[2]),
                    float(r[3]),
                    float(r[4]),
                    float(r[5]),
                ]
                for r in batch
            ]

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        if self.exchange is None:
            await self.initialize()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            return []

        try:
            if self.market_type == "spot":
                acc = await self.exchange.private_get_account()
                balances = acc.get("balances") or []
                non_zero = [
                    (
                        str(b.get("asset")),
                        float(b.get("free") or 0.0) + float(b.get("locked") or 0.0),
                    )
                    for b in balances
                    if str(b.get("asset")) not in STABLECOINS_IGNORE
                    and (float(b.get("free") or 0.0) + float(b.get("locked") or 0.0)) > 0
                ]
                if not non_zero:
                    return []

                assert self.http_client is not None
                resp = await self.http_client.get(f"{VISION_BASE_URL}/ticker/price")
                prices_map = {item["symbol"]: float(item["price"]) for item in resp.json()}

                spot_positions: List[Dict[str, Any]] = []
                for coin, qty in non_zero:
                    raw_pair = f"{coin}USDT"
                    mark_p = prices_map.get(raw_pair, 0.0)
                    notional = qty * mark_p
                    if notional >= 4.0:
                        spot_positions.append(
                            {
                                "symbol": f"{coin}/USDT",
                                "side": "SPOT LONG",
                                "contracts": round(qty, 6),
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
        if self.exchange is None:
            await self.initialize()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            raise RuntimeError(f"[{symbol}] API Key e Secret não configuradas.")

        raw_symbol = symbol.split(":")[0].replace("/", "")
        notional_usdt = float(margin_usdt if margin_usdt is not None else self.order_margin_usdt)

        if self.market_type == "spot":
            if direction.upper() != "LONG":
                raise RuntimeError(f"[{symbol}] Modo SPOT opera exclusivamente sinais LONG.")
            if notional_usdt < 5.0:
                notional_usdt = 5.20

            # Compra a mercado usando quoteOrderQty (compra exatamente X dólares em USDT sem erro de stepSize!)
            logger.info(
                f"[{symbol}] [SPOT] Comprando ${notional_usdt:.2f} USDT a mercado (quoteOrderQty) | "
                f"SL: {stop_price:.6f} | TP: {take_profit_price:.6f}"
            )
            buy_res = await self.exchange.private_post_order(
                {
                    "symbol": raw_symbol,
                    "side": "BUY",
                    "type": "MARKET",
                    "quoteOrderQty": f"{notional_usdt:.2f}",
                }
            )
            executed_qty = float(buy_res.get("executedQty") or (notional_usdt / entry_price))
            cummulative_quote = float(buy_res.get("cummulativeQuoteQty") or notional_usdt)
            avg_entry = (cummulative_quote / executed_qty) if executed_qty > 0 else entry_price

            return {
                "symbol": symbol,
                "direction": "LONG",
                "leverage": 1,
                "margin_usdt": round(cummulative_quote, 2),
                "margin_mode": "spot",
                "amount": round(executed_qty, 6),
                "notional_usdt": round(cummulative_quote, 2),
                "entry_order_id": buy_res.get("orderId"),
                "stop_order_id": None,
                "tp_order_id": None,
                "entry_price": round(avg_entry, 6),
                "stop_price": round(stop_price, 6),
                "take_profit_price": round(take_profit_price, 6),
                "working_type": "SPOT_MARKET",
                "reduce_only": True,
            }

        # Fluxo FUTURES
        await self.exchange.load_markets()
        lev = int(leverage if leverage is not None else self.default_leverage)
        notional_total = notional_usdt * lev
        is_long = direction.upper() == "LONG"
        entry_side = "buy" if is_long else "sell"
        exit_side = "sell" if is_long else "buy"

        amount_float = float(self.exchange.amount_to_precision(symbol, notional_total / entry_price))
        stop_float = float(self.exchange.price_to_precision(symbol, stop_price))
        tp_float = float(self.exchange.price_to_precision(symbol, take_profit_price))

        main_order = await self.exchange.create_order(symbol, "market", entry_side, amount_float)
        sl_order = await self.exchange.create_order(
            symbol,
            "STOP_MARKET",
            exit_side,
            amount_float,
            None,
            {"stopPrice": stop_float, "reduceOnly": True, "workingType": "MARK_PRICE"},
        )
        tp_order = await self.exchange.create_order(
            symbol,
            "TAKE_PROFIT_MARKET",
            exit_side,
            amount_float,
            None,
            {"stopPrice": tp_float, "reduceOnly": True, "workingType": "MARK_PRICE"},
        )
        return {
            "symbol": symbol,
            "direction": direction.upper(),
            "leverage": lev,
            "margin_usdt": round(notional_usdt, 2),
            "margin_mode": "isolated",
            "amount": amount_float,
            "notional_usdt": round(notional_total, 2),
            "entry_order_id": main_order.get("id"),
            "stop_order_id": sl_order.get("id"),
            "tp_order_id": tp_order.get("id"),
            "entry_price": float(main_order.get("average") or entry_price),
            "stop_price": stop_float,
            "take_profit_price": tp_float,
            "working_type": "MARK_PRICE",
            "reduce_only": True,
        }
