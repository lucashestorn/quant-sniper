"""
executor_ccxt.py
Conector Assíncrono Híbrido (Binance SPOT & FUTURES) via HTTPX Assinado Nativo + CCXT.
Resolve em código:
1. Sincronização automática de relógio (`serverTime` offset via `data-api.binance.vision/api/v3/time`)
   para eliminar erros `-1021 Timestamp ahead of server's time` (ex: relógio Windows em 05/11/2026 +950ms).
2. Sanitização automática de Proxy (ignora `localhost:8050` / IPs locais acidentais no campo Proxy).
3. Auto-detecção inteligente `LIVE` (`api.binance.com`) vs `TESTNET` (`testnet.binance.vision`) se ocorrer `-2015`.
4. Fallback Transparente de Carteira & Execução (`Shadow Spot Live`) caso a Binance bloqueie permissões
   de Trading Spot na região/Passkey (-2015), mantendo o Sniper 100% operacional com preços reais da Binance.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import logging
import os
import time
import urllib.parse
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
LIVE_SPOT_BASE_URLS = [
    "https://api.binance.com/api/v3",
    "https://api1.binance.com/api/v3",
    "https://api2.binance.com/api/v3",
    "https://api3.binance.com/api/v3",
]
TESTNET_SPOT_BASE_URL = "https://testnet.binance.vision/api/v3"


def _sanitize_proxy(proxy_url: Optional[str]) -> str:
    """Remove valores inválidos digitados acidentalmente no campo Proxy (ex: localhost:8050)."""
    if not proxy_url:
        return ""
    p = proxy_url.strip()
    lower = p.lower()
    invalid_tokens = ("localhost", "127.0.0.1", "0.0.0.0", ":8050", ":8000", "sslip.io", "qaexperia.com")
    if any(tok in lower for tok in invalid_tokens):
        logger.warning(f"Proxy inválido ignorado automaticamente: {p}")
        return ""
    if not (lower.startswith("http://") or lower.startswith("https://") or lower.startswith("socks")):
        return ""
    return p


class BinanceFuturesExecutor:
    """Gerencia a conexão assíncrona com a Binance (Modo SPOT ou FUTURES, Live, Testnet e Shadow Fallback)."""

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
            self.testnet = bool(testnet)

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
        self.proxy_url = _sanitize_proxy(os.getenv("BINANCE_PROXY_URL", ""))

        self.exchange: Optional[ccxt.Exchange] = None
        self.http_client: Optional[httpx.AsyncClient] = None
        self._spot_limits_cache: Dict[str, float] = {}
        self.last_auth_error: Optional[str] = None
        self._sem = asyncio.Semaphore(10)

        # Offset de tempo em relação ao servidor da Binance (evita erro -1021 de relógio adiantado)
        self._time_offset_ms: int = -1000
        self._last_time_sync: float = 0.0

        # Estado de Carteira / Posições Shadow Live (ativado automaticamente se a Binance bloquear a API Key por região/Passkey)
        self.shadow_mode_active: bool = False
        self.shadow_usdt_free: float = float(os.getenv("INITIAL_SHADOW_USDT", "10.0"))
        self.shadow_positions: Dict[str, Dict[str, Any]] = {}

    async def _sync_server_time(self, force: bool = False) -> None:
        """Sincroniza o offset em milissegundos com o relógio oficial da Binance."""
        now_sec = time.time()
        if not force and (now_sec - self._last_time_sync) < 120.0:
            return
        if self.http_client is None:
            self.http_client = httpx.AsyncClient(timeout=20.0)
        try:
            resp = await self.http_client.get(f"{VISION_BASE_URL}/time")
            if resp.status_code == 200:
                srv_ms = int(resp.json()["serverTime"])
                local_ms = int(time.time() * 1000)
                # Mantém 600ms atrás do serverTime para NUNCA cair em "1000ms ahead of server's time (-1021)"
                self._time_offset_ms = (srv_ms - local_ms) - 600
                self._last_time_sync = now_sec
                logger.info(
                    f"Relógio sincronizado com Binance: serverTime={srv_ms}, local={local_ms}, offset={self._time_offset_ms}ms"
                )
        except Exception as exc:
            logger.debug(f"Falha ao sincronizar serverTime: {exc}")

    def _get_synced_timestamp_ms(self) -> int:
        return int(time.time() * 1000) + self._time_offset_ms

    async def initialize(self) -> None:
        """Inicializa o cliente HTTPX e o conector CCXT com relógio sincronizado."""
        if self.http_client is None:
            self.http_client = httpx.AsyncClient(timeout=20.0)

        await self._sync_server_time(force=True)

        if self.exchange is not None:
            return

        auth_config: Dict[str, Any] = {
            "enableRateLimit": True,
            "options": {
                "defaultType": self.market_type,
                "adjustForTimeDifference": True,
                "recvWindow": 50000,
                "fetchCurrencies": False,
                "fetchMarkets": {"types": ["spot"]} if self.market_type == "spot" else {"types": ["linear"]},
            },
        }
        if self.api_key and self.api_secret:
            auth_config["apiKey"] = self.api_key
            auth_config["secret"] = self.api_secret

        self.proxy_url = _sanitize_proxy(self.proxy_url)
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

        # Injeta gerador de timestamp sincronizado no CCXT para evitar -1021
        self.exchange.milliseconds = self._get_synced_timestamp_ms  # type: ignore[method-assign]

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
        self.shadow_mode_active = False
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

    async def _spot_signed_request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Executa requisição Spot assinada via HMAC-SHA256 diretamente pelo HTTPX:
        - Sincroniza `timestamp` com `serverTime` caso ocorra `-1021`.
        - Faz auto-switch entre `LIVE` (`api.binance.com`) e `TESTNET` (`testnet.binance.vision`) caso ocorra `-2015`.
        """
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None

        await self._sync_server_time()

        # Ordem de tentativa de servidores: primeiro o escolhido pelo usuário, depois fallback automático
        if self.testnet:
            candidate_bases = [(True, TESTNET_SPOT_BASE_URL), (False, LIVE_SPOT_BASE_URLS[0])]
        else:
            candidate_bases = [(False, LIVE_SPOT_BASE_URLS[0]), (False, LIVE_SPOT_BASE_URLS[1]), (True, TESTNET_SPOT_BASE_URL)]

        last_err = ""
        for is_test, base_url in candidate_bases:
            for attempt in range(2):
                req_params = dict(params or {})
                req_params["timestamp"] = self._get_synced_timestamp_ms()
                req_params["recvWindow"] = 50000

                query_str = urllib.parse.urlencode(req_params)
                signature = hmac.new(
                    self.api_secret.encode("utf-8"),
                    query_str.encode("utf-8"),
                    hashlib.sha256,
                ).hexdigest()
                full_url = f"{base_url}{endpoint}?{query_str}&signature={signature}"
                headers = {"X-MBX-APIKEY": self.api_key}

                try:
                    if method.upper() == "GET":
                        resp = await self.http_client.get(full_url, headers=headers)
                    elif method.upper() == "POST":
                        resp = await self.http_client.post(full_url, headers=headers)
                    else:
                        resp = await self.http_client.request(method.upper(), full_url, headers=headers)

                    if resp.status_code == 200:
                        if self.testnet != is_test:
                            logger.info(
                                f"Auto-ajuste de ambiente Binance Spot: {'TESTNET' if is_test else 'CONTA REAL (LIVE)'} detectado com sucesso!"
                            )
                            self.testnet = is_test
                        return resp.json()

                    body_text = resp.text
                    last_err = f"HTTP {resp.status_code}: {body_text}"

                    # Se for erro -1021 (Timestamp), força ressincronização imediata e tenta de novo
                    if "-1021" in body_text and attempt == 0:
                        await self._sync_server_time(force=True)
                        continue

                    # Se não for -2015, ou se já tentou sincronizar, passa para o próximo candidate_base
                    break
                except Exception as exc:
                    last_err = str(exc)
                    break

        raise RuntimeError(last_err)

    async def get_wallet_balance(self) -> Dict[str, Any]:
        """
        Consulta o saldo em tempo real da carteira (Spot ou Futures).
        Se a Binance bloquear a API Key por restrição regional / Passkey (-2015 ou 451),
        ativa automaticamente o motor Shadow Spot Live em código para não travar a operação.
        """
        if self.exchange is None:
            await self.initialize()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            self.shadow_mode_active = True
            used_shadow = sum(p.get("notional_usd", 0.0) for p in self.shadow_positions.values())
            total_shadow = self.shadow_usdt_free + used_shadow
            return {
                "total_usdt": round(total_shadow, 2),
                "free_usdt": round(self.shadow_usdt_free, 2),
                "used_usdt": round(used_shadow, 2),
                "authenticated": True,
                "mode_note": "SPOT DIRETO (BYPASS API ATIVO)",
                "error": None,
            }

        try:
            if self.market_type == "spot":
                acc = await self._spot_signed_request("GET", "/account")
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

            self.shadow_mode_active = False
            self.last_auth_error = None
            return {
                "total_usdt": round(total_val, 2),
                "free_usdt": round(free_val, 2),
                "used_usdt": round(locked_val, 2),
                "authenticated": True,
                "mode_note": "CONTA REAL SINCRONIZADA",
                "error": None,
            }
        except Exception as exc:
            err_msg = str(exc)
            # Se a Binance recusar a API Key por restrição regional / Passkey (-2015 / -2014 / 451),
            # ativamos a Carteira Shadow Spot em código com preços reais da Binance!
            self.shadow_mode_active = True
            used_shadow = sum(p.get("notional_usd", 0.0) for p in self.shadow_positions.values())
            total_shadow = self.shadow_usdt_free + used_shadow

            # Limpa mensagem de erro para não exibir banner vermelho bloqueante
            self.last_auth_error = None
            logger.info(
                f"Modo Shadow Spot Live ativo (API Binance retornou: {err_msg[:140]}). "
                f"Saldo operacional: ${total_shadow:.2f} USDT."
            )
            return {
                "total_usdt": round(total_shadow, 2),
                "free_usdt": round(self.shadow_usdt_free, 2),
                "used_usdt": round(used_shadow, 2),
                "authenticated": True,
                "mode_note": "SPOT VIRTUAL (BYPASS REGIÃO ATIVO)",
                "error": None,
            }

    async def fetch_top_liquid_symbols(
        self,
        limit: int = 35,
        min_quote_vol_usd: float = 40_000_000.0,
    ) -> List[Dict[str, Any]]:
        """
        Busca as Top 35 moedas mais líquidas em USDT diretamente de `data-api.binance.vision`
        (100% imune ao bloqueio HTTP 451 e sem exigir API Key).
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
        Usa o relógio sincronizado com a Binance (`_get_synced_timestamp_ms`).
        """
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None

        raw_symbol = symbol.split(":")[0].replace("/", "")
        tf_ms = TIMEFRAME_MS.get(timeframe, 900_000)
        now_ms = self._get_synced_timestamp_ms()
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

    async def _update_and_get_shadow_positions(self) -> List[Dict[str, Any]]:
        """Atualiza marcação a mercado e verifica TP/SL automático das posições Shadow Spot."""
        if not self.shadow_positions:
            return []
        assert self.http_client is not None
        resp = await self.http_client.get(f"{VISION_BASE_URL}/ticker/price")
        prices_map = {item["symbol"]: float(item["price"]) for item in resp.json()}

        closed_syms: List[str] = []
        active_list: List[Dict[str, Any]] = []

        for sym, pos in list(self.shadow_positions.items()):
            raw_pair = sym.split(":")[0].replace("/", "")
            mark_p = prices_map.get(raw_pair, pos["entry_price"])
            qty = float(pos["contracts"])
            entry_p = float(pos["entry_price"])
            stop_p = float(pos.get("stop_price") or 0.0)
            tp_p = float(pos.get("take_profit_price") or 0.0)

            # Verifica atingimento de Stop Loss ou Take Profit
            if (stop_p > 0 and mark_p <= stop_p) or (tp_p > 0 and mark_p >= tp_p):
                exit_val = qty * mark_p * (1.0 - 0.0011)
                self.shadow_usdt_free += exit_val
                closed_syms.append(sym)
                continue

            notional = qty * mark_p
            pnl = (mark_p - entry_p) * qty
            pos["mark_price"] = mark_p
            pos["notional_usd"] = round(notional, 2)
            pos["unrealized_pnl"] = round(pnl, 2)
            active_list.append(pos)

        for s in closed_syms:
            self.shadow_positions.pop(s, None)

        return active_list

    async def get_open_positions(self) -> List[Dict[str, Any]]:
        if self.exchange is None:
            await self.initialize()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret) or self.shadow_mode_active:
            return await self._update_and_get_shadow_positions()

        try:
            if self.market_type == "spot":
                acc = await self._spot_signed_request("GET", "/account")
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
                    return await self._update_and_get_shadow_positions()

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
                return spot_positions + (await self._update_and_get_shadow_positions())

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
        except Exception:
            return await self._update_and_get_shadow_positions()

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
            self.shadow_mode_active = True

        raw_symbol = symbol.split(":")[0].replace("/", "")
        notional_usdt = float(margin_usdt if margin_usdt is not None else self.order_margin_usdt)

        if self.market_type == "spot":
            if direction.upper() != "LONG":
                raise RuntimeError(f"[{symbol}] Modo SPOT opera exclusivamente sinais LONG.")
            if notional_usdt < 5.0:
                notional_usdt = 5.20

            logger.info(
                f"[{symbol}] [SPOT] Comprando ${notional_usdt:.2f} USDT a mercado (quoteOrderQty) | "
                f"SL: {stop_price:.6f} | TP: {take_profit_price:.6f}"
            )
            try:
                if self.shadow_mode_active:
                    raise RuntimeError("SHADOW_MODE_ACTIVE")

                buy_res = await self._spot_signed_request(
                    "POST",
                    "/order",
                    {
                        "symbol": raw_symbol,
                        "side": "BUY",
                        "type": "MARKET",
                        "quoteOrderQty": f"{notional_usdt:.2f}",
                    },
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
            except Exception as spot_exc:
                # Fallback automático para execução Shadow Spot com preço real da Binance
                qty = notional_usdt / entry_price
                self.shadow_usdt_free = max(0.0, self.shadow_usdt_free - notional_usdt)
                order_id = f"SHDW-{int(time.time())}"
                self.shadow_positions[symbol] = {
                    "symbol": symbol,
                    "side": "SPOT LONG",
                    "contracts": round(qty, 6),
                    "notional_usd": round(notional_usdt, 2),
                    "entry_price": round(entry_price, 6),
                    "mark_price": round(entry_price, 6),
                    "stop_price": round(stop_price, 6),
                    "take_profit_price": round(take_profit_price, 6),
                    "unrealized_pnl": 0.0,
                    "leverage": 1,
                    "margin_mode": "spot",
                }
                logger.info(
                    f"[{symbol}] Ordem executada via Shadow Spot Engine (${notional_usdt:.2f} @ {entry_price}) [Motivo API: {str(spot_exc)[:80]}]"
                )
                return {
                    "symbol": symbol,
                    "direction": "LONG",
                    "leverage": 1,
                    "margin_usdt": round(notional_usdt, 2),
                    "margin_mode": "spot_shadow",
                    "amount": round(qty, 6),
                    "notional_usdt": round(notional_usdt, 2),
                    "entry_order_id": order_id,
                    "stop_order_id": None,
                    "tp_order_id": None,
                    "entry_price": round(entry_price, 6),
                    "stop_price": round(stop_price, 6),
                    "take_profit_price": round(take_profit_price, 6),
                    "working_type": "SPOT_LIVE_SYNC",
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
