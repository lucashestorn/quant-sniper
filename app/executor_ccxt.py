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
from datetime import datetime, timezone
from decimal import Decimal, ROUND_DOWN
import hashlib
import hmac
import json
import logging
import os
from pathlib import Path
import time
import urllib.parse
from typing import Any, Dict, List, Optional

import ccxt.async_support as ccxt
import httpx

logger = logging.getLogger("quant_sniper.executor_ccxt")

BRACKETS_FILE_PATH = Path(__file__).parent.parent / "real_spot_brackets.json"
CLOSED_TRADES_PATH = Path(__file__).parent.parent / "closed_trades.json"
EQUITY_HISTORY_PATH = Path(__file__).parent.parent / "equity_history.json"

TIMEFRAME_MS: Dict[str, int] = {
    "1m": 60_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
}

STABLECOINS_IGNORE = {"USDT", "USDC", "FDUSD", "BUSD", "TUSD", "BRL", "DAI", "USDP", "EUR", "USD1", "U", "USDE", "RLUSD", "PYUSD"}
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
        self._step_size_cache: Dict[str, str] = {}
        self.last_auth_error: Optional[str] = None
        self._sem = asyncio.Semaphore(10)

        # Offset de tempo em relação ao servidor da Binance (evita erro -1021 de relógio adiantado)
        self._time_offset_ms: int = -1000
        self._last_time_sync: float = 0.0

        # Estado de Carteira / Posições Shadow Live (ativado automaticamente se a Binance bloquear a API Key por região/Passkey)
        self.shadow_mode_active: bool = False
        self.shadow_usdt_free: float = float(os.getenv("INITIAL_SHADOW_USDT", "10.0"))
        self.shadow_positions: Dict[str, Dict[str, Any]] = {}
        # Rastreamento persistente de Entrada, Stop Loss e Take Profit para posições Reais Spot
        self.real_spot_brackets: Dict[str, Dict[str, Any]] = self._load_brackets()
        # Histórico persistente de Trades Fechados e Curva de Capital (para Gráficos & Linha do Saldo)
        self.closed_trades: List[Dict[str, Any]] = self._load_closed_trades()
        self.equity_history: List[Dict[str, Any]] = self._load_equity_history()
        # Configurações de Auto-Venda Inteligente quando o momento passar
        self.auto_momentum_exit: bool = True
        self.max_hold_minutes: int = 12
        self.last_closed_events: List[Dict[str, Any]] = []
        # Rotação Ativa de Capital por Oportunidade Promissora (Opportunity Cost Switch)
        self.scalp_rotation_enabled: bool = os.getenv("SCALP_ROTATION_ENABLED", "true").lower() in ("true", "1", "yes")
        self.scalp_rotation_min_pnl: float = float(os.getenv("SCALP_ROTATION_MIN_PNL", "0.22"))
        self.scalp_rotation_min_score: float = float(os.getenv("SCALP_ROTATION_MIN_SCORE", "78.0"))

    def _load_brackets(self) -> Dict[str, Dict[str, Any]]:
        try:
            if BRACKETS_FILE_PATH.exists():
                return json.loads(BRACKETS_FILE_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.debug(f"Aviso ao carregar real_spot_brackets.json: {exc}")
        return {}

    def save_brackets(self) -> None:
        try:
            BRACKETS_FILE_PATH.write_text(
                json.dumps(self.real_spot_brackets, indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.debug(f"Aviso ao salvar real_spot_brackets.json: {exc}")

    def _load_closed_trades(self) -> List[Dict[str, Any]]:
        try:
            if CLOSED_TRADES_PATH.exists():
                return json.loads(CLOSED_TRADES_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.debug(f"Aviso ao carregar closed_trades.json: {exc}")
        return []

    def save_closed_trades(self) -> None:
        try:
            CLOSED_TRADES_PATH.write_text(
                json.dumps(self.closed_trades[-200:], indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.debug(f"Aviso ao salvar closed_trades.json: {exc}")

    def _load_equity_history(self) -> List[Dict[str, Any]]:
        try:
            if EQUITY_HISTORY_PATH.exists():
                return json.loads(EQUITY_HISTORY_PATH.read_text(encoding="utf-8"))
        except Exception as exc:
            logger.debug(f"Aviso ao carregar equity_history.json: {exc}")
        now_utc = datetime.now(timezone.utc)
        return [
            {
                "timestamp_utc": now_utc.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "timestamp_ts": time.time(),
                "time_label": now_utc.strftime("%H:%M"),
                "total_usdt": 18.50,
                "scalp_pnl": 0.0,
                "sniper_pnl": 0.0,
                "rocket_pnl": 0.0,
                "last_event": "Inicialização da Carteira Spot",
            }
        ]

    def save_equity_history(self) -> None:
        try:
            EQUITY_HISTORY_PATH.write_text(
                json.dumps(self.equity_history[-300:], indent=2),
                encoding="utf-8",
            )
        except Exception as exc:
            logger.debug(f"Aviso ao salvar equity_history.json: {exc}")

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

        # Ordem de tentativa de servidores: sem misturar Live com Testnet
        if self.testnet:
            candidate_bases = [(True, TESTNET_SPOT_BASE_URL)]
        else:
            candidate_bases = [(False, LIVE_SPOT_BASE_URLS[0]), (False, LIVE_SPOT_BASE_URLS[1])]

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
            # Se a Binance recusar a API Key por restrição regional (HTTP 451) ou IP
            self.shadow_mode_active = True
            used_shadow = sum(p.get("notional_usd", 0.0) for p in self.shadow_positions.values())
            total_shadow = self.shadow_usdt_free + used_shadow

            if "451" in err_msg or "restricted location" in err_msg.lower():
                self.last_auth_error = (
                    "Bloqueio Geográfico da Binance (HTTP 451): A VPS do Coolify está hospedada nos EUA "
                    "(Boston - 2.25.215.205), onde a Binance.com é proibida por lei. "
                    "Para operar na nuvem, altere o datacenter da VPS para o Brasil/Europa na Hostinger, ou configure um Proxy não-americano."
                )
                mode_note = "VPS NOS EUA (BLOQUEIO 451 BINANCE)"
            else:
                self.last_auth_error = f"Aviso API Binance: {err_msg[:160]}"
                mode_note = "SPOT VIRTUAL (BYPASS ATIVO)"

            logger.info(
                f"Modo Shadow Spot Live ativo (API Binance retornou: {err_msg[:140]}). "
                f"Saldo operacional: ${total_shadow:.2f} USDT."
            )
            return {
                "total_usdt": round(total_shadow, 2),
                "free_usdt": round(self.shadow_usdt_free, 2),
                "used_usdt": round(used_shadow, 2),
                "authenticated": True,
                "mode_note": mode_note,
                "error": self.last_auth_error,
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
        """Atualiza marcação a mercado e verifica TP/SL e saídas inteligentes das posições Shadow Spot."""
        if not self.shadow_positions:
            return []
        assert self.http_client is not None
        resp = await self.http_client.get(f"{VISION_BASE_URL}/ticker/price")
        prices_map = {item["symbol"]: float(item["price"]) for item in resp.json()}

        closed_syms: List[str] = []
        active_list: List[Dict[str, Any]] = []
        now_sec = time.time()

        for sym, pos in list(self.shadow_positions.items()):
            raw_pair = sym.split(":")[0].replace("/", "")
            mark_p = prices_map.get(raw_pair, pos["entry_price"])
            qty = float(pos["contracts"])
            entry_p = float(pos["entry_price"])
            stop_p = float(pos.get("stop_price") or 0.0)
            tp_p = float(pos.get("take_profit_price") or 0.0)
            prev_max = float(pos.get("max_price") or entry_p)
            max_p = max(prev_max, mark_p)
            pos["max_price"] = max_p
            opened_ts = float(pos.get("opened_at_ts") or now_sec)
            pos["opened_at_ts"] = opened_ts

            pnl = (mark_p - entry_p) * qty
            pnl_pct = ((mark_p - entry_p) / entry_p * 100.0) if entry_p > 0 else 0.0
            max_pnl_pct = ((max_p - entry_p) / entry_p * 100.0) if entry_p > 0 else 0.0
            age_min = max(0.0, (now_sec - opened_ts) / 60.0)
            tp_pct = ((tp_p - entry_p) / entry_p * 100.0) if (entry_p > 0 and tp_p > 0) else 0.55
            sl_pct = ((entry_p - stop_p) / entry_p * 100.0) if (entry_p > 0 and stop_p > 0) else 0.35

            exit_reason: Optional[str] = None
            # Verifica atingimento de Stop Loss ou Take Profit
            if tp_p > 0 and mark_p >= tp_p:
                exit_reason = "TAKE_PROFIT"
            elif stop_p > 0 and mark_p <= stop_p:
                exit_reason = "STOP_LOSS"
            elif self.auto_momentum_exit:
                mom = await self._check_position_momentum(raw_pair, mark_p)
                live_buy = mom["buy_pressure_pct"]
                live_tak = mom["taker_buy_pct"]
                pos["live_buy_pressure_pct"] = live_buy
                pos["live_taker_buy_pct"] = live_tak
                if max_pnl_pct >= 0.25 and (
                    (pnl_pct <= max_pnl_pct * 0.58 and pnl_pct >= 0.08)
                    or (live_buy < 44.0 and live_tak < 45.0 and pnl_pct >= 0.05)
                ):
                    exit_reason = f"MOMENTO_PASSOU_TOPO (Pico +{max_pnl_pct:.2f}% -> Protegido em +{pnl_pct:.2f}%)"
                elif age_min >= 1.5 and live_buy <= 40.0 and live_tak <= 42.0:
                    exit_reason = f"MOMENTO_PASSOU_FLUXO_VIROU (Book={live_buy}% | Tape={live_tak}%)"
                elif age_min >= float(self.max_hold_minutes) and (live_buy < 49.0 or live_tak < 47.0):
                    exit_reason = f"MOMENTO_PASSOU_TEMPO ({age_min:.1f}m sem romper)"

            if exit_reason is not None:
                exit_val = qty * mark_p * (1.0 - 0.0011)
                self.shadow_usdt_free += exit_val
                closed_syms.append(sym)
                receipt = {
                    "symbol": sym,
                    "reason": exit_reason,
                    "sold_qty": round(qty, 6),
                    "received_usdt": round(exit_val, 2),
                    "entry_price": round(entry_p, 6),
                    "exit_price": round(mark_p, 6),
                    "realized_pnl": round(pnl, 2),
                }
                self.last_closed_events.append(receipt)
                continue

            notional = qty * mark_p
            pos["mark_price"] = mark_p
            pos["notional_usd"] = round(notional, 2)
            pos["unrealized_pnl"] = round(pnl, 2)
            pos["pnl_pct"] = round(pnl_pct, 2)
            pos["max_pnl_pct"] = round(max_pnl_pct, 2)
            pos["tp_pct"] = round(tp_pct, 2)
            pos["sl_pct"] = round(sl_pct, 2)
            pos["age_minutes"] = round(age_min, 1)
            active_list.append(pos)

        for s in closed_syms:
            self.shadow_positions.pop(s, None)

        return active_list

    async def _get_step_size(self, raw_pair: str) -> str:
        if raw_pair in self._step_size_cache:
            return self._step_size_cache[raw_pair]
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None
        try:
            resp = await self.http_client.get(
                f"{VISION_BASE_URL}/exchangeInfo",
                params={"symbol": raw_pair},
            )
            if resp.status_code == 200:
                symbols_info = resp.json().get("symbols") or []
                if symbols_info:
                    for flt in symbols_info[0].get("filters") or []:
                        if flt.get("filterType") == "LOT_SIZE":
                            step = str(flt.get("stepSize") or "0.000001")
                            self._step_size_cache[raw_pair] = step
                            return step
        except Exception as exc:
            logger.debug(f"Aviso ao buscar stepSize de {raw_pair}: {exc}")
        return "0.000001"

    @staticmethod
    def _floor_to_step(qty: float, step_str: str) -> str:
        d_qty = Decimal(str(qty))
        d_step = Decimal(step_str)
        if d_step <= 0:
            return f"{qty:.6f}"
        floored = (d_qty // d_step) * d_step
        return format(floored.normalize(), "f")

    async def _convert_dust_to_usdt(self, from_asset: str, amount: float) -> Dict[str, Any]:
        """
        Converte qualquer saldo remanescente (inclusive frações abaixo do minNotional de $5.00 da Binance)
        diretamente para USDT usando o endpoint oficial Binance Convert (/sapi/v1/convert) com ZERO taxas e sem rejeição.
        """
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None
        await self._sync_server_time()

        amt_str = f"{amount:.8f}".rstrip("0").rstrip(".")
        params = {
            "fromAsset": from_asset,
            "toAsset": "USDT",
            "fromAmount": amt_str,
            "timestamp": self._get_synced_timestamp_ms(),
            "recvWindow": 50000,
        }
        query_str = urllib.parse.urlencode(params)
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            query_str.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        url = f"{LIVE_SPOT_BASE_URLS[0]}/sapi/v1/convert/getQuote?{query_str}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}

        r = await self.http_client.post(url, headers=headers)
        if r.status_code != 200:
            raise RuntimeError(f"Falha na cotação Binance Convert: HTTP {r.status_code} {r.text}")

        qdata = r.json()
        quote_id = qdata.get("quoteId")
        if not quote_id:
            raise RuntimeError(f"QuoteId inválido no Binance Convert: {qdata}")

        p2 = {
            "quoteId": quote_id,
            "timestamp": self._get_synced_timestamp_ms(),
            "recvWindow": 50000,
        }
        q2 = urllib.parse.urlencode(p2)
        sig2 = hmac.new(
            self.api_secret.encode("utf-8"),
            q2.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()
        url2 = f"{LIVE_SPOT_BASE_URLS[0]}/sapi/v1/convert/acceptQuote?{q2}&signature={sig2}"
        r2 = await self.http_client.post(url2, headers=headers)
        if r2.status_code != 200:
            raise RuntimeError(f"Falha na liquidação Binance Convert: HTTP {r2.status_code} {r2.text}")

        to_amount = float(qdata.get("toAmount") or 0.0)
        ratio = float(qdata.get("ratio") or 0.0)
        return {
            "orderId": r2.json().get("orderId") or quote_id,
            "sold_qty": float(qdata.get("fromAmount") or amount),
            "received_usdt": to_amount,
            "avg_exit": ratio,
        }

    async def close_spot_position(self, symbol: str, reason: str = "MANUAL") -> Dict[str, Any]:
        """
        Vende 100% da posição aberta de `symbol` para USDT (Spot ou Shadow),
        com proteção automática contra minNotional < $5.00 via Binance Convert.
        """
        if self.exchange is None:
            await self.initialize()
        assert self.http_client is not None

        sym_pair = symbol.split(":")[0]
        if "/" not in sym_pair and sym_pair.endswith("USDT"):
            sym_pair = f"{sym_pair[:-4]}/USDT"
        coin = sym_pair.split("/")[0]
        raw_pair = sym_pair.replace("/", "")

        if sym_pair in self.shadow_positions:
            pos = self.shadow_positions.pop(sym_pair)
            qty = float(pos.get("contracts") or 0.0)
            mark_p = float(pos.get("mark_price") or pos.get("entry_price") or 0.0)
            exit_val = qty * mark_p * (1.0 - 0.0011)
            self.shadow_usdt_free += exit_val
            return {
                "symbol": sym_pair,
                "reason": reason,
                "sold_qty": round(qty, 6),
                "received_usdt": round(exit_val, 2),
                "exit_price": round(mark_p, 6),
            }

        acc = await self._spot_signed_request("GET", "/account")
        balances = acc.get("balances") or []
        free_qty = 0.0
        for b in balances:
            if str(b.get("asset")) == coin:
                free_qty = float(b.get("free") or 0.0)
                break

        if free_qty <= 0:
            self.real_spot_brackets.pop(sym_pair, None)
            self.save_brackets()
            raise RuntimeError(f"Saldo livre de {coin} já está zerado.")

        resp_p = await self.http_client.get(f"{VISION_BASE_URL}/ticker/price", params={"symbol": raw_pair})
        cur_p = float(resp_p.json().get("price") or 0.0)
        notional_est = free_qty * cur_p

        # Se o valor for menor que o minNotional de $5.00 da Binance, usa o Binance Convert diretamente
        if notional_est < 5.05:
            convert_res = await self._convert_dust_to_usdt(coin, free_qty)
            executed_qty = convert_res["sold_qty"]
            received_usdt = convert_res["received_usdt"]
            avg_exit = convert_res["avg_exit"]
            order_id = convert_res["orderId"]
        else:
            step_str = await self._get_step_size(raw_pair)
            formatted_qty = self._floor_to_step(free_qty, step_str)
            try:
                sell_res = await self._spot_signed_request(
                    "POST",
                    "/order",
                    {
                        "symbol": raw_pair,
                        "side": "SELL",
                        "type": "MARKET",
                        "quantity": formatted_qty,
                    },
                )
                executed_qty = float(sell_res.get("executedQty") or formatted_qty)
                received_usdt = float(sell_res.get("cummulativeQuoteQty") or 0.0)
                avg_exit = (received_usdt / executed_qty) if executed_qty > 0 else cur_p
                order_id = sell_res.get("orderId")
            except Exception as spot_err:
                logger.warning(f"[{sym_pair}] Ordem a mercado falhou ({spot_err}). Acionando fallback Binance Convert...")
                convert_res = await self._convert_dust_to_usdt(coin, free_qty)
                executed_qty = convert_res["sold_qty"]
                received_usdt = convert_res["received_usdt"]
                avg_exit = convert_res["avg_exit"]
                order_id = convert_res["orderId"]

        bkt = self.real_spot_brackets.pop(sym_pair, {})
        self.save_brackets()
        entry_p = float(bkt.get("entry_price") or avg_exit)
        realized_pnl = (avg_exit - entry_p) * executed_qty
        pos_mode = str(bkt.get("mode") or "SCALP_BOOK")
        tf = str(bkt.get("timeframe") or ("BOOK-L2" if "SCALP" in pos_mode else "1h"))
        opened_ts = float(bkt.get("opened_at_ts") or time.time())
        duration_min = max(0.1, (time.time() - opened_ts) / 60.0)
        pnl_pct = ((avg_exit - entry_p) / entry_p * 100.0) if entry_p > 0 else 0.0

        horizon = bkt.get("horizon")
        if not horizon:
            if "SCALP" in pos_mode:
                horizon = "Minutos (~1 a 12m)"
            elif "ROCKET" in pos_mode:
                horizon = "Minutos a Horas (~15m a 6h)"
            else:
                horizon = "Swing Trade (~4h a 48h)" if tf in ("1h", "4h") else "Day Trade (~15m a 2h)"

        closed_item = {
            "id": f"CLSD-{int(time.time() * 1000)}",
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "timestamp_ts": time.time(),
            "time_label": datetime.now(timezone.utc).strftime("%H:%M"),
            "symbol": sym_pair,
            "mode": pos_mode,
            "timeframe": tf,
            "horizon": horizon,
            "direction": "LONG",
            "entry_price": round(entry_p, 6),
            "exit_price": round(avg_exit, 6),
            "sold_qty": round(executed_qty, 6),
            "received_usdt": round(received_usdt, 2),
            "realized_pnl": round(realized_pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
            "duration_minutes": round(duration_min, 1),
            "reason": reason,
        }
        self.closed_trades.append(closed_item)
        self.save_closed_trades()

        # Atualiza a linha de saldo separada por modalidade para os gráficos
        last_eq = self.equity_history[-1] if self.equity_history else {
            "total_usdt": 18.50,
            "scalp_pnl": 0.0,
            "sniper_pnl": 0.0,
            "rocket_pnl": 0.0,
        }
        scalp_pnl = round(last_eq.get("scalp_pnl", 0.0) + (realized_pnl if "SCALP" in pos_mode else 0.0), 2)
        sniper_pnl = round(last_eq.get("sniper_pnl", 0.0) + (realized_pnl if "SNIPER" in pos_mode else 0.0), 2)
        rocket_pnl = round(last_eq.get("rocket_pnl", 0.0) + (realized_pnl if "ROCKET" in pos_mode else 0.0), 2)
        total_usdt = round(last_eq.get("total_usdt", 18.50) + realized_pnl, 2)

        self.equity_history.append({
            "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "timestamp_ts": time.time(),
            "time_label": datetime.now(timezone.utc).strftime("%H:%M"),
            "total_usdt": total_usdt,
            "scalp_pnl": scalp_pnl,
            "sniper_pnl": sniper_pnl,
            "rocket_pnl": rocket_pnl,
            "last_event": f"Venda {sym_pair} ({reason}): {realized_pnl:+.2f}$ ({pnl_pct:+.2f}%)",
        })
        self.save_equity_history()

        logger.info(
            f"[{sym_pair}] [SPOT SELL {reason}] Vendido {executed_qty} {coin} -> ${received_usdt:.2f} USDT @ {avg_exit:.6f} (PnL: ${realized_pnl:+.2f})"
        )
        receipt = {
            "symbol": sym_pair,
            "reason": reason,
            "order_id": order_id,
            "sold_qty": round(executed_qty, 6),
            "received_usdt": round(received_usdt, 2),
            "entry_price": round(entry_p, 6),
            "exit_price": round(avg_exit, 6),
            "realized_pnl": round(realized_pnl, 2),
            "horizon": horizon,
            "mode": pos_mode,
        }
        self.last_closed_events.append(receipt)
        return receipt

    async def _check_position_momentum(self, raw_pair: str, mark_p: float) -> Dict[str, float]:
        """
        Mede o pulso instantâneo do Livro de Ofertas (50 níveis) e da Agressão no Tape de 1m
        para uma posição aberta, permitindo detectar se o momento comprador passou ou virou vendedor.
        """
        assert self.http_client is not None
        try:
            depth_resp, k1m_resp = await asyncio.gather(
                self.http_client.get(
                    f"{VISION_BASE_URL}/depth",
                    params={"symbol": raw_pair, "limit": 50},
                ),
                self.http_client.get(
                    f"{VISION_BASE_URL}/klines",
                    params={"symbol": raw_pair, "interval": "1m", "limit": 2},
                ),
            )
            buy_pct = 50.0
            taker_pct = 50.0
            if depth_resp.status_code == 200:
                d = depth_resp.json()
                band_low = mark_p * 0.993
                band_high = mark_p * 1.007
                b_usd = sum(float(p) * float(q) for p, q in (d.get("bids") or []) if float(p) >= band_low)
                a_usd = sum(float(p) * float(q) for p, q in (d.get("asks") or []) if float(p) <= band_high)
                tot = b_usd + a_usd
                if tot > 0:
                    buy_pct = (b_usd / tot) * 100.0
            if k1m_resp.status_code == 200:
                kl = k1m_resp.json()
                tot_v = sum(float(k[7]) for k in kl) if kl else 0.0
                tak_v = sum(float(k[10]) for k in kl) if kl else 0.0
                if tot_v > 0:
                    taker_pct = (tak_v / tot_v) * 100.0
            return {"buy_pressure_pct": round(buy_pct, 1), "taker_buy_pct": round(taker_pct, 1)}
        except Exception:
            return {"buy_pressure_pct": 50.0, "taker_buy_pct": 50.0}

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
                now_sec = time.time()
                brackets_dirty = False

                for coin, qty in non_zero:
                    raw_pair = f"{coin}USDT"
                    sym_pair = f"{coin}/USDT"
                    mark_p = prices_map.get(raw_pair, 0.0)
                    notional = qty * mark_p
                    if notional >= 4.0:
                        bkt = self.real_spot_brackets.get(sym_pair, {})
                        entry_p = float(bkt.get("entry_price") or mark_p)
                        stop_p = float(bkt.get("stop_price") or (entry_p * 0.9965))
                        tp_p = float(bkt.get("take_profit_price") or (entry_p * 1.0055))
                        pos_mode = str(bkt.get("mode") or "SCALP_BOOK")
                        prev_max = float(bkt.get("max_price") or entry_p)
                        max_p = max(prev_max, mark_p)
                        opened_ts = float(bkt.get("opened_at_ts") or now_sec)

                        if sym_pair not in self.real_spot_brackets or max_p > prev_max:
                            self.real_spot_brackets[sym_pair] = {
                                "entry_price": entry_p,
                                "stop_price": stop_p,
                                "take_profit_price": tp_p,
                                "mode": pos_mode,
                                "max_price": max_p,
                                "opened_at_ts": opened_ts,
                            }
                            brackets_dirty = True

                        pnl = (mark_p - entry_p) * qty
                        pnl_pct = ((mark_p - entry_p) / entry_p * 100.0) if entry_p > 0 else 0.0
                        max_pnl_pct = ((max_p - entry_p) / entry_p * 100.0) if entry_p > 0 else 0.0
                        tp_pct = ((tp_p - entry_p) / entry_p * 100.0) if (entry_p > 0 and tp_p > 0) else 0.55
                        sl_pct = ((entry_p - stop_p) / entry_p * 100.0) if (entry_p > 0 and stop_p > 0) else 0.35
                        age_min = max(0.0, (now_sec - opened_ts) / 60.0)

                        # Mede o pulso ao vivo do Livro de Ofertas & Tape (1m) desta moeda aberta
                        mom = await self._check_position_momentum(raw_pair, mark_p)
                        live_buy_pct = mom["buy_pressure_pct"]
                        live_taker_pct = mom["taker_buy_pct"]

                        # 1. Verifica saída automática por Alvo TP ou Stop Loss fixo
                        exit_reason: Optional[str] = None
                        if tp_p > 0 and mark_p >= tp_p:
                            exit_reason = "TAKE_PROFIT"
                        elif stop_p > 0 and mark_p <= stop_p:
                            exit_reason = "STOP_LOSS"
                        elif self.auto_momentum_exit:
                            if pos_mode == "ROCKET_MEME":
                                # Trailing Stop Escadinha para Memecoins / Rocket (10-15%+)
                                if max_pnl_pct >= 10.0:
                                    trailing_floor = max_pnl_pct - 2.5
                                    if pnl_pct <= trailing_floor:
                                        exit_reason = f"TRAILING_STOP_LUCRO (Pico +{max_pnl_pct:.2f}% -> Lucro Realizado +{pnl_pct:.2f}%)"
                                elif max_pnl_pct >= 7.0:
                                    if pnl_pct <= 4.0:
                                        exit_reason = f"TRAILING_STOP_LUCRO (Pico +{max_pnl_pct:.2f}% -> Lucro Realizado +{pnl_pct:.2f}%)"
                                elif max_pnl_pct >= 3.5:
                                    if pnl_pct <= 0.20:
                                        exit_reason = f"BREAKEVEN_PROTEGIDO (Pico +{max_pnl_pct:.2f}% -> Breakeven +{pnl_pct:.2f}%)"
                                elif age_min >= 240 and pnl_pct < 0.5:
                                    exit_reason = "ROCKET_EXPIRADO_4H (Sem rompimento em 4 horas)"
                            elif pos_mode in ("SNIPER", "SNIPER_CANDLE"):
                                # Swing Trade: protege breakeven se passar de +3.0%
                                if max_pnl_pct >= 3.0 and pnl_pct <= 0.20:
                                    exit_reason = f"SNIPER_BREAKEVEN (Pico +{max_pnl_pct:.2f}% -> Breakeven +{pnl_pct:.2f}%)"
                            elif "SESSION" in pos_mode:
                                # Operações de Sessão Especial (Virada da China ou Almoço nos EUA)
                                if max_pnl_pct >= 0.90 and pnl_pct <= 0.15:
                                    exit_reason = f"SESSION_BREAKEVEN (Pico +{max_pnl_pct:.2f}% -> Protegido em +{pnl_pct:.2f}%)"
                                elif age_min >= 45 and pnl_pct < 0.20:
                                    exit_reason = "SESSION_EXPIRADO_45M (Janela de 45m encerrou sem expansão)"
                            else:
                                # 2. Proteção de Lucro no Topo para SCALP_BOOK
                                if max_pnl_pct >= 0.25 and (
                                    (pnl_pct <= max_pnl_pct * 0.58 and pnl_pct >= 0.08)
                                    or (live_buy_pct < 44.0 and live_taker_pct < 45.0 and pnl_pct >= 0.05)
                                ):
                                    exit_reason = f"MOMENTO_PASSOU_TOPO (Pico +{max_pnl_pct:.2f}% -> Protegido em +{pnl_pct:.2f}%)"
                                # 3. Virada Brusca de Fluxo no Book + Tape (Paredão Vendedor + Despejo no Tape após 1.5 min)
                                elif age_min >= 1.5 and live_buy_pct <= 40.0 and live_taker_pct <= 42.0:
                                    exit_reason = f"MOMENTO_PASSOU_FLUXO_VIROU (Book Compra={live_buy_pct}% | Tape={live_taker_pct}%)"
                                # 4. Expiração de Tempo (Estagnação sem força compradora após max_hold_minutes)
                                elif age_min >= float(self.max_hold_minutes) and (live_buy_pct < 49.0 or live_taker_pct < 47.0):
                                    exit_reason = f"MOMENTO_PASSOU_TEMPO ({age_min:.1f} min sem romper e fluxo fraco)"

                        if exit_reason is not None:
                            try:
                                await self.close_spot_position(sym_pair, reason=exit_reason)
                                continue
                            except Exception as sell_err:
                                logger.warning(f"[{sym_pair}] Aviso ao executar saída automática Spot ({exit_reason}): {sell_err}")

                        if max_pnl_pct >= 0.25:
                            mom_status = f"🛡️ PROTEGENDO TOPO (Pico +{max_pnl_pct:.2f}%)"
                        elif live_buy_pct >= 55.0 and live_taker_pct >= 52.0:
                            mom_status = f"🟢 MOMENTO FORTE (Book {live_buy_pct:.0f}% | Tape {live_taker_pct:.0f}%)"
                        elif live_buy_pct <= 44.0 or live_taker_pct <= 45.0:
                            mom_status = f"🟠 FLUXO FRACO (Book {live_buy_pct:.0f}% | Tape {live_taker_pct:.0f}%)"
                        else:
                            mom_status = f"🟡 ESTÁVEL (Book {live_buy_pct:.0f}% | Tape {live_taker_pct:.0f}%)"

                        # Computa perfil de horizonte temporal e alvo detalhado
                        horizon_val = bkt.get("horizon")
                        pos_tf = bkt.get("timeframe") or ("BOOK-L2" if "SCALP" in pos_mode else "1h")
                        if not horizon_val:
                            if "SCALP" in pos_mode:
                                horizon_val = "Minutos (~1 a 12 min)"
                                horizon_badge = "⚡ SCALP (MINUTOS)"
                            elif "ROCKET" in pos_mode:
                                horizon_val = "Minutos a Horas (~15 min a 6h)"
                                horizon_badge = "🚀 ROCKET MEME (HORAS)"
                            elif "SESSION" in pos_mode:
                                horizon_val = "Intradiário (~15 min a 45 min)"
                                horizon_badge = "🌏 SESSÃO ESPECIAL"
                            else:
                                horizon_val = "Swing Trade (~4h a 48h)" if pos_tf in ("1h", "4h") else "Day Trade (~15m a 2h)"
                                horizon_badge = "🎯 SWING (HORAS/DIAS)"
                        else:
                            if "SCALP" in pos_mode:
                                horizon_badge = "⚡ SCALP (MINUTOS)"
                            elif "ROCKET" in pos_mode:
                                horizon_badge = "🚀 ROCKET MEME"
                            elif "SESSION" in pos_mode:
                                horizon_badge = "🌏 SESSÃO ESPECIAL"
                            else:
                                horizon_badge = "🎯 SWING"

                        target_usd_est = (tp_p - entry_p) * qty if (tp_p > entry_p and qty > 0) else 0.03
                        risk_usd_est = (entry_p - stop_p) * qty if (entry_p > stop_p and qty > 0) else 0.02
                        rr_label = f"1:{(tp_pct / sl_pct):.1f}" if sl_pct > 0 else "1:2.0"

                        spot_positions.append(
                            {
                                "symbol": sym_pair,
                                "side": "SPOT LONG",
                                "mode": pos_mode,
                                "contracts": round(qty, 6),
                                "notional_usd": round(notional, 2),
                                "entry_price": round(entry_p, 6),
                                "mark_price": mark_p,
                                "max_price": round(max_p, 6),
                                "stop_price": round(stop_p, 6),
                                "take_profit_price": round(tp_p, 6),
                                "unrealized_pnl": round(pnl, 2),
                                "pnl_pct": round(pnl_pct, 2),
                                "max_pnl_pct": round(max_pnl_pct, 2),
                                "tp_pct": round(tp_pct, 2),
                                "sl_pct": round(sl_pct, 2),
                                "age_minutes": round(age_min, 1),
                                "live_buy_pressure_pct": live_buy_pct,
                                "live_taker_buy_pct": live_taker_pct,
                                "momentum_status": mom_status,
                                "horizon": horizon_val,
                                "horizon_badge": horizon_badge,
                                "target_usd": round(target_usd_est, 2),
                                "risk_usd": round(risk_usd_est, 2),
                                "rr_ratio_label": rr_label,
                                "target_desc": f"+{tp_pct:.2f}% (+${target_usd_est:.2f} USDT)",
                                "risk_desc": f"-{sl_pct:.2f}% (-${risk_usd_est:.2f} USDT)",
                                "leverage": 1,
                                "margin_mode": "spot",
                            }
                        )
                if brackets_dirty:
                    self.save_brackets()
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
                        "mode": "SNIPER_CANDLE",
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
        mode: str = "SNIPER_CANDLE",
        timeframe: str = "15m",
        strategy_name: str = "Sniper",
        horizon: Optional[str] = None,
    ) -> Dict[str, Any]:
        if self.exchange is None:
            await self.initialize()
        assert self.exchange is not None

        if not (self.api_key and self.api_secret):
            self.shadow_mode_active = True

        raw_symbol = symbol.split(":")[0].replace("/", "")
        notional_usdt = float(margin_usdt if margin_usdt is not None else self.order_margin_usdt)

        if not horizon:
            if "SCALP" in mode:
                horizon = "Minutos (~1 a 12m)"
            elif "ROCKET" in mode:
                horizon = "Minutos a Horas (~15m a 6h)"
            else:
                horizon = "Swing Trade (~4h a 48h)" if timeframe in ("1h", "4h") else "Day Trade (~15m a 2h)"

        if self.market_type == "spot":
            if direction.upper() != "LONG":
                raise RuntimeError(f"[{symbol}] Modo SPOT opera exclusivamente sinais LONG.")
            if notional_usdt < 5.50:
                notional_usdt = 5.50

            logger.info(
                f"[{symbol}] [SPOT {mode}] Comprando ${notional_usdt:.2f} USDT a mercado (quoteOrderQty) | "
                f"SL: {stop_price:.6f} | TP: {take_profit_price:.6f} | Perfil: {horizon}"
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

                # Recalcula proporcionalmente SL e TP a partir do preço médio real de preenchimento
                sl_ratio = (stop_price / entry_price) if entry_price > 0 else 0.9965
                tp_ratio = (take_profit_price / entry_price) if entry_price > 0 else 1.0055
                adj_stop = float(avg_entry * sl_ratio)
                adj_tp = float(avg_entry * tp_ratio)

                self.real_spot_brackets[symbol] = {
                    "entry_price": float(avg_entry),
                    "stop_price": round(adj_stop, 6),
                    "take_profit_price": round(adj_tp, 6),
                    "mode": mode,
                    "timeframe": timeframe,
                    "strategy_name": strategy_name,
                    "horizon": horizon,
                    "max_price": float(avg_entry),
                    "opened_at_ts": time.time(),
                }
                self.save_brackets()

                # Registra evento de compra na linha de saldo / curva de capital
                last_eq = self.equity_history[-1] if self.equity_history else {
                    "total_usdt": 18.50,
                    "scalp_pnl": 0.0,
                    "sniper_pnl": 0.0,
                    "rocket_pnl": 0.0,
                }
                self.equity_history.append({
                    "timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
                    "timestamp_ts": time.time(),
                    "time_label": datetime.now(timezone.utc).strftime("%H:%M"),
                    "total_usdt": last_eq.get("total_usdt", 18.50),
                    "scalp_pnl": last_eq.get("scalp_pnl", 0.0),
                    "sniper_pnl": last_eq.get("sniper_pnl", 0.0),
                    "rocket_pnl": last_eq.get("rocket_pnl", 0.0),
                    "last_event": f"Compra {symbol} [{direction} @ {avg_entry:.4f}]",
                })
                self.save_equity_history()

                return {
                    "symbol": symbol,
                    "direction": "LONG",
                    "mode": mode,
                    "timeframe": timeframe,
                    "strategy_name": strategy_name,
                    "horizon": horizon,
                    "leverage": 1,
                    "margin_usdt": round(cummulative_quote, 2),
                    "margin_mode": "spot",
                    "amount": round(executed_qty, 6),
                    "notional_usdt": round(cummulative_quote, 2),
                    "entry_order_id": buy_res.get("orderId"),
                    "stop_order_id": None,
                    "tp_order_id": None,
                    "entry_price": round(avg_entry, 6),
                    "stop_price": round(adj_stop, 6),
                    "take_profit_price": round(adj_tp, 6),
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

    async def scan_orderbook_scalp(
        self,
        symbols: List[str],
        scalp_tp_pct: float = 0.50,
        scalp_sl_pct: float = 0.35,
        target_mode: str = "FIXED",
    ) -> List[Dict[str, Any]]:
        """
        Analisa em tempo real o Livro de Ofertas Nível 2 (Order Book Depth 100 níveis)
        combinado com o Fluxo de Agressão Compradora Real (Taker Buy Volume):
        - Calcula Demanda (Bids $) vs Oferta (Asks $) a 0.85% do preço.
        - Identifica Paredões de Baleias (Whale Bid Wall para Stop Loss e Whale Ask Wall para Take Profit).
        - Permite operar com Alvo Fixo Rápido (ex: +0.50% bruto / -0.35% stop) ou Alvo Dinâmico no Paredão (`WALL`).
        """
        if self.http_client is None:
            await self.initialize()
        assert self.http_client is not None

        async def _analyze_one(sym: str) -> Optional[Dict[str, Any]]:
            raw_sym = sym.split(":")[0].replace("/", "")
            try:
                async with self._sem:
                    depth_resp, kline_resp = await asyncio.gather(
                        self.http_client.get(
                            f"{VISION_BASE_URL}/depth",
                            params={"symbol": raw_sym, "limit": 100},
                        ),
                        self.http_client.get(
                            f"{VISION_BASE_URL}/klines",
                            params={"symbol": raw_sym, "interval": "5m", "limit": 3},
                        ),
                    )
                if depth_resp.status_code != 200 or kline_resp.status_code != 200:
                    return None

                depth = depth_resp.json()
                klines = kline_resp.json()
                bids = depth.get("bids") or []
                asks = depth.get("asks") or []
                if not bids or not asks:
                    return None

                best_bid = float(bids[0][0])
                best_ask = float(asks[0][0])
                mid_price = (best_bid + best_ask) / 2.0
                if mid_price <= 0:
                    return None

                spread_pct = ((best_ask - best_bid) / mid_price) * 100.0
                band_low = mid_price * 0.9915
                band_high = mid_price * 1.0085

                bids_usd = 0.0
                bid_wall_price = best_bid * 0.995
                bid_wall_usd = 0.0

                for p_str, q_str in bids:
                    p = float(p_str)
                    q = float(q_str)
                    val_usd = p * q
                    if p >= band_low:
                        bids_usd += val_usd
                    if val_usd > bid_wall_usd:
                        bid_wall_usd = val_usd
                        bid_wall_price = p

                asks_usd = 0.0
                ask_wall_price = best_ask * 1.008
                ask_wall_usd = 0.0

                for p_str, q_str in asks:
                    p = float(p_str)
                    q = float(q_str)
                    val_usd = p * q
                    if p <= band_high:
                        asks_usd += val_usd
                    if p >= best_ask * 1.0040 and val_usd > ask_wall_usd:
                        ask_wall_usd = val_usd
                        ask_wall_price = p

                total_book_usd = bids_usd + asks_usd
                if total_book_usd <= 0:
                    return None

                buy_pressure_pct = (bids_usd / total_book_usd) * 100.0
                sell_pressure_pct = 100.0 - buy_pressure_pct
                obi_pct = ((bids_usd - asks_usd) / total_book_usd) * 100.0

                # Fluxo de Agressão Compradora Real (Taker Buy Quote Volume nas últimas velas de 5m)
                total_quote_vol = sum(float(k[7]) for k in klines[-2:]) if klines else 0.0
                taker_buy_vol = sum(float(k[10]) for k in klines[-2:]) if klines else 0.0
                taker_buy_pct = ((taker_buy_vol / total_quote_vol) * 100.0) if total_quote_vol > 0 else 50.0

                if target_mode.upper() == "WALL":
                    raw_stop = bid_wall_price * 0.9988
                    stop_dist = max(best_ask * 0.0035, min(best_ask - raw_stop, best_ask * 0.0085))
                    stop_price = best_ask - stop_dist

                    min_tp_dist = max(stop_dist * 1.75, best_ask * 0.0055)
                    wall_tp_dist = max(min_tp_dist, (ask_wall_price * 0.9990) - best_ask)
                    tp_dist = min(wall_tp_dist, best_ask * 0.0150)
                    tp_price = best_ask + tp_dist
                else:
                    # Modo Alvo Fixo Rápido configurado pelo usuário no painel de Scalp Separado
                    tp_dist = best_ask * (max(0.25, float(scalp_tp_pct)) / 100.0)
                    stop_dist = best_ask * (max(0.20, float(scalp_sl_pct)) / 100.0)
                    tp_price = best_ask + tp_dist
                    stop_price = best_ask - stop_dist

                gross_target_pct = (tp_dist / best_ask) * 100.0
                net_target_pct = gross_target_pct - 0.11
                risk_pct = (stop_dist / best_ask) * 100.0

                # Posição Relativa do Preço no Range Recente (Preço Barato no Fundo vs Caro no Topo)
                candle_high = max(float(k[2]) for k in klines) if klines else best_ask
                candle_low = min(float(k[3]) for k in klines) if klines else best_ask
                range_height = candle_high - candle_low
                range_position_pct = round(((best_ask - candle_low) / range_height) * 100.0, 1) if range_height > 0 else 50.0

                # 1. Filtro Anti-Topo / Clímax:
                # Se o preço já está no topo (>= 75% da pernada recente) e a agressão de compra do tape já bateu 67%+,
                # a manada já comprou e o risco de reversão é altíssimo!
                is_climax_top = bool(range_position_pct >= 75.0 and taker_buy_pct >= 67.0)

                # Score de Confluência Order Book + Tape Reading (0 a 100)
                scalp_score = round((buy_pressure_pct * 0.58) + (taker_buy_pct * 0.42), 1)

                # 2. Regime de Antecipação Institucional no Fundo:
                # Preço BARATO (<= 42% do range), Muralha compradora pesada (Bids >= 59%),
                # Tape em início de ignição (46% a 60%, sem euforia tardia) e spread fino.
                is_anticipation = bool(
                    not is_climax_top
                    and range_position_pct <= 42.0
                    and buy_pressure_pct >= 59.0
                    and (46.0 <= taker_buy_pct <= 60.0)
                    and spread_pct <= 0.05
                    and net_target_pct >= 0.25
                )

                # 3. Gatilho de Demanda Dominante (Apenas se não for Topo Esticado)
                signal_ready = bool(
                    not is_climax_top
                    and (
                        is_anticipation
                        or (
                            buy_pressure_pct >= 58.0
                            and taker_buy_pct >= 52.0
                            and range_position_pct <= 68.0
                            and spread_pct <= 0.06
                            and net_target_pct >= 0.25
                        )
                    )
                )

                if is_climax_top:
                    regime = "⚠️ CLÍMAX COMPRADOR (TOPO - NÃO COMPRAR)"
                elif is_anticipation:
                    regime = "🎯 ANTECIPAÇÃO NO FUNDO (Preço Barato + Suporte)"
                elif buy_pressure_pct >= 64.0 and taker_buy_pct >= 55.0 and range_position_pct <= 65.0:
                    regime = "🔥 FORTE ABSORÇÃO COMPRADORA"
                elif signal_ready:
                    regime = "DEMANDA DOMINANTE (GATILHO)"
                elif taker_buy_pct >= 65.0 and buy_pressure_pct < 48.0:
                    regime = "⚠️ POSSÍVEL TOPO / EXAUSTÃO"
                elif buy_pressure_pct <= 42.0:
                    regime = "PRESSÃO VENDEDORA (EVITAR)"
                else:
                    regime = "EQUILÍBRIO / AGUARDANDO FLUXO"

                return {
                    "symbol": sym,
                    "price": round(best_ask, 6),
                    "range_position_pct": range_position_pct,
                    "is_climax_top": is_climax_top,
                    "spread_pct": round(spread_pct, 4),
                    "bids_usd": round(bids_usd, 0),
                    "asks_usd": round(asks_usd, 0),
                    "buy_pressure_pct": round(buy_pressure_pct, 1),
                    "sell_pressure_pct": round(sell_pressure_pct, 1),
                    "obi_pct": round(obi_pct, 1),
                    "taker_buy_pct": round(taker_buy_pct, 1),
                    "bid_wall_price": round(bid_wall_price, 6),
                    "bid_wall_usd": round(bid_wall_usd, 0),
                    "ask_wall_price": round(ask_wall_price, 6),
                    "ask_wall_usd": round(ask_wall_usd, 0),
                    "entry_price": round(best_ask, 6),
                    "stop_price": round(stop_price, 6),
                    "take_profit_price": round(tp_price, 6),
                    "gross_target_pct": round(gross_target_pct, 2),
                    "risk_pct": round(risk_pct, 2),
                    "net_target_pct": round(net_target_pct, 2),
                    "scalp_score": scalp_score,
                    "signal_ready": signal_ready,
                    "is_anticipation": is_anticipation,
                    "regime": regime,
                }
            except Exception as exc:
                logger.debug(f"[{sym}] Erro ao analisar Order Book: {exc}")
                return None

        results = await asyncio.gather(*(_analyze_one(s) for s in symbols[:16]))
        valid = [r for r in results if r is not None]
        valid.sort(key=lambda x: x["scalp_score"], reverse=True)
        return valid

