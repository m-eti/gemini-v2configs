#!/usr/bin/env python3
"""
Fetch V2Ray/Xray configs per country from v2nodes.com, test connectivity and
Google Gemini accessibility through xray-core, and save working configs for v2rayNG.
"""

from __future__ import annotations

import base64
import concurrent.futures
import json
import os
import queue
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path
from typing import Any

import requests

V2NODES_HOME_URL = "https://www.v2nodes.com/"
V2NODES_COUNTRY_SUB_TEMPLATE = (
    "https://www.v2nodes.com/subscriptions/country/{country}/?key={key}"
)
CONNECTIVITY_CHECK_URL = "https://www.gstatic.com/generate_204"
GEMINI_CHECK_URL = "https://gemini.google.com/app"

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "20"))
CONNECT_TIMEOUT = float(os.getenv("CONNECT_TIMEOUT", "8"))
GEMINI_TIMEOUT = float(os.getenv("GEMINI_TIMEOUT", "12"))
XRAY_STARTUP_TIMEOUT = float(os.getenv("XRAY_STARTUP_TIMEOUT", "3.0"))
COUNTRY_REQUEST_DELAY = float(os.getenv("COUNTRY_REQUEST_DELAY", "0.25"))
BASE_PORT = int(os.getenv("BASE_PORT", "20000"))

OUTPUT_SUB_FILE = Path(os.getenv("OUTPUT_SUB_FILE", "sub.txt"))
OUTPUT_PLAIN_FILE = Path(os.getenv("OUTPUT_PLAIN_FILE", "sub_plain.txt"))

# Fallback list of countries listed on v2nodes.com if homepage parsing misses them
DEFAULT_COUNTRIES = (
    "al", "at", "by", "ca", "cz", "ee", "fi", "fr", "de", "gr",
    "hk", "in", "id", "il", "it", "jp", "kz", "lv", "my", "nl",
    "no", "pl", "ru", "sa", "sg", "za", "kr", "es", "se", "ch",
    "tr", "th", "ua", "ae", "gb", "us", "vn",
)

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,"
        "image/avif,image/webp,*/*;q=0.8"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
}

SUPPORTED_SS_METHODS = {
    "aes-128-gcm",
    "aes-256-gcm",
    "chacha20-ietf-poly1305",
    "xchacha20-ietf-poly1305",
    "2022-blake3-aes-128-gcm",
    "2022-blake3-aes-256-gcm",
    "2022-blake3-chacha20-poly1305",
    "none",
    "plain",
    "aes-128-cfb",
    "aes-256-cfb",
    "chacha20",
    "chacha20-ietf",
    "rc4-md5",
}

GEMINI_BLOCK_PHRASES = (
    "gemini isn't currently supported in your country",
    "gemini is not currently supported in your country",
    "isn't available in your country",
    "not available in your country",
    "not supported in your country",
    "403. that’s an error",
    "403. that's an error",
    "your client does not have permission",
    "unusual traffic from your computer network",
    "our systems have detected unusual traffic",
)

GEMINI_BLOCK_URL_TOKENS = (
    "google.com/sorry",
    "support.google.com/gemini/answer/13575153",
    "/unsupported",
    "not-available",
    "unavailable",
)


def decode_base64_str(data: str) -> str:
    """Decode standard or URL-safe base64 string with automatic padding."""
    cleaned = data.strip().replace("\n", "").replace("\r", "")
    missing_padding = len(cleaned) % 4
    if missing_padding:
        cleaned += "=" * (4 - missing_padding)
    try:
        return base64.b64decode(cleaned).decode("utf-8", errors="ignore")
    except Exception:
        return base64.urlsafe_b64decode(cleaned).decode("utf-8", errors="ignore")


def extract_v2nodes_key(html: str) -> str:
    """Extract the dynamic subscription key from the v2nodes homepage HTML."""
    patterns = (
        r"https?://(?:www\.)?v2nodes\.com/subscriptions/country/[^\"'\s<>/?]+/\?key=([A-Za-z0-9_\-]+)",
        r"/subscriptions/country/[^\"'\s<>/?]+/\?key=([A-Za-z0-9_\-]+)",
        r"subscriptions/[^\"'\s<>]+\?key=([A-Za-z0-9_\-]+)",
        r"[?&]key=([A-Za-z0-9_\-]{8,})",
    )
    for pattern in patterns:
        match = re.search(pattern, html)
        if match:
            return match.group(1)
    raise RuntimeError("Could not find a subscription key on v2nodes.com homepage.")


def extract_v2nodes_countries(html: str) -> list[str]:
    """
    Extract available 2-letter country codes from v2nodes homepage HTML.
    Falls back to DEFAULT_COUNTRIES if fewer than 5 countries are detected.
    """
    env_countries = os.getenv("COUNTRIES", "").strip()
    if env_countries:
        return sorted(
            {
                c.strip().lower()
                for c in env_countries.split(",")
                if re.fullmatch(r"[a-zA-Z]{2}", c.strip())
            }
        )

    found = {
        code.lower()
        for code in re.findall(
            r"/(?:servers|subscriptions)/country/([a-zA-Z]{2})(?:/|\"|'|\?)",
            html,
        )
    }
    found.discard("all")

    if len(found) >= 5:
        return sorted(found)
    return sorted(set(DEFAULT_COUNTRIES) | found)


def parse_subscription_payload(raw_text: str) -> list[str]:
    """Parse either plain-text or base64-encoded subscription content."""
    text = raw_text.strip()
    if not text:
        return []

    prefixes = ("vless://", "vmess://", "trojan://", "ss://")
    if not any(line.strip().startswith(prefixes) for line in text.splitlines()[:5]):
        decoded = decode_base64_str(text)
        if any(line.strip().startswith(prefixes) for line in decoded.splitlines()[:10]):
            text = decoded

    seen: set[str] = set()
    configs: list[str] = []
    for line in text.splitlines():
        uri = line.strip()
        if uri.startswith(prefixes) and uri not in seen:
            seen.add(uri)
            configs.append(uri)
    return configs


def fetch_v2nodes_configs() -> list[str]:
    """
    Request v2nodes.com, extract a fresh key and country list, then fetch
    each country's subscription endpoint individually.
    """
    session = requests.Session()
    session.headers.update(BROWSER_HEADERS)

    home_resp = session.get(V2NODES_HOME_URL, timeout=20)
    home_resp.raise_for_status()
    key = extract_v2nodes_key(home_resp.text)
    countries = extract_v2nodes_countries(home_resp.text)
    print(f"[INFO] Extracted v2nodes subscription key: {key[:6]}***")
    print(f"[INFO] Fetching configs separately for {len(countries)} countries...")

    seen: set[str] = set()
    all_configs: list[str] = []
    key_refreshed = False

    for country in countries:
        sub_url = V2NODES_COUNTRY_SUB_TEMPLATE.format(country=country, key=key)
        try:
            resp = session.get(sub_url, timeout=25)

            # Refresh the subscription key once if the server rejects the current key
            if resp.status_code in (401, 403) and not key_refreshed:
                print(
                    f"[WARN] HTTP {resp.status_code} for {country.upper()}; "
                    "refreshing v2nodes key..."
                )
                refresh_resp = session.get(V2NODES_HOME_URL, timeout=20)
                refresh_resp.raise_for_status()
                key = extract_v2nodes_key(refresh_resp.text)
                key_refreshed = True
                sub_url = V2NODES_COUNTRY_SUB_TEMPLATE.format(
                    country=country, key=key
                )
                resp = session.get(sub_url, timeout=25)

            if resp.status_code != 200:
                print(
                    f"[WARN] [{country.upper()}] skipped (HTTP {resp.status_code})"
                )
                continue

            country_configs = parse_subscription_payload(resp.text)
            added = 0
            for uri in country_configs:
                if uri not in seen:
                    seen.add(uri)
                    all_configs.append(uri)
                    added += 1

            print(
                f"[INFO] [{country.upper()}] fetched {len(country_configs)} "
                f"configs ({added} new)"
            )
        except requests.RequestException as exc:
            print(f"[WARN] [{country.upper()}] request failed: {exc}")
        finally:
            if COUNTRY_REQUEST_DELAY > 0:
                time.sleep(COUNTRY_REQUEST_DELAY)

    print(
        f"[INFO] Total unique configs collected across {len(countries)} "
        f"countries: {len(all_configs)}"
    )
    return all_configs


def build_stream_settings(
    network: str,
    security: str,
    params: dict[str, str],
    default_sni: str,
) -> dict[str, Any]:
    """Build Xray streamSettings for TCP, WS, gRPC, HTTP/2, HTTPUpgrade, or XHTTP."""
    net = (network or "tcp").lower()
    if net == "h2":
        net = "http"
    elif net == "splithttp":
        net = "xhttp"

    sec = (security or "none").lower()
    stream: dict[str, Any] = {"network": net, "security": sec}

    host = params.get("host", "")
    path = params.get("path", "/")
    header_type = params.get("headerType", params.get("type", "none")).lower()

    if net == "ws":
        ws_settings: dict[str, Any] = {"path": path or "/"}
        if host:
            ws_settings["headers"] = {"Host": host}
        stream["wsSettings"] = ws_settings
    elif net == "grpc":
        service_name = params.get("serviceName", path.lstrip("/"))
        stream["grpcSettings"] = {
            "serviceName": service_name,
            "multiMode": params.get("mode", "").lower() == "multi",
        }
    elif net == "http":
        http_settings: dict[str, Any] = {"path": path or "/"}
        if host:
            http_settings["host"] = [h.strip() for h in host.split(",") if h.strip()]
        stream["httpSettings"] = http_settings
    elif net == "httpupgrade":
        hu_settings: dict[str, Any] = {"path": path or "/"}
        if host:
            hu_settings["host"] = host
        stream["httpupgradeSettings"] = hu_settings
    elif net == "xhttp":
        xh_settings: dict[str, Any] = {"path": path or "/"}
        if host:
            xh_settings["host"] = host
        stream["xhttpSettings"] = xh_settings
    elif net == "tcp" and header_type == "http":
        tcp_req: dict[str, Any] = {"path": [path or "/"]}
        if host:
            tcp_req["headers"] = {
                "Host": [h.strip() for h in host.split(",") if h.strip()]
            }
        stream["tcpSettings"] = {"header": {"type": "http", "request": tcp_req}}

    sni = params.get("sni", "") or host or default_sni
    fp = params.get("fp", "")
    alpn_raw = params.get("alpn", "")
    alpn = [a.strip() for a in alpn_raw.split(",") if a.strip()]

    if sec == "tls":
        tls_settings: dict[str, Any] = {
            "serverName": sni,
            "allowInsecure": True,
        }
        if alpn:
            tls_settings["alpn"] = alpn
        if fp:
            tls_settings["fingerprint"] = fp
        stream["tlsSettings"] = tls_settings
    elif sec == "reality":
        pbk = params.get("pbk", "")
        if not pbk:
            raise ValueError("Missing Reality public key (pbk)")
        stream["realitySettings"] = {
            "serverName": sni,
            "fingerprint": fp or "chrome",
            "publicKey": pbk,
            "shortId": params.get("sid", ""),
            "spiderX": params.get("spx", "/"),
        }

    return stream


def parse_vmess(uri: str) -> dict[str, Any]:
    """Convert a vmess:// URI into an Xray outbound dictionary."""
    payload = decode_base64_str(uri[len("vmess://") :])
    data = json.loads(payload)

    address = str(data.get("add", "")).strip()
    port = int(data.get("port", 0))
    user_id = str(data.get("id", "")).strip()
    if not address or not port or not user_id:
        raise ValueError("Incomplete vmess fields")

    alter_id = int(data.get("aid", 0) or 0)
    security_cipher = str(data.get("scy", "") or "auto")
    network = str(data.get("net", "tcp") or "tcp")
    tls_val = str(data.get("tls", "") or "").lower()
    security = "tls" if tls_val in ("tls", "1", "true") else "none"

    params = {
        "host": str(data.get("host", "") or ""),
        "path": str(data.get("path", "/") or "/"),
        "type": str(data.get("type", "none") or "none"),
        "sni": str(data.get("sni", "") or ""),
        "alpn": str(data.get("alpn", "") or ""),
        "fp": str(data.get("fp", "") or ""),
    }

    return {
        "tag": "proxy",
        "protocol": "vmess",
        "settings": {
            "vnext": [
                {
                    "address": address,
                    "port": port,
                    "users": [
                        {
                            "id": user_id,
                            "alterId": alter_id,
                            "security": security_cipher,
                        }
                    ],
                }
            ]
        },
        "streamSettings": build_stream_settings(network, security, params, address),
    }


def parse_vless(uri: str) -> dict[str, Any]:
    """Convert a vless:// URI into an Xray outbound dictionary."""
    parsed = urllib.parse.urlsplit(uri)
    address = parsed.hostname
    port = parsed.port
    user_id = urllib.parse.unquote(parsed.username or "")
    if not address or not port or not user_id:
        raise ValueError("Incomplete vless fields")

    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    params = {k: v[0] for k, v in query.items() if v}

    network = params.get("type", "tcp")
    security = params.get("security", "none")
    encryption = params.get("encryption", "none") or "none"
    flow = params.get("flow", "")

    user_obj: dict[str, Any] = {
        "id": user_id,
        "encryption": encryption,
    }
    if flow and network.lower() == "tcp" and security.lower() in ("tls", "reality"):
        user_obj["flow"] = flow

    return {
        "tag": "proxy",
        "protocol": "vless",
        "settings": {
            "vnext": [
                {
                    "address": address,
                    "port": port,
                    "users": [user_obj],
                }
            ]
        },
        "streamSettings": build_stream_settings(network, security, params, address),
    }


def parse_trojan(uri: str) -> dict[str, Any]:
    """Convert a trojan:// URI into an Xray outbound dictionary."""
    parsed = urllib.parse.urlsplit(uri)
    address = parsed.hostname
    port = parsed.port
    password = urllib.parse.unquote(parsed.username or "")
    if not address or not port or not password:
        raise ValueError("Incomplete trojan fields")

    query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
    params = {k: v[0] for k, v in query.items() if v}

    network = params.get("type", "tcp")
    security = params.get("security", "tls")

    return {
        "tag": "proxy",
        "protocol": "trojan",
        "settings": {
            "servers": [
                {
                    "address": address,
                    "port": port,
                    "password": password,
                }
            ]
        },
        "streamSettings": build_stream_settings(network, security, params, address),
    }


def parse_shadowsocks(uri: str) -> dict[str, Any]:
    """Convert an ss:// (Shadowsocks / Outline) URI into an Xray outbound dictionary."""
    raw = uri[len("ss://") :]
    if "#" in raw:
        raw = raw.split("#", 1)[0]

    query_str = ""
    if "?" in raw:
        raw, query_str = raw.split("?", 1)
        query = urllib.parse.parse_qs(query_str)
        if "plugin" in query:
            raise ValueError("External SIP003 plugins are not supported in xray-core")

    if "@" not in raw:
        raw = decode_base64_str(raw)
        if "@" not in raw:
            raise ValueError("Invalid Shadowsocks URI format")

    userinfo, hostport = raw.rsplit("@", 1)
    hostport = hostport.rstrip("/")

    # Parse IPv6 or IPv4 host:port safely via urlsplit
    hp = urllib.parse.urlsplit(f"ss://{hostport}")
    address = hp.hostname
    port = hp.port
    if not address or not port:
        raise ValueError("Missing Shadowsocks host or port")

    userinfo_unquoted = urllib.parse.unquote(userinfo)
    if ":" not in userinfo_unquoted:
        userinfo_unquoted = decode_base64_str(userinfo_unquoted)

    if ":" not in userinfo_unquoted:
        raise ValueError("Invalid Shadowsocks method:password")

    method, password = userinfo_unquoted.split(":", 1)
    method = method.strip().lower()
    if method not in SUPPORTED_SS_METHODS:
        raise ValueError(f"Unsupported Shadowsocks cipher: {method}")

    return {
        "tag": "proxy",
        "protocol": "shadowsocks",
        "settings": {
            "servers": [
                {
                    "address": address,
                    "port": port,
                    "method": method,
                    "password": password,
                }
            ]
        },
    }


def parse_uri_to_outbound(uri: str) -> dict[str, Any] | None:
    """Dispatch a proxy URI to the appropriate protocol parser."""
    try:
        if uri.startswith("vmess://"):
            return parse_vmess(uri)
        if uri.startswith("vless://"):
            return parse_vless(uri)
        if uri.startswith("trojan://"):
            return parse_trojan(uri)
        if uri.startswith("ss://"):
            return parse_shadowsocks(uri)
    except Exception:
        return None
    return None


def build_xray_config(outbound: dict[str, Any], socks_port: int) -> dict[str, Any]:
    """Create a minimal Xray configuration with a local SOCKS5 inbound."""
    return {
        "log": {"loglevel": "none"},
        "inbounds": [
            {
                "listen": "127.0.0.1",
                "port": socks_port,
                "protocol": "socks",
                "settings": {"auth": "noauth", "udp": False},
            }
        ],
        "outbounds": [outbound],
    }


def wait_for_port(
    port: int,
    proc: subprocess.Popen[bytes],
    timeout: float = XRAY_STARTUP_TIMEOUT,
) -> bool:
    """Wait until Xray opens the local SOCKS5 port or exits prematurely."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            return False
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return True
        except OSError:
            time.sleep(0.05)
    return False


def is_gemini_accessible(resp: requests.Response) -> bool:
    """Verify that Gemini loaded cleanly without 403, CAPTCHA, or region errors."""
    if resp.status_code != 200:
        return False

    final_url = resp.url.lower()
    if any(token in final_url for token in GEMINI_BLOCK_URL_TOKENS):
        return False
    if "gemini.google.com" not in final_url:
        return False

    body = resp.text.lower()
    if any(phrase in body for phrase in GEMINI_BLOCK_PHRASES):
        return False

    return True


def test_config(
    uri: str,
    port_queue: queue.Queue[int],
    xray_bin: str,
) -> tuple[str, float | None, bool]:
    """
    Test a single config in two stages:
    1. Basic internet connectivity via gstatic generate_204.
    2. Google Gemini accessibility without 403 or region restrictions.
    Returns (uri, latency_ms_or_None, gemini_ok).
    """
    outbound = parse_uri_to_outbound(uri)
    if outbound is None:
        return (uri, None, False)

    port = port_queue.get()
    cfg_path: str | None = None
    proc: subprocess.Popen[bytes] | None = None

    try:
        xray_cfg = build_xray_config(outbound, port)
        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".json",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            json.dump(xray_cfg, tmp)
            cfg_path = tmp.name

        proc = subprocess.Popen(
            [xray_bin, "run", "-c", cfg_path],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        if not wait_for_port(port, proc):
            return (uri, None, False)

        proxies = {
            "http": f"socks5h://127.0.0.1:{port}",
            "https": f"socks5h://127.0.0.1:{port}",
        }

        # Stage 1: Connectivity test
        start_time = time.monotonic()
        conn_resp = requests.get(
            CONNECTIVITY_CHECK_URL,
            proxies=proxies,
            headers=BROWSER_HEADERS,
            timeout=CONNECT_TIMEOUT,
            allow_redirects=False,
        )
        if conn_resp.status_code not in (200, 204):
            return (uri, None, False)
        latency_ms = round((time.monotonic() - start_time) * 1000, 1)

        # Stage 2: Google Gemini accessibility test
        gemini_resp = requests.get(
            GEMINI_CHECK_URL,
            proxies=proxies,
            headers=BROWSER_HEADERS,
            timeout=GEMINI_TIMEOUT,
            allow_redirects=True,
        )
        gemini_ok = is_gemini_accessible(gemini_resp)
        return (uri, latency_ms, gemini_ok)

    except Exception:
        return (uri, None, False)
    finally:
        if proc is not None and proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=2)
        if cfg_path and os.path.exists(cfg_path):
            try:
                os.remove(cfg_path)
            except OSError:
                pass
        port_queue.put(port)


def find_xray_binary() -> str:
    """Locate the xray binary in PATH or XRAY_BIN environment variable."""
    env_bin = os.getenv("XRAY_BIN")
    if env_bin and os.path.isfile(env_bin):
        return env_bin
    found = shutil.which("xray")
    if found:
        return found
    raise FileNotFoundError(
        "xray binary not found. Install xray-core or set XRAY_BIN."
    )


def save_outputs(working_configs: list[tuple[str, float]]) -> None:
    """Save sorted working configs as both base64 (sub.txt) and plain text."""
    working_configs.sort(key=lambda item: item[1])
    uris = [uri for uri, _ in working_configs]
    plain_text = "\n".join(uris) + ("\n" if uris else "")
    b64_text = base64.b64encode(plain_text.encode("utf-8")).decode("ascii")

    OUTPUT_PLAIN_FILE.write_text(plain_text, encoding="utf-8")
    OUTPUT_SUB_FILE.write_text(b64_text, encoding="utf-8")
    print(f"[INFO] Saved {len(uris)} Gemini-ready configs to {OUTPUT_SUB_FILE}")


def main() -> int:
    xray_bin = find_xray_binary()
    print(f"[INFO] Using xray binary: {xray_bin}")

    configs = fetch_v2nodes_configs()
    if not configs:
        print(
            "[WARN] No configs fetched from v2nodes.com; keeping previous subscription."
        )
        return 0

    port_queue: queue.Queue[int] = queue.Queue()
    for offset in range(MAX_WORKERS):
        port_queue.put(BASE_PORT + offset)

    alive_count = 0
    gemini_ready: list[tuple[str, float]] = []

    print(
        f"[INFO] Testing {len(configs)} configs with {MAX_WORKERS} parallel workers..."
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(test_config, uri, port_queue, xray_bin)
            for uri in configs
        ]
        for idx, future in enumerate(concurrent.futures.as_completed(futures), start=1):
            uri, latency_ms, gemini_ok = future.result()
            if latency_ms is not None:
                alive_count += 1
                if gemini_ok:
                    gemini_ready.append((uri, latency_ms))
            if idx % 25 == 0 or idx == len(configs):
                print(
                    f"[PROGRESS] {idx}/{len(configs)} tested | "
                    f"Alive: {alive_count} | Gemini OK: {len(gemini_ready)}"
                )

    print(
        f"[SUMMARY] Total: {len(configs)} | "
        f"Connected: {alive_count} | Gemini OK: {len(gemini_ready)}"
    )

    if not gemini_ready and OUTPUT_SUB_FILE.exists():
        print(
            "[WARN] 0 configs passed Gemini check; keeping existing sub.txt unchanged."
        )
        return 0

    save_outputs(gemini_ready)
    return 0


if __name__ == "__main__":
    sys.exit(main())