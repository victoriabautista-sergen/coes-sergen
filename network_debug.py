"""
network_debug.py
Diagnóstico de conectividad hacia el portal COES.

Ejecutar:  python network_debug.py

Identifica si el problema es DNS, proxy, firewall o configuración de requests.
"""

import socket
import sys

import requests

_URL_GOOGLE = "https://www.google.com"
_URL_COES   = "https://www.coes.org.pe"
_URL_ENDPOINT = "https://www.coes.org.pe/Portal/portalinformacion/Demanda"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": "https://www.coes.org.pe/Portal/portalinformacion/demanda?indicador=maxima",
    "Origin": "https://www.coes.org.pe",
}

_TIMEOUT = 15
_PASSED = []
_FAILED = []


def _ok(label: str, detail: str = "") -> None:
    msg = f"[OK]   {label}" + (f" — {detail}" if detail else "")
    print(msg)
    _PASSED.append(label)


def _fail(label: str, detail: str = "") -> None:
    msg = f"[FAIL] {label}" + (f" — {detail}" if detail else "")
    print(msg)
    _FAILED.append(label)


# ---------------------------------------------------------------------------
# A. Resolución DNS
# ---------------------------------------------------------------------------

def test_dns() -> bool:
    label = "A. DNS: www.coes.org.pe"
    print(f"\n--- {label} ---")
    try:
        ip = socket.gethostbyname("www.coes.org.pe")
        _ok(label, f"IP resuelta: {ip}")
        return True
    except socket.gaierror as exc:
        _fail(label, f"[ERROR RED] No se pudo resolver dominio — {exc}")
        print("  >> Posible causa: DNS bloqueado, sin conexión, o proxy interceptando.")
        return False


# ---------------------------------------------------------------------------
# B. Conexión general (Google)
# ---------------------------------------------------------------------------

def test_google() -> bool:
    label = "B. Conexión general: Google"
    print(f"\n--- {label} ---")
    try:
        r = requests.get(_URL_GOOGLE, timeout=_TIMEOUT)
        _ok(label, f"HTTP {r.status_code}")
        return True
    except requests.exceptions.ConnectionError as exc:
        _fail(label, f"[ERROR CONEXIÓN] Sin acceso a internet — {exc}")
        return False
    except requests.exceptions.Timeout:
        _fail(label, "[ERROR CONEXIÓN] Timeout al conectar con Google")
        return False


# ---------------------------------------------------------------------------
# C. Conexión COES sin proxy
# ---------------------------------------------------------------------------

def test_coes_sin_proxy() -> bool:
    label = "C. COES sin proxy"
    print(f"\n--- {label} ---")
    try:
        r = requests.get(
            _URL_COES,
            proxies={"http": None, "https": None},
            timeout=_TIMEOUT,
        )
        _ok(label, f"HTTP {r.status_code}")
        return True
    except requests.exceptions.ConnectionError as exc:
        _fail(label, f"[ERROR CONEXIÓN] No se pudo conectar a COES — {exc}")
        print("  >> Posible causa: firewall bloqueando la IP, o COES caído.")
        return False
    except requests.exceptions.Timeout:
        _fail(label, "[ERROR CONEXIÓN] Timeout al conectar con COES")
        return False


# ---------------------------------------------------------------------------
# D. Session con trust_env = False
# ---------------------------------------------------------------------------

def test_coes_trust_env_false() -> bool:
    label = "D. COES — Session(trust_env=False)"
    print(f"\n--- {label} ---")
    try:
        session = requests.Session()
        session.trust_env = False
        r = session.get(_URL_COES, timeout=_TIMEOUT)
        _ok(label, f"HTTP {r.status_code}")
        return True
    except requests.exceptions.ConnectionError as exc:
        _fail(label, f"[ERROR CONEXIÓN] trust_env=False tampoco conecta — {exc}")
        print("  >> Si C pasó pero D falla: problema en variables de entorno/proxy del sistema.")
        return False
    except requests.exceptions.Timeout:
        _fail(label, "[ERROR CONEXIÓN] Timeout (trust_env=False)")
        return False


# ---------------------------------------------------------------------------
# E. Endpoint real del COES
# ---------------------------------------------------------------------------

def test_endpoint_coes() -> bool:
    label = "E. Endpoint POST /Demanda"
    print(f"\n--- {label} ---")
    payload = {
        "indicador": "maxima",
        "fechaInicial": "01/03/2025",
        "fechaFinal": "31/03/2025",
    }
    try:
        session = requests.Session()
        session.trust_env = False
        r = session.post(
            _URL_ENDPOINT,
            data=payload,
            headers=_HEADERS,
            timeout=_TIMEOUT,
        )
        if r.status_code == 200:
            try:
                data = r.json()
                claves = list(data.keys()) if isinstance(data, dict) else type(data).__name__
                _ok(label, f"HTTP 200 — JSON OK — claves: {claves}")
            except ValueError:
                _fail(label, f"HTTP {r.status_code} pero respuesta no es JSON — primeros 200 chars: {r.text[:200]!r}")
                return False
        else:
            _fail(label, f"HTTP {r.status_code} — respuesta: {r.text[:200]!r}")
            return False
        return True
    except requests.exceptions.ConnectionError as exc:
        _fail(label, f"[ERROR RED] No se pudo conectar al endpoint — {exc}")
        return False
    except requests.exceptions.Timeout:
        _fail(label, "[ERROR CONEXIÓN] Timeout al conectar con endpoint COES")
        return False
    except socket.gaierror as exc:
        _fail(label, f"[ERROR RED] No se pudo resolver dominio — {exc}")
        return False


# ---------------------------------------------------------------------------
# Resumen
# ---------------------------------------------------------------------------

def _imprimir_diagnostico() -> None:
    print("\n" + "=" * 60)
    print("DIAGNÓSTICO FINAL")
    print("=" * 60)

    dns_ok      = "A. DNS: www.coes.org.pe"     in _PASSED
    google_ok   = "B. Conexión general: Google"  in _PASSED
    sin_proxy_ok= "C. COES sin proxy"            in _PASSED
    trust_ok    = "D. COES — Session(trust_env=False)" in _PASSED
    endpoint_ok = "E. Endpoint POST /Demanda"    in _PASSED

    if endpoint_ok:
        print("RESULTADO: Todo OK — el script debería funcionar correctamente.")
        return

    if not google_ok:
        print("CAUSA PROBABLE: Sin conexión a internet.")
    elif not dns_ok:
        print("CAUSA PROBABLE: Fallo de DNS — el sistema no resuelve www.coes.org.pe.")
        print("  Sugerencias:")
        print("  - Cambiar DNS a 8.8.8.8 o 1.1.1.1")
        print("  - Verificar si hay VPN o proxy activo")
    elif not sin_proxy_ok and not trust_ok:
        print("CAUSA PROBABLE: Firewall o bloqueo de red hacia www.coes.org.pe.")
        print("  Sugerencias:")
        print("  - Probar desde otra red (datos móviles, VPN)")
        print("  - Verificar reglas de firewall corporativo")
    elif sin_proxy_ok and not trust_ok:
        print("CAUSA PROBABLE: Variables de entorno de proxy (HTTP_PROXY/HTTPS_PROXY) interfieren.")
        print("  Solución: usar session.trust_env = False en el código principal.")
    elif trust_ok and not endpoint_ok:
        print("CAUSA PROBABLE: El endpoint /Demanda responde distinto al esperado.")
        print("  Sugerencias:")
        print("  - Verificar si el portal COES cambió su API")
        print("  - Revisar headers o payload")
    else:
        print("No se pudo determinar la causa exacta. Revisar los FAIL arriba.")

    print(f"\nTests pasados : {len(_PASSED)}/5 — {_PASSED}")
    print(f"Tests fallados: {len(_FAILED)}/5 — {_FAILED}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print("=" * 60)
    print("DIAGNÓSTICO DE RED — COES scraper")
    print("=" * 60)

    test_dns()
    test_google()
    test_coes_sin_proxy()
    test_coes_trust_env_false()
    test_endpoint_coes()

    _imprimir_diagnostico()

    sys.exit(0 if not _FAILED else 1)
