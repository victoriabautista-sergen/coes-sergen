"""
data/coes_historica.py
Extrae la máxima demanda diaria HP desde el portal COES.

Endpoint: POST https://www.coes.org.pe/Portal/portalinformacion/Demanda
Respuesta: JSON con dos fuentes de datos:
  - raw["Chart"]["Series"][0]["Data"]  → serie gráfica (tiempo real / operativa)
  - raw["Data"]                        → ValorEjecutado (demanda oficial ejecutada)

Lógica de selección de fuente:
  - Mes actual  → Chart.Series[0].Data  (datos operativos en tiempo real)
  - Mes anterior → raw["Data"].ValorEjecutado (datos oficiales ejecutados)

Función pública principal:
    obtener_potencia_historica_coes() -> pd.DataFrame
        Columnas: fecha (str YYYY-MM-DD), potencia_maxima (float), hora (int), minuto (int)
        Una fila por día = máxima demanda en Hora Punta (18:00–23:59).
"""

import calendar
import logging
import re
import socket
import time
from datetime import date, datetime

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_URL_DEMANDA = "https://www.coes.org.pe/Portal/portalinformacion/Demanda"

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

# Hora Punta: 18:00 – 23:59
_HP_HORA_INICIO = 18
_HP_HORA_FIN = 23


# ---------------------------------------------------------------------------
# Rango de fechas
# ---------------------------------------------------------------------------

def _rango_mes_anterior() -> tuple[str, str, int, int]:
    """
    Devuelve (fecha_inicial, fecha_final, anio, mes) del mes anterior.
    fecha_inicial / fecha_final en formato DD/MM/YYYY.
    """
    hoy = date.today()
    if hoy.month == 1:
        anio, mes = hoy.year - 1, 12
    else:
        anio, mes = hoy.year, hoy.month - 1

    ultimo_dia = calendar.monthrange(anio, mes)[1]
    fecha_inicial = f"01/{mes:02d}/{anio}"
    fecha_final = f"{ultimo_dia:02d}/{mes:02d}/{anio}"
    return fecha_inicial, fecha_final, anio, mes


def _es_mes_actual(anio: int, mes: int) -> bool:
    """Retorna True si (anio, mes) corresponde al mes actual."""
    hoy = date.today()
    return hoy.year == anio and hoy.month == mes


# ---------------------------------------------------------------------------
# Llamada al endpoint
# ---------------------------------------------------------------------------

_FETCH_RETRIES = 3
_FETCH_RETRY_DELAY = 5  # segundos entre reintentos


def _fetch_demanda(fecha_inicial: str, fecha_final: str, timeout: int = 30) -> dict:
    """
    Llama al endpoint AJAX del COES y devuelve el JSON parseado.

    - Usa session.trust_env = False para ignorar proxies del entorno.
    - Reintenta hasta _FETCH_RETRIES veces ante errores de red.
    - En cada fallo loggea el tipo de error con mensajes claros.

    Raises:
        requests.HTTPError: Si el servidor devuelve un código de error HTTP.
        ValueError: Si la respuesta no es JSON válido.
        requests.exceptions.ConnectionError: Si no se pudo establecer conexión tras todos los reintentos.
    """
    payload = {
        "indicador": "maxima",
        "fechaInicial": fecha_inicial,
        "fechaFinal": fecha_final,
    }

    logger.info("[NETWORK] POST %s — payload: %s", _URL_DEMANDA, payload)

    session = requests.Session()
    session.trust_env = False  # ignora HTTP_PROXY / HTTPS_PROXY del entorno

    last_exc: Exception | None = None

    for intento in range(1, _FETCH_RETRIES + 1):
        try:
            logger.debug("[NETWORK] Intento %d/%d …", intento, _FETCH_RETRIES)
            resp = session.post(
                _URL_DEMANDA,
                data=payload,
                headers=_HEADERS,
                timeout=timeout,
                proxies={"http": None, "https": None},  # fallback explícito sin proxy
            )
        except socket.gaierror as exc:
            last_exc = exc
            logger.error(
                "[NETWORK ERROR] Intento %d — No se pudo resolver dominio '%s': %s",
                intento, "www.coes.org.pe", exc,
            )
            print(f"[ERROR RED] No se pudo resolver dominio — intento {intento}/{_FETCH_RETRIES}")
        except requests.exceptions.Timeout as exc:
            last_exc = exc
            logger.error(
                "[NETWORK ERROR] Intento %d — Timeout al conectar con COES (timeout=%ds): %s",
                intento, timeout, exc,
            )
            print(f"[ERROR CONEXIÓN] Timeout al conectar con COES — intento {intento}/{_FETCH_RETRIES}")
        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            # Distinguir DNS de otros errores de conexión
            cause = str(exc)
            if "getaddrinfo" in cause or "gaierror" in cause:
                logger.error(
                    "[NETWORK ERROR] Intento %d — Fallo DNS (getaddrinfo): %s", intento, exc
                )
                print(f"[ERROR RED] No se pudo resolver dominio — intento {intento}/{_FETCH_RETRIES}")
            else:
                logger.error(
                    "[NETWORK ERROR] Intento %d — Error de conexión: %s", intento, exc
                )
                print(f"[ERROR CONEXIÓN] Timeout al conectar con COES — intento {intento}/{_FETCH_RETRIES}")
        else:
            # Conexión exitosa — validar respuesta HTTP
            logger.info("[NETWORK] COES OK — HTTP %d", resp.status_code)

            if resp.status_code != 200:
                raise requests.HTTPError(
                    f"El endpoint COES devolvió status {resp.status_code}. "
                    f"URL: {_URL_DEMANDA} — Respuesta: {resp.text[:200]}"
                )

            try:
                data = resp.json()
            except ValueError as exc:
                raise ValueError(
                    f"La respuesta del endpoint COES no es JSON válido. "
                    f"Primeros 300 chars: {resp.text[:300]!r}"
                ) from exc

            logger.info(
                "[NETWORK] Endpoint COES OK — claves raíz: %s",
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return data

        # Esperar antes del siguiente intento (no esperar tras el último)
        if intento < _FETCH_RETRIES:
            logger.debug("[NETWORK] Reintentando en %ds …", _FETCH_RETRY_DELAY)
            time.sleep(_FETCH_RETRY_DELAY)

    # Todos los intentos fallaron
    raise requests.exceptions.ConnectionError(
        f"No se pudo conectar al endpoint COES tras {_FETCH_RETRIES} intentos. "
        f"Último error: {last_exc}"
    ) from last_exc


# ---------------------------------------------------------------------------
# FUENTE OFICIAL: raw["Data"] → ValorEjecutado (meses cerrados)
# ---------------------------------------------------------------------------

_RE_FECHA_OFICIAL = re.compile(r"^(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2})$")


def _parsear_punto_oficial(punto: dict) -> dict | None:
    """
    Convierte un punto de raw["Data"] a dict con fecha, hora, minuto, potencia_maxima.
    Formato de Fecha: "YYYY/MM/DD HH:MM"
    Retorna None si el punto no es válido.
    """
    fecha_str = (punto.get("Fecha") or "").strip()
    valor = punto.get("ValorEjecutado")

    if not fecha_str or valor is None:
        return None

    m = _RE_FECHA_OFICIAL.match(fecha_str)
    if not m:
        logger.debug("Timestamp oficial con formato inesperado: %r — omitido", fecha_str)
        return None

    anio, mes, dia, hora, minuto = m.groups()
    try:
        potencia = float(valor)
    except (TypeError, ValueError):
        logger.debug("ValorEjecutado no numérico: %r — omitido", valor)
        return None

    return {
        "fecha": f"{anio}-{mes}-{dia}",
        "hora": int(hora),
        "minuto": int(minuto),
        "potencia_maxima": potencia,
    }


def _max_hp_oficial(raw: dict) -> pd.DataFrame:
    """
    Extrae la máxima HP por día desde raw["Data"] (ValorEjecutado).
    Esta es la fuente oficial de datos ejecutados del COES.

    Raises:
        RuntimeError: Si raw["Data"] no existe, está vacío o no hay datos HP.
    """
    puntos = raw.get("Data")
    if not puntos:
        raise RuntimeError(
            "raw['Data'] vacío o ausente — la fuente oficial no devolvió datos."
        )

    logger.debug("[COES] raw['Data'] recibido: %d puntos", len(puntos))

    registros = [r for p in puntos if (r := _parsear_punto_oficial(p)) is not None]

    if not registros:
        raise RuntimeError("No se pudieron parsear puntos válidos desde raw['Data'].")

    df = pd.DataFrame(registros)

    # Filtro Hora Punta
    mask_hp = (df["hora"] >= _HP_HORA_INICIO) & (df["hora"] <= _HP_HORA_FIN)
    df_hp = df[mask_hp].copy()

    if df_hp.empty:
        raise RuntimeError(
            f"No hay datos en Hora Punta ({_HP_HORA_INICIO}:00–{_HP_HORA_FIN}:59) "
            "en raw['Data']."
        )

    # Máximo por día (ValorEjecutado = valor oficial COES)
    idx_max = df_hp.groupby("fecha")["potencia_maxima"].idxmax()
    df_max = df_hp.loc[idx_max].reset_index(drop=True)

    df_max["hora"] = df_max["hora"].astype(int)
    df_max["minuto"] = df_max["minuto"].astype(int)
    df_max["potencia_maxima"] = df_max["potencia_maxima"].astype(float)
    df_max = df_max.sort_values("fecha").reset_index(drop=True)

    return df_max[["fecha", "potencia_maxima", "hora", "minuto"]]


# ---------------------------------------------------------------------------
# FUENTE OPERATIVA: raw["Chart"]["Series"][0]["Data"] → Valor (tiempo real)
# ---------------------------------------------------------------------------

_RE_TIMESTAMP = re.compile(r"^(\d{4})/(\d{2})/(\d{2})\s+(\d{2}):(\d{2}):\d{2}$")


def _extraer_puntos_chart(raw: dict) -> list[dict]:
    """
    Navega raw["Chart"]["Series"][0]["Data"] y devuelve la lista de puntos.

    Raises:
        RuntimeError: Si la estructura del JSON no coincide con lo esperado.
    """
    try:
        series = raw["Chart"]["Series"]
    except (KeyError, TypeError) as exc:
        raise RuntimeError(
            f"La estructura JSON no contiene 'Chart.Series'. "
            f"Claves encontradas: {list(raw.keys()) if isinstance(raw, dict) else type(raw)}"
        ) from exc

    if not series:
        raise RuntimeError("Chart.Series está vacío — no hay datos en la respuesta.")

    puntos = series[0].get("Data", [])
    if not puntos:
        raise RuntimeError("Chart.Series[0].Data está vacío — sin datos para el período.")

    logger.debug("[COES] Chart.Series[0].Data recibido: %d puntos", len(puntos))
    return puntos


def _parsear_punto_chart(punto: dict) -> dict | None:
    """
    Convierte un punto de Chart.Series[0].Data a dict con fecha, hora, minuto, potencia_maxima.
    Formato de Nombre: "YYYY/MM/DD HH:MM:SS"
    Retorna None si el punto no es válido.
    """
    nombre = (punto.get("Nombre") or "").strip()
    valor = punto.get("Valor")

    if not nombre or valor is None:
        return None

    m = _RE_TIMESTAMP.match(nombre)
    if not m:
        logger.debug("Timestamp chart con formato inesperado: %r — omitido", nombre)
        return None

    anio, mes, dia, hora, minuto = m.groups()
    try:
        potencia = float(valor)
    except (TypeError, ValueError):
        logger.debug("Valor chart no numérico: %r — omitido", valor)
        return None

    return {
        "fecha": f"{anio}-{mes}-{dia}",
        "hora": int(hora),
        "minuto": int(minuto),
        "potencia_maxima": potencia,
    }


def _max_hp_chart(raw: dict) -> pd.DataFrame:
    """
    Extrae la máxima HP por día desde Chart.Series[0].Data.
    Usado para el mes actual (datos operativos en tiempo real).

    Raises:
        RuntimeError: Si no hay datos HP o el parseo falla.
    """
    puntos = _extraer_puntos_chart(raw)
    registros = [r for p in puntos if (r := _parsear_punto_chart(p)) is not None]

    if not registros:
        raise RuntimeError("No se pudieron parsear puntos válidos desde Chart.Series[0].Data.")

    df = pd.DataFrame(registros)

    mask_hp = (df["hora"] >= _HP_HORA_INICIO) & (df["hora"] <= _HP_HORA_FIN)
    df_hp = df[mask_hp].copy()

    if df_hp.empty:
        raise RuntimeError(
            f"No hay datos en Hora Punta ({_HP_HORA_INICIO}:00–{_HP_HORA_FIN}:59) "
            "en Chart.Series[0].Data."
        )

    idx_max = df_hp.groupby("fecha")["potencia_maxima"].idxmax()
    df_max = df_hp.loc[idx_max].reset_index(drop=True)

    df_max["hora"] = df_max["hora"].astype(int)
    df_max["minuto"] = df_max["minuto"].astype(int)
    df_max["potencia_maxima"] = df_max["potencia_maxima"].astype(float)
    df_max = df_max.sort_values("fecha").reset_index(drop=True)

    return df_max[["fecha", "potencia_maxima", "hora", "minuto"]]


# ---------------------------------------------------------------------------
# Validaciones comunes
# ---------------------------------------------------------------------------

def _validar_dataframe(df: pd.DataFrame, dias_esperados: int, fuente: str) -> None:
    """
    Valida que el DataFrame tenga sentido:
    - Filas ≈ días del período (tolerancia ±2)
    - Sin duplicados por fecha
    - Sin valores nulos

    Logs de advertencia sin levantar excepción para no interrumpir el pipeline.
    """
    if df.empty:
        logger.warning("[COES] %s: DataFrame vacío — sin datos para validar.", fuente)
        return

    # Nulos
    nulos = df.isnull().sum().sum()
    if nulos > 0:
        logger.warning("[COES] %s: %d valores nulos detectados.", fuente, nulos)

    # Duplicados
    duplicados = df["fecha"].duplicated().sum()
    if duplicados > 0:
        logger.warning("[COES] %s: %d fechas duplicadas detectadas.", fuente, duplicados)

    # Cobertura de días
    filas = len(df)
    if abs(filas - dias_esperados) > 2:
        logger.warning(
            "[COES] %s: se esperaban ~%d días pero se obtuvieron %d filas.",
            fuente, dias_esperados, filas,
        )


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def obtener_potencia_historica_coes() -> pd.DataFrame:
    """
    Obtiene la máxima demanda HP diaria del mes anterior desde el COES.

    Selecciona automáticamente la fuente según el período:
      - Mes anterior → raw["Data"].ValorEjecutado  (datos oficiales ejecutados)
      - Mes actual   → Chart.Series[0].Data.Valor  (datos operativos en tiempo real)

    Returns:
        DataFrame con columnas: fecha (str YYYY-MM-DD), potencia_maxima (float),
        hora (int), minuto (int). Una fila por día = máxima demanda en Hora Punta.

    Raises:
        requests.HTTPError: Si el portal devuelve un error HTTP.
        RuntimeError: Si la estructura de la respuesta no es la esperada o no hay datos.
        ValueError: Si la respuesta no es JSON válido.
    """
    fecha_inicial, fecha_final, anio_mes, mes_mes = _rango_mes_anterior()
    dias_esperados = calendar.monthrange(anio_mes, mes_mes)[1]

    es_actual = _es_mes_actual(anio_mes, mes_mes)
    fuente = "JSON operativo (Chart)" if es_actual else "JSON oficial (ValorEjecutado)"

    print(f"[COES] Usando fuente: {fuente}")
    logger.info("[COES] Período solicitado: %s → %s | Fuente: %s", fecha_inicial, fecha_final, fuente)

    raw = _fetch_demanda(fecha_inicial, fecha_final)

    if es_actual:
        df = _max_hp_chart(raw)
    else:
        # Intentar fuente oficial; fallback a Chart si falla
        try:
            df = _max_hp_oficial(raw)
        except RuntimeError as exc:
            logger.warning(
                "[COES] Fuente oficial falló (%s) — usando Chart como fallback.", exc
            )
            print(f"[COES] ADVERTENCIA: fuente oficial no disponible ({exc}). Usando Chart como fallback.")
            df = _max_hp_chart(raw)
            fuente = "JSON operativo (Chart) [fallback]"

    _validar_dataframe(df, dias_esperados, fuente)

    if df.empty:
        logger.warning("[COES] DataFrame resultante vacío.")
    else:
        print(f"[COES] Filas obtenidas: {len(df)}")
        print(f"[COES] Rango: {df['fecha'].min()} → {df['fecha'].max()}")
        logger.info(
            "[COES] HP extraído (%s): %d días (%s → %s)",
            fuente, len(df), df["fecha"].min(), df["fecha"].max(),
        )

    return df
