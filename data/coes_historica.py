"""
data/coes_historica.py
Extrae la máxima demanda diaria HP oficial desde la tabla HTML del portal COES.

Fuente:
  GET https://www.coes.org.pe/Portal/portalinformacion/demanda?indicador=maxima

Lógica:
  - Descarga la página HTML con la tabla de demanda máxima.
  - Extrae la tabla que contiene las columnas Fecha / HFP / HP DEMANDA SEIN.
  - Filtra al mes anterior completo.
  - Devuelve el valor oficial publicado por COES (usado en facturación).

Función pública principal:
    obtener_potencia_historica_coes() -> pd.DataFrame
        Columnas: fecha (str YYYY-MM-DD), potencia_maxima (float),
                  hora (None), minuto (None), source (str)
        Una fila por día = máxima demanda HP oficial COES.
"""

import calendar
import logging
import socket
import time
from datetime import date
from io import StringIO

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_URL_HTML = (
    "https://www.coes.org.pe/Portal/portalinformacion/demanda?indicador=maxima"
)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    "Referer": "https://www.coes.org.pe/Portal/portalinformacion/demanda",
}

_FETCH_RETRIES = 3
_FETCH_RETRY_DELAY = 5  # segundos entre reintentos

# Columna objetivo en la tabla HTML (búsqueda case-insensitive parcial)
_COL_HP_SEIN = "hp demanda sein"
_COL_FECHA = "fecha"


# ---------------------------------------------------------------------------
# Rango de fechas
# ---------------------------------------------------------------------------

def _rango_mes_anterior() -> tuple[str, str, int, int, int]:
    """
    Devuelve (fecha_inicial_iso, fecha_final_iso, anio, mes, dias) del mes anterior.
    Las fechas están en formato YYYY-MM-DD.
    """
    hoy = date.today()
    if hoy.month == 1:
        anio, mes = hoy.year - 1, 12
    else:
        anio, mes = hoy.year, hoy.month - 1

    ultimo_dia = calendar.monthrange(anio, mes)[1]
    fecha_inicial = f"{anio}-{mes:02d}-01"
    fecha_final = f"{anio}-{mes:02d}-{ultimo_dia:02d}"
    return fecha_inicial, fecha_final, anio, mes, ultimo_dia


# ---------------------------------------------------------------------------
# Descarga HTML
# ---------------------------------------------------------------------------

def _fetch_html(timeout: int = 30) -> str:
    """
    Descarga el HTML de la página de demanda máxima del COES.
    Reintenta hasta _FETCH_RETRIES veces ante errores de red.

    Returns:
        Contenido HTML como string.

    Raises:
        requests.HTTPError: Si el servidor devuelve un código de error HTTP.
        requests.exceptions.ConnectionError: Si no se pudo establecer conexión.
    """
    logger.info("[NETWORK] GET %s", _URL_HTML)

    session = requests.Session()
    session.trust_env = False

    last_exc: Exception | None = None

    for intento in range(1, _FETCH_RETRIES + 1):
        try:
            logger.debug("[NETWORK] Intento %d/%d …", intento, _FETCH_RETRIES)
            resp = session.get(
                _URL_HTML,
                headers=_HEADERS,
                timeout=timeout,
                proxies={"http": None, "https": None},
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
            print(f"[ERROR CONEXIÓN] Timeout — intento {intento}/{_FETCH_RETRIES}")
        except requests.exceptions.ConnectionError as exc:
            last_exc = exc
            cause = str(exc)
            if "getaddrinfo" in cause or "gaierror" in cause:
                logger.error("[NETWORK ERROR] Intento %d — Fallo DNS: %s", intento, exc)
                print(f"[ERROR RED] No se pudo resolver dominio — intento {intento}/{_FETCH_RETRIES}")
            else:
                logger.error("[NETWORK ERROR] Intento %d — Error de conexión: %s", intento, exc)
                print(f"[ERROR CONEXIÓN] Error de conexión — intento {intento}/{_FETCH_RETRIES}")
        else:
            logger.info("[NETWORK] COES OK — HTTP %d", resp.status_code)
            if resp.status_code != 200:
                raise requests.HTTPError(
                    f"El portal COES devolvió status {resp.status_code}. "
                    f"URL: {_URL_HTML}"
                )
            return resp.text

        if intento < _FETCH_RETRIES:
            logger.debug("[NETWORK] Reintentando en %ds …", _FETCH_RETRY_DELAY)
            time.sleep(_FETCH_RETRY_DELAY)

    raise requests.exceptions.ConnectionError(
        f"No se pudo conectar al portal COES tras {_FETCH_RETRIES} intentos. "
        f"Último error: {last_exc}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Parseo de la tabla HTML
# ---------------------------------------------------------------------------

def _encontrar_columna(columnas: list[str], patron: str) -> str | None:
    """
    Busca case-insensitivamente una columna cuyo nombre contenga `patron`.
    Retorna el nombre real de la columna o None si no se encuentra.
    """
    patron_lower = patron.lower()
    for col in columnas:
        if patron_lower in str(col).lower():
            return col
    return None


def _limpiar_numero(valor) -> float | None:
    """
    Convierte un valor con posibles separadores de miles a float.
    Ej: "6,789.12" → 6789.12 | "6.789,12" → 6789.12
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    texto = str(valor).strip().replace(" ", "")
    # Si tiene coma y punto, detectar cuál es el separador decimal
    if "," in texto and "." in texto:
        # Formato europeo: 6.789,12 → punto=miles, coma=decimal
        if texto.rindex(",") > texto.rindex("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            # Formato anglosajón: 6,789.12 → coma=miles, punto=decimal
            texto = texto.replace(",", "")
    elif "," in texto:
        # Solo comas: puede ser separador de miles o decimal
        # Si hay exactamente una coma con 3 dígitos después → separador de miles
        partes = texto.split(",")
        if len(partes) == 2 and len(partes[1]) == 3 and partes[1].isdigit():
            texto = texto.replace(",", "")
        else:
            texto = texto.replace(",", ".")
    try:
        return float(texto)
    except ValueError:
        return None


def _parsear_fecha(valor) -> str | None:
    """
    Intenta convertir el valor de fecha a formato YYYY-MM-DD.
    Acepta: datetime, date, strings con formatos DD/MM/YYYY, YYYY-MM-DD, DD-MM-YYYY.
    """
    if pd.isnull(valor) if not isinstance(valor, str) else not valor:
        return None

    if hasattr(valor, "strftime"):
        return valor.strftime("%Y-%m-%d")

    texto = str(valor).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return pd.to_datetime(texto, format=fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue

    # Último recurso: pandas inferencia
    try:
        return pd.to_datetime(texto, dayfirst=True).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


def _extraer_tabla_hp(html: str) -> pd.DataFrame:
    """
    Parsea todas las tablas del HTML y devuelve la que contiene
    las columnas Fecha y HP DEMANDA SEIN.

    Returns:
        DataFrame crudo con columnas normalizadas.

    Raises:
        RuntimeError: Si no se encuentra ninguna tabla con la estructura esperada.
    """
    try:
        tablas = pd.read_html(StringIO(html), header=0)
    except ValueError as exc:
        raise RuntimeError(
            f"No se encontraron tablas HTML en la página del COES: {exc}"
        ) from exc

    logger.debug("[HTML] Tablas encontradas: %d", len(tablas))

    for i, tabla in enumerate(tablas):
        # Aplanar MultiIndex en columnas si existiera
        if isinstance(tabla.columns, pd.MultiIndex):
            tabla.columns = [" ".join(str(c) for c in col).strip() for col in tabla.columns]
        else:
            tabla.columns = [str(c).strip() for c in tabla.columns]

        col_fecha = _encontrar_columna(tabla.columns.tolist(), _COL_FECHA)
        col_hp = _encontrar_columna(tabla.columns.tolist(), _COL_HP_SEIN)

        if col_fecha and col_hp:
            logger.debug(
                "[HTML] Tabla %d seleccionada — columna fecha: %r | columna HP: %r",
                i, col_fecha, col_hp,
            )
            return tabla.rename(columns={col_fecha: "__fecha__", col_hp: "__hp__"})

    raise RuntimeError(
        f"Ninguna de las {len(tablas)} tablas HTML contiene las columnas "
        f"'{_COL_FECHA}' y '{_COL_HP_SEIN}'. "
        "Verifique que la página del COES sigue la misma estructura."
    )


def _construir_dataframe(tabla: pd.DataFrame, fecha_ini: str, fecha_fin: str) -> pd.DataFrame:
    """
    Limpia la tabla cruda y devuelve el DataFrame final filtrado al rango indicado.

    Returns:
        DataFrame con columnas: fecha, potencia_maxima, hora, minuto, source.

    Raises:
        RuntimeError: Si tras la limpieza no quedan filas válidas en el período.
    """
    registros = []
    for _, fila in tabla.iterrows():
        fecha = _parsear_fecha(fila["__fecha__"])
        potencia = _limpiar_numero(fila["__hp__"])

        if fecha is None or potencia is None:
            logger.debug("Fila omitida — fecha: %r | hp: %r", fila["__fecha__"], fila["__hp__"])
            continue

        registros.append({
            "fecha": fecha,
            "potencia_maxima": potencia,
            "hora": None,
            "minuto": None,
            "source": "html_coes_oficial",
        })

    if not registros:
        raise RuntimeError(
            "No se pudieron parsear filas válidas desde la tabla HTML del COES."
        )

    df = pd.DataFrame(registros)

    # Filtrar al mes anterior
    df = df[(df["fecha"] >= fecha_ini) & (df["fecha"] <= fecha_fin)].copy()

    if df.empty:
        raise RuntimeError(
            f"La tabla HTML no contiene datos para el período {fecha_ini} → {fecha_fin}. "
            "Es posible que el COES aún no haya publicado los datos oficiales del mes anterior."
        )

    df = df.sort_values("fecha").reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------

def _validar_dataframe(df: pd.DataFrame, dias_esperados: int) -> None:
    """
    Valida cobertura y consistencia del DataFrame resultante.
    Emite advertencias sin interrumpir el pipeline.
    """
    if df.empty:
        logger.warning("[COES HTML] DataFrame vacío — sin datos para validar.")
        return

    nulos = df[["fecha", "potencia_maxima"]].isnull().sum().sum()
    if nulos > 0:
        logger.warning("[COES HTML] %d valores nulos en columnas clave.", nulos)

    duplicados = df["fecha"].duplicated().sum()
    if duplicados > 0:
        logger.warning("[COES HTML] %d fechas duplicadas.", duplicados)

    filas = len(df)
    if filas < 28 or filas > 31:
        logger.warning(
            "[COES HTML] Filas fuera del rango esperado (28–31): %d filas obtenidas.", filas
        )
    elif filas != dias_esperados:
        logger.warning(
            "[COES HTML] Se esperaban %d días pero se obtuvieron %d filas.",
            dias_esperados, filas,
        )


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def obtener_potencia_historica_coes() -> pd.DataFrame:
    """
    Obtiene la máxima demanda HP diaria oficial del mes anterior desde la tabla HTML del COES.

    Fuente: https://www.coes.org.pe/Portal/portalinformacion/demanda?indicador=maxima

    Returns:
        DataFrame con columnas:
          - fecha          (str YYYY-MM-DD)
          - potencia_maxima (float, MW)
          - hora            (None)
          - minuto          (None)
          - source          ("html_coes_oficial")
        Una fila por día. Solo el mes anterior completo.

    Raises:
        requests.HTTPError: Si el portal devuelve un error HTTP.
        requests.exceptions.ConnectionError: Si no se pudo conectar al portal.
        RuntimeError: Si la tabla no se encontró o no contiene datos del período esperado.
    """
    fecha_ini, fecha_fin, anio, mes, dias_esperados = _rango_mes_anterior()

    print(f"[COES] Fuente: HTML oficial — período {fecha_ini} → {fecha_fin}")
    logger.info("[COES] Período solicitado: %s → %s", fecha_ini, fecha_fin)

    html = _fetch_html()
    tabla = _extraer_tabla_hp(html)
    df = _construir_dataframe(tabla, fecha_ini, fecha_fin)

    _validar_dataframe(df, dias_esperados)

    print(f"[COES] Filas obtenidas: {len(df)}")
    print(f"[COES] Rango: {df['fecha'].min()} → {df['fecha'].max()}")
    logger.info(
        "[COES] HP oficial extraído: %d días (%s → %s)",
        len(df), df["fecha"].min(), df["fecha"].max(),
    )

    return df
