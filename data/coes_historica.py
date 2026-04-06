"""
data/coes_historica.py
Extrae la sección HP del ranking de demanda de potencia desde el portal COES.

Fuente:
  POST https://www.coes.org.pe/Portal/portalinformacion/Demanda
  payload: {"indicador": "maxima", "fechaInicial": ..., "fechaFinal": ...}

Lógica:
  - Consulta el endpoint AJAX que alimenta la tabla "Ranking de la demanda de potencia".
  - Extrae ÚNICAMENTE la sección HP (Hora Punta). Excluye HFP completamente.
  - Filtra al mes anterior completo.
  - Devuelve los datos oficiales publicados por COES sin cálculos ni transformaciones.

Función pública principal:
    obtener_potencia_historica_coes() -> pd.DataFrame
        Columnas: fecha (str YYYY-MM-DD), hp_hora (str HH:MM o None),
                  hp_total (float), hp_importacion (float), hp_exportacion (float)
        Una fila por día = sección HP del ranking oficial COES.
"""

import calendar
import logging
import socket
import time
from datetime import date

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_URL_AJAX = "https://www.coes.org.pe/Portal/portalinformacion/Demanda"

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
}

_FETCH_RETRIES = 2
_FETCH_RETRY_DELAY = 5  # segundos entre reintentos


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
# Descarga AJAX
# ---------------------------------------------------------------------------

def _fetch_ranking_hp(fecha_inicial: str, fecha_final: str, timeout: int = 30) -> dict:
    """
    Hace POST al endpoint AJAX del COES y devuelve el JSON parseado.

    Args:
        fecha_inicial: Fecha de inicio en formato YYYY-MM-DD.
        fecha_final:   Fecha de fin en formato YYYY-MM-DD.
        timeout:       Segundos de espera por intento.

    Returns:
        Diccionario con la respuesta JSON completa.

    Raises:
        requests.HTTPError: Si el servidor devuelve un código de error HTTP.
        requests.exceptions.ConnectionError: Si no se pudo establecer conexión.
        ValueError: Si la respuesta no es JSON válido.
    """
    payload = {
        "indicador": "maxima",
        "fechaInicial": fecha_inicial,
        "fechaFinal": fecha_final,
    }

    logger.info("[NETWORK] POST %s | payload=%s", _URL_AJAX, payload)
    print(f"[COES AJAX] POST {_URL_AJAX}")
    print(f"[COES AJAX] Payload: {payload}")

    session = requests.Session()
    session.trust_env = False

    last_exc: Exception | None = None

    for intento in range(1, _FETCH_RETRIES + 1):
        try:
            logger.debug("[NETWORK] Intento %d/%d …", intento, _FETCH_RETRIES)
            resp = session.post(
                _URL_AJAX,
                headers=_HEADERS,
                data=payload,
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
            logger.info("[NETWORK] COES AJAX OK — HTTP %d", resp.status_code)

            print("[DEBUG] JSON COMPLETO:")
            print(resp.text)

            if resp.status_code != 200:
                raise requests.HTTPError(
                    f"El portal COES devolvió status {resp.status_code}. URL: {_URL_AJAX}"
                )

            try:
                return resp.json()
            except ValueError as exc:
                raise ValueError(
                    f"La respuesta del COES no es JSON válido. "
                    f"Primeros 500 chars: {resp.text[:500]}"
                ) from exc

        if intento < _FETCH_RETRIES:
            logger.debug("[NETWORK] Reintentando en %ds …", _FETCH_RETRY_DELAY)
            time.sleep(_FETCH_RETRY_DELAY)

    raise requests.exceptions.ConnectionError(
        f"No se pudo conectar al portal COES tras {_FETCH_RETRIES} intentos. "
        f"Último error: {last_exc}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Utilidades de parseo
# ---------------------------------------------------------------------------

def _limpiar_numero(valor) -> float | None:
    """
    Convierte un valor con posibles separadores de miles a float.
    Ej: "6,789.12" → 6789.12 | "6.789,12" → 6789.12
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)):
        return None
    texto = str(valor).strip().replace(" ", "")
    if not texto or texto == "-":
        return None
    # Si tiene coma y punto, detectar cuál es el separador decimal
    if "," in texto and "." in texto:
        if texto.rindex(",") > texto.rindex("."):
            texto = texto.replace(".", "").replace(",", ".")
        else:
            texto = texto.replace(",", "")
    elif "," in texto:
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
    if valor is None:
        return None
    if hasattr(valor, "strftime"):
        return valor.strftime("%Y-%m-%d")

    texto = str(valor).strip()
    if not texto:
        return None

    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return pd.to_datetime(texto, format=fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            continue

    try:
        return pd.to_datetime(texto, dayfirst=True).strftime("%Y-%m-%d")
    except (ValueError, TypeError):
        return None


# ---------------------------------------------------------------------------
# Extracción del ranking HP desde JSON
# ---------------------------------------------------------------------------

def _campo_hp(campos: list[str], terminos: tuple[str, ...]) -> str | None:
    """
    Busca en `campos` el primero que contenga alguno de los `terminos`
    Y que también contenga 'hp' (sin 'hfp') en su nombre.
    """
    for termino in terminos:
        for k in campos:
            k_lower = k.lower()
            if termino in k_lower and "hp" in k_lower and "hfp" not in k_lower:
                return k
    return None


def _extraer_ranking_hp(data: dict) -> pd.DataFrame:
    """
    Extrae ÚNICAMENTE la sección HP de la tabla de ranking desde el JSON AJAX.

    Excluye completamente HFP. No realiza cálculos ni agregaciones.

    Args:
        data: Diccionario JSON completo de la respuesta AJAX.

    Returns:
        DataFrame con columnas exactas:
          fecha           (str YYYY-MM-DD)
          hp_hora         (str HH:MM o None)
          hp_total        (float)
          hp_importacion  (float o None)
          hp_exportacion  (float o None)
        28–31 filas, una por día.

    Raises:
        RuntimeError: Si no se encuentra la estructura esperada.
        ValueError:   Si ningún candidato pasa la validación de rango/unicidad.
    """
    print("\n[DEBUG] Claves raíz del JSON:", list(data.keys()) if isinstance(data, dict) else type(data))

    # ── 1. Encontrar lista de registros ──────────────────────────────────────
    # Prioridad: claves explícitas de tabla/ranking; fallback: cualquier lista
    # que NO sea Chart/Series/gráfico.
    filas: list | None = None
    clave_usada: str | None = None

    if isinstance(data, dict):
        # Primero intentar claves que sugieren tabla de ranking
        for clave in ("tablaHp", "TablaHp", "tabla_hp", "rankingHp",
                      "Ranking", "ranking", "Data", "data", "Tabla", "tabla"):
            valor = data.get(clave)
            if isinstance(valor, list) and len(valor) > 0:
                filas = valor
                clave_usada = clave
                print(f"[DEBUG] Candidato directo: {clave!r} ({len(filas)} filas)")
                break

        # Si no se encontró por nombre, buscar la primera lista no-gráfico con ≥28 filas
        if filas is None:
            for clave, valor in data.items():
                if any(x in clave.lower() for x in ("chart", "serie", "grafico", "graf", "eje")):
                    continue
                if isinstance(valor, list) and len(valor) >= 28:
                    filas = valor
                    clave_usada = clave
                    print(f"[DEBUG] Candidato fallback: {clave!r} ({len(filas)} filas)")
                    break

    if filas is None or not filas:
        raise RuntimeError(
            "No se encontró ninguna lista de datos en el JSON. "
            f"Claves disponibles: {list(data.keys()) if isinstance(data, dict) else 'N/A'}. "
            "Revise el JSON COMPLETO impreso arriba."
        )

    primera = filas[0]
    if not isinstance(primera, dict):
        raise RuntimeError(
            f"Los registros en {clave_usada!r} no son diccionarios. "
            f"Tipo: {type(primera)}"
        )

    campos = list(primera.keys())
    print(f"[DEBUG] Campos disponibles en {clave_usada!r}: {campos}")

    # ── 2. Mapear campos HP ──────────────────────────────────────────────────
    # Buscar campo de fecha (no necesariamente tiene "hp" en el nombre)
    campo_fecha = next(
        (k for k in campos if "fecha" in k.lower() or "date" in k.lower()), None
    )
    if not campo_fecha:
        raise RuntimeError(f"No se encontró campo de fecha. Campos: {campos}")

    # Buscar campos HP específicos (deben contener "hp" pero no "hfp")
    campo_hp_hora = _campo_hp(campos, ("hora", "time"))
    campo_hp_total = _campo_hp(campos, ("total", "sein", "potencia", "valor", "mw", "demanda"))
    campo_hp_importacion = _campo_hp(campos, ("import", "imp"))
    campo_hp_exportacion = _campo_hp(campos, ("export", "exp"))

    if not campo_hp_total:
        raise RuntimeError(
            f"No se encontró campo HP Total. "
            f"Campos con 'hp': {[k for k in campos if 'hp' in k.lower()]}"
        )

    print(
        f"[DEBUG] Mapeo HP — fecha:{campo_fecha!r}, hora:{campo_hp_hora!r}, "
        f"total:{campo_hp_total!r}, import:{campo_hp_importacion!r}, export:{campo_hp_exportacion!r}"
    )

    # ── 3. Construir registros HP ────────────────────────────────────────────
    registros = []
    for fila in filas:
        if not isinstance(fila, dict):
            continue

        fecha = _parsear_fecha(fila.get(campo_fecha))
        if fecha is None:
            logger.debug("Fila omitida — fecha inválida: %r", fila.get(campo_fecha))
            continue

        hp_hora_raw = fila.get(campo_hp_hora) if campo_hp_hora else None
        hp_hora = str(hp_hora_raw).strip() if hp_hora_raw is not None else None

        hp_total = _limpiar_numero(fila.get(campo_hp_total))
        hp_importacion = _limpiar_numero(fila.get(campo_hp_importacion)) if campo_hp_importacion else None
        hp_exportacion = _limpiar_numero(fila.get(campo_hp_exportacion)) if campo_hp_exportacion else None

        registros.append({
            "fecha": fecha,
            "hp_hora": hp_hora,
            "hp_total": hp_total,
            "hp_importacion": hp_importacion,
            "hp_exportacion": hp_exportacion,
        })

    if not registros:
        raise RuntimeError(
            f"No se pudieron parsear filas válidas desde {clave_usada!r}. "
            "Revise el JSON COMPLETO impreso arriba."
        )

    df = pd.DataFrame(registros)
    print(f"[DEBUG] Registros HP extraídos: {len(df)}")
    return df


# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------

_COLUMNAS_HP = ("fecha", "hp_hora", "hp_total", "hp_importacion", "hp_exportacion")


def _validar_dataframe(df: pd.DataFrame, dias_esperados: int = None) -> None:
    """
    Valida cobertura y consistencia del DataFrame HP resultante.
    Lanza ValueError si los datos no cumplen los criterios de calidad.
    """
    columnas_faltantes = [c for c in _COLUMNAS_HP if c not in df.columns]
    if columnas_faltantes:
        raise ValueError(f"Columnas HP faltantes: {columnas_faltantes}")

    if len(df) < 28 or len(df) > 31:
        raise ValueError(
            f"Mes incompleto: se obtuvieron {len(df)} filas (se requieren 28–31)"
        )

    if df["fecha"].duplicated().any():
        raise ValueError("Fechas duplicadas")

    if any(":" in str(f) for f in df["fecha"]):
        raise ValueError("Contiene timestamps inválidos en columna fecha")


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def obtener_potencia_historica_coes() -> pd.DataFrame:
    """
    Obtiene la sección HP del ranking de demanda del mes anterior desde el COES.

    Fuente: POST https://www.coes.org.pe/Portal/portalinformacion/Demanda
    Tabla:  Ranking de la demanda de potencia — sección HP (Hora Punta).

    Returns:
        DataFrame con columnas:
          - fecha           (str YYYY-MM-DD)
          - hp_hora         (str HH:MM o None)
          - hp_total        (float, MW)
          - hp_importacion  (float o None, MW)
          - hp_exportacion  (float o None, MW)
        28–31 filas, una por día, solo el mes anterior completo.
        Sin cálculos ni transformaciones adicionales.

    Raises:
        requests.HTTPError: Si el portal devuelve un error HTTP.
        requests.exceptions.ConnectionError: Si no se pudo conectar al portal.
        ValueError: Si la respuesta no es JSON válido.
        RuntimeError: Si no se encontró la estructura HP esperada
                      o no hay datos para el período solicitado.
    """
    fecha_ini, fecha_fin, anio, mes, dias_esperados = _rango_mes_anterior()

    print("[COES] Fuente: AJAX ranking oficial — sección HP")
    print(f"[COES] Período solicitado: {fecha_ini} → {fecha_fin}")
    logger.info("[COES] Período solicitado: %s → %s", fecha_ini, fecha_fin)

    try:
        raw = _fetch_ranking_hp(fecha_ini, fecha_fin)
    except Exception as exc:
        raise RuntimeError("No se pudo obtener histórico oficial COES") from exc

    df = _extraer_ranking_hp(raw)

    # Filtrar al mes solicitado
    df = df[(df["fecha"] >= fecha_ini) & (df["fecha"] <= fecha_fin)].copy()

    if df.empty:
        raise RuntimeError(
            f"La respuesta AJAX no contiene datos HP para el período {fecha_ini} → {fecha_fin}. "
            "Es posible que el COES aún no haya publicado los datos oficiales del mes anterior."
        )

    df = df.sort_values("fecha").reset_index(drop=True)

    _validar_dataframe(df)

    print(f"[COES] Filas HP obtenidas: {len(df)}")
    print(f"[COES] Rango: {df['fecha'].min()} → {df['fecha'].max()}")
    logger.info(
        "[COES] HP oficial extraído: %d días (%s → %s)",
        len(df), df["fecha"].min(), df["fecha"].max(),
    )

    return df
