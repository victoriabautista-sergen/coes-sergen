"""
data/coes_historica.py
Extrae la máxima demanda diaria HP oficial desde el endpoint AJAX del portal COES.

Fuente:
  POST https://www.coes.org.pe/Portal/portalinformacion/Demanda
  payload: {"indicador": "maxima", "fechaInicial": ..., "fechaFinal": ...}

Lógica:
  - Consulta el endpoint AJAX que alimenta la tabla "Ranking de la demanda de potencia".
  - Extrae la tabla de ranking (NO Chart.Series) con columnas fecha / hora / TOTAL HP.
  - Filtra al mes anterior completo.
  - Devuelve el valor oficial publicado por COES (usado en facturación).

Función pública principal:
    obtener_potencia_historica_coes() -> pd.DataFrame
        Columnas: fecha (str YYYY-MM-DD), potencia_maxima (float),
                  hora (str HH:MM o None), minuto (None), source (str)
        Una fila por día = máxima demanda HP oficial COES.
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

def _extraer_ranking_hp(data: dict) -> pd.DataFrame:
    """
    Extrae la tabla de ranking HP (columna TOTAL) desde el JSON del endpoint AJAX.

    El JSON contiene secciones de gráfico (Chart/Series) y tablas (tabla/ranking).
    Esta función busca la sección de tabla/ranking, NO usa datos de Chart.Series.

    Estructura esperada (a confirmar con el JSON real):
      data["tablaHp"] o data["ranking"] o data["data"] con lista de registros
      Cada registro: {"fecha": ..., "hora": ..., "total": ...}

    Args:
        data: Diccionario JSON completo de la respuesta AJAX.

    Returns:
        DataFrame con columnas: fecha, potencia_maxima, hora, minuto, source.

    Raises:
        RuntimeError: Si no se encuentra la estructura de ranking esperada.
        KeyError: Si faltan campos clave en los registros.
    """
    print("\n[DEBUG] Claves raíz del JSON:", list(data.keys()) if isinstance(data, dict) else type(data))

    # Estrategia de búsqueda: buscar la sección de tabla/ranking
    # (NO Chart, NO Series, NO grafico — solo datos tabulares de HP)
    candidatos_tabla = []

    if isinstance(data, dict):
        for clave, valor in data.items():
            clave_lower = clave.lower()
            # Excluir secciones de gráfico
            if any(x in clave_lower for x in ("chart", "serie", "grafico", "graf")):
                print(f"[DEBUG] Ignorando clave de gráfico: {clave!r}")
                continue
            if isinstance(valor, list) and len(valor) > 0:
                print(f"[DEBUG] Candidato tabla: clave={clave!r}, filas={len(valor)}, primera={valor[0]}")
                candidatos_tabla.append((clave, valor))

    if not candidatos_tabla:
        # Si no hay listas en el nivel raíz, mostrar estructura completa para diagnóstico
        raise RuntimeError(
            f"No se encontraron listas de datos en el JSON. "
            f"Claves disponibles: {list(data.keys()) if isinstance(data, dict) else 'N/A'}. "
            "Revise el JSON COMPLETO impreso arriba para identificar la estructura."
        )

    # Seleccionar únicamente el dataset que cumpla TODAS las condiciones:
    #   1. Entre 28 y 31 filas
    #   2. Una fila por día (sin fechas duplicadas)
    #   3. Sin timestamps con hora:minuto en el campo de fecha
    #   4. Tiene campos de fecha y valor numérico (representa agregados diarios)
    import re as _re
    _PATRON_TIEMPO = _re.compile(r"\d{1,2}:\d{2}")

    tabla_filas = None
    clave_usada = None

    for clave, filas in candidatos_tabla:
        n = len(filas)
        primera = filas[0] if filas else {}
        if not isinstance(primera, dict):
            print(f"[DEBUG] {clave!r}: descartado — registros no son dicts")
            continue

        campos = list(primera.keys())
        campos_lower = [k.lower() for k in campos]
        print(f"[DEBUG] Evaluando dataset con {n} filas")

        # Condición 4 — debe tener campo de fecha y campo de valor numérico
        tiene_fecha = any("fecha" in c or "date" in c for c in campos_lower)
        tiene_valor = any(
            x in c for c in campos_lower
            for x in ("total", "potencia", "valor", "mw", "hp", "demanda")
        )
        if not (tiene_fecha and tiene_valor):
            print("[DEBUG] Rechazado: filas inválidas")
            continue

        # Condición 1 — entre 28 y 31 filas
        if n < 28 or n > 31:
            print("[DEBUG] Rechazado: filas inválidas")
            continue

        # Obtener campo de fecha para las siguientes validaciones
        campo_fecha_cand = next(
            (k for k in campos if "fecha" in k.lower() or "date" in k.lower()), None
        )

        # Condición 3 — sin timestamps con hora:minuto en los valores de fecha
        tiene_timestamp = any(
            _PATRON_TIEMPO.search(str(fila.get(campo_fecha_cand, "")))
            for fila in filas
            if isinstance(fila, dict)
        )
        if tiene_timestamp:
            print("[DEBUG] Rechazado: contiene timestamps")
            continue

        # Condición 2 — una fila por día (sin duplicados)
        fechas_parseadas = [
            _parsear_fecha(fila.get(campo_fecha_cand))
            for fila in filas
            if isinstance(fila, dict)
        ]
        fechas_validas = [f for f in fechas_parseadas if f is not None]
        if len(set(fechas_validas)) != n:
            print("[DEBUG] Rechazado: duplicados")
            continue

        # Dataset cumple todas las condiciones
        tabla_filas = filas
        clave_usada = clave
        print("[DEBUG] ✅ Dataset válido encontrado")
        break

    if tabla_filas is None:
        resumen = ", ".join(
            f"{c!r}({len(f)} filas)" for c, f in candidatos_tabla
        )
        raise ValueError(
            "❌ No se encontró dataset válido de ranking diario del COES. "
            f"Candidatos evaluados: {resumen}. "
            "Revise el JSON COMPLETO impreso arriba para identificar la estructura."
        )

    # Identificar campos de fecha, hora y total en la primera fila
    primera = tabla_filas[0] if tabla_filas else {}
    if not isinstance(primera, dict):
        raise RuntimeError(
            f"Los registros en {clave_usada!r} no son diccionarios. "
            f"Tipo encontrado: {type(primera)}. Valor: {primera}"
        )

    campos_disponibles = list(primera.keys())
    print(f"[DEBUG] Campos de la tabla seleccionada: {campos_disponibles}")

    # Buscar campo de fecha
    campo_fecha = None
    for k in campos_disponibles:
        if "fecha" in k.lower() or "date" in k.lower():
            campo_fecha = k
            break

    # Buscar campo de hora
    campo_hora = None
    for k in campos_disponibles:
        if "hora" in k.lower() or "time" in k.lower():
            campo_hora = k
            break

    # Buscar campo de total/potencia (TOTAL HP SEIN)
    campo_total = None
    prioridad_total = ("total", "hp", "potencia", "valor", "mw", "demanda")
    for prioridad in prioridad_total:
        for k in campos_disponibles:
            if prioridad in k.lower():
                campo_total = k
                break
        if campo_total:
            break

    if not campo_fecha:
        raise RuntimeError(
            f"No se encontró campo de fecha en los registros. "
            f"Campos disponibles: {campos_disponibles}"
        )
    if not campo_total:
        raise RuntimeError(
            f"No se encontró campo de total/potencia en los registros. "
            f"Campos disponibles: {campos_disponibles}"
        )

    print(f"[DEBUG] Mapeando — fecha: {campo_fecha!r}, hora: {campo_hora!r}, total: {campo_total!r}")

    # Construir registros
    registros = []
    for fila in tabla_filas:
        if not isinstance(fila, dict):
            continue

        fecha = _parsear_fecha(fila.get(campo_fecha))
        potencia = _limpiar_numero(fila.get(campo_total))
        hora_raw = fila.get(campo_hora) if campo_hora else None
        hora = str(hora_raw).strip() if hora_raw is not None else None

        if fecha is None or potencia is None:
            logger.debug(
                "Fila omitida — fecha: %r | total: %r",
                fila.get(campo_fecha), fila.get(campo_total),
            )
            continue

        registros.append({
            "fecha": fecha,
            "potencia_maxima": potencia,
            "hora": hora,
            "minuto": None,
            "source": "coes_oficial_ajax",
        })

    if not registros:
        raise RuntimeError(
            f"No se pudieron parsear filas válidas desde la sección {clave_usada!r}. "
            "Revise el JSON COMPLETO impreso arriba."
        )

    df = pd.DataFrame(registros)
    print(f"[DEBUG] Registros extraídos antes de filtro: {len(df)}")
    return df


# ---------------------------------------------------------------------------
# Validación
# ---------------------------------------------------------------------------

def _validar_dataframe(df: pd.DataFrame, dias_esperados: int = None) -> None:
    """
    Valida cobertura y consistencia del DataFrame resultante.
    Lanza ValueError si los datos no cumplen los criterios de calidad.
    """
    if len(df) < 28 or len(df) > 31:
        raise ValueError(
            f"Mes incompleto: se obtuvieron {len(df)} filas (se requieren 28–31)"
        )

    if df["fecha"].duplicated().any():
        raise ValueError("Fechas duplicadas")

    if any(":" in str(f) for f in df["fecha"]):
        raise ValueError("Contiene timestamps inválidos")


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def obtener_potencia_historica_coes() -> pd.DataFrame:
    """
    Obtiene la máxima demanda HP diaria oficial del mes anterior desde el endpoint AJAX del COES.

    Fuente: POST https://www.coes.org.pe/Portal/portalinformacion/Demanda
    Tabla:  Ranking de la demanda de potencia (columna TOTAL HP SEIN).

    Returns:
        DataFrame con columnas:
          - fecha           (str YYYY-MM-DD)
          - potencia_maxima (float, MW)
          - hora            (str HH:MM o None)
          - minuto          (None)
          - source          ("coes_oficial_ajax")
        Una fila por día. Solo el mes anterior completo.

    Raises:
        requests.HTTPError: Si el portal devuelve un error HTTP.
        requests.exceptions.ConnectionError: Si no se pudo conectar al portal.
        ValueError: Si la respuesta no es JSON válido.
        RuntimeError: Si no se encontró la estructura de ranking esperada
                      o no hay datos para el período solicitado.
    """
    fecha_ini, fecha_fin, anio, mes, dias_esperados = _rango_mes_anterior()

    print("[COES] Fuente: AJAX ranking oficial")
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
            f"La respuesta AJAX no contiene datos para el período {fecha_ini} → {fecha_fin}. "
            "Es posible que el COES aún no haya publicado los datos oficiales del mes anterior."
        )

    df = df.sort_values("fecha").reset_index(drop=True)

    _validar_dataframe(df)

    print(f"[COES] Filas obtenidas: {len(df)}")
    print(f"[COES] Rango: {df['fecha'].min()} → {df['fecha'].max()}")
    logger.info(
        "[COES] HP oficial extraído: %d días (%s → %s)",
        len(df), df["fecha"].min(), df["fecha"].max(),
    )

    return df
