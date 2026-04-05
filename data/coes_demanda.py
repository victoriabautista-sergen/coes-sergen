"""
data/coes_demanda.py
Responsabilidad única: obtener datos de demanda eléctrica desde el portal COES.
Consolida cliente HTTP, parsing y orquestación de fechas.
No calcula métricas ni indicadores — eso es responsabilidad de dashboards/.

Función principal:
    obtener_demanda_mes_actual() -> list[dict]
        Cada dict: {"fecha_hora": datetime, "demanda": float (MW)}
"""

import logging
from datetime import date, datetime
from typing import Any

import requests

from utils.date_utils import get_first_day_of_current_month, get_today, validate_date_range

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Cliente HTTP
# ---------------------------------------------------------------------------

_BASE_URL = "https://www.coes.org.pe/Portal"

_PORTAL_URL = f"{_BASE_URL}/portalinformacion/demanda"

_DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Referer": _PORTAL_URL,
}

_POST_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "Accept-Language": "es-PE,es;q=0.9,en;q=0.8",
    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
    "X-Requested-With": "XMLHttpRequest",
    "Origin": "https://www.coes.org.pe",
}


class CoesClient:
    """
    Cliente HTTP para el portal COES.
    Usa requests.Session para reutilizar conexiones y cookies.
    El GET inicial al portal es obligatorio: genera las cookies de sesión
    que el servidor requiere para responder correctamente al POST de datos.
    """

    def __init__(self, timeout: int = 30):
        self._timeout = timeout
        self._session = requests.Session()
        self._session.headers.update(_DEFAULT_HEADERS)

    def _init_session(self) -> None:
        """
        GET al portal de demanda para obtener cookies de sesión.
        Sin este paso el POST devuelve datos incompletos.
        """
        logger.debug("GET %s — inicializando sesión y cookies.", _PORTAL_URL)
        response = self._session.get(_PORTAL_URL, timeout=self._timeout)
        response.raise_for_status()
        logger.debug("Sesión inicializada. Cookies: %s", dict(self._session.cookies))

    def get_demand(self, fecha_inicio: str, fecha_fin: str) -> dict:
        """
        Inicializa la sesión con un GET y luego hace POST al endpoint de demanda.

        Args:
            fecha_inicio: Fecha inicial en formato dd/mm/yyyy.
            fecha_fin:    Fecha final en formato dd/mm/yyyy.

        Returns:
            JSON crudo de la respuesta (dict), sin modificar.

        Raises:
            requests.HTTPError:   Si el status code no es 200.
            requests.Timeout:     Si la petición supera el timeout.
            ValueError:           Si la respuesta no contiene JSON válido.
        """
        # Paso 1: GET para inicializar cookies de sesión
        self._init_session()

        # Paso 2: POST con las cookies ya establecidas
        url = f"{_BASE_URL}/portalinformacion/Demanda"
        payload = {
            "fechaInicial": fecha_inicio,
            "fechaFinal": fecha_fin,
        }

        logger.debug("POST %s — payload: %s", url, payload)

        response = self._session.post(url, data=payload, headers=_POST_HEADERS, timeout=self._timeout)

        logger.debug("Status: %s — Content-Length: %s", response.status_code, len(response.content))

        response.raise_for_status()

        try:
            return response.json()
        except ValueError as exc:
            raise ValueError(
                f"La respuesta del COES no es JSON válido. "
                f"Primeros 200 chars: {response.text[:200]!r}"
            ) from exc

    def close(self) -> None:
        """Cierra la sesión HTTP."""
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

# Formatos de fecha que puede devolver el COES
_DATE_FORMATS = [
    "%Y/%m/%d %H:%M:%S",   # Confirmado: "2026/04/01 00:30:00"
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%Y-%m-%dT%H:%M:%S",
]


def _parse_nombre(nombre: Any) -> datetime:
    """
    Convierte el campo 'Nombre' de un punto de la serie a datetime.
    Prueba todos los formatos conocidos del COES.
    """
    if not isinstance(nombre, str):
        raise ValueError(f"'Nombre' debe ser str, se recibió {type(nombre).__name__}: {nombre!r}")

    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(nombre, fmt)
        except ValueError:
            pass

    raise ValueError(
        f"No se puede convertir 'Nombre' a datetime: {nombre!r}. "
        f"Formatos probados: {_DATE_FORMATS}"
    )


def _parse_demand_response(raw: dict) -> list[dict]:
    """
    Extrae y normaliza los registros de demanda desde la respuesta cruda.

    Ruta: raw["Chart"]["Series"][0]["Data"]
    Cada elemento de Data tiene:
        - "Nombre" : string de fecha/hora  ("yyyy/mm/dd HH:MM:SS")
        - "Valor"  : valor de demanda (float)

    Returns:
        Lista de dicts con claves:
            - "fecha_hora" (datetime)
            - "demanda"    (float, MW)
    """
    try:
        series = raw["Chart"]["Series"]
    except (KeyError, TypeError) as exc:
        raise KeyError(
            "Estructura inesperada: no se encontró raw['Chart']['Series']. "
            f"Claves disponibles: {list(raw.keys()) if isinstance(raw, dict) else type(raw)}"
        ) from exc

    if not series:
        raise ValueError("raw['Chart']['Series'] está vacío.")

    try:
        data_points = series[0]["Data"]
    except (KeyError, IndexError, TypeError) as exc:
        raise KeyError(
            "Estructura inesperada: no se encontró raw['Chart']['Series'][0]['Data']."
        ) from exc

    records = []
    skipped = 0
    for i, point in enumerate(data_points):
        try:
            fecha_hora = _parse_nombre(point["Nombre"])
            demanda = float(point["Valor"])
            records.append({"fecha_hora": fecha_hora, "demanda": demanda})
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Registro %d omitido: %s — %s", i, point, exc)
            skipped += 1

    logger.debug(
        "Parser: %d registros extraídos, %d omitidos (total raw: %d).",
        len(records), skipped, len(data_points),
    )
    return records


# ---------------------------------------------------------------------------
# Servicio de demanda
# ---------------------------------------------------------------------------

class DemandService:
    """
    Servicio de demanda eléctrica del COES.
    Coordina fechas, cliente HTTP y parser. No hace requests ni parsing directamente.
    """

    def __init__(self, client: CoesClient | None = None):
        self._client = client or CoesClient()

    def get_current_month_demand(self) -> list[dict]:
        """
        Descarga la demanda del mes actual (1er día → hoy).

        Returns:
            Lista de dicts {"fecha_hora": datetime, "demanda": float}.

        Raises:
            ValueError: Si las fechas están invertidas o la respuesta no tiene datos.
            KeyError:   Si la respuesta no tiene la estructura esperada.
        """
        fecha_inicio = get_first_day_of_current_month()
        fecha_fin = get_today()

        logger.info("Consultando demanda del %s al %s.", fecha_inicio, fecha_fin)

        validate_date_range(fecha_inicio, fecha_fin)

        raw = self._client.get_demand(fecha_inicio, fecha_fin)
        logger.debug("Respuesta recibida. Claves raíz: %s", list(raw.keys()) if isinstance(raw, dict) else type(raw))

        self._validate_raw_response(raw)

        records = _parse_demand_response(raw)

        if not records:
            raise ValueError(
                "El parser no pudo extraer ningún registro válido de la respuesta del COES."
            )

        logger.info("Demanda obtenida: %d registros.", len(records))
        return records

    def get_demand_for_range(self, fecha_inicio: str, fecha_fin: str) -> list[dict]:
        """
        Descarga la demanda para un rango explícito dd/mm/yyyy.
        Útil para testing o consultas históricas puntuales.
        """
        validate_date_range(fecha_inicio, fecha_fin)
        logger.info("Consultando demanda del %s al %s.", fecha_inicio, fecha_fin)

        raw = self._client.get_demand(fecha_inicio, fecha_fin)
        self._validate_raw_response(raw)

        records = _parse_demand_response(raw)
        if not records:
            raise ValueError("El parser no extrajo ningún registro válido.")

        logger.info("Demanda obtenida: %d registros.", len(records))
        return records

    def _validate_raw_response(self, raw: dict) -> None:
        """
        Comprobación temprana de que la respuesta tiene la forma mínima esperada.
        Falla rápido con un mensaje claro antes de intentar parsear.
        """
        if not isinstance(raw, dict):
            raise ValueError(
                f"Se esperaba un dict como respuesta, se recibió {type(raw).__name__}."
            )

        if "Chart" not in raw:
            raise KeyError(
                f"La respuesta no contiene la clave 'Chart'. "
                f"Claves disponibles: {list(raw.keys())}"
            )

        chart = raw["Chart"]
        if not isinstance(chart.get("Series"), list) or len(chart["Series"]) == 0:
            raise ValueError(
                "La respuesta tiene 'Chart' pero 'Series' está vacío o ausente. "
                "Revisa que el rango de fechas tenga datos disponibles en el COES."
            )

        data = chart["Series"][0].get("Data")
        if not data:
            raise ValueError(
                "raw['Chart']['Series'][0]['Data'] está vacío. "
                "El COES no devolvió datos para el rango solicitado. "
                "Verifica las fechas — suele ocurrir con fechas futuras o sin publicar."
            )


# ---------------------------------------------------------------------------
# Función pública principal
# ---------------------------------------------------------------------------

def obtener_demanda_mes_actual() -> list[dict]:
    """
    Descarga la demanda eléctrica del mes actual desde el portal COES.

    Returns:
        Lista de dicts {"fecha_hora": datetime, "demanda": float (MW)},
        ordenados cronológicamente tal como los devuelve el COES.

    Raises:
        ValueError: Si las fechas están invertidas o la respuesta no tiene datos.
        KeyError:   Si la respuesta no tiene la estructura esperada.
        requests.HTTPError: Si el portal COES devuelve un error HTTP.
    """
    return DemandService().get_current_month_demand()
