"""
dashboards/max_hp_mes_actual.py
Responsabilidad única: calcular la máxima potencia en hora punta por día del mes actual.

Hora punta definida: 18:00 – 23:59

No hace requests ni accede al COES directamente — usa data/coes_demanda.py.

Función principal:
    calcular_max_hp_mes_actual() -> list[dict]
        Cada dict: {"fecha": date, "max_demanda": float (MW), "hora": time}
"""

import logging
from datetime import time
from itertools import groupby

from data.coes_demanda import obtener_demanda_mes_actual

logger = logging.getLogger(__name__)

_HORA_PUNTA_INICIO = time(18, 0)
_HORA_PUNTA_FIN = time(23, 59, 59)


def _es_hora_punta(fecha_hora) -> bool:
    hora = fecha_hora.time()
    return _HORA_PUNTA_INICIO <= hora <= _HORA_PUNTA_FIN


def calcular_max_hp_mes_actual() -> list[dict]:
    """
    Obtiene la demanda del mes actual y calcula la máxima potencia
    en hora punta (18:00–23:59) para cada día.

    Returns:
        Lista de dicts, un elemento por día con registros de hora punta:
            - "fecha"       (date):  día del registro
            - "max_demanda" (float): máxima demanda MW en ese día durante hora punta
            - "hora"        (time):  hora exacta del máximo

    Raises:
        ValueError: Si no hay registros de hora punta en el mes actual.
    """
    records = obtener_demanda_mes_actual()

    hora_punta = [r for r in records if _es_hora_punta(r["fecha_hora"])]

    if not hora_punta:
        raise ValueError(
            "No hay registros de hora punta (18:00–23:59) en el mes actual."
        )

    logger.info("Registros en hora punta: %d de %d totales.", len(hora_punta), len(records))

    # Agrupar por día y encontrar el máximo de cada día
    hora_punta.sort(key=lambda r: r["fecha_hora"].date())

    resultados = []
    for fecha, grupo in groupby(hora_punta, key=lambda r: r["fecha_hora"].date()):
        registros_dia = list(grupo)
        max_rec = max(registros_dia, key=lambda r: r["demanda"])
        resultados.append({
            "fecha": fecha,
            "max_demanda": max_rec["demanda"],
            "hora": max_rec["fecha_hora"].time(),
        })
        logger.debug("Día %s → máx HP: %.2f MW a las %s", fecha, max_rec["demanda"], max_rec["fecha_hora"].time())

    logger.info("Máximas HP calculadas: %d días.", len(resultados))
    return resultados
