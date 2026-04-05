"""
data/almacenamiento.py
Responsabilidad única: procesamiento, cálculo y almacenamiento de demanda
y potencia máxima en hora punta.

No hace requests al COES — opera sobre registros ya obtenidos.

Pipeline principal:
    ejecutar_pipeline(records) -> None
        1. Enriquece los registros crudos.
        2. Upserta toda la data en demanda_coes (Supabase).
        3. Recalcula potencia máxima HP para las fechas relevantes
           y upserta en potencia_hora_punta (Supabase).
"""

import logging
import traceback
from datetime import date, datetime, timedelta

from supabase_client import supabase

logger = logging.getLogger(__name__)

# Hora punta: 18:00 – 23:59 (sin conversión de zona horaria)
_HP_HORA_INICIO = 18
_HP_HORA_FIN = 23

# Registros esperados por día en hora punta: 18:00–23:30 cada 30 min = 12
_REGISTROS_HP_ESPERADOS = 12

# Días que se recalculan siempre para corregir datos incompletos
_DIAS_RECALCULAR = 3


# ---------------------------------------------------------------------------
# Enriquecimiento de datos crudos
# ---------------------------------------------------------------------------

def _enriquecer(records: list[dict]) -> list[dict]:
    """
    Convierte la lista de dicts con datetime a rows listas para Supabase.
    Agrega campos derivados: fecha, hora, minuto.
    """
    rows = []
    for r in records:
        dt: datetime = r["fecha_hora"]
        rows.append({
            "fecha_hora": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "demanda":    r["demanda"],
            "fecha":      dt.strftime("%Y-%m-%d"),
            "hora":       dt.hour,
            "minuto":     dt.minute,
        })
    return rows


# ---------------------------------------------------------------------------
# Upserts en Supabase
# ---------------------------------------------------------------------------

def upsert_demanda_supabase(rows: list[dict]) -> None:
    """
    Inserta o actualiza registros en demanda_coes.
    Clave de conflicto: fecha_hora.
    No interrumpe el flujo si falla.
    """
    print(f"[upsert_demanda_supabase] filas recibidas: {len(rows)}")
    if not rows:
        print("[upsert_demanda_supabase] lista vacía — saliendo sin hacer upsert")
        return

    print(f"[upsert_demanda_supabase] primera fila de muestra: {rows[0]}")
    print("[upsert_demanda_supabase] ejecutando upsert en Supabase...")
    try:
        resp = supabase.table("demanda_coes").upsert(rows).execute()
        print(f"[upsert_demanda_supabase] respuesta recibida: {resp}")
        if hasattr(resp, "data") and resp.data is not None:
            print(f"[upsert_demanda_supabase] OK — {len(resp.data)} filas confirmadas por Supabase")
            logger.info("demanda_coes: %d registros procesados.", len(rows))
        else:
            print(f"[upsert_demanda_supabase] ADVERTENCIA — resp.data vacío o None. Posible error RLS o esquema.")
            logger.warning("demanda_coes: upsert sin datos confirmados. resp=%s", resp)
    except Exception as exc:
        print(f"[upsert_demanda_supabase] EXCEPCION: {exc}")
        traceback.print_exc()
        logger.error("demanda_coes: error al hacer upsert — %s", exc)


def upsert_hp_supabase(rows: list[dict]) -> None:
    """
    Inserta o actualiza registros en potencia_hora_punta.
    Campos: fecha, max_demanda, hora, minuto, registros_hp, completo.
    No interrumpe el flujo si falla.
    """
    if not rows:
        return
    try:
        supabase.table("potencia_hora_punta").upsert(rows).execute()
        logger.info("potencia_hora_punta: %d fechas actualizadas.", len(rows))
    except Exception as exc:
        logger.error("potencia_hora_punta: error al hacer upsert — %s", exc)


# ---------------------------------------------------------------------------
# Cálculo de potencia máxima en hora punta
# ---------------------------------------------------------------------------

def _fechas_a_recalcular(records: list[dict]) -> set[str]:
    """
    Devuelve el conjunto de fechas (YYYY-MM-DD) que deben recalcularse:
    - Las fechas presentes en los registros recibidos.
    - Los últimos N días (para corregir días incompletos de ejecuciones anteriores).
    """
    fechas = {r["fecha_hora"].strftime("%Y-%m-%d") for r in records}
    today = date.today()
    for i in range(_DIAS_RECALCULAR):
        fechas.add((today - timedelta(days=i)).strftime("%Y-%m-%d"))
    return fechas


def _max_hp_para_fecha(fecha: str) -> dict | None:
    """
    Consulta demanda_coes en Supabase y calcula la potencia máxima en hora punta
    para una fecha dada.
    Retorna None si no hay datos de hora punta o si la consulta falla.
    """
    try:
        resp = (
            supabase.table("demanda_coes")
            .select("demanda, hora, minuto")
            .eq("fecha", fecha)
            .gte("hora", _HP_HORA_INICIO)
            .lte("hora", _HP_HORA_FIN)
            .execute()
        )
        hp_rows = resp.data
    except Exception as exc:
        logger.error("demanda_coes: error al consultar fecha %s — %s", fecha, exc)
        return None

    if not hp_rows:
        return None

    max_row = max(hp_rows, key=lambda r: r["demanda"])
    total_hp = len(hp_rows)
    completo = 1 if total_hp >= _REGISTROS_HP_ESPERADOS else 0

    if not completo:
        logger.debug(
            "Día %s: %d/%d registros HP — marcado como incompleto.",
            fecha, total_hp, _REGISTROS_HP_ESPERADOS,
        )

    return {
        "fecha":        fecha,
        "max_demanda":  max_row["demanda"],
        "hora":         max_row["hora"],
        "minuto":       max_row["minuto"],
        "registros_hp": total_hp,
        "completo":     completo,
    }


def _calcular_potencia_hp(records: list[dict]) -> list[dict]:
    """
    Para cada fecha a recalcular, consulta Supabase y calcula la potencia máxima HP.
    Retorna la lista de resultados listos para upsert.
    """
    fechas = _fechas_a_recalcular(records)
    resultados = []

    for fecha in sorted(fechas):
        resultado = _max_hp_para_fecha(fecha)
        if resultado is not None:
            resultados.append(resultado)

    if not resultados:
        logger.info("potencia_hora_punta: sin datos de hora punta para recalcular.")

    return resultados


# ---------------------------------------------------------------------------
# Pipeline público
# ---------------------------------------------------------------------------

def ejecutar_pipeline(records: list[dict]) -> None:
    """
    Pipeline completo de procesamiento y almacenamiento.

    1. Valida que haya registros.
    2. Enriquece los datos crudos con campos derivados.
    3. Upserta la demanda en demanda_coes (Supabase).
    4. Recalcula la potencia máxima HP y upserta en potencia_hora_punta (Supabase).
    """
    print(f"[ejecutar_pipeline] registros recibidos: {len(records)}")
    if not records:
        logger.info("Pipeline: sin registros para procesar.")
        print("[ejecutar_pipeline] lista vacía — abortando")
        return

    rows = _enriquecer(records)
    print(f"[ejecutar_pipeline] registros después de enriquecer: {len(rows)}")
    upsert_demanda_supabase(rows)

    hp_rows = _calcular_potencia_hp(records)
    upsert_hp_supabase(hp_rows)
