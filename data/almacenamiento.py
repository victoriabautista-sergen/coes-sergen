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
    5. Si es día 1 del mes o la tabla histórica está vacía, extrae y almacena
       la máxima demanda mensual histórica HP (mes anterior).
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

    # --- Módulo histórico ---
    if debe_ejecutar_historico(supabase):
        print("[pipeline] Ejecutando módulo histórico")
        logger.info("Módulo histórico: iniciando ejecución.")
        try:
            from data.coes_historica import obtener_potencia_historica_coes
            df_historico = obtener_potencia_historica_coes()
            upsert_potencia_historica(df_historico)
        except Exception as exc:
            logger.error("Módulo histórico: error — %s", exc)
            print(f"[ejecutar_pipeline] ERROR en módulo histórico: {exc}")
    else:
        print("[pipeline] Histórico no requerido")


# ---------------------------------------------------------------------------
# Histórico de máxima demanda mensual (HP)
# ---------------------------------------------------------------------------

def _tabla_historica_vacia() -> bool:
    """
    Consulta Supabase para verificar si potencia_hora_punta_historica está vacía.
    Retorna True si no hay registros (tabla vacía o error de consulta).
    """
    try:
        resp = (
            supabase.table("potencia_hora_punta_historica")
            .select("fecha")
            .limit(1)
            .execute()
        )
        vacia = not bool(resp.data)
        logger.debug("Tabla histórica vacía: %s", vacia)
        return vacia
    except Exception as exc:
        logger.warning("No se pudo verificar tabla histórica — se asume vacía: %s", exc)
        return True


def historico_mes_ya_cargado(supabase_client) -> bool:
    """
    Verifica si el mes anterior ya está cargado en potencia_hora_punta_historica.

    Consulta la fecha máxima de la tabla y la compara con el mes anterior
    calculado desde la fecha actual.

    Returns:
        True  si la fecha máxima pertenece al mes anterior.
        False si no hay datos o la fecha máxima no coincide con el mes anterior.
    """
    try:
        resp = (
            supabase_client.table("potencia_hora_punta_historica")
            .select("fecha")
            .order("fecha", desc=True)
            .limit(1)
            .execute()
        )
    except Exception as exc:
        logger.warning("historico_mes_ya_cargado: error al consultar — %s", exc)
        return False

    if not resp.data:
        return False

    fecha_max_str = resp.data[0]["fecha"]
    try:
        fecha_max = datetime.strptime(fecha_max_str, "%Y-%m-%d").date()
    except ValueError:
        logger.warning("historico_mes_ya_cargado: fecha inválida en tabla — %r", fecha_max_str)
        return False

    hoy = date.today()
    if hoy.month == 1:
        anio_ant, mes_ant = hoy.year - 1, 12
    else:
        anio_ant, mes_ant = hoy.year, hoy.month - 1

    return fecha_max.year == anio_ant and fecha_max.month == mes_ant


def debe_ejecutar_historico(supabase_client) -> bool:
    """
    Determina si el módulo histórico debe ejecutarse.

    Returns:
        True  si la tabla está vacía o el mes anterior no está cargado.
        False si el histórico ya está actualizado.
    """
    try:
        resp = (
            supabase_client.table("potencia_hora_punta_historica")
            .select("fecha")
            .limit(1)
            .execute()
        )
        tabla_vacia = not bool(resp.data)
    except Exception as exc:
        logger.warning("debe_ejecutar_historico: error al verificar tabla — se asume vacía: %s", exc)
        tabla_vacia = True

    if tabla_vacia:
        print("[pipeline] Tabla histórica vacía")
        logger.info("Módulo histórico: tabla vacía — ejecución requerida.")
        return True

    if not historico_mes_ya_cargado(supabase_client):
        print("[pipeline] Mes anterior no cargado")
        logger.info("Módulo histórico: mes anterior no cargado — ejecución requerida.")
        return True

    print("[pipeline] Histórico ya actualizado")
    logger.info("Módulo histórico: mes anterior ya cargado — omitiendo ejecución.")
    return False


def upsert_potencia_historica(df) -> None:
    """
    Inserta o actualiza registros en potencia_hora_punta_historica.
    Clave de conflicto: fecha.

    Args:
        df: DataFrame con columnas fecha, potencia_maxima, hora, minuto.
    """
    if df is None or df.empty:
        print("[upsert_potencia_historica] DataFrame vacío — nada que insertar.")
        logger.info("potencia_hora_punta_historica: DataFrame vacío, sin inserción.")
        return

    print(f"\n[upsert_potencia_historica] Vista previa del DataFrame:")
    print(df.head())

    rows = df.to_dict(orient="records")

    print(f"\n[upsert_potencia_historica] {len(rows)} registros listos para insertar")
    print(f"[upsert_potencia_historica] Rango de fechas: {df['fecha'].min()} → {df['fecha'].max()}")
    print(f"[upsert_potencia_historica] Ejemplo de fila: {rows[0]}")

    try:
        resp = supabase.table("potencia_hora_punta_historica").upsert(rows).execute()
        if hasattr(resp, "data") and resp.data is not None:
            print(f"[upsert_potencia_historica] OK — {len(resp.data)} filas confirmadas por Supabase.")
            logger.info("potencia_hora_punta_historica: %d registros procesados.", len(rows))
        else:
            print(f"[upsert_potencia_historica] ADVERTENCIA — resp.data vacío o None. resp={resp}")
            logger.warning("potencia_hora_punta_historica: upsert sin datos confirmados. resp=%s", resp)
    except Exception as exc:
        print(f"[upsert_potencia_historica] EXCEPCION: {exc}")
        logger.error("potencia_hora_punta_historica: error al hacer upsert — %s", exc)
