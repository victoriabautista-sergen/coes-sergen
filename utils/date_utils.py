"""
utils/date_utils.py
Responsabilidad única: operaciones sobre fechas.
No conoce nada del COES, ni de HTTP, ni de parsing.
"""

from datetime import date, datetime


def get_first_day_of_current_month() -> str:
    """Retorna el primer día del mes actual en formato dd/mm/yyyy."""
    today = date.today()
    first_day = today.replace(day=1)
    return first_day.strftime("%d/%m/%Y")


def get_today() -> str:
    """Retorna la fecha de hoy en formato dd/mm/yyyy."""
    return date.today().strftime("%d/%m/%Y")


def parse_date_str(date_str: str) -> date:
    """Convierte string dd/mm/yyyy a objeto date."""
    try:
        return datetime.strptime(date_str, "%d/%m/%Y").date()
    except ValueError as exc:
        raise ValueError(
            f"Formato de fecha inválido '{date_str}'. Se espera dd/mm/yyyy."
        ) from exc


def validate_date_range(fecha_inicio: str, fecha_fin: str) -> None:
    """
    Lanza ValueError si fecha_inicio > fecha_fin.
    Ambas deben estar en formato dd/mm/yyyy.
    """
    inicio = parse_date_str(fecha_inicio)
    fin = parse_date_str(fecha_fin)
    if inicio > fin:
        raise ValueError(
            f"Rango de fechas inválido: {fecha_inicio} es posterior a {fecha_fin}."
        )
