"""
test_client.py
Prueba aislada del cliente HTTP. Verifica que devuelve JSON real del COES.
Ejecutar antes de integrar parser y servicio:
    python test_client.py
"""

import json
import logging
import sys

logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")

from data.coes_demanda import CoesClient
from utils.date_utils import get_first_day_of_current_month, get_today


def main():
    fecha_inicio = get_first_day_of_current_month()
    fecha_fin = get_today()

    print(f"Probando cliente: {fecha_inicio} al {fecha_fin}")

    with CoesClient() as client:
        raw = client.get_demand(fecha_inicio, fecha_fin)

    print(f"\nTipo de respuesta: {type(raw)}")
    print(f"Claves raíz: {list(raw.keys()) if isinstance(raw, dict) else 'N/A'}")

    if isinstance(raw, dict) and "Chart" in raw:
        chart = raw["Chart"]
        series = chart.get("Series", [])
        print(f"Series count: {len(series)}")
        if series:
            data = series[0].get("Data", [])
            print(f"Data points: {len(data)}")
            if data:
                print(f"Primer punto: {data[0]}")
                print(f"Último punto: {data[-1]}")
    else:
        print("Respuesta completa (primeros 500 chars):")
        print(json.dumps(raw, indent=2, ensure_ascii=False)[:500])

    print("\n[OK] Cliente OK - JSON recibido correctamente.")


if __name__ == "__main__":
    main()
