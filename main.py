"""
main.py
Punto de entrada. Solo llama al servicio y presenta los resultados.
No contiene lógica de negocio, fechas, ni HTTP.
"""

import logging
import sys

from data.coes_demanda import obtener_demanda_mes_actual
from data.almacenamiento import ejecutar_pipeline
from data.coes_historica import obtener_potencia_historica_coes

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> None:
    try:
        records = obtener_demanda_mes_actual()
    except (ValueError, KeyError) as exc:
        logger.error("Error de datos: %s", exc)
        sys.exit(1)
    except Exception as exc:
        logger.error("Error inesperado: %s", exc)
        sys.exit(1)

    # Muestra resumen en consola
    print(f"\n{'='*55}")
    print(f"  Demanda COES - mes actual ({len(records)} registros)")
    print(f"{'='*55}")
    print(f"  {'Fecha/Hora':<22} {'Demanda (MW)':>12}")
    print(f"  {'-'*22} {'-'*12}")
    for rec in records[:10]:
        dt_str = rec["fecha_hora"].strftime("%Y-%m-%d %H:%M")
        print(f"  {dt_str:<22} {rec['demanda']:>12.2f}")
    if len(records) > 10:
        print(f"  ... ({len(records) - 10} registros más)")
    print(f"{'='*55}\n")

    ejecutar_pipeline(records)

    # --- DEBUG: visualizar datos históricos COES ---
    for intento in range(1, 3):
        try:
            df = obtener_potencia_historica_coes()
            print("\n===== DATOS HISTORICOS =====")
            print(df.head(10))
            print("\n===== ULTIMAS FILAS =====")
            print(df.tail(10))
            print("\nTOTAL FILAS:", len(df))
            break
        except Exception as e:
            if intento < 2:
                print(f"\n⚠️  Intento {intento} fallido, reintentando...")
            else:
                print("\n❌ ERROR AL CONSULTAR COES:")
                print(str(e))


if __name__ == "__main__":
    main()
