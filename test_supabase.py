from supabase_client import supabase

registro = {
    "fecha_hora": "2025-01-01 00:00:00",
    "demanda": 7500.0,
    "fecha": "2025-01-01",
    "hora": 0,
    "minuto": 0,
}

try:
    response = supabase.table("demanda_coes").upsert(registro).execute()
    print("Conexion exitosa. Registro insertado:", response.data)
except Exception as e:
    print("Error al conectar con Supabase:", e)
