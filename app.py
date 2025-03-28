import os
import time
import requests
import gc
from datetime import datetime, timezone
import pandas as pd
from pyhomebroker import HomeBroker

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# Variables de PyHomeBroker
BROKER_ID = int(os.getenv("BROKER_ID"))
DNI = os.getenv("DNI")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# Aquí definís tus listas de símbolos exactas:
TASA_FIJA = {
    "S31M5", "S16A5", "BBA2S", "S28A5", "S16Y5", "BBY5", "S30Y5", "S18J5", "BJ25",
    "S30J5", "S31L5", "S29G5", "S29S5", "S30S5", "T17O5", "S30L5", "S10N5",
    "S28N5", "T30E6", "T3F6", "T30J6", "T15E7"
}
BONOS_SOBERANOS = {
    "AL29", "AL29D", "AL30", "AL30D", "AL35", "AL35D", "AL41D", "AL41",
    "AL14D", "GD29", "GD29D", "GD30", "GD30D", "GD35", "GD35D", "GD38",
    "GD38D", "GD41", "GD41D", "GD46", "GD46D"
}
# ... etc. para el resto de categorías

def guardar_en_supabase(tabla: str, df: pd.DataFrame):
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?on_conflict=symbol"
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    data = df.to_dict(orient="records")

    for record in data:
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        if not record.get("symbol"):
            record["symbol"] = "SIN_SYMBOL"

        print(f"📤 Insertando en Supabase -> {tabla}:", record)
        resp = requests.post(url, headers=headers, json=record)
        if resp.status_code not in (200, 201):
            print(f"❌ Error {resp.status_code}: {resp.text}")

def main_loop():
    hb = HomeBroker(BROKER_ID)
    hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)

    print("✅ Conectado a PyHomeBroker. Polling cada 5 minutos...")

    while True:
        try:
            print("🔄 get_bonds(): consultando todos los bonos (versión sin get_quotes).")
            df_bonds = hb.get_bonds()   # <-- suponiendo que tu versión tenga este método

            # df_bonds ahora tendría filas con 'symbol', 'last', 'bid', 'ask', etc.
            # Filtramos cada categoría que nos interese

            # 1) Tasa Fija
            df_tasa_fija = df_bonds[df_bonds["symbol"].isin(TASA_FIJA)]
            if not df_tasa_fija.empty:
                guardar_en_supabase("tasa_fija", df_tasa_fija)

            # 2) Bonos Soberanos
            df_b_sob = df_bonds[df_bonds["symbol"].isin(BONOS_SOBERANOS)]
            if not df_b_sob.empty:
                guardar_en_supabase("bonos_soberanos", df_b_sob)

            # ... Y repetís para el resto: DOLAR_LINKED, BOPREALES, etc.

            print("⌛ Esperando 5 minutos antes de la próxima consulta...")
            time.sleep(300)

        except Exception as e:
            print(f"❌ Error en la consulta: {e}")
            time.sleep(60)  # reintentar en 1 min

        gc.collect()

if __name__ == "__main__":
    main_loop()
