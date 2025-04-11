import os
import time
import requests
import gc
import pandas as pd
from datetime import datetime, timezone
from pyhomebroker import HomeBroker

# from dotenv import load_dotenv
# load_dotenv()

BROKER_ID = os.getenv("BROKER_ID")
DNI = os.getenv("DNI")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

if not BROKER_ID or not DNI or not USER or not PASSWORD:
    raise ValueError("❌ Faltan credenciales PyHomeBroker (BROKER_ID, DNI, USER, PASSWORD).")
if not SUPABASE_URL or not SUPABASE_API_KEY:
    raise ValueError("❌ Faltan credenciales Supabase (SUPABASE_URL, SUPABASE_API_KEY).")

BROKER_ID = int(BROKER_ID)

# Inicializar PyHomeBroker
hb = HomeBroker(BROKER_ID)
hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)
print("✅ Conectado a PyHomeBroker (usando hb.history).")

# Lista de tickers (tasa_fija, bonos_soberanos, etc.)
TICKERS = [
    # TASA_FIJA
    "S31M5", "S16A5", "BBA2S", "S28A5", "S16Y5", "BBY5", "S30Y5", "S18J5", "BJ25",
    "S30J5", "S31L5", "S29G5", "S29S5", "S30S5", "T17O5", "S30L5", "S10N5",
    "S28N5", "T30E6", "T3F6", "T30J6", "T15E7",
    # BONOS_SOBERANOS
    "AL29", "AL29D", "AL30", "AL30D", "AL35", "AL35D", "AL41D", "AL41",
    "AL14D", "GD29", "GD29D", "GD30", "GD30D", "GD35", "GD35D", "GD38",
    "GD38D", "GD41", "GD41D", "GD46", "GD46D",
    # DOLAR_LINKED
    "TV25", "TZV25", "TZVD5", "D16F6", "TZV26",
    # BOPREALES
    "BPJ5D", "BPA7D", "BPB7D", "BPC7D", "BPD7D",
    # BONOS_CER
    "TZXM5", "TC24", "TZXJ5", "TZX05", "TZXKD5", "TZXM6", "TX06", "TX26",
    "TZXM7", "TX27", "TXD7", "TX28",
    # CAUCIONES
    "CAUCI1", "CAUCI2",
    # FUTUROS_DOLAR
    "DOFUTABR24", "DOFUTJUN24"
]

def get_intraday_history(ticker: str) -> pd.DataFrame:
    """
    Retorna sólo la última fila df_t[-1:] de hb.history.get_intraday_history(ticker).
    """
    df_t = hb.history.get_intraday_history(ticker)
    if df_t.empty:
        return df_t  # df vacío
    # Retornar solo la última fila
    return df_t[-1:]

def get_intraday_history_for_tickers(tickers: list[str]) -> pd.DataFrame:
    frames = []
    for t in tickers:
        try:
            df_t = get_intraday_history(t)
            if not df_t.empty:
                df_t["symbol"] = t
                # Debug: print para ver data
                print(f"🔎 Data intradiaria última fila para {t}:")
                print(df_t)
                frames.append(df_t)
            else:
                print(f"⚠️ Ticker {t}: DataFrame vacío.")
        except Exception as e:
            print(f"❌ Error al obtener histórico de {t}: {e}")
    if frames:
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.DataFrame()

def guardar_en_supabase(tabla: str, df: pd.DataFrame):
    """
    Upsert en Supabase con on_conflict=symbol.
    """
    if df.empty:
        print("⚠️ DF vacío, nada que guardar en Supabase.")
        return

    rows = df.to_dict(orient="records")
    for row in rows:
        row["updated_at"] = datetime.now(timezone.utc).isoformat()

    supabase_headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?on_conflict=symbol"
    resp = requests.post(url, headers=supabase_headers, json=rows)
    if resp.status_code not in (200, 201):
        print(f"❌ Error Supabase ({tabla}): {resp.status_code} {resp.text}")
    else:
        print(f"✅ Insertadas/Upserteadas {len(rows)} filas en '{tabla}'.")

def main_loop():
    """
    Cada 5 minutos:
    - Toma df con la última fila intradia de cada ticker
    - Guarda en tabla 'pyhomebroker_intraday'
    """
    tabla_destino = "pyhomebroker_intraday"
    while True:
        try:
            print(f"🔄 Obteniendo última fila intradiaria para {len(TICKERS)} tickers.")
            df_all = get_intraday_history_for_tickers(TICKERS)
            print(f"   Obtenidas {len(df_all)} filas totales.")

            if not df_all.empty:
                guardar_en_supabase(tabla_destino, df_all)

            print("⌛ Esperando 5 min antes de la próxima consulta...\n")
            time.sleep(300)
        except Exception as e:
            print(f"❌ Error en el ciclo principal: {e}")
            time.sleep(60)
        gc.collect()

if __name__ == "__main__":
    main_loop()


