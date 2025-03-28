import os
import time
import requests
import gc
from datetime import datetime, timezone
import pandas as pd
from pyhomebroker import HomeBroker

# Configuración de Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    raise ValueError("❌ Faltan credenciales de Supabase")

# Configuración de PyHomeBroker
BROKER_ID = int(os.getenv("BROKER_ID"))
DNI = os.getenv("DNI")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# Listas EXACTAS de tus símbolos por categoría
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
DOLAR_LINKED = {"TV25", "TZV25", "TZVD5", "D16F6", "TZV26"}
BOPREALES = {"BPJ5D", "BPA7D", "BPB7D", "BPC7D", "BPD7D"}
BONOS_CER = {
    "TZXM5", "TC24", "TZXJ5", "TZX05", "TZXKD5", "TZXM6", "TX06", "TX26",
    "TZXM7", "TX27", "TXD7", "TX28"
}
CAUCIONES = {"CAUCI1", "CAUCI2"}
FUTUROS_DOLAR = {"DOFUTABR24", "DOFUTJUN24"}

def guardar_en_supabase(tabla: str, df: pd.DataFrame):
    """
    Upsert de df en la tabla (con on_conflict=symbol).
    """
    url = f"{SUPABASE_URL}/rest/v1/{tabla}?on_conflict=symbol"
    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    data = df.to_dict(orient="records")
    for record in data:
        # Timestamp de actualización
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        # Asignar un symbol si viene vacío
        if not record.get("symbol"):
            record["symbol"] = "SIN_SYMBOL"

        print(f"📤 Insertando en Supabase -> Tabla: {tabla}\n", record)

        resp = requests.post(url, headers=headers, json=record)
        if resp.status_code not in (200, 201):
            print(f"❌ Error {resp.status_code} {resp.text}")

def main_loop():
    """
    Polling cada 5 minutos.
    Llama a get_quotes para cada categoría (tasa_fija, bonos_soberanos, etc.)
    y upserta en la tabla correspondiente.
    """
    hb = HomeBroker(BROKER_ID)
    hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)

    print("✅ Conectado a PyHomeBroker. Empezamos el polling de 5 minutos...")

    while True:
        try:
            # 1) Tasa fija
            if TASA_FIJA:
                print("\n🔄 Consultando Tasa Fija...")
                df_tasa_fija = hb.get_quotes(list(TASA_FIJA), settlement="24hs")
                print(f"   Obtenidas {len(df_tasa_fija)} filas de Tasa Fija.")
                if not df_tasa_fija.empty:
                    guardar_en_supabase("tasa_fija", df_tasa_fija)

            # 2) Bonos Soberanos
            if BONOS_SOBERANOS:
                print("\n🔄 Consultando Bonos Soberanos...")
                df_bonos_sob = hb.get_quotes(list(BONOS_SOBERANOS), settlement="24hs")
                print(f"   Obtenidas {len(df_bonos_sob)} filas de Bonos Soberanos.")
                if not df_bonos_sob.empty:
                    guardar_en_supabase("bonos_soberanos", df_bonos_sob)

            # 3) Dólar Linked
            if DOLAR_LINKED:
                print("\n🔄 Consultando Dólar Linked...")
                df_dolar_linked = hb.get_quotes(list(DOLAR_LINKED), settlement="24hs")
                print(f"   Obtenidas {len(df_dolar_linked)} filas de Dólar Linked.")
                if not df_dolar_linked.empty:
                    guardar_en_supabase("dolar_linked", df_dolar_linked)

            # 4) Bopreales
            if BOPREALES:
                print("\n🔄 Consultando Bopreales...")
                df_bopreales = hb.get_quotes(list(BOPREALES), settlement="24hs")
                print(f"   Obtenidas {len(df_bopreales)} filas de Bopreales.")
                if not df_bopreales.empty:
                    guardar_en_supabase("bopreales", df_bopreales)

            # 5) Bonos CER
            if BONOS_CER:
                print("\n🔄 Consultando Bonos CER...")
                df_bonos_cer = hb.get_quotes(list(BONOS_CER), settlement="24hs")
                print(f"   Obtenidas {len(df_bonos_cer)} filas de Bonos CER.")
                if not df_bonos_cer.empty:
                    guardar_en_supabase("bonos_cer", df_bonos_cer)

            # 6) Cauciones
            if CAUCIONES:
                print("\n🔄 Consultando Cauciones...")
                df_cauciones = hb.get_quotes(list(CAUCIONES), settlement="24hs")
                print(f"   Obtenidas {len(df_cauciones)} filas de Cauciones.")
                if not df_cauciones.empty:
                    guardar_en_supabase("cauciones", df_cauciones)

            # 7) Futuros de dólar
            if FUTUROS_DOLAR:
                print("\n🔄 Consultando Futuros Dólar...")
                df_futuros_dolar = hb.get_quotes(list(FUTUROS_DOLAR), settlement="24hs")
                print(f"   Obtenidas {len(df_futuros_dolar)} filas de Futuros Dólar.")
                if not df_futuros_dolar.empty:
                    guardar_en_supabase("futuros_dolar", df_futuros_dolar)

            # Esperamos 5 minutos antes de la próxima consulta
            print("\n⌛ Esperando 5 minutos para la próxima actualización...")
            time.sleep(300)

        except Exception as e:
            print(f"❌ Error en la consulta: {e}")
            time.sleep(60)  # intentar de nuevo en 1 min

        gc.collect()

if __name__ == "__main__":
    main_loop()
