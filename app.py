import os
import time
import requests
import gc
from datetime import datetime, timezone
import pandas as pd
from pyhomebroker import HomeBroker
from collections import defaultdict
import pytz

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

if not SUPABASE_URL or not SUPABASE_API_KEY:
    raise ValueError("❌ Faltan credenciales de Supabase")

BROKER_ID = int(os.getenv("BROKER_ID"))
DNI = os.getenv("DNI")
USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")

# Define acá las categorías exactas, si querés
# TASA_FIJA = {...}
# BONOS_SOBERANOS = {...}
# etc. 
# O podés ir todo a una sola tabla, según tu gusto.

contador_categorias = defaultdict(int)

def guardar_en_supabase(tabla: str, df: pd.DataFrame):
    """Inserta/upserta los datos en Supabase (tabla), con on_conflict=symbol."""
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

        print(f"📤 Insertando en Supabase -> {tabla} | record:", record)

        resp = requests.post(url, headers=headers, json=record)
        if resp.status_code not in (200, 201):
            print(f"❌ Error Supabase [{tabla}] → {resp.status_code}: {resp.text}")
        else:
            contador_categorias[tabla] += 1

def on_securities(online, quotes):
    """Callback que se llama cuando PyHomeBroker recibe data de los instrumentos suscriptos."""
    print(f"📥 [on_securities] Cantidad de instrumentos: {len(quotes)}")
    if len(quotes) == 0:
        return

    df = quotes.reset_index()
    # Ejemplo: Si lo querés todo en 'bonos_soberanos', lo hacés directo:
    guardar_en_supabase("bonos_soberanos", df)

    # O si querés clasificar, hacés un filtrado por símbolo, etc.
    # e insertás en tablas separadas.


def ciclo_breve_suscripcion():
    """Conecta, suscribe, espera unos segundos y se desconecta."""
    global contador_categorias
    contador_categorias = defaultdict(int)

    hb = HomeBroker(BROKER_ID, on_securities=on_securities)
    hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)
    hb.online.connect()

    print("📡 Suscribiendo: government_bonds - 24hs")
    hb.online.subscribe_securities('government_bonds', '24hs')

    print("⌛ Esperamos 10 segundos para recibir data...")
    time.sleep(10)

    print("🔌 Desconectando de PyHomeBroker.")
    hb.online.disconnect()

    # Log final
    if contador_categorias:
        print("📊 Resumen de inserciones:")
        for tabla, cantidad in contador_categorias.items():
            print(f"   - {tabla}: {cantidad} registros insertados/actualizados")
    else:
        print("📊 No se insertó nada en este ciclo.")

    gc.collect()

def dentro_de_horario():
    """Ejemplo: solo funciona entre 10 y 19 (hora Argentina)."""
    ahora = datetime.now(pytz.timezone("America/Argentina/Buenos_Aires"))
    return 10 <= ahora.hour < 19

if __name__ == "__main__":
    print("🚀 Iniciando script con suscripción fugaz cada 5 minutos.")
    while True:
        if dentro_de_horario():
            try:
                ciclo_breve_suscripcion()
            except Exception as e:
                print(f"❌ Error en el ciclo: {e}")
                time.sleep(60)  # reintentar en 1 min
        else:
            print("🌙 Fuera de horario (10 a 19). Esperamos 60s.")
            time.sleep(60)

        # Esperar 5 minutos hasta el próximo “GET”
        print("⌛ Esperando 5 minutos para la próxima suscripción.")
        time.sleep(300)

