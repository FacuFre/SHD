import os
import time
import gc
import requests
import pandas as pd
from datetime import datetime, date, timezone
from pyhomebroker import HomeBroker

# ────────────────────────────────  ENV  ────────────────────────────────
BROKER_ID = os.getenv("BROKER_ID")
DNI       = os.getenv("DNI")
USER      = os.getenv("USER")
PASSWORD  = os.getenv("PASSWORD")

SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_API_KEY")

# ── BCRA ENV opcional ──────────────────────────────────────────────────
BCRA_BASE_URL = os.getenv("BCRA_BASE_URL", "https://api.bcra.gob.ar")
BCRA_TOKEN    = os.getenv("BCRA_TOKEN")        # la API pública no lo pide; por si acaso

# ────────────────────────────────  CHECKS  ─────────────────────────────
if not BROKER_ID or not DNI or not USER or not PASSWORD:
    raise ValueError("Faltan credenciales PyHomeBroker (BROKER_ID, DNI, USER, PASSWORD).")
if not SUPABASE_URL or not SUPABASE_API_KEY:
    raise ValueError("Faltan credenciales Supabase (SUPABASE_URL, SUPABASE_API_KEY).")

BROKER_ID = int(BROKER_ID)

# ────────────────────────────────  PYHOMEBROKER  ───────────────────────
hb = HomeBroker(BROKER_ID)
hb.auth.login(dni=DNI, user=USER, password=PASSWORD, raise_exception=True)
print("Conectado a PyHomeBroker (usando hb.history).")

# Lista de tickers (la tuya original, copy-paste sin cambios)
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
    "BPJ5D", "BPA7D", "BPB7D", "BPC7D", "BPD7D", "BPOD7", "BPY26", "BPY26D", "BPO27", "BPJ25", "BPJ25D",
    # BONOS_CER
    "TZXM5", "TC24", "TZXJ5", "TZX05", "TZXKD5", "TZXM6", "TX06", "TX26",
    "TZXM7", "TX27", "TXD7", "TX28",
    # CAUCIONES
    "CAUCI1", "CAUCI2",
    # FUTUROS_DOLAR
    "DOFUTABR24", "DOFUTJUN24"
]

# ────────────────────────────────  FUNCIONES ORIGINALES  ───────────────
def get_intraday_history(ticker: str) -> pd.DataFrame:
    """Retorna la última fila del histórico intradiario de 'ticker'."""
    df_t = hb.history.get_intraday_history(ticker)
    return df_t[-1:] if not df_t.empty else df_t

def get_intraday_history_for_tickers(tickers: list[str]) -> pd.DataFrame:
    frames = []
    for t in tickers:
        try:
            df_t = get_intraday_history(t)
            if not df_t.empty:
                df_t["symbol"] = t
                frames.append(df_t)
        except Exception as e:
            print(f"Error intradiario {t}: {e}")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def guardar_en_supabase(tabla: str, df: pd.DataFrame):
    """INSERT puro en Supabase."""
    if df.empty:
        return
    rows = df.to_dict("records")
    for r in rows:
        for k, v in r.items():
            if isinstance(v, pd.Timestamp):
                r[k] = v.isoformat()
        r["updated_at"] = datetime.now(timezone.utc).isoformat()

    headers = {
        "apikey": SUPABASE_API_KEY,
        "Authorization": f"Bearer {SUPABASE_API_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/{tabla}"
    resp = requests.post(url, headers=headers, json=rows)
    if resp.status_code not in (200, 201):
        print(f"[{tabla}] ERROR {resp.status_code}: {resp.text}")
    else:
        print(f"[{tabla}] Insertadas {len(rows)} filas en '{tabla}'.")

# ── BCRA SECTION ───────────────────────────────────────────────────────
BCRA_HEADERS = {"Content-Type": "application/json"}
if BCRA_TOKEN:
    BCRA_HEADERS["Authorization"] = f"BEARER {BCRA_TOKEN}"

def bcra_get(path: str, params: dict | None = None):
    url = f"{BCRA_BASE_URL}{path}"
    r = requests.get(url, headers=BCRA_HEADERS, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# Catálogo para encontrar IDs de CER y TAMAR (solo se ejecuta al iniciar)
_catalog = pd.DataFrame(bcra_get("/estadisticas/v3.0/Monetarias"))
CER_ID   = _catalog[_catalog["nombre"].str.contains("CER",   case=False)].iloc[0]["id_variable"]
TAMAR_ID = _catalog[_catalog["nombre"].str.contains("TAMAR", case=False)].iloc[0]["id_variable"]

def bcra_series(id_var: int, desde: str = "2003-01-01") -> pd.DataFrame:
    data = bcra_get(f"/estadisticas/v3.0/Monetarias/{id_var}",
                    params={"limit": 3000, "desde": desde})
    df = pd.DataFrame(data).rename(columns={"d": "date", "v": "value"})
    df["date"] = pd.to_datetime(df["date"])
    return df

def actualizar_bcra(desde="2003-01-01"):
    cer  = bcra_series(CER_ID,   desde)
    tamr = bcra_series(TAMAR_ID, desde)

    cer["serie"]  = "CER"
    tamr["serie"] = "TAMAR"

    guardar_en_supabase("bcra_rates", cer)
    guardar_en_supabase("bcra_rates", tamr)
# ── END BCRA SECTION ───────────────────────────────────────────────────

# ────────────────────────────────  MAIN LOOP  ──────────────────────────
def main_loop():
    last_bcra_update = date.min  # asegura correr la 1ª vez
    while True:
        try:
            # 1) PyHomeBroker intradiario
            df_all = get_intraday_history_for_tickers(TICKERS)
            if not df_all.empty:
                guardar_en_supabase("pyhomebroker_intraday", df_all)
            print(f"Obtenidas {len(df_all)} filas intradiarias. Esperando 5 min…")

            # 2) BCRA una vez al día ~20:00
            now = datetime.now()
            if now.hour == 20 and now.date() != last_bcra_update:
                try:
                    print("Actualizando CER y TAMAR desde BCRA…")
                    actualizar_bcra()
                    last_bcra_update = now.date()
                except Exception as e:
                    print(f"Error BCRA: {e}")

            time.sleep(300)  # 5 minutos
        except Exception as e:
            print(f"Error en el ciclo principal: {e}")
            time.sleep(60)
        gc.collect()

if __name__ == "__main__":
    main_loop()
