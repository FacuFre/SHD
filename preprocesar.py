import os
import pandas as pd
from datetime import datetime, timedelta
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_API_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def cargar_intraday():
    res = supabase.table("pyhomebroker_intraday").select("*").limit(5000).execute()
    df = pd.DataFrame(res.data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    return df

def inferir_vencimiento(symbol):
    for sufijo in ["J5", "M5", "L5", "G5", "S5", "Y5", "N5", "O5", "E6", "F6", "J6", "E7"]:
        if sufijo in symbol:
            try:
                i = symbol.index(sufijo)
                s = symbol[i-2:i+2]
                return pd.to_datetime("20" + s[-1] + "-01-01") + pd.offsets.MonthEnd(month=int("ABCDEFGHIJKLMNOPS".index(s[0]) + 1))
            except:
                return None
    return None

def calcular_tir(precio, dias, nominal=1000):
    try:
        return (nominal / precio)**(365/dias) - 1
    except:
        return None

def preprocesar_y_subir():
    df = cargar_intraday()
    ahora = datetime.now()

    lecaps = df[df["symbol"].str.startswith("S")].copy()
    lecaps["vencimiento"] = lecaps["symbol"].apply(inferir_vencimiento)
    lecaps = lecaps.dropna(subset=["vencimiento"])
    lecaps["dias"] = (lecaps["vencimiento"] - ahora).dt.days
    lecaps = lecaps[lecaps["dias"] > 0]
    lecaps["tir"] = lecaps.apply(lambda r: calcular_tir(r["last_price"], r["dias"]), axis=1)
    lecaps = lecaps[["symbol", "vencimiento", "tir"]].dropna()
    guardar("lecaps", lecaps)

    cauciones = df[df["symbol"].str.startswith("CAUCI")].copy()
    cauciones["tasa"] = cauciones["last_price"] / 100  # suponemos tasa expresada así
    guardar("cauciones", cauciones[["symbol", "tasa"]])

    futuros = df[df["symbol"].str.startswith("DOFUT")].copy()
    futuros["vencimiento"] = futuros["symbol"].apply(inferir_vencimiento)
    guardar("futuros_dolar", futuros[["symbol", "precio", "vencimiento"]].rename(columns={"last_price": "precio"}))

    bonos = df[df["symbol"].str.match(r"(AL|GD)[0-9]{2}D?$")].copy()
    bonos["tipo"] = bonos["symbol"].apply(lambda x: "synthetic" if x.endswith("D") else "directo")
    bonos["tir"] = 0.12  # ejemplo placeholder
    bonos["precio_senebi"] = bonos["last_price"]
    bonos["precio_pantalla"] = bonos["last_price"] * 0.985  # simulamos brecha
    guardar("bonos_soberanos", bonos[["symbol", "tir", "tipo", "precio_senebi", "precio_pantalla"]])

def guardar(tabla, df):
    supabase.table(tabla).delete().neq("symbol", "").execute()
    for row in df.to_dict(orient="records"):
        for k, v in row.items():
            if isinstance(v, pd.Timestamp):
                row[k] = v.isoformat()
        supabase.table(tabla).insert(row).execute()
    print(f"✅ Guardada tabla '{tabla}' con {len(df)} filas.")

if __name__ == "__main__":
    preprocesar_y_subir()