# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Flujo continuo (modo live: ingesta incremental + evolución de esquema en vivo)
# MAGIC
# MAGIC Notebook **opcional** para ver Auto Loader actuando sobre datos que llegan **de verdad**, no
# MAGIC estáticos. En vez de dejar caer un único lote (como el `02`), este notebook escribe pequeñas
# MAGIC tandas de pedidos nuevos al landing **cada pocos segundos**, durante una ventana de tiempo
# MAGIC acotada. Y, a partir de cierta tanda, los pedidos empiezan a traer una **columna nueva**
# MAGIC (`canal_captacion`) que no existía antes — para ver la **evolución de esquema** ocurriendo en vivo.
# MAGIC
# MAGIC ### ▶️ Cómo usarlo (modo live)
# MAGIC 1. Corre primero el `01` (y, si quieres, el `02`) para tener la carga base.
# MAGIC 2. Pon **tu pipeline en modo continuo** (Settings → *Continuous*) y arráncalo, o re-ejecútalo
# MAGIC    cada tanto en modo *triggered*.
# MAGIC 3. Corre este notebook (a mano o como **job**). Verás en el event log del pipeline cómo Auto
# MAGIC    Loader va ingiriendo **solo** las tandas nuevas y cómo, al llegar la columna nueva, el bronze
# MAGIC    la **absorbe** (con `schemaEvolutionMode = addNewColumns` el flujo puede reiniciarse solo una
# MAGIC    vez y retomar — es lo esperado).
# MAGIC
# MAGIC > ⚠️ Es un modo de **demostración**. Corre por `duracion_minutos` y se detiene solo. Si pones el
# MAGIC > pipeline en continuo, **acuérdate de apagarlo** al terminar para no dejar compute encendido.
# MAGIC > No borra el landing: solo **agrega** archivos `pedidos_stream_*.json`.

# COMMAND ----------

import json
import os
import random
import time
from datetime import date, datetime, timedelta

_DEFAULTS = {
    "catalogo": "dacamargovws_catalog",
    "volumen": "landing",
    "n_clientes": "2500",
    "filas_por_tanda": "50",
    "intervalo_segundos": "20",
    "duracion_minutos": "10",
    "tanda_nueva_columna": "4",
    "pct_error": "0.15",
    "semilla": "20260101",
}
for _nombre, _valor in _DEFAULTS.items():
    try:
        dbutils.widgets.text(_nombre, _valor)
    except Exception:
        pass


def parametro(nombre: str, variable_entorno: str) -> str:
    valor_env = os.environ.get(variable_entorno)
    if valor_env not in (None, ""):
        return valor_env
    try:
        valor_widget = dbutils.widgets.get(nombre)
        if valor_widget not in (None, ""):
            return valor_widget
    except Exception:
        pass
    return _DEFAULTS[nombre]


def slug_identificador(texto: str) -> str:
    limpio = "".join(c if c.isalnum() else "_" for c in texto.lower())
    while "__" in limpio:
        limpio = limpio.replace("__", "_")
    limpio = limpio.strip("_")
    if limpio and limpio[0].isdigit():
        limpio = f"u_{limpio}"
    return limpio[:60] or "usuario"


CATALOGO = parametro("catalogo", "WORKSHOP_CATALOG")
VOLUMEN = parametro("volumen", "WORKSHOP_VOLUME")
ESQUEMA_BASE = "lakeflow_workshop"
N_CLIENTES = int(parametro("n_clientes", "WORKSHOP_N_CUSTOMERS"))
FILAS_POR_TANDA = int(parametro("filas_por_tanda", "WORKSHOP_STREAM_ROWS"))
INTERVALO_SEGUNDOS = int(parametro("intervalo_segundos", "WORKSHOP_STREAM_INTERVAL"))
DURACION_MINUTOS = float(parametro("duracion_minutos", "WORKSHOP_STREAM_MINUTES"))
TANDA_NUEVA_COLUMNA = int(parametro("tanda_nueva_columna", "WORKSHOP_STREAM_NEWCOL_BATCH"))
PCT_ERROR = float(parametro("pct_error", "WORKSHOP_PCT_ERROR"))
SEMILLA = int(parametro("semilla", "WORKSHOP_SEED"))

USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO_USUARIO = slug_identificador(USUARIO.split("@")[0])
ESQUEMA = f"{ESQUEMA_BASE}_{SUFIJO_USUARIO}"
RUTA_LANDING = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"

print("Flujo continuo (modo live) — configuración:")
print(f"  usuario             = {USUARIO}")
print(f"  destino             = {CATALOGO}.{ESQUEMA}  (volumen: {VOLUMEN})")
print(f"  ruta landing        = {RUTA_LANDING}")
print(f"  filas por tanda     = {FILAS_POR_TANDA}")
print(f"  intervalo (seg)     = {INTERVALO_SEGUNDOS}")
print(f"  duración (min)      = {DURACION_MINUTOS}")
print(f"  columna nueva desde = tanda {TANDA_NUEVA_COLUMNA}  (canal_captacion)")
print(f"  (el landing NO se borra; solo agregamos archivos pedidos_stream_*)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogos de referencia (IDs válidos, como en el notebook 01)

# COMMAND ----------

CANALES = ["Web", "App", "Tienda", "Call Center", "Marketplace", "web", "APP", "tienda"]
METODOS_PAGO = ["Tarjeta", "Efectivo", "Transferencia", "Billetera digital", "tarjeta", "EFECTIVO"]
MONEDAS = ["MXN", "COP", "CLP", "PEN", "ARS", "USD"]
ESTADOS_PEDIDO = ["Creado", "Pagado", "Enviado", "Entregado", "Cancelado",
                  "PAGADO", "enviado", "entregado", "cancelado"]
# Valores de la COLUMNA NUEVA que aparece a mitad del flujo (evolución de esquema):
CANALES_CAPTACION = ["SEO", "Ads", "Referido", "Email", "Orgánico", "Afiliados"]
N_TIENDAS = 25

ids_cliente_validos = [f"CLI-{i:06d}" for i in range(N_CLIENTES)]
ids_tienda_validos = [f"TDA-{i:03d}" for i in range(N_TIENDAS)]


def formatear_fecha(valor: date, rnd: random.Random) -> str:
    estilo = rnd.random()
    if estilo < 0.6:
        return valor.strftime("%Y-%m-%d")
    if estilo < 0.85:
        return valor.strftime("%d/%m/%Y")
    return valor.strftime("%m-%d-%Y")


def formatear_timestamp(valor: datetime, rnd: random.Random) -> str:
    return valor.strftime("%Y-%m-%d %H:%M:%S") if rnd.random() < 0.8 else valor.strftime("%d/%m/%Y %H:%M")


def generar_pedido(rnd: random.Random, id_pedido: str, con_columna_nueva: bool) -> dict:
    ahora = datetime.now()
    fecha_pedido = ahora.date() - timedelta(days=rnd.randint(0, 3))
    momento = datetime.combine(fecha_pedido, datetime.min.time()) + timedelta(
        hours=rnd.randint(8, 22), minutes=rnd.randint(0, 59))
    monto = round(rnd.uniform(50, 5000), 2)
    pedido = {
        "id_pedido": id_pedido,
        "id_cliente": rnd.choice(ids_cliente_validos),
        "id_tienda": rnd.choice(ids_tienda_validos),
        "fecha_pedido": formatear_fecha(fecha_pedido, rnd),
        "timestamp_pedido": formatear_timestamp(momento, rnd),
        "canal": rnd.choice(CANALES),
        "metodo_pago": rnd.choice(METODOS_PAGO),
        "moneda": rnd.choice(MONEDAS),
        "estado": rnd.choice(ESTADOS_PEDIDO),
        "monto_total": f"{monto:.2f}",
        "fecha_actualizacion": formatear_timestamp(ahora, rnd),
    }
    if con_columna_nueva:
        # >>> Columna NUEVA, inexistente en las tandas anteriores: evolución de esquema <<<
        pedido["canal_captacion"] = rnd.choice(CANALES_CAPTACION)
    # Suciedad intencional (mismas reglas de negocio que el lote 2):
    if rnd.random() < PCT_ERROR:
        error = rnd.choices(
            ["cliente_huerfano", "tienda_huerfana", "monto_negativo", "monto_no_numerico",
             "estado_desconocido", "moneda_desconocida"],
            weights=[16, 12, 14, 10, 24, 24],
        )[0]
        if error == "cliente_huerfano":
            pedido["id_cliente"] = f"CLI-9{rnd.randint(100000, 999999)}"
        elif error == "tienda_huerfana":
            pedido["id_tienda"] = f"TDA-9{rnd.randint(10, 99)}"
        elif error == "monto_negativo":
            pedido["monto_total"] = f"-{monto:.2f}"
        elif error == "monto_no_numerico":
            pedido["monto_total"] = "N/D"
        elif error == "estado_desconocido":
            pedido["estado"] = rnd.choice(["??", "EN REVISION", "X"])
        else:
            pedido["moneda"] = rnd.choice(["EUR", "??", "pesos"])
    return pedido


# COMMAND ----------

# MAGIC %md
# MAGIC ## Bucle: una tanda de pedidos nuevos cada `intervalo_segundos`

# COMMAND ----------

directorio = f"{RUTA_LANDING}/pedidos"
dbutils.fs.mkdirs(directorio)

inicio = time.time()
fin = inicio + DURACION_MINUTOS * 60
tanda = 0
total_pedidos = 0

print(f"Generando pedidos en vivo cada {INTERVALO_SEGUNDOS}s durante {DURACION_MINUTOS} min...\n")
while time.time() < fin:
    tanda += 1
    con_columna_nueva = tanda >= TANDA_NUEVA_COLUMNA
    registros = []
    for k in range(FILAS_POR_TANDA):
        rnd = random.Random(f"{SEMILLA}-stream-{tanda}-{k}-{time.time()}")
        id_pedido = f"PED-S{tanda:04d}-{k:05d}"
        registros.append(generar_pedido(rnd, id_pedido, con_columna_nueva))

    marca_tiempo = datetime.now().strftime("%Y%m%d_%H%M%S")
    archivo = f"{directorio}/pedidos_stream_{tanda:04d}_{marca_tiempo}.json"
    with open(archivo, "w", encoding="utf-8") as fh:
        for registro in registros:
            fh.write(json.dumps({k: ("" if v is None else str(v)) for k, v in registro.items()},
                                ensure_ascii=False) + "\n")
    total_pedidos += len(registros)
    marca = "  ← ¡columna nueva canal_captacion!" if con_columna_nueva else ""
    print(f"  tanda {tanda:>3}: +{len(registros)} pedidos → {archivo.split('/')[-1]}{marca}")

    if time.time() < fin:
        time.sleep(INTERVALO_SEGUNDOS)

print(f"\nListo. Tandas escritas: {tanda}  ·  pedidos nuevos: {total_pedidos}")
print(f"La columna `canal_captacion` empezó en la tanda {TANDA_NUEVA_COLUMNA}.")
print("Revisa el bronze de pedidos: la columna nueva aparece nula en las filas antiguas y con valor en las nuevas.")

resumen = {
    "usuario": USUARIO,
    "destino": f"{CATALOGO}.{ESQUEMA}",
    "ruta_landing": RUTA_LANDING,
    "tandas": tanda,
    "pedidos_nuevos": total_pedidos,
    "columna_nueva": "canal_captacion",
    "tanda_nueva_columna": TANDA_NUEVA_COLUMNA,
    "generado_en": datetime.now().isoformat(),
}
print()
print(json.dumps(resumen, ensure_ascii=False, indent=2))
dbutils.notebook.exit(json.dumps(resumen, ensure_ascii=False))
