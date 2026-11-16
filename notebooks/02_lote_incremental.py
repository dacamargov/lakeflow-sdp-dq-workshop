# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Lote incremental (ingesta incremental + evolución de esquema + CDC/SCD2)
# MAGIC
# MAGIC Este es el segundo notebook del workshop. Después de haber corrido el `01_generar_datos_retail`,
# MAGIC ejecútalo para simular que **llegaron datos nuevos** al mismo volumen de *landing*, **sin
# MAGIC borrar** los archivos existentes. Con él trabajas, con Genie code + Lakeflow SDP:
# MAGIC
# MAGIC 1. **Ingesta incremental** — Auto Loader procesa **solo los archivos nuevos** en el próximo
# MAGIC    update del pipeline (no reprocesa todo).
# MAGIC 2. **Evolución de esquema** — el nuevo lote de `pedidos` trae una columna de más
# MAGIC    (`cupon_descuento`) que no existía en el lote original.
# MAGIC 3. **CDC / SCD Tipo 2** — un nuevo archivo de `clientes` trae **versiones actualizadas** de
# MAGIC    clientes existentes (mismo `id_cliente`, `fecha_actualizacion` más reciente), para que lo
# MAGIC    historices con `AUTO CDC`.
# MAGIC
# MAGIC > ⚠️ **No** corras con `limpiar_landing=true`: este notebook **siempre** preserva el landing y
# MAGIC > solo **agrega** archivos nuevos (sufijo `_lote2`).
# MAGIC
# MAGIC ### ▶️ Cómo ejecutar
# MAGIC 1. Corre la primera celda de código de abajo (habilita los widgets).
# MAGIC 2. Confirma que el `catalogo` es el **mismo** que usaste en el notebook 01.
# MAGIC 3. *Run all*.

# COMMAND ----------

import json
import os
import random
from datetime import date, datetime, timedelta

_DEFAULTS = {
    "catalogo": "dacamargovws_catalog",
    "volumen": "landing",
    "n_clientes": "2500",
    "n_pedidos_lote": "3000",
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
N_PEDIDOS_LOTE = int(parametro("n_pedidos_lote", "WORKSHOP_N_ORDERS_BATCH"))
PCT_ERROR = float(parametro("pct_error", "WORKSHOP_PCT_ERROR"))
SEMILLA = int(parametro("semilla", "WORKSHOP_SEED"))

USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO_USUARIO = slug_identificador(USUARIO.split("@")[0])
ESQUEMA = f"{ESQUEMA_BASE}_{SUFIJO_USUARIO}"
RUTA_LANDING = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"
HOY = date.today()
INICIO_VENTANA = HOY - timedelta(days=180)
AHORA = datetime.now()

print("Lote incremental — configuración:")
print(f"  usuario        = {USUARIO}")
print(f"  destino        = {CATALOGO}.{ESQUEMA}  (volumen: {VOLUMEN})")
print(f"  ruta landing   = {RUTA_LANDING}")
print(f"  n_pedidos_lote = {N_PEDIDOS_LOTE}")
print(f"  (el landing NO se borra; solo agregamos archivos *_lote2)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Catálogos de referencia (los mismos del notebook 01, para reproducir IDs válidos)

# COMMAND ----------

PAISES = [
    ("MX", "México", ["CDMX", "Guadalajara", "Monterrey", "Puebla", "Querétaro", "Mérida"]),
    ("CO", "Colombia", ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena"]),
    ("CL", "Chile", ["Santiago", "Valparaíso", "Concepción", "Antofagasta"]),
    ("PE", "Perú", ["Lima", "Arequipa", "Trujillo", "Cusco"]),
    ("AR", "Argentina", ["Buenos Aires", "Córdoba", "Rosario", "Mendoza"]),
]
PAIS_VARIANTES = {
    "MX": ["MX", "México", "MEXICO", "Mexico", "mx"],
    "CO": ["CO", "Colombia", "COLOMBIA", "col"],
    "CL": ["CL", "Chile", "CHILE", "chl"],
    "PE": ["PE", "Perú", "PERU", "Peru", "per"],
    "AR": ["AR", "Argentina", "ARGENTINA", "arg"],
}
SEGMENTOS = ["Nuevo", "Recurrente", "VIP", "Inactivo"]
NIVELES_LEALTAD = ["Bronce", "Plata", "Oro", "Platino"]
CANALES = ["Web", "App", "Tienda", "Call Center", "Marketplace", "web", "APP", "tienda"]
METODOS_PAGO = ["Tarjeta", "Efectivo", "Transferencia", "Billetera digital", "tarjeta", "EFECTIVO"]
MONEDAS = ["MXN", "COP", "CLP", "PEN", "ARS", "USD"]
ESTADOS_PEDIDO = ["Creado", "Pagado", "Enviado", "Entregado", "Cancelado",
                  "PAGADO", "enviado", "entregado", "cancelado"]
CUPONES = ["", "", "", "CUPON5", "CUPON10", "ENVIOGRATIS", "BLACKFRIDAY"]
NOMBRES = ["María", "José", "Juan", "Ana", "Luis", "Carmen", "Jorge", "Sofía", "Pedro", "Lucía",
           "Miguel", "Valentina", "Diego", "Camila", "Andrés", "Daniela", "Carlos", "Paula",
           "Fernando", "Gabriela", "Ricardo", "Isabela", "Javier", "Mariana", "Roberto", "Natalia"]
APELLIDOS = ["García", "Rodríguez", "Martínez", "López", "González", "Pérez", "Sánchez", "Ramírez",
             "Torres", "Flores", "Rivera", "Gómez", "Díaz", "Vargas", "Castro", "Ortiz", "Morales",
             "Reyes", "Cruz", "Herrera", "Jiménez", "Mendoza", "Rojas", "Núñez", "Silva"]
DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "correo.com", "mail.com"]
CATALOGO_PRODUCTOS = [
    ("Abarrotes", "ABA", 10), ("Bebidas", "BEB", 7), ("Cuidado personal", "CPE", 6),
    ("Limpieza", "LIM", 5), ("Snacks", "SNK", 5), ("Hogar", "HOG", 5), ("Electrónica", "ELE", 4),
]
N_TIENDAS = 25

ids_cliente_validos = [f"CLI-{i:06d}" for i in range(N_CLIENTES)]
ids_tienda_validos = [f"TDA-{i:03d}" for i in range(N_TIENDAS)]
skus_validos = [f"{codigo}-{j:03d}" for _, codigo, n in CATALOGO_PRODUCTOS for j in range(n)]


def formatear_fecha(valor: date, rnd: random.Random) -> str:
    estilo = rnd.random()
    if estilo < 0.6:
        return valor.strftime("%Y-%m-%d")
    if estilo < 0.85:
        return valor.strftime("%d/%m/%Y")
    return valor.strftime("%m-%d-%Y")


def formatear_timestamp(valor: datetime, rnd: random.Random) -> str:
    return valor.strftime("%Y-%m-%d %H:%M:%S") if rnd.random() < 0.8 else valor.strftime("%d/%m/%Y %H:%M")


def formatear_telefono(rnd: random.Random) -> str:
    numero = f"{rnd.randint(2, 9)}{rnd.randint(10000000, 99999999)}"
    return f"+{rnd.choice(['52', '57', '56', '51', '54'])} {numero[:4]} {numero[4:]}"


def slug(texto: str) -> str:
    limpio = (texto.lower()
              .replace("á", "a").replace("é", "e").replace("í", "i")
              .replace("ó", "o").replace("ú", "u").replace("ñ", "n"))
    return "".join(c for c in limpio if c.isalnum())


# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Nuevo lote de pedidos (ingesta incremental + nueva columna `cupon_descuento`)

# COMMAND ----------

contador_errores = {}


def registrar_error(entidad, error):
    contador_errores[(entidad, error)] = contador_errores.get((entidad, error), 0) + 1


pedidos_lote2 = []
pedidos_items_lote2 = []

for k in range(N_PEDIDOS_LOTE):
    rnd = random.Random(f"{SEMILLA}-lote2-pedido-{k}")
    id_pedido = f"PED-L2-{k:07d}"
    id_cliente = rnd.choice(ids_cliente_validos)
    id_tienda = rnd.choice(ids_tienda_validos)
    # Pedidos nuevos son recientes (últimos 20 días): refuerza la idea de "datos que llegaron ahora".
    fecha_pedido = HOY - timedelta(days=rnd.randint(0, 20))
    momento = datetime.combine(fecha_pedido, datetime.min.time()) + timedelta(
        hours=rnd.randint(8, 22), minutes=rnd.randint(0, 59))

    n_lineas = rnd.randint(1, 8)
    total = 0.0
    for linea in range(1, n_lineas + 1):
        sku = rnd.choice(skus_validos)
        cantidad = rnd.randint(1, 20)
        precio_unitario = round(rnd.uniform(8, 600), 2)
        descuento = rnd.choice([0, 0, 0, 5, 10, 15, 20])
        total += cantidad * precio_unitario * (1 - descuento / 100.0)
        item = {
            "id_pedido": id_pedido,
            "num_linea": str(linea),
            "sku": sku,
            "cantidad": str(cantidad),
            "precio_unitario": f"{precio_unitario:.2f}",
            "descuento_pct": str(descuento),
        }
        rnd_err = random.Random(f"{SEMILLA}-lote2-item-err-{k}-{linea}")
        if rnd_err.random() < PCT_ERROR:
            error = rnd_err.choices(
                ["cantidad_invalida", "precio_negativo", "descuento_fuera_rango",
                 "sku_huerfano", "linea_no_numerica"],
                weights=[26, 22, 22, 20, 10],
            )[0]
            if error == "cantidad_invalida":
                item["cantidad"] = rnd_err.choice(["0", "-2", "muchas"])
            elif error == "precio_negativo":
                item["precio_unitario"] = f"-{precio_unitario:.2f}"
            elif error == "descuento_fuera_rango":
                item["descuento_pct"] = rnd_err.choice(["150", "-10"])
            elif error == "sku_huerfano":
                item["sku"] = f"XXX-{rnd_err.randint(900, 999)}"
            else:
                item["num_linea"] = "uno"
            registrar_error("pedidos_items", error)
        pedidos_items_lote2.append(item)

    pedido = {
        "id_pedido": id_pedido,
        "id_cliente": id_cliente,
        "id_tienda": id_tienda,
        "fecha_pedido": formatear_fecha(fecha_pedido, rnd),
        "timestamp_pedido": formatear_timestamp(momento, rnd),
        "canal": rnd.choice(CANALES),
        "metodo_pago": rnd.choice(METODOS_PAGO),
        "moneda": rnd.choice(MONEDAS),
        "estado": rnd.choice(ESTADOS_PEDIDO),
        "monto_total": f"{total:.2f}",
        # >>> Columna NUEVA, inexistente en el lote original: evolución de esquema <<<
        "cupon_descuento": rnd.choice(CUPONES),
        "fecha_actualizacion": formatear_timestamp(momento + timedelta(hours=rnd.randint(1, 72)), rnd),
    }
    rnd_err = random.Random(f"{SEMILLA}-lote2-pedido-err-{k}")
    if rnd_err.random() < PCT_ERROR:
        error = rnd_err.choices(
            ["cliente_huerfano", "tienda_huerfana", "monto_negativo", "monto_no_numerico",
             "estado_desconocido", "moneda_desconocida"],
            weights=[16, 12, 14, 10, 24, 24],
        )[0]
        if error == "cliente_huerfano":
            pedido["id_cliente"] = f"CLI-9{rnd_err.randint(100000, 999999)}"
        elif error == "tienda_huerfana":
            pedido["id_tienda"] = f"TDA-9{rnd_err.randint(10, 99)}"
        elif error == "monto_negativo":
            pedido["monto_total"] = f"-{total:.2f}"
        elif error == "monto_no_numerico":
            pedido["monto_total"] = "N/D"
        elif error == "estado_desconocido":
            pedido["estado"] = rnd_err.choice(["??", "EN REVISION", "X"])
        else:
            pedido["moneda"] = rnd_err.choice(["EUR", "??", "pesos"])
        registrar_error("pedidos", error)
    pedidos_lote2.append(pedido)

print(f"nuevos pedidos (lote 2): {len(pedidos_lote2)}  ·  items: {len(pedidos_items_lote2)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Clientes actualizados (feed para CDC / SCD Tipo 2)
# MAGIC
# MAGIC Toma un subconjunto de clientes existentes y emite **versiones nuevas** con `fecha_actualizacion`
# MAGIC más reciente y atributos cambiados (`segmento`, `nivel_lealtad`). Misma clave `id_cliente`.

# COMMAND ----------

clientes_actualizados = []
n_actualizar = max(1, N_CLIENTES // 10)

for i in range(n_actualizar):
    rnd = random.Random(f"{SEMILLA}-lote2-cliente-upd-{i}")
    idx = rnd.randint(0, N_CLIENTES - 1)
    id_cliente = f"CLI-{idx:06d}"
    codigo_pais, _, ciudades = rnd.choice(PAISES)
    nombre = rnd.choice(NOMBRES)
    apellido = rnd.choice(APELLIDOS)
    registro = {
        "id_cliente": id_cliente,
        "nombre_completo": f"{nombre} {apellido}",
        "email": f"{slug(nombre)}.{slug(apellido)}{rnd.randint(1, 999)}@{rnd.choice(DOMINIOS)}",
        "telefono": formatear_telefono(rnd),
        "fecha_nacimiento": formatear_fecha(
            date(rnd.randint(1955, 2006), rnd.randint(1, 12), rnd.randint(1, 28)), rnd),
        "fecha_registro": formatear_fecha(HOY - timedelta(days=rnd.randint(30, 2200)), rnd),
        "ciudad": rnd.choice(ciudades),
        "pais": rnd.choice(PAIS_VARIANTES[codigo_pais]),
        # Atributos que "cambiaron" — el foco del SCD2:
        "segmento": rnd.choice(SEGMENTOS),
        "nivel_lealtad": rnd.choice(NIVELES_LEALTAD),
        # fecha_actualizacion MÁS RECIENTE que la del lote original (clave de secuencia del CDC):
        "fecha_actualizacion": formatear_timestamp(AHORA - timedelta(minutes=rnd.randint(0, 120)), rnd),
    }
    clientes_actualizados.append(registro)

print(f"clientes actualizados (versiones nuevas p/ SCD2): {len(clientes_actualizados)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Escritura incremental al landing (archivos *_lote2, sin borrar nada)

# COMMAND ----------

def anexar_json(entidad: str, registros: list, sufijo: str) -> None:
    directorio = f"{RUTA_LANDING}/{entidad}"
    dbutils.fs.mkdirs(directorio)
    with open(f"{directorio}/{entidad}_{sufijo}.json", "w", encoding="utf-8") as fh:
        for registro in registros:
            fh.write(json.dumps({k: ("" if v is None else str(v)) for k, v in registro.items()},
                                ensure_ascii=False) + "\n")
    print(f"  + {entidad}/{entidad}_{sufijo}.json  ({len(registros)} filas)")


print("Agregando nuevos archivos al landing (incremental):")
anexar_json("pedidos", pedidos_lote2, "lote2")
anexar_json("pedidos_items", pedidos_items_lote2, "lote2")
anexar_json("clientes", clientes_actualizados, "lote2")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verificación y tips
# MAGIC
# MAGIC Relee la carpeta de `pedidos` completa (lote 1 + lote 2). Observa que la nueva columna
# MAGIC `cupon_descuento` aparece solo en las filas del lote 2 (nula en las antiguas) — eso es
# MAGIC **evolución de esquema** que Auto Loader resuelve en el bronze.

# COMMAND ----------

df_pedidos = spark.read.option("recursiveFileLookup", "true").json(f"{RUTA_LANDING}/pedidos")
tiene_cupon = "cupon_descuento" in df_pedidos.columns
total_pedidos = df_pedidos.count()
con_cupon = df_pedidos.where("cupon_descuento is not null").count() if tiene_cupon else 0

print(f"pedidos en el landing (lote1+lote2): {total_pedidos}")
print(f"columna cupon_descuento presente   : {tiene_cupon}")
print(f"filas con cupon_descuento          : {con_cupon}")
print(f"clientes con versión nueva (SCD2)  : {len(clientes_actualizados)}")

resumen = {
    "usuario": USUARIO,
    "destino": f"{CATALOGO}.{ESQUEMA}",
    "ruta_landing": RUTA_LANDING,
    "nuevos_pedidos": len(pedidos_lote2),
    "nuevos_items": len(pedidos_items_lote2),
    "clientes_actualizados": len(clientes_actualizados),
    "columna_nueva": "cupon_descuento",
    "columna_nueva_presente": tiene_cupon,
    "total_pedidos_landing": total_pedidos,
    "generado_en": AHORA.isoformat(),
}
print()
print(json.dumps(resumen, ensure_ascii=False, indent=2))

dbutils.notebook.exit(json.dumps(resumen, ensure_ascii=False))
