# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Generador de datos sintéticos retail (capa *landing*)
# MAGIC
# MAGIC Este notebook es el **único paso que ejecutan los participantes del workshop**. Genera las
# MAGIC extracciones planas de un retailer LATAM ficticio (**Mercado Andino**) y las deja como
# MAGIC archivos crudos en un **volumen de Unity Catalog**. A partir de ahí, el resto del workshop
# MAGIC —ingesta *bronze → silver → gold* con **Lakeflow Spark Declarative Pipelines (SDP)** y las
# MAGIC **reglas de calidad de datos**— se construye **con Genie code** (Databricks Assistant),
# MAGIC no con código escrito a mano.
# MAGIC
# MAGIC ## Qué produce
# MAGIC
# MAGIC | Archivo | Entidad | Formato | Rol en el modelo |
# MAGIC |---|---|---|---|
# MAGIC | `clientes`     | Clientes             | JSON | Dimensión |
# MAGIC | `productos`    | Catálogo de productos| CSV  | Dimensión |
# MAGIC | `tiendas`      | Tiendas / sucursales | CSV  | Dimensión |
# MAGIC | `pedidos`      | Pedidos (cabecera)   | JSON | Hecho |
# MAGIC | `pedidos_items`| Detalle de pedido    | JSON | Hecho (grano línea) |
# MAGIC
# MAGIC Todo se escribe **como texto**, igual que una extracción plana real de un sistema origen.
# MAGIC La "suciedad" es **intencional** para que las reglas de calidad tengan algo que atrapar:
# MAGIC
# MAGIC 1. **Ruido cosmético** (siempre presente): mayúsculas/minúsculas, acentos, espacios de más,
# MAGIC    distintos formatos de fecha y teléfono.
# MAGIC 2. **Errores de negocio** (controlados por `pct_error`): correos inválidos, montos negativos,
# MAGIC    fechas futuras, referencias huérfanas (FK que no existen), duplicados, valores fuera de rango.
# MAGIC
# MAGIC ## Compute
# MAGIC
# MAGIC Pensado para **Serverless** (o cualquier cluster con Spark). No instala librerías: usa solo la
# MAGIC librería estándar de Python + Spark.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuración
# MAGIC
# MAGIC El notebook corre en **workspaces distintos**, así que **nada está fijo en el código**. Cada
# MAGIC parámetro se resuelve en este orden:
# MAGIC
# MAGIC 1. **Variable de entorno** (`WORKSHOP_*`) — útil para jobs / automatización.
# MAGIC 2. **Widget** del notebook — útil para correrlo a mano desde la UI.
# MAGIC 3. **Valor por defecto**.
# MAGIC
# MAGIC | Parámetro | Variable de entorno | Default | Descripción |
# MAGIC |---|---|---|---|
# MAGIC | `catalogo`        | `WORKSHOP_CATALOG`      | `dacamargovws_catalog` | Catálogo destino, **compartido** (debe existir) |
# MAGIC | `esquema`         | `WORKSHOP_SCHEMA`       | `lakeflow_workshop`    | Prefijo del esquema; el real es `{esquema}_{usuario}` |
# MAGIC | `volumen`         | `WORKSHOP_VOLUME`       | `landing`              | Volumen para los archivos crudos |
# MAGIC | `n_clientes`      | `WORKSHOP_N_CUSTOMERS`  | `2500`                 | Tamaño del universo de clientes |
# MAGIC | `n_pedidos`       | `WORKSHOP_N_ORDERS`     | `15000`                | Número de pedidos a generar |
# MAGIC | `pct_error`       | `WORKSHOP_PCT_ERROR`    | `0.15`                 | Proporción de registros con error de negocio |
# MAGIC | `semilla`         | `WORKSHOP_SEED`         | `20260101`             | Semilla de reproducibilidad |
# MAGIC | `limpiar_landing` | `WORKSHOP_CLEAN`        | `true`                 | Borra el volumen antes de escribir |
# MAGIC
# MAGIC > **Multiusuario:** se comparte **solo el catálogo**. Cada participante escribe en su propio
# MAGIC > esquema derivado de `current_user()` — p. ej. `lakeflow_workshop_daniel_vargas` — con su
# MAGIC > propio volumen de `landing` dentro. Así varios pueden correr el workshop **al mismo tiempo**
# MAGIC > sin pisarse, y las tablas que cada quien cree luego con Genie code quedan en su esquema
# MAGIC > (nada de un único esquema convertido en pantano de tablas). Cada participante solo necesita
# MAGIC > permiso `USE CATALOG` + `CREATE SCHEMA` sobre el catálogo compartido.

# COMMAND ----------

import csv
import io
import json
import os
import random
from datetime import date, datetime, timedelta

# --- Declarar widgets (ignora el error si ya existen o si no hay UI) ---
_DEFAULTS = {
    "catalogo": "dacamargovws_catalog",
    "esquema": "lakeflow_workshop",
    "volumen": "landing",
    "n_clientes": "2500",
    "n_pedidos": "15000",
    "pct_error": "0.15",
    "semilla": "20260101",
    "limpiar_landing": "true",
}
for _nombre, _valor in _DEFAULTS.items():
    try:
        dbutils.widgets.text(_nombre, _valor)
    except Exception:
        pass


def parametro(nombre: str, variable_entorno: str) -> str:
    """Resuelve un parámetro: variable de entorno > widget > default."""
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


CATALOGO = parametro("catalogo", "WORKSHOP_CATALOG")
ESQUEMA_BASE = parametro("esquema", "WORKSHOP_SCHEMA")
VOLUMEN = parametro("volumen", "WORKSHOP_VOLUME")
N_CLIENTES = int(parametro("n_clientes", "WORKSHOP_N_CUSTOMERS"))
N_PEDIDOS = int(parametro("n_pedidos", "WORKSHOP_N_ORDERS"))
PCT_ERROR = float(parametro("pct_error", "WORKSHOP_PCT_ERROR"))
SEMILLA = int(parametro("semilla", "WORKSHOP_SEED"))
LIMPIAR_LANDING = parametro("limpiar_landing", "WORKSHOP_CLEAN").lower() == "true"


def slug_identificador(texto: str) -> str:
    """Convierte un texto en un identificador SQL válido (minúsculas, letras/dígitos/_)."""
    limpio = "".join(c if c.isalnum() else "_" for c in texto.lower())
    while "__" in limpio:
        limpio = limpio.replace("__", "_")
    limpio = limpio.strip("_")
    if limpio and limpio[0].isdigit():
        limpio = f"u_{limpio}"
    return limpio[:60] or "usuario"


# Aislamiento por usuario (siempre): se comparte SOLO el catálogo; cada participante escribe en su
# propio esquema derivado de su identidad (current_user), p. ej. `lakeflow_workshop_daniel_vargas`.
# Así varios pueden correr el workshop en paralelo sin pisarse, y las tablas que cada quien cree
# luego con Genie code quedan en su propio esquema (no se vuelve un pantano de tablas compartidas).
USUARIO = spark.sql("SELECT current_user()").collect()[0][0]
SUFIJO_USUARIO = slug_identificador(USUARIO.split("@")[0])
ESQUEMA = f"{ESQUEMA_BASE}_{SUFIJO_USUARIO}"

RUTA_LANDING = f"/Volumes/{CATALOGO}/{ESQUEMA}/{VOLUMEN}"
HOY = date.today()
# Ventana de datos: últimos ~6 meses hasta hoy, para que los dashboards se vean "vivos".
INICIO_VENTANA = HOY - timedelta(days=180)

print("Configuración resuelta:")
print(f"  usuario        = {USUARIO}")
print(f"  destino        = {CATALOGO}.{ESQUEMA}  (volumen: {VOLUMEN})")
print(f"  ruta landing   = {RUTA_LANDING}")
print(f"  n_clientes     = {N_CLIENTES}")
print(f"  n_pedidos      = {N_PEDIDOS}")
print(f"  pct_error      = {PCT_ERROR}")
print(f"  semilla        = {SEMILLA}")
print(f"  limpiar_landing= {LIMPIAR_LANDING}")
print(f"  ventana        = {INICIO_VENTANA} -> {HOY}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Infraestructura en Unity Catalog
# MAGIC
# MAGIC Crea el esquema y el volumen si no existen. **No intenta crear el catálogo** (asume que ya
# MAGIC existe y que tienes permiso `USE CATALOG` + `CREATE SCHEMA`/`CREATE VOLUME` sobre él).

# COMMAND ----------

# El catálogo se asume existente y compartido (best-effort por si tienes permiso de crearlo).
# El esquema y el volumen son propios del usuario, así que se crean aquí.
try:
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOGO}")
except Exception as error:
    print(f"(aviso) no se pudo crear el catálogo '{CATALOGO}', se asume existente: {error}")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOGO}.{ESQUEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOGO}.{ESQUEMA}.{VOLUMEN}")
print(f"OK · esquema y volumen listos en {CATALOGO}.{ESQUEMA}  (volumen: {VOLUMEN})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Catálogos de referencia del negocio

# COMMAND ----------

PAISES = [
    ("MX", "México", ["CDMX", "Guadalajara", "Monterrey", "Puebla", "Querétaro", "Mérida"]),
    ("CO", "Colombia", ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena"]),
    ("CL", "Chile", ["Santiago", "Valparaíso", "Concepción", "Antofagasta"]),
    ("PE", "Perú", ["Lima", "Arequipa", "Trujillo", "Cusco"]),
    ("AR", "Argentina", ["Buenos Aires", "Córdoba", "Rosario", "Mendoza"]),
]
# Variantes "sucias" de cómo cada sistema escribe el país (la normalización es tarea de silver).
PAIS_VARIANTES = {
    "MX": ["MX", "México", "MEXICO", "Mexico", "mx"],
    "CO": ["CO", "Colombia", "COLOMBIA", "col"],
    "CL": ["CL", "Chile", "CHILE", "chl"],
    "PE": ["PE", "Perú", "PERU", "Peru", "per"],
    "AR": ["AR", "Argentina", "ARGENTINA", "arg"],
}

SEGMENTOS = ["Nuevo", "Recurrente", "VIP", "nuevo", "RECURRENTE", "vip", "Inactivo"]
NIVELES_LEALTAD = ["Bronce", "Plata", "Oro", "Platino", "bronce", "ORO"]
CANALES = ["Web", "App", "Tienda", "Call Center", "Marketplace", "web", "APP", "tienda"]
METODOS_PAGO = ["Tarjeta", "Efectivo", "Transferencia", "Billetera digital", "tarjeta", "EFECTIVO"]
MONEDAS = ["MXN", "COP", "CLP", "PEN", "ARS", "USD"]
MONEDA_PAIS = {"MX": "MXN", "CO": "COP", "CL": "CLP", "PE": "PEN", "AR": "ARS"}
ESTADOS_PEDIDO = ["Creado", "Pagado", "Enviado", "Entregado", "Cancelado",
                  "PAGADO", "enviado", "entregado", "cancelado"]

NOMBRES = ["María", "José", "Juan", "Ana", "Luis", "Carmen", "Jorge", "Sofía", "Pedro", "Lucía",
           "Miguel", "Valentina", "Diego", "Camila", "Andrés", "Daniela", "Carlos", "Paula",
           "Fernando", "Gabriela", "Ricardo", "Isabela", "Javier", "Mariana", "Roberto", "Natalia"]
APELLIDOS = ["García", "Rodríguez", "Martínez", "López", "González", "Pérez", "Sánchez", "Ramírez",
             "Torres", "Flores", "Rivera", "Gómez", "Díaz", "Vargas", "Castro", "Ortiz", "Morales",
             "Reyes", "Cruz", "Herrera", "Jiménez", "Mendoza", "Rojas", "Núñez", "Silva"]
DOMINIOS = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com", "correo.com", "mail.com"]

CATALOGO_PRODUCTOS = [
    ("Abarrotes", "ABA", ["Arroz 1kg", "Frijol 900g", "Aceite 1L", "Azúcar 1kg", "Harina 1kg",
                          "Pasta 500g", "Sal 1kg", "Café molido 250g", "Atún en lata", "Lentejas 500g"]),
    ("Bebidas", "BEB", ["Agua 1L", "Gaseosa 2L", "Jugo de naranja 1L", "Cerveza 355ml",
                        "Bebida energética 250ml", "Té frío 500ml", "Leche 1L"]),
    ("Cuidado personal", "CPE", ["Shampoo 400ml", "Jabón 3pzas", "Pasta dental 100ml",
                                 "Desodorante 150ml", "Papel higiénico 4pzas", "Maquinilla 5pzas"]),
    ("Limpieza", "LIM", ["Detergente 2kg", "Cloro 1L", "Lavaplatos 500ml", "Limpiavidrios 1L",
                        "Bolsas de basura 30pzas"]),
    ("Snacks", "SNK", ["Papas fritas 150g", "Galletas 200g", "Chocolate 100g", "Maní 200g",
                      "Chicles 10pzas"]),
    ("Hogar", "HOG", ["Foco LED 9W", "Pilas AA 4pzas", "Vela aromática", "Toalla de baño",
                     "Juego de vasos 6pzas"]),
    ("Electrónica", "ELE", ["Audífonos", "Cable USB-C", "Cargador 20W", "Mouse inalámbrico"]),
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Utilidades de formato, ruido y errores

# COMMAND ----------

contador_errores = {}


def registrar_error(entidad: str, error: str) -> None:
    clave = (entidad, error)
    contador_errores[clave] = contador_errores.get(clave, 0) + 1


def ruido_texto(texto: str, rnd: random.Random) -> str:
    """Ruido cosmético: el mismo dato escrito distinto en distintos sistemas."""
    estilo = rnd.random()
    if estilo < 0.55:
        return texto
    if estilo < 0.70:
        return f"  {texto} "
    if estilo < 0.82:
        return texto.lower()
    if estilo < 0.92:
        return texto.upper()
    return texto.title()


def formatear_fecha(valor: date, rnd: random.Random) -> str:
    estilo = rnd.random()
    if estilo < 0.6:
        return valor.strftime("%Y-%m-%d")
    if estilo < 0.85:
        return valor.strftime("%d/%m/%Y")
    return valor.strftime("%m-%d-%Y")


def formatear_timestamp(valor: datetime, rnd: random.Random) -> str:
    return valor.strftime("%Y-%m-%d %H:%M:%S") if rnd.random() < 0.8 else valor.strftime("%d/%m/%Y %H:%M")


def formatear_telefono(pais: str, rnd: random.Random) -> str:
    numero = f"{rnd.randint(2, 9)}{rnd.randint(10000000, 99999999)}"
    estilo = rnd.random()
    if estilo < 0.35:
        return numero
    if estilo < 0.6:
        return f"+{rnd.choice(['52', '57', '56', '51', '54'])} {numero[:4]} {numero[4:]}"
    if estilo < 0.8:
        return f"({numero[:3]}) {numero[3:6]}-{numero[6:]}"
    return f"{numero[:4]}-{numero[4:]}"


def slug(texto: str) -> str:
    limpio = (texto.lower()
              .replace("á", "a").replace("é", "e").replace("í", "i")
              .replace("ó", "o").replace("ú", "u").replace("ñ", "n"))
    return "".join(c for c in limpio if c.isalnum())


def fecha_en_ventana(rnd: random.Random) -> date:
    return INICIO_VENTANA + timedelta(days=rnd.randint(0, 180))


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Clientes (dimensión)
# MAGIC
# MAGIC Universo reproducible: cada cliente se genera con su propia semilla derivada del índice.

# COMMAND ----------

clientes = []
ids_cliente_validos = []

for i in range(N_CLIENTES):
    rnd = random.Random(f"{SEMILLA}-cliente-{i}")
    codigo_pais, nombre_pais, ciudades = rnd.choice(PAISES)
    nombre = rnd.choice(NOMBRES)
    apellido = rnd.choice(APELLIDOS)
    nombre_completo = f"{nombre} {apellido}"
    id_cliente = f"CLI-{i:06d}"
    ids_cliente_validos.append(id_cliente)

    registro = {
        "id_cliente": id_cliente,
        "nombre_completo": ruido_texto(nombre_completo, rnd),
        "email": f"{slug(nombre)}.{slug(apellido)}{rnd.randint(1, 999)}@{rnd.choice(DOMINIOS)}",
        "telefono": formatear_telefono(codigo_pais, rnd),
        "fecha_nacimiento": formatear_fecha(
            date(rnd.randint(1955, 2006), rnd.randint(1, 12), rnd.randint(1, 28)), rnd),
        "fecha_registro": formatear_fecha(
            HOY - timedelta(days=rnd.randint(30, 2200)), rnd),
        "ciudad": ruido_texto(rnd.choice(ciudades), rnd),
        "pais": rnd.choice(PAIS_VARIANTES[codigo_pais]),
        "segmento": rnd.choice(SEGMENTOS),
        "nivel_lealtad": rnd.choice(NIVELES_LEALTAD),
        "fecha_actualizacion": formatear_timestamp(
            datetime.now() - timedelta(minutes=rnd.randint(0, 5000)), rnd),
    }

    # --- Inyección de errores de negocio ---
    rnd_err = random.Random(f"{SEMILLA}-cliente-err-{i}")
    if rnd_err.random() < PCT_ERROR:
        error = rnd_err.choices(
            ["email_invalido", "email_vacio", "fecha_registro_futura", "fecha_ilegible",
             "nombre_corto", "ciudad_vacia", "nacimiento_invalido"],
            weights=[24, 14, 14, 12, 10, 14, 12],
        )[0]
        if error == "email_invalido":
            registro["email"] = registro["email"].replace("@", ".")
        elif error == "email_vacio":
            registro["email"] = ""
        elif error == "fecha_registro_futura":
            registro["fecha_registro"] = formatear_fecha(HOY + timedelta(days=rnd_err.randint(5, 300)), rnd_err)
        elif error == "fecha_ilegible":
            registro["fecha_registro"] = rnd_err.choice(["N/D", "0000-00-00", "31/02/2025", "sin fecha"])
        elif error == "nombre_corto":
            registro["nombre_completo"] = rnd_err.choice(["X", "-", "AB"])
        elif error == "ciudad_vacia":
            registro["ciudad"] = ""
        else:
            registro["fecha_nacimiento"] = rnd_err.choice(["N/D", "1800-01-01", "32/13/1990"])
        registrar_error("clientes", error)

    clientes.append(registro)

    # Duplicado real: el mismo cliente capturado dos veces con otro id (lo resuelve DQ/dedupe).
    if rnd.random() < 0.03:
        dup = dict(registro)
        dup["id_cliente"] = f"{id_cliente}-D"
        dup["telefono"] = formatear_telefono(codigo_pais, rnd)
        clientes.append(dup)
        registrar_error("clientes", "duplicado")

print(f"clientes generados: {len(clientes)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Productos (dimensión)

# COMMAND ----------

productos = []
skus_validos = []

for categoria, codigo, nombres in CATALOGO_PRODUCTOS:
    for j, nombre in enumerate(nombres):
        rnd = random.Random(f"{SEMILLA}-producto-{codigo}-{j}")
        sku = f"{codigo}-{j:03d}"
        skus_validos.append(sku)
        costo = round(rnd.uniform(5, 500), 2)
        precio = round(costo * rnd.uniform(1.2, 2.1), 2)
        registro = {
            "sku": sku,
            "nombre_producto": nombre,
            "categoria": categoria,
            "marca": rnd.choice(["Genérica", "Premium", "Nacional", "Importada", "Propia"]),
            "precio_lista": f"{precio:.2f}",
            "costo_unitario": f"{costo:.2f}",
            "unidad": rnd.choice(["UND", "und", "PZA", "pza", "PAQ"]),
            "activo": rnd.choice(["true", "false", "1", "0", "SI", "NO"]),
            "fecha_actualizacion": formatear_timestamp(
                datetime.now() - timedelta(days=rnd.randint(0, 120)), rnd),
        }
        rnd_err = random.Random(f"{SEMILLA}-producto-err-{codigo}-{j}")
        if rnd_err.random() < PCT_ERROR:
            error = rnd_err.choices(
                ["precio_negativo", "precio_no_numerico", "margen_negativo",
                 "categoria_vacia", "costo_vacio"],
                weights=[22, 20, 24, 18, 16],
            )[0]
            if error == "precio_negativo":
                registro["precio_lista"] = f"-{precio:.2f}"
            elif error == "precio_no_numerico":
                registro["precio_lista"] = rnd_err.choice(["N/D", "consultar", "-"])
            elif error == "margen_negativo":
                registro["precio_lista"] = f"{round(costo * 0.85, 2):.2f}"
            elif error == "categoria_vacia":
                registro["categoria"] = ""
            else:
                registro["costo_unitario"] = ""
            registrar_error("productos", error)
        productos.append(registro)

# SKU duplicado con distinta capitalización (caso típico de dedupe de catálogo).
if productos:
    dup = dict(productos[0])
    dup["sku"] = dup["sku"].lower()
    dup["nombre_producto"] = dup["nombre_producto"].upper()
    productos.append(dup)
    registrar_error("productos", "sku_duplicado")

print(f"productos generados: {len(productos)}  (skus únicos: {len(skus_validos)})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Tiendas (dimensión)

# COMMAND ----------

tiendas = []
ids_tienda_validos = []
N_TIENDAS = 25

for i in range(N_TIENDAS):
    rnd = random.Random(f"{SEMILLA}-tienda-{i}")
    codigo_pais, nombre_pais, ciudades = PAISES[i % len(PAISES)]
    ciudad = rnd.choice(ciudades)
    id_tienda = f"TDA-{i:03d}"
    ids_tienda_validos.append(id_tienda)
    registro = {
        "id_tienda": id_tienda,
        "nombre_tienda": f"Mercado Andino {ciudad} {i:02d}",
        "ciudad": ciudad,
        "pais": rnd.choice(PAIS_VARIANTES[codigo_pais]),
        "formato": rnd.choice(["Express", "Supermercado", "Mayorista", "express", "SUPERMERCADO"]),
        "latitud": f"{round(rnd.uniform(-40, 25), 5)}",
        "longitud": f"{round(rnd.uniform(-110, -55), 5)}",
        "fecha_apertura": formatear_fecha(HOY - timedelta(days=rnd.randint(200, 5000)), rnd),
        "activa": rnd.choice(["true", "1", "SI"]),
        "fecha_actualizacion": formatear_timestamp(
            datetime.now() - timedelta(days=rnd.randint(0, 30)), rnd),
    }
    rnd_err = random.Random(f"{SEMILLA}-tienda-err-{i}")
    if rnd_err.random() < PCT_ERROR:
        error = rnd_err.choice(["latitud_fuera_rango", "longitud_fuera_rango", "ciudad_vacia"])
        if error == "latitud_fuera_rango":
            registro["latitud"] = "999.9"
        elif error == "longitud_fuera_rango":
            registro["longitud"] = "999.9"
        else:
            registro["ciudad"] = ""
        registrar_error("tiendas", error)
    tiendas.append(registro)

print(f"tiendas generadas: {len(tiendas)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pedidos y detalle (hechos)
# MAGIC
# MAGIC Datos **crudos y transaccionales**: una fila = un evento. **No se calcula ningún agregado**
# MAGIC (`total_`, `sum_`, `avg_`…): esas métricas las computa la capa *gold* del pipeline que se
# MAGIC construye luego con Genie code.

# COMMAND ----------

pedidos = []
pedidos_items = []

for k in range(N_PEDIDOS):
    rnd = random.Random(f"{SEMILLA}-pedido-{k}")
    id_pedido = f"PED-{k:07d}"
    id_cliente = rnd.choice(ids_cliente_validos)
    id_tienda = rnd.choice(ids_tienda_validos)
    fecha_pedido = fecha_en_ventana(rnd)
    momento = datetime.combine(fecha_pedido, datetime.min.time()) + timedelta(
        hours=rnd.randint(8, 22), minutes=rnd.randint(0, 59))
    codigo_pais = next((c for c, _, cs in PAISES), "MX")

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
        rnd_err = random.Random(f"{SEMILLA}-item-err-{k}-{linea}")
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
        pedidos_items.append(item)

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
        "fecha_actualizacion": formatear_timestamp(momento + timedelta(hours=rnd.randint(1, 72)), rnd),
    }
    rnd_err = random.Random(f"{SEMILLA}-pedido-err-{k}")
    if rnd_err.random() < PCT_ERROR:
        error = rnd_err.choices(
            ["cliente_huerfano", "tienda_huerfana", "monto_negativo", "monto_no_numerico",
             "fecha_futura", "estado_desconocido", "moneda_desconocida"],
            weights=[16, 12, 14, 10, 14, 18, 16],
        )[0]
        if error == "cliente_huerfano":
            pedido["id_cliente"] = f"CLI-9{rnd_err.randint(100000, 999999)}"
        elif error == "tienda_huerfana":
            pedido["id_tienda"] = f"TDA-9{rnd_err.randint(10, 99)}"
        elif error == "monto_negativo":
            pedido["monto_total"] = f"-{total:.2f}"
        elif error == "monto_no_numerico":
            pedido["monto_total"] = "N/D"
        elif error == "fecha_futura":
            pedido["fecha_pedido"] = formatear_fecha(HOY + timedelta(days=rnd_err.randint(3, 60)), rnd_err)
        elif error == "estado_desconocido":
            pedido["estado"] = rnd_err.choice(["??", "EN REVISION", "X"])
        else:
            pedido["moneda"] = rnd_err.choice(["EUR", "??", "pesos"])
        registrar_error("pedidos", error)

    pedidos.append(pedido)

    # Pedido duplicado en el origen (lo resuelve la unicidad en gold).
    if rnd.random() < 0.01:
        pedidos.append(dict(pedido))
        registrar_error("pedidos", "pedido_duplicado")

print(f"pedidos generados: {len(pedidos)}  ·  items: {len(pedidos_items)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Escritura al volumen de *landing*
# MAGIC
# MAGIC Cada entidad se escribe en su propia carpeta. Si `limpiar_landing=true` el volumen se vacía
# MAGIC primero, de modo que volver a correr el notebook deja un conjunto limpio y reproducible.

# COMMAND ----------

ENTIDADES = ["clientes", "productos", "tiendas", "pedidos", "pedidos_items"]

if LIMPIAR_LANDING:
    for entidad in ENTIDADES:
        try:
            dbutils.fs.rm(f"{RUTA_LANDING}/{entidad}", recurse=True)
        except Exception as error:
            print(f"  (nada que limpiar en {entidad}: {error})")
    print(f"volumen de landing limpio: {RUTA_LANDING}")


def escribir_json(entidad: str, registros: list) -> None:
    directorio = f"{RUTA_LANDING}/{entidad}"
    dbutils.fs.mkdirs(directorio)
    with open(f"{directorio}/{entidad}.json", "w", encoding="utf-8") as fh:
        for registro in registros:
            fh.write(json.dumps({k: ("" if v is None else str(v)) for k, v in registro.items()},
                                ensure_ascii=False) + "\n")
    print(f"  {entidad:14} JSON  {len(registros):>7} filas")


def escribir_csv(entidad: str, registros: list) -> None:
    directorio = f"{RUTA_LANDING}/{entidad}"
    dbutils.fs.mkdirs(directorio)
    buffer = io.StringIO()
    escritor = csv.DictWriter(buffer, fieldnames=list(registros[0].keys()), quoting=csv.QUOTE_MINIMAL)
    escritor.writeheader()
    for registro in registros:
        escritor.writerow({k: ("" if v is None else str(v)) for k, v in registro.items()})
    with open(f"{directorio}/{entidad}.csv", "w", encoding="utf-8") as fh:
        fh.write(buffer.getvalue())
    print(f"  {entidad:14} CSV   {len(registros):>7} filas")


print("Escribiendo archivos crudos:")
escribir_json("clientes", clientes)
escribir_csv("productos", productos)
escribir_csv("tiendas", tiendas)
escribir_json("pedidos", pedidos)
escribir_json("pedidos_items", pedidos_items)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Bitácora de generación (opcional, útil para el módulo de calidad)
# MAGIC
# MAGIC Guarda cuántos errores se inyectaron a propósito. Al final del workshop, cuando el pipeline
# MAGIC SDP esté corriendo, se puede contrastar **inyectado vs. detectado** por las reglas de calidad.

# COMMAND ----------

from pyspark.sql import Row

TABLA_CONTROL = f"{CATALOGO}.{ESQUEMA}._bitacora_generacion"
GENERADO_EN = datetime.now()

conteos = {
    "clientes": len(clientes),
    "productos": len(productos),
    "tiendas": len(tiendas),
    "pedidos": len(pedidos),
    "pedidos_items": len(pedidos_items),
}

filas = [
    Row(entidad=entidad, tipo="total_registros", detalle="(total)", cantidad=cantidad, generado_en=GENERADO_EN)
    for entidad, cantidad in conteos.items()
] + [
    Row(entidad=entidad, tipo="error_inyectado", detalle=error, cantidad=cantidad, generado_en=GENERADO_EN)
    for (entidad, error), cantidad in sorted(contador_errores.items())
]

(spark.createDataFrame(filas)
    .write.mode("overwrite").option("overwriteSchema", "true")
    .saveAsTable(TABLA_CONTROL))

total_errores = sum(v for _, v in contador_errores.items())
print(f"bitácora escrita en {TABLA_CONTROL}: {sum(conteos.values())} registros, "
      f"{total_errores} errores inyectados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Validación: releer desde el volumen
# MAGIC
# MAGIC Prueba el viaje de ida y vuelta (escribir → leer). Si esto cuadra, los archivos están listos
# MAGIC para que construyas el pipeline SDP con Genie code.

# COMMAND ----------

lectura = {
    "clientes": spark.read.json(f"{RUTA_LANDING}/clientes"),
    "productos": spark.read.option("header", "true").csv(f"{RUTA_LANDING}/productos"),
    "tiendas": spark.read.option("header", "true").csv(f"{RUTA_LANDING}/tiendas"),
    "pedidos": spark.read.json(f"{RUTA_LANDING}/pedidos"),
    "pedidos_items": spark.read.json(f"{RUTA_LANDING}/pedidos_items"),
}

conteos_leidos = {}
print("Verificación (filas releídas desde el volumen):")
for entidad, df in lectura.items():
    n = df.count()
    conteos_leidos[entidad] = n
    esperado = conteos[entidad]
    marca = "OK " if n == esperado else "!! "
    print(f"  {marca}{entidad:14} leídas={n:>7}  esperadas={esperado:>7}")

print("\nMuestra de clientes:")
lectura["clientes"].select("id_cliente", "nombre_completo", "email", "ciudad", "pais").show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Tu hoja de datos para construir el pipeline con Genie code
# MAGIC
# MAGIC **Importante:** cada participante tiene su propio esquema y volumen. Usa **exactamente** los
# MAGIC valores que imprime la celda de abajo en tus prompts de Genie code, para que el pipeline lea
# MAGIC *tus* datos y escriba en *tu* esquema (y no choque con el de nadie más). Fíjate que:
# MAGIC
# MAGIC - El **catálogo** es compartido, pero el **esquema** es tuyo: `{esquema}_{usuario}`.
# MAGIC - El **nombre del pipeline** lleva tu sufijo, p. ej. `lakeflow_dq_daniel_vargas`.
# MAGIC - El **esquema destino (target)** del pipeline es tu propio esquema.

# COMMAND ----------

PIPELINE_SUGERIDO = f"lakeflow_dq_{SUFIJO_USUARIO}"

cheatsheet = f"""
================================================================================
  HOJA DE DATOS · {USUARIO}
================================================================================
  Catálogo (compartido) : {CATALOGO}
  TU esquema            : {ESQUEMA}
  TU ruta de landing    : {RUTA_LANDING}
  Nombre de pipeline    : {PIPELINE_SUGERIDO}
  Esquema destino (target del pipeline): {CATALOGO}.{ESQUEMA}

  Archivos de origen (úsalos tal cual en tus prompts):
    JSON : {RUTA_LANDING}/clientes
    CSV  : {RUTA_LANDING}/productos      (header = true)
    CSV  : {RUTA_LANDING}/tiendas        (header = true)
    JSON : {RUTA_LANDING}/pedidos
    JSON : {RUTA_LANDING}/pedidos_items
--------------------------------------------------------------------------------
  PASOS EN DATABRICKS:
   1. Crea un pipeline Lakeflow (SDP) llamado '{PIPELINE_SUGERIDO}'.
   2. Como 'target' / esquema de destino pon: {CATALOGO}.{ESQUEMA}
   3. En el editor del pipeline, pega los prompts de abajo en Genie code.
--------------------------------------------------------------------------------
  PROMPT 1 · BRONZE (ingesta con Auto Loader)
  "Crea tablas de streaming bronze que ingieran con Auto Loader desde
   {RUTA_LANDING}/ . clientes, pedidos y pedidos_items son JSON; productos y
   tiendas son CSV con header. Deja todas las columnas como string y agrega
   columnas de linaje con el nombre de archivo y la fecha de ingesta."

  PROMPT 2 · SILVER (tipado, normalización y calidad)
  "Crea tablas silver que tipen y normalicen cada entidad con expectativas de
   calidad (@dlt.expect / @dlt.expect_or_drop): email válido, monto_total
   numérico y >= 0, fecha_pedido no futura, descuento_pct entre 0 y 100,
   cantidad > 0, e id_cliente/id_tienda/sku que existan en sus dimensiones.
   Normaliza el país a código ISO y deduplica clientes y productos."

  PROMPT 3 · GOLD (modelo dimensional y métricas)
  "Crea tablas gold: dimensiones limpias de clientes, productos y tiendas, un
   hecho de ventas al grano de línea, y KPIs mensuales de ventas por tienda y
   categoría. Usa CLUSTER BY en vez de particionar."

  PROMPT 4 · OBSERVABILIDAD DE CALIDAD
  "Publica las métricas de las expectativas del pipeline y compáralas contra la
   tabla {TABLA_CONTROL} para mostrar inyectado vs. detectado."
================================================================================
"""
print(cheatsheet)

# COMMAND ----------

resumen = {
    "usuario": USUARIO,
    "destino": f"{CATALOGO}.{ESQUEMA}",
    "ruta_landing": RUTA_LANDING,
    "pipeline_sugerido": PIPELINE_SUGERIDO,
    "entidades": ENTIDADES,
    "conteos_escritos": conteos,
    "conteos_leidos": conteos_leidos,
    "errores_inyectados": total_errores,
    "cuadra": conteos == conteos_leidos,
    "generado_en": GENERADO_EN.isoformat(),
}
print(json.dumps(resumen, ensure_ascii=False, indent=2))

dbutils.notebook.exit(json.dumps(resumen, ensure_ascii=False))
