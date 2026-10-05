# Workshop · Lakeflow SDP + Data Quality con Genie code

Workshop hands-on para construir un pipeline de datos **de principio a fin con Databricks
Lakeflow Spark Declarative Pipelines (SDP)** y **reglas de calidad de datos**, pero con un giro:
**todo el pipeline se construye con Genie code** (Databricks Assistant), no escribiendo el código
a mano.

La idea es simple:

1. **Tú ejecutas un solo notebook** que genera datos sintéticos crudos de un retailer LATAM
   ficticio (**Mercado Andino**) y los deja como archivos en un volumen de Unity Catalog.
2. **A partir de ahí, construyes el pipeline con Genie code**: bronze → silver → gold, con
   expectativas de calidad (`@dlt.expect`), deduplicación, normalización y métricas.

Cada participante corre el notebook **en su propio workspace**, por eso **todo está
parametrizado** y no hay nada fijo en el código.

---

## 1. Prerrequisitos

| Requisito | Detalle |
|---|---|
| **Workspace Databricks** | Con **Unity Catalog** habilitado. |
| **Compute Serverless** | El notebook está pensado para serverless (o cualquier cluster con Spark). No instala librerías. |
| **Un catálogo existente** | Necesitas un catálogo sobre el que tengas `USE CATALOG`, `CREATE SCHEMA` y `CREATE VOLUME`. El notebook **crea el esquema y el volumen**, pero **no crea el catálogo**. |
| **Genie code / Databricks Assistant** | Para la segunda parte del workshop (construir el pipeline). |
| **Databricks CLI** (opcional) | Solo si prefieres importar el notebook por línea de comandos en vez de la UI. |

> El notebook usa **solo la librería estándar de Python + Spark**: no hace `pip install` de nada,
> así corre igual en cualquier workspace.

---

## 2. Configuración por variables de entorno

Como el notebook corre en **workspaces distintos**, cada parámetro se resuelve en este orden:

1. **Variable de entorno** (`WORKSHOP_*`) — útil para jobs / automatización.
2. **Widget** del notebook — útil para correrlo a mano desde la UI.
3. **Valor por defecto**.

| Parámetro (widget) | Variable de entorno | Default | Descripción |
|---|---|---|---|
| `catalogo`        | `WORKSHOP_CATALOG`     | `dacamargovws_catalog` | Catálogo destino, **compartido** (**cámbialo por el tuyo**). |
| `esquema`         | `WORKSHOP_SCHEMA`      | `lakeflow_workshop`    | Prefijo del esquema; el real es `{esquema}_{usuario}`. |
| `volumen`         | `WORKSHOP_VOLUME`      | `landing`              | Volumen para los archivos crudos. |
| `n_clientes`      | `WORKSHOP_N_CUSTOMERS` | `2500`                 | Tamaño del universo de clientes. |
| `n_pedidos`       | `WORKSHOP_N_ORDERS`    | `15000`                | Número de pedidos a generar. |
| `pct_error`       | `WORKSHOP_PCT_ERROR`   | `0.15`                 | Proporción de registros con error de negocio inyectado. |
| `semilla`         | `WORKSHOP_SEED`        | `20260101`             | Semilla de reproducibilidad. |
| `limpiar_landing` | `WORKSHOP_CLEAN`       | `true`                 | Vacía el volumen antes de escribir (re-ejecución idempotente). |

> **Participantes:** lo único que *tienes* que ajustar es **`catalogo`** para apuntar a un catálogo
> donde tengas permisos. El resto funciona con los defaults.

### Varios participantes al mismo tiempo

Se comparte **solo el catálogo**. Cada participante escribe en su **propio esquema**, derivado
automáticamente de `current_user()` (`{esquema}_{usuario}`), con su propio volumen de `landing`
dentro. No hay nada que configurar ni coordinar.

```
dacamargovws_catalog                          ← único recurso compartido
├── lakeflow_workshop_daniel_vargas           ← esquema por usuario
│   ├── landing/                              ← volumen por usuario (dentro de su esquema)
│   ├── _bitacora_generacion
│   └── (bronze/silver/gold que creas con Genie code)   ← tus tablas, aisladas
├── lakeflow_workshop_maria_lopez
│   └── ...
└── lakeflow_workshop_juan_perez
    └── ...
```

Por qué así: al tener cada quien su propio esquema, **las tablas del pipeline que construyas luego
con Genie code quedan aisladas** y el catálogo no se vuelve un pantano de tablas de todos.
Cada participante solo necesita `USE CATALOG` + `CREATE SCHEMA` sobre el catálogo compartido.

> Ejemplo real (validado) para `daniel.vargas@databricks.com`:
> `dacamargovws_catalog.lakeflow_workshop_daniel_vargas` · volumen `landing`.

Para fijarlo por variable de entorno (por ejemplo en un cluster o job):

```bash
export WORKSHOP_CATALOG=mi_catalogo
export WORKSHOP_SCHEMA=lakeflow_workshop
```

---

## 3. Cómo ejecutar el notebook

### Opción A — Desde la UI (recomendada para el workshop)

1. Sube `notebooks/01_generar_datos_retail.py` a tu workspace
   (**Workspace → Import → File**). Databricks lo reconoce como notebook de Python.
2. Ábrelo y conéctalo a **Serverless**.
3. En los **widgets** de arriba, cambia `catalogo` por tu catálogo.
4. **Run all**. En ~1–2 minutos deja los datos en el volumen.

### Opción B — Con la CLI

```bash
# Autenticar (una sola vez)
databricks auth login -p mi-perfil

# Importar el notebook
databricks workspace import \
  "/Users/<tu-usuario>/lakeflow_workshop/01_generar_datos_retail" \
  --file notebooks/01_generar_datos_retail.py \
  --language PYTHON --format SOURCE --overwrite -p mi-perfil
```

Luego ejecútalo desde la UI, o como run de job serverless apuntando a ese notebook.

Al terminar, el notebook imprime un resumen JSON y valida el viaje de ida y vuelta
(escribir → releer). Si ves `"cuadra": true`, los datos están listos.

---

## 4. Qué genera

Se crean **5 datasets crudos** en `{catalogo}.{esquema}_{usuario}`, dentro del volumen `landing`:

```
/Volumes/<catalogo>/<esquema>/landing/
├── clientes/       clientes.json       (dimensión)
├── productos/      productos.csv       (dimensión)
├── tiendas/        tiendas.csv         (dimensión)
├── pedidos/        pedidos.json        (hecho, cabecera)
└── pedidos_items/  pedidos_items.json  (hecho, grano línea)
```

Más una tabla de control: `{catalogo}.{esquema}._bitacora_generacion` con el conteo de registros
y **cuántos errores se inyectaron a propósito** (para contrastar *inyectado vs. detectado* cuando
tus reglas de calidad estén corriendo).

### Modelo (estrella)

```
         clientes ─┐
         tiendas  ─┼──< pedidos >──< pedidos_items >── productos
```

Son **datos crudos y transaccionales**: una fila = un evento. **No hay ningún agregado**
(`total_`, `sum_`, `avg_`): esas métricas las calcula la capa *gold* que construyes con Genie code.

### Calidad de datos intencional

Todo se escribe **como texto**, y la suciedad es deliberada para que las reglas de calidad tengan
algo que atrapar:

- **Ruido cosmético** (siempre): mayúsculas/minúsculas, acentos, espacios, formatos de fecha y
  teléfono distintos, país escrito de varias formas (`MX`, `México`, `MEXICO`…).
- **Errores de negocio** (según `pct_error`):
  - **clientes:** email inválido o vacío, fecha de registro futura, fecha ilegible, nombre muy
    corto, ciudad vacía, duplicados.
  - **productos:** precio negativo o no numérico, margen negativo (precio < costo), categoría
    vacía, SKU duplicado.
  - **tiendas:** latitud/longitud fuera de rango, ciudad vacía.
  - **pedidos:** cliente/tienda huérfanos (FK inexistente), monto negativo o no numérico, fecha
    futura, estado/moneda desconocidos, pedido duplicado.
  - **pedidos_items:** cantidad inválida, precio negativo, descuento fuera de `[0,100]`,
    SKU huérfano, número de línea no numérico.

---

## 5. Siguiente paso: construir el pipeline con Genie code

Esta es la parte central del workshop. Con los datos ya en el volumen, abre un
**Lakeflow Spark Declarative Pipeline** y pídele a **Genie code** que lo construya por capas.
Prompts sugeridos:

**Bronze (ingesta cruda con Auto Loader):**
> "Crea tablas de streaming bronze que ingieran con Auto Loader los archivos de
> `/Volumes/<catalogo>/<esquema>/landing/` — `clientes` y `pedidos` y `pedidos_items` son JSON,
> `productos` y `tiendas` son CSV con header. Deja todas las columnas como string y agrega columnas
> de linaje con el nombre de archivo y la fecha de ingesta."

**Silver (tipado, normalización y expectativas de calidad):**
> "Crea tablas silver que tipen y normalicen cada entidad. Agrega expectativas de calidad con
> `@dlt.expect_or_drop` / `@dlt.expect`: email con formato válido, `monto_total` numérico y >= 0,
> `fecha_pedido` no futura, `descuento_pct` entre 0 y 100, cantidades > 0, y que `id_cliente` e
> `id_tienda` existan en sus dimensiones. Normaliza el país a código ISO y deduplica clientes y
> productos."

**Gold (modelo dimensional y métricas):**
> "Crea tablas gold: dimensiones limpias de clientes, productos y tiendas, un hecho de ventas al
> grano de línea, y KPIs mensuales de ventas por tienda y por categoría. Usa `CLUSTER BY` en vez
> de particionar."

**Observabilidad de calidad:**
> "Crea una tabla que publique las métricas de las expectativas del pipeline y compárala contra
> `_bitacora_generacion` para mostrar *inyectado vs. detectado* por dimensión de calidad."

> El objetivo pedagógico: ver cómo Genie code arma el DAG, la incrementalidad y la calidad
> declarativa con muchísimo menos código que un pipeline tradicional.

---

## 6. Estructura del repo

```
lakeflow-sdp-dq-workshop/
├── README.md
└── notebooks/
    └── 01_generar_datos_retail.py   # el único paso que ejecutan los participantes
```

---

## 7. Validado de punta a punta

El generador se probó end-to-end en un workspace serverless: escribe los 5 datasets al volumen,
los relee y confirma que los conteos cuadran. Ejemplo de ejecución (con `n_clientes=800`,
`n_pedidos=4000`):

| Entidad | Filas |
|---|---|
| clientes | 825 |
| productos | 43 |
| tiendas | 25 |
| pedidos | 4.048 |
| pedidos_items | 17.870 |

Y los problemas de calidad quedan presentes y consultables (ejemplo sobre `pedidos`): 74 montos no
numéricos, 94 montos negativos y 99 pedidos con cliente huérfano — justo lo que tus reglas de
calidad deben atrapar.
