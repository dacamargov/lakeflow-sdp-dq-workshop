# Workshop · Lakeflow SDP + Data Quality con Genie code

Workshop hands-on para construir un pipeline de datos completo con Databricks Lakeflow Spark
Declarative Pipelines (SDP) y calidad de datos, todo con Genie code (Databricks Assistant) y sin
escribir el código a mano.

El recorrido es el siguiente:

1. Corres un notebook que genera datos crudos de un retailer LATAM ficticio (Mercado Andino) y los
   deja como archivos en un volumen de Unity Catalog.
2. A partir de ahí construyes el pipeline con Genie code. Primero el medallion bronze → silver →
   gold con expectativas de calidad, deduplicación y métricas; después los recursos que hacen que
   SDP valga la pena: CDC/SCD2, cuarentena de datos, observabilidad con el event log, ingesta
   incremental y evolución de esquema.

Cada persona corre el notebook en su propio workspace, así que todo está parametrizado — no hay
nada fijo en el código.

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

> 👤 **¿Eres el admin del cliente?** Lo que debes preparar antes del workshop (grupo, permisos de
> catálogo, entitlements y compute) está en **[`docs/prerequisitos-admin.md`](docs/prerequisitos-admin.md)**,
> con el SQL exacto y un mensaje listo para reenviar.

---

## 2. Configuración por variables de entorno

Como el notebook corre en **workspaces distintos**, cada parámetro se resuelve en este orden:

1. **Variable de entorno** (`WORKSHOP_*`) — útil para jobs / automatización.
2. **Widget** del notebook — útil para correrlo a mano desde la UI.
3. **Valor por defecto**.

| Parámetro (widget) | Variable de entorno | Default | Descripción |
|---|---|---|---|
| `catalogo`        | `WORKSHOP_CATALOG`     | `dacamargovws_catalog` | Catálogo destino, **compartido** (**cámbialo por el tuyo**). |
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
automáticamente de `current_user()` (`lakeflow_workshop_{usuario}`), con su propio volumen de `landing`
dentro. No hay nada que configurar ni coordinar.

```
dacamargovws_catalog                          ← único recurso compartido
├── lakeflow_workshop_ana_gomez               ← esquema por usuario
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

> Por ejemplo, para `ana.gomez@empresa.com` el destino sería
> `<catalogo>.lakeflow_workshop_ana_gomez`, con el volumen `landing` dentro.

Para fijarlo por variable de entorno (por ejemplo en un cluster o job):

```bash
export WORKSHOP_CATALOG=mi_catalogo
```

---

## 3. Cómo ejecutar el notebook

### Opción A — Desde la UI (recomendada para el workshop)

1. Sube `notebooks/01_generar_datos_retail.py` a tu workspace
   (**Workspace → Import → File**). Databricks lo reconoce como notebook de Python.
2. Ábrelo y conéctalo a **Serverless**.
3. **Ejecuta solo la primera celda de código** (la de configuración, con `dbutils.widgets...`).
   Esto **habilita los widgets** que aparecen arriba del notebook.
4. En el widget **`catalogo`**, escribe tu catálogo (y ajusta otros si quieres).
5. Ahora **Run all** (o corre el resto de celdas). En ~1–2 minutos deja los datos en el volumen.

> ⚠️ Los widgets **solo aparecen tras ejecutar la celda que los crea**. Si haces *Run all* de una
> vez sin pasar por el paso 3–4, el notebook usará los **valores por defecto** (incluido el catálogo
> `dacamargovws_catalog`).

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

Se crean **5 datasets crudos** en `{catalogo}.lakeflow_workshop_{usuario}`, dentro del volumen `landing`:

```
/Volumes/<catalogo>/<esquema>/landing/
├── clientes/       clientes.json       (dimensión)
├── productos/      productos.csv       (dimensión)
├── tiendas/        tiendas.csv         (dimensión)
├── pedidos/        pedidos.json        (hecho, cabecera)
└── pedidos_items/  pedidos_items.json  (hecho, grano línea)
```

Más una tabla de control: `{catalogo}.lakeflow_workshop_{usuario}._bitacora_generacion` con el conteo de registros
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

> 🔑 **Cada participante usa SUS propios valores.** Como la data vive en tu esquema
> `lakeflow_workshop_{usuario}`, los prompts apuntan a **tu** ruta de landing. Para que nadie se equivoque,
> **el notebook imprime al final (sección 12) los 4 prompts ya rellenados con tus valores** — solo
> cópialos y pégalos en Genie code dentro del pipeline.

No hace falta crear ni nombrar el pipeline a mano: **deja que Genie code lo cree** al pegar los
prompts. Aunque dos participantes terminen con un pipeline del mismo nombre, cada uno queda a nombre
de su **owner** y escribe en **su propio esquema**, así que no hay colisión de datos.

> 💡 Para encontrar el tuyo en la lista **Jobs & Pipelines → Pipelines**, filtra por
> **"Created by me"** (u owner): verás solo el tuyo aunque haya nombres repetidos.

Todos los prompts asumen un pipeline **Lakeflow SDP (Spark Declarative Pipelines)** y lo refuerzan
explícitamente, para que Genie use los idioms de SDP.

**Bronze (ingesta cruda con Auto Loader):**
> "Usando Lakeflow SDP (Spark Declarative Pipelines), crea la capa bronze con tablas de streaming que
> ingieran con Auto Loader los archivos de `<TU_RUTA_LANDING>/` — `clientes`, `pedidos` y
> `pedidos_items` son JSON, `productos` y `tiendas` son CSV con header. Deja todas las columnas como
> string y agrega columnas de linaje con el nombre de archivo y la fecha de ingesta."

**Silver (tipado, normalización y expectativas de calidad):**
> "Con Lakeflow SDP, crea la capa silver con tablas que tipen y normalicen cada entidad a partir de
> las tablas bronze. Agrega expectativas de calidad (expectations de SDP): email con formato válido,
> `monto_total` numérico y >= 0, `fecha_pedido` no futura, `descuento_pct` entre 0 y 100, cantidades
> > 0, y que `id_cliente` e `id_tienda` existan en sus dimensiones. Normaliza el país a código ISO y
> deduplica clientes y productos."

**Gold (modelo dimensional y métricas):**
> "Con Lakeflow SDP, crea la capa gold a partir de las tablas silver, con tablas de dimensiones
> limpias de clientes, productos y tiendas, un hecho de ventas al grano de línea, y KPIs mensuales de
> ventas por tienda y por categoría. Usa `CLUSTER BY` en vez de particionar."

**Observabilidad de calidad:**
> "Con Lakeflow SDP, crea la capa de observabilidad de calidad: primero publica el event log del
> pipeline en Unity Catalog (en la configuración del pipeline, como tabla dentro de tu esquema) y
> luego crea una vista en SQL que lea esa tabla del event log y publique, por dataset y por
> expectativa, cuántos registros pasaron y cuántos fallaron, comparándolos contra
> `{catalogo}.lakeflow_workshop_{usuario}._bitacora_generacion` para mostrar *inyectado vs.
> detectado*. Calcula los conteos en vivo desde las tablas (sin números fijos) y usa una vista
> normal, no materializada."

> 💡 Publicar el event log en UC lo deja como **tabla consultable y compartible**; la función
> `event_log(...)` directa solo la puede leer el owner del pipeline. Una **vista normal** siempre
> refleja la última corrida terminada (una materializada quedaría una actualización atrasada).

> El objetivo pedagógico: ver cómo Genie code arma el DAG, la incrementalidad y la calidad
> declarativa con muchísimo menos código que un pipeline tradicional.

---

Con el pipeline base en marcha, el workshop sigue hacia los **recursos de SDP más allá del
medallion** — lo que realmente diferencia a Lakeflow SDP. Todos se siguen construyendo con Genie
code dentro del mismo pipeline, apuntando a tu esquema `lakeflow_workshop_<usuario>`.

Los recursos de CDC/SCD2, ingesta incremental y evolución de esquema trabajan sobre un segundo lote
de datos: corre el notebook [`notebooks/02_lote_incremental.py`](notebooks/02_lote_incremental.py)
después del `01` (preserva el landing y solo agrega archivos nuevos, con la columna `cupon_descuento`
y versiones actualizadas de clientes). Se ejecuta igual que el notebook 01.

## 6. CDC / SCD Tipo 2 con `AUTO CDC`

Historizar los cambios de una dimensión **sin escribir MERGE**, usando las versiones actualizadas de
clientes que genera el notebook 02.

> "Con Lakeflow SDP, crea una dimensión `dim_clientes_scd2` de tipo SCD Tipo 2 usando AUTO CDC a
> partir de la tabla bronze de clientes. Usa `id_cliente` como clave y `fecha_actualizacion` como
> columna de secuencia. Mantén el historial de cambios de `segmento` y `nivel_lealtad`, con columnas
> de inicio y fin de vigencia y una bandera de la versión actual."

Cada cliente actualizado queda con dos filas (la versión antigua cerrada y la nueva vigente). Como
variante, pide una versión SCD Tipo 1 (`dim_clientes_actual`) para comparar.

## 7. Cuarentena de datos malos

En vez de `expect_or_drop` (que descarta la fila), enruta las filas malas a una tabla de cuarentena
para auditarlas o reprocesarlas.

> "Con Lakeflow SDP, a partir del bronze de pedidos crea dos tablas: `pedidos_validos` con las filas
> que pasan las expectativas (monto_total numérico y >= 0, id_cliente e id_tienda existentes, moneda
> válida) y `pedidos_cuarentena` con las filas que fallan, agregando una columna `motivo_falla` que
> indique qué regla se violó y la fecha de detección."

Al final, `count(pedidos_validos) + count(pedidos_cuarentena)` = total del bronze: no se pierde nada.

## 8. Observabilidad con el Event Log del pipeline

SDP graba un event log con las métricas de cada expectativa; publícalo como tabla para cerrar el
ciclo de calidad.

> "Con Lakeflow SDP, crea una tabla `calidad_expectativas` que lea el event log del pipeline
> (eventos de tipo `flow_progress`) y extraiga, por dataset y por expectativa, cuántos registros
> pasaron y cuántos fallaron. Luego crea una vista que compare esos números contra la tabla
> `_bitacora_generacion` (errores inyectados) para mostrar inyectado vs. detectado."

El event log se consulta con la función `event_log(...)`:
`SELECT * FROM event_log(TABLE(<tabla_del_pipeline>))`.

## 9. Ingesta incremental (streaming de verdad)

Después de correr el notebook 02 (que agrega `pedidos_lote2.json`), corre de nuevo tu pipeline y
observa en el event log que Auto Loader procesó solo los archivos nuevos.

> "Con Lakeflow SDP, asegúrate de que el bronze de pedidos sea una streaming table con Auto Loader,
> de modo que una nueva ejecución solo procese los archivos nuevos que llegaron al directorio de
> landing, sin reprocesar los antiguos."

El `num_output_rows` del bronze en el segundo update debería ser ≈ solo las filas del lote 2.

## 10. Evolución de esquema

El lote 2 de `pedidos` trae la columna nueva `cupon_descuento`; muestra que el pipeline la absorbe
sin romperse.

> "Con Lakeflow SDP y Auto Loader, configura la ingesta de pedidos con evolución de esquema
> (`cloudFiles.schemaEvolutionMode = addNewColumns`) para que la nueva columna `cupon_descuento` se
> agregue automáticamente al bronze cuando aparezca en los nuevos archivos."

La columna aparece en el bronze: nula en las filas antiguas y con valor en las nuevas.

## 11. Calidad dirigida por metadatos

En vez de reglas fijas en el código, lee las expectativas de una tabla de configuración, para
escalar a decenas de reglas.

> "Con Lakeflow SDP, crea una tabla `reglas_calidad` (columna_objetivo, nombre_regla, condicion_sql,
> accion) con varias reglas de pedidos, y aplica esas expectativas dinámicamente en el silver leyendo
> las reglas de esa tabla (usando `expect_all`), en vez de escribirlas una por una."

## 12. Append flows (unificar varias fuentes)

Simula que cada país llega como un archivo separado y únelo todo en una sola streaming table.

> "Con Lakeflow SDP, crea una streaming table `pedidos_todos` y varios append flows que ingieran
> diferentes archivos/carpetas de pedidos (por ejemplo, por lote) hacia la misma tabla destino, sin
> duplicar la definición de la tabla."

## 13. Genie Space sobre la capa gold

Fuera del pipeline, crea un Genie Space sobre las tablas gold para preguntar en lenguaje natural, y
así cerrar el hilo: construiste el pipeline con Genie code y ahora lo consultas con Genie.

> Pasos: **Genie → New**, selecciona el esquema `lakeflow_workshop_<usuario>` y las tablas gold
> (hecho de ventas + dimensiones). Prueba preguntas como *"ventas por país el último mes"* o
> *"top 10 productos por ingreso"*.

## 14. Dashboard AI/BI sobre la capa gold

Crea un dashboard AI/BI con los KPIs mensuales de la capa gold.

> Pasos: **Dashboards → Create**, usa el SQL warehouse y las tablas gold. Gráficos sugeridos:
> ingreso por mes, ingreso por tienda/país y mix por categoría.

> Secuencia sugerida: pipeline base (sección 5) → cuarentena (7) y event log (8) → notebook 02 e
> incremental (9) + evolución de esquema (10) → SCD2 (6) → cierre con Genie Space (13) y dashboard (14).

---

## 15. Estructura del repo

```
lakeflow-sdp-dq-workshop/
├── README.md
├── docs/
│   └── prerequisitos-admin.md       # qué prepara el admin del cliente (permisos, grupo, compute)
└── notebooks/
    ├── 01_generar_datos_retail.py   # genera los datos crudos de retail en tu volumen
    └── 02_lote_incremental.py       # segundo lote: alimenta CDC/SCD2, incremental y evolución de esquema
```
