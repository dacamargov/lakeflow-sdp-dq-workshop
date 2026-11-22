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

Es la **boleta de calificaciones** de tu capa de calidad. Cruza los errores que el notebook 01
inyectó a propósito (registrados en `_bitacora_generacion`) contra los que tus expectativas
realmente atraparon, para responder *¿mis reglas están cachando la basura que planté?*. El resultado
es una tabla consultable —`inyectado | detectado | % detección` por regla— que puedes graficar y
compartir. En producción, donde no tienes esa "verdad" inyectada, el mismo patrón sirve para
**monitorear la tasa de fallas por regla en el tiempo** y detectar cuándo se degrada una fuente.

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

> El objetivo pedagógico de toda esta sección: ver cómo Genie code arma el DAG, la incrementalidad y
> la calidad declarativa con muchísimo menos código que un pipeline tradicional.

---

Con el pipeline base en marcha, el workshop sigue hacia los **recursos de SDP más allá del
medallion** — lo que realmente diferencia a Lakeflow SDP. Todos se siguen construyendo con Genie
code dentro del mismo pipeline, apuntando a tu esquema `lakeflow_workshop_<usuario>`.

Los recursos de CDC/SCD2, ingesta incremental y evolución de esquema trabajan sobre un segundo lote
de datos: corre el notebook [`notebooks/02_lote_incremental.py`](notebooks/02_lote_incremental.py)
después del `01` (preserva el landing y solo agrega archivos nuevos, con la columna `cupon_descuento`
y versiones actualizadas de clientes). Se ejecuta igual que el notebook 01.

## 6. CDC / SCD Tipo 2 con `AUTO CDC`

**Qué es.** *CDC* (Change Data Capture) es capturar los cambios que llegan de una fuente (altas y
modificaciones). *SCD Tipo 2* es la forma de guardar una dimensión conservando el **historial**: en
vez de sobrescribir, cierra la versión vieja y abre una nueva. *AUTO CDC* (`apply_changes`) es el
idiom de SDP que aplica esos cambios por ti, **sin escribir un `MERGE`**, sobre las versiones
actualizadas de clientes que genera el notebook 02.

> "Con Lakeflow SDP, crea una dimensión de clientes `dim_clientes_scd2` de tipo SCD Tipo 2, usando
> la captura de cambios automática que SDP ofrece para esto. A partir de la tabla bronze de clientes,
> identifica cada cliente por `id_cliente` y usa `fecha_actualizacion` para ordenar los cambios en el
> tiempo. Mantén el historial de `segmento` y `nivel_lealtad`: cuando alguno de esos valores cambie,
> conserva la versión anterior y abre una nueva. **Deja que SDP maneje por sí solo el control de
> vigencia de cada versión con sus columnas estándar; no agregues columnas de fecha ni banderas
> propias.**"

**Qué esperar.** Una tabla `dim_clientes_scd2` con las columnas de vigencia `__START_AT` / `__END_AT`
que **SDP agrega automáticamente** (no se escriben a mano). Cada cliente actualizado queda con **dos
filas**: la antigua cerrada (`__END_AT` con fecha) y la nueva vigente (`__END_AT` en `NULL`); los
clientes sin cambios tienen una sola fila. Con los defaults se actualiza ~10% de los clientes. Como
variante, pide una versión SCD Tipo 1 (`dim_clientes_actual`) para comparar: ahí cada cliente tendría
una sola fila, sin historia.

> 💡 Si Genie code genera la tabla **sin** `__START_AT` / `__END_AT` (p. ej. una sola fila por
> cliente, o columnas de fecha con nombres propios como `fecha_inicio_vigencia`), es que **no** usó
> AUTO CDC nativo: quedó como SCD Tipo 1. Verifícalo con `DESCRIBE TABLE dim_clientes_scd2;` y, si
> falta, vuelve a pedirle el recurso insistiendo en `STORED AS SCD TYPE 2` / `stored_as_scd_type=2`.
> Para ver el historial de los clientes con cambios:
>
> ```sql
> SELECT id_cliente, segmento, nivel_lealtad, __START_AT, __END_AT
> FROM dim_clientes_scd2
> WHERE id_cliente IN (
>   SELECT id_cliente FROM dim_clientes_scd2 GROUP BY id_cliente HAVING count(*) > 1)
> ORDER BY id_cliente, __START_AT;
> ```

## 7. Cuarentena de datos malos

**Qué es.** La *cuarentena* es un patrón de calidad: en vez de descartar las filas que fallan las
reglas (como haría `expect_or_drop`) o dejar que rompan el pipeline, las **desvías a una tabla
aparte** para auditarlas o reprocesarlas. Así no pierdes nada y conservas el porqué de cada rechazo.

> "Con Lakeflow SDP, a partir del bronze de pedidos crea dos tablas: `pedidos_validos` con las filas
> que pasan las expectativas (monto_total numérico y >= 0, id_cliente e id_tienda existentes, moneda
> válida) y `pedidos_cuarentena` con las filas que fallan, agregando una columna `motivo_falla` que
> indique qué regla se violó y la fecha de detección."

**Qué esperar.** Dos tablas: `pedidos_validos` y `pedidos_cuarentena` (esta última con la columna
`motivo_falla`). Se cumple que `count(pedidos_validos) + count(pedidos_cuarentena)` = total del
bronze: **no se pierde ninguna fila**, solo se separan las buenas de las malas.

## 8. Observabilidad con el Event Log del pipeline

**Qué es.** El *event log* es una bitácora que **SDP graba por sí solo** en cada corrida: estados de
los flujos, linaje y, sobre todo, las métricas de calidad (cuántos registros pasaron y fallaron cada
expectativa). Publicarlo como tabla te permite monitorear la calidad con SQL o un dashboard. (Es el
mismo mecanismo del ítem de observabilidad de la sección 5, aquí como módulo aparte.)

> "Con Lakeflow SDP, crea una tabla `calidad_expectativas` que lea el event log del pipeline
> (eventos de tipo `flow_progress`) y extraiga, por dataset y por expectativa, cuántos registros
> pasaron y cuántos fallaron. Luego crea una vista que compare esos números contra la tabla
> `_bitacora_generacion` (errores inyectados) para mostrar inyectado vs. detectado."

**Qué esperar.** Una tabla con el conteo de pasados/fallados por expectativa y dataset, y una vista
que la cruza contra `_bitacora_generacion` (inyectado vs. detectado). El event log se consulta con la
función `event_log(...)`: `SELECT * FROM event_log(TABLE(<tabla_del_pipeline>))`.

## 9. Ingesta incremental (streaming de verdad)

**Qué es.** *Ingesta incremental* significa procesar **solo los datos nuevos** en cada corrida, sin
reprocesar lo ya ingerido. Con *Auto Loader* sobre una *streaming table*, SDP lleva el control de qué
archivos ya leyó, así que cuando llega un lote nuevo solo toma esos.

Después de correr el notebook 02 (que agrega `pedidos_lote2.json`), corre de nuevo tu pipeline y
observa en el event log que Auto Loader procesó solo los archivos nuevos.

> "Con Lakeflow SDP, asegúrate de que el bronze de pedidos sea una streaming table con Auto Loader,
> de modo que una nueva ejecución solo procese los archivos nuevos que llegaron al directorio de
> landing, sin reprocesar los antiguos."

**Qué esperar.** En el segundo update, el `num_output_rows` del bronze de pedidos debería ser ≈ solo
las filas del lote 2 (no el total), confirmando que no reprocesó lo viejo.

## 10. Evolución de esquema

**Qué es.** La *evolución de esquema* es la capacidad de Auto Loader de **adaptarse cuando el origen
cambia de forma** —por ejemplo, aparece una columna nueva— sin que tengas que redefinir nada ni que
el pipeline falle. Con `schemaEvolutionMode = addNewColumns`, la columna nueva se suma sola.

El lote 2 de `pedidos` trae la columna nueva `cupon_descuento`; muestra que el pipeline la absorbe
sin romperse.

> "Con Lakeflow SDP y Auto Loader, configura la ingesta de pedidos con evolución de esquema
> (`cloudFiles.schemaEvolutionMode = addNewColumns`) para que la nueva columna `cupon_descuento` se
> agregue automáticamente al bronze cuando aparezca en los nuevos archivos."

**Qué esperar.** La columna `cupon_descuento` aparece en el bronze: **nula en las filas antiguas**
(lote 1, que no la traían) y **con valor en las nuevas** (lote 2). Auto Loader puede reiniciar el
update al detectar el cambio de esquema y retomarlo solo — es el comportamiento esperado, no un
error.

### Modo live (opcional): verlo en vivo con un flujo continuo

El lote 2 demuestra la ingesta incremental (punto 9) y la evolución de esquema (punto 10) de forma
**estática**: dejas los archivos y re-ejecutas. Si quieres verlo **en vivo** —archivos cayendo cada
pocos segundos y Auto Loader reaccionando— corre el notebook
[`notebooks/03_flujo_continuo.py`](notebooks/03_flujo_continuo.py).

Durante una ventana acotada escribe pequeñas tandas de pedidos nuevos al landing cada
`intervalo_segundos`, y **a partir de la tanda `tanda_nueva_columna` empieza a incluir una columna
nueva (`canal_captacion`)**. Pon tu pipeline en **modo continuo** (Settings → *Continuous*) y
arráncalo; verás en el event log cómo ingiere solo las tandas nuevas y cómo absorbe la columna nueva
cuando aparece.

> ⚠️ Es un modo de **demostración** (ideal para que lo maneje el presentador). Corre por
> `duracion_minutos` y se detiene solo; si pones el pipeline en continuo, **apágalo al terminar**
> para no dejar compute encendido.

## 11. Calidad dirigida por metadatos

**Qué es.** En vez de escribir cada expectativa fija en el código, las *defines como datos* en una
tabla de configuración y el pipeline las **lee y aplica dinámicamente** (con `expect_all`). Así
escalas a decenas de reglas y las cambias sin tocar el código del pipeline.

> "Con Lakeflow SDP, crea una tabla `reglas_calidad` (columna_objetivo, nombre_regla, condicion_sql,
> accion) con varias reglas de pedidos, y aplica esas expectativas dinámicamente en el silver leyendo
> las reglas de esa tabla (usando `expect_all`), en vez de escribirlas una por una."

**Qué esperar.** Una tabla `reglas_calidad` con las reglas como filas y un silver cuyas expectativas
salen de esa tabla. Agregar o quitar una regla es editar la tabla, no el pipeline; las métricas de
cada regla aparecen en el event log igual que las expectativas escritas a mano.

## 12. Append flows (unificar varias fuentes)

**Qué es.** Un *append flow* permite que **varios flujos independientes alimenten una misma streaming
table** sin duplicar su definición. Sirve para unir fuentes que llegan por separado (por país, por
lote, por sistema) en una sola tabla destino.

Simula que cada país llega como un archivo separado y únelo todo en una sola streaming table.

> "Con Lakeflow SDP, crea una streaming table `pedidos_todos` y varios append flows que ingieran
> diferentes archivos/carpetas de pedidos (por ejemplo, por lote) hacia la misma tabla destino, sin
> duplicar la definición de la tabla."

**Qué esperar.** Una sola tabla `pedidos_todos` alimentada por varios flujos; su conteo es la **suma
de todas las fuentes** unidas, con una única definición de la tabla destino.

## 13. Genie Space sobre la capa gold

**Qué es.** Un *Genie Space* es un espacio de Databricks donde haces **preguntas en lenguaje natural**
sobre tus tablas y Genie genera el SQL por ti. Cierra el hilo del workshop: construiste el pipeline
con Genie code y ahora lo consultas con Genie.

> Pasos: **Genie → New**, selecciona el esquema `lakeflow_workshop_<usuario>` y las tablas gold
> (hecho de ventas + dimensiones). Prueba preguntas como *"ventas por país el último mes"* o
> *"top 10 productos por ingreso"*.

**Qué esperar.** Un espacio que responde tus preguntas con tablas/gráficos y te muestra el SQL que
generó sobre la capa gold.

## 14. Dashboard AI/BI sobre la capa gold

**Qué es.** Un *dashboard AI/BI* es el tablero visual de Databricks para publicar los KPIs de la capa
gold a usuarios de negocio, con gráficos y filtros.

Crea un dashboard AI/BI con los KPIs mensuales de la capa gold.

> Pasos: **Dashboards → Create**, usa el SQL warehouse y las tablas gold. Gráficos sugeridos:
> ingreso por mes, ingreso por tienda/país y mix por categoría.

**Qué esperar.** Un dashboard con los KPIs mensuales (ingreso por mes, por tienda/país y mix por
categoría) consultando directamente la capa gold.

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
    ├── 02_lote_incremental.py       # segundo lote: alimenta CDC/SCD2, incremental y evolución de esquema
    └── 03_flujo_continuo.py         # (opcional) modo live: pedidos cada pocos segundos + columna nueva en vivo
```
