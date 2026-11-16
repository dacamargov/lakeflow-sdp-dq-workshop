# Módulos avanzados de Lakeflow SDP (opcional)

Después de construir el pipeline básico bronze → silver → gold (ver `README.md`, sección 5), estos
módulos llevan el workshop a lo más interesante de **Lakeflow SDP**. Todos se siguen haciendo
**con Genie code** dentro del pipeline.

> 🔁 **Prerrequisito de datos:** los módulos de **ingesta incremental**, **evolución de esquema** y
> **CDC/SCD2** necesitan un segundo lote de datos. Corre el notebook
> [`notebooks/02_lote_incremental.py`](../notebooks/02_lote_incremental.py) **después** del `01`
> (agrega archivos `*_lote2` a tu landing, sin borrar nada, incluyendo una columna nueva
> `cupon_descuento` y versiones actualizadas de clientes).

Cada prompt asume que ya estás dentro de tu pipeline Lakeflow SDP, apuntando a tu esquema
`lakeflow_workshop_<usuario>`.

---

## 1. CDC / SCD Tipo 2 con `AUTO CDC` ⭐

La feature más marcante de SDP: historizar cambios de una dimensión **sin escribir MERGE**. Usa las
versiones actualizadas de clientes que genera el notebook 02.

> **Prompt:** "Con Lakeflow SDP, crea una dimensión `dim_clientes_scd2` de tipo SCD Tipo 2 usando
> AUTO CDC a partir de la tabla bronze de clientes. Usa `id_cliente` como clave y
> `fecha_actualizacion` como columna de secuencia. Quiero mantener el historial de cambios de
> `segmento` y `nivel_lealtad`, con columnas de inicio y fin de vigencia y una bandera de la versión
> actual."

- Qué observar: al releer, cada cliente actualizado tiene **dos filas** (la versión antigua cerrada
  y la nueva vigente). En SCD Tipo 1 solo quedaría la última.
- Variante: pide también una versión **SCD Tipo 1** (`dim_clientes_actual`) para comparar.

---

## 2. Cuarentena de datos malos (no solo descartar) ⭐

En vez de `expect_or_drop` (que bota la fila), enruta las filas malas a una tabla de cuarentena para
auditoría/reproceso.

> **Prompt:** "Con Lakeflow SDP, a partir del bronze de pedidos crea dos tablas: `pedidos_validos`
> con las filas que pasan las expectativas (monto_total numérico y >= 0, id_cliente e id_tienda
> existentes, moneda válida) y `pedidos_cuarentena` con las filas que **fallan**, agregando una
> columna `motivo_falla` que indique qué regla se violó y la fecha de detección."

- Qué observar: `count(pedidos_validos) + count(pedidos_cuarentena)` = total del bronze. No se pierde
  nada.

---

## 3. Observabilidad con el Event Log del pipeline ⭐

SDP graba un **event log** con métricas de cada expectativa. Se puede publicar como tabla y cerrar el
ciclo de calidad.

> **Prompt:** "Con Lakeflow SDP, crea una tabla `calidad_expectativas` que lea el event log del
> pipeline (eventos de tipo `flow_progress`) y extraiga, por dataset y por expectativa, cuántos
> registros pasaron y cuántos fallaron. Luego crea una vista que compare esos números contra la
> tabla `_bitacora_generacion` (errores inyectados) para mostrar **inyectado vs. detectado**."

- Tip: el event log se consulta con la función `event_log(...)` o se publica vía la configuración
  `event_log` del pipeline. En SQL: `SELECT * FROM event_log(TABLE(<tabla_del_pipeline>))`.

---

## 4. Ingesta incremental (streaming de verdad)

Después de correr el notebook 02 (que agrega `pedidos_lote2.json`), corre **de nuevo** tu pipeline y
observa en el event log que Auto Loader procesó **solo los archivos nuevos**.

> **Prompt (si aún no tienes bronze streaming):** "Con Lakeflow SDP, asegúrate de que el bronze de
> pedidos sea una **streaming table** con Auto Loader, de modo que una nueva ejecución solo procese
> los archivos nuevos que llegaron al directorio de landing, sin reprocesar los antiguos."

- Qué observar: `num_output_rows` del bronze en el segundo update ≈ solo las filas del lote 2.

---

## 5. Evolución de esquema

El lote 2 de `pedidos` trae la columna nueva `cupon_descuento`. Muestra que el pipeline la absorbe
sin romperse.

> **Prompt:** "Con Lakeflow SDP y Auto Loader, configura la ingesta de pedidos con **evolución de
> esquema** (`cloudFiles.schemaEvolutionMode = addNewColumns`) para que la nueva columna
> `cupon_descuento` se agregue automáticamente al bronze cuando aparezca en los nuevos archivos."

- Qué observar: la columna aparece en el bronze, **nula** en las filas antiguas y con valor en las
  nuevas.

---

## 6. Calidad dirigida por metadatos

En vez de reglas fijas en el código, lee las expectativas de una **tabla de configuración** — escala
a decenas de reglas.

> **Prompt:** "Con Lakeflow SDP, crea una tabla `reglas_calidad` (columna_objetivo, nombre_regla,
> condicion_sql, accion) con varias reglas de pedidos, y aplica esas expectativas dinámicamente en el
> silver leyendo las reglas de esa tabla (usando `expect_all`), en vez de escribirlas una por una."

---

## 7. Append flows (unificar varias fuentes)

Simula que cada país llega como un archivo separado y únelo todo en una sola streaming table.

> **Prompt:** "Con Lakeflow SDP, crea una streaming table `pedidos_todos` y varios **append flows**
> que ingieran diferentes archivos/carpetas de pedidos (por ejemplo, por lote) hacia la misma tabla
> destino, sin duplicar la definición de la tabla."

---

## 8. Genie Space sobre la capa gold (cierra el tema "Genie")

Fuera del pipeline, crea un **Genie Space** sobre las tablas gold para preguntar en lenguaje natural.

> Pasos: **Genie → New → selecciona el esquema `lakeflow_workshop_<usuario>`** y las tablas gold
> (hecho de ventas + dimensiones). Haz preguntas como *"ventas por país el último mes"* o
> *"top 10 productos por ingreso"*. Amarra el hilo: construiste el pipeline con Genie code y ahora
> consultas con Genie.

---

## 9. Dashboard AI/BI sobre la gold

Crea un dashboard AI/BI con los KPIs mensuales de la capa gold.

> Pasos: **Dashboards → Create → usa el SQL warehouse** y las tablas gold. Gráficos sugeridos:
> ingreso por mes, ingreso por tienda/país, mix por categoría. Opcionalmente pídele al Assistant que
> genere las queries.

---

### Secuencia sugerida para el workshop

1. Pipeline base bronze/silver/gold (README sección 5).
2. **Cuarentena (#2)** y **Event Log (#3)** → tema Data Quality.
3. Correr notebook 02 → **Incremental (#4)** + **Evolución de esquema (#5)**.
4. **SCD2 (#1)** con los clientes actualizados.
5. Remate: **Genie Space (#8)** y/o **Dashboard (#9)**.
