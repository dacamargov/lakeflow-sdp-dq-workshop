# Prerrequisitos para el administrador (antes del workshop)

Esta guía resume **todo lo que el admin del cliente debe preparar** antes del
workshop de **Lakeflow SDP + Data Quality con Genie code**. La idea central:

> Se comparte **solo el catálogo**. Cada participante crea su **propio esquema**
> (`lakeflow_workshop_<usuario>`) al correr el notebook y, al ser **owner** de ese
> esquema, hereda automáticamente los permisos para crear sus volúmenes, tablas y
> su pipeline. **No hay que otorgar permisos objeto por objeto.**

---

## 1. Resumen (lo mínimo)

| # | Qué | Detalle |
|---|---|---|
| 1 | **Grupo** | Un grupo (ej. `workshop_lakeflow`) con los participantes. |
| 2 | **Catálogo compartido** | Con *managed storage* y vinculado/accesible al workspace del workshop. |
| 3 | **Privilegios UC** | Solo `USE CATALOG` + `CREATE SCHEMA` sobre el catálogo, al grupo. |
| 4 | **Entitlements** | **Workspace access** (requerido); **Databricks SQL access** (opcional). |
| 5 | **Compute** | **Serverless** habilitado para notebooks y pipelines (o un cluster compartido). |

---

## 2. Por qué basta con `CREATE SCHEMA`

En Unity Catalog, **quien crea un objeto es su owner** y tiene **todos los
privilegios** sobre él. Cuando un participante corre el notebook:

1. Crea su esquema `lakeflow_workshop_<usuario>` → queda como **owner**.
2. Como owner, puede crear dentro de él: **volúmenes**, **tablas**, **streaming
   tables** y **materialized views** (las que genera el pipeline SDP).

Por eso **no** hay que otorgar `CREATE VOLUME`, `CREATE TABLE`, etc. — se heredan.

---

## 3. SQL exacto para el admin

```sql
-- (1) Crear el grupo 'workshop_lakeflow' en la consola de administración
--     y agregar a los participantes (Admin Settings > Identity and access > Groups).

-- (2) Privilegios sobre el catálogo compartido (reemplaza <CATALOGO>):
GRANT USE CATALOG   ON CATALOG <CATALOGO> TO `workshop_lakeflow`;
GRANT CREATE SCHEMA ON CATALOG <CATALOGO> TO `workshop_lakeflow`;
```

> El catálogo debe tener una **ubicación de almacenamiento administrada** (managed
> storage) para poder crear volúmenes y tablas administradas, y estar **vinculado**
> al workspace del workshop (Workspace binding, si el catálogo es *isolated*).

---

## 4. Entitlements del grupo (consola de administración)

| Entitlement | ¿Requerido? | Para qué |
|---|---|---|
| **Workspace access** | ✅ Sí | Crear notebooks, **pipelines** y jobs. |
| **Databricks SQL access** | ⬜ Opcional | Consultar resultados en el SQL editor / warehouse. |
| **Allow unrestricted cluster creation** | ❌ No | No se necesita si usan serverless. |

---

## 5. Compute

- **Opción recomendada — Serverless:** habilitar **Serverless** para notebooks y
  para pipelines (lo más simple, sin administrar clusters). Si la organización usa
  **budget policies** de serverless, asignar una al grupo `workshop_lakeflow`.
- **Alternativa — Cluster compartido:** un cluster all-purpose con permiso
  **Can Attach To** para el grupo, por si no hay serverless disponible.

---

## 6. Lo que NO hace falta

- ❌ Grants de `CREATE VOLUME` / `CREATE TABLE` (se heredan como owner del esquema).
- ❌ `ALL PRIVILEGES` sobre el catálogo.
- ❌ Rol de administrador de metastore.
- ❌ Permiso especial para "crear jobs": el workshop corre el notebook de forma
  **interactiva** (Run all). Crear **pipelines** ya viene con *Workspace access*.

---

## 7. Mensaje listo para enviar al admin

> **Asunto: Preparación de ambiente — Workshop Lakeflow SDP + Data Quality**
>
> Hola [nombre], para el workshop necesitaríamos que prepares lo siguiente en el
> workspace **[URL del workspace]** antes de la sesión:
>
> **1. Grupo de participantes**
> - Crear un grupo (ej. `workshop_lakeflow`) y agregar a los asistentes.
>
> **2. Un catálogo compartido para el workshop** (ej. `workshop_retail`)
> - Que tenga *managed storage* y esté vinculado/accesible a este workspace.
> - Otorgar al grupo solo estos dos privilegios:
>   ```sql
>   GRANT USE CATALOG   ON CATALOG workshop_retail TO `workshop_lakeflow`;
>   GRANT CREATE SCHEMA ON CATALOG workshop_retail TO `workshop_lakeflow`;
>   ```
> - Con esto, cada participante crea su **propio esquema**
>   (`lakeflow_workshop_<usuario>`) y queda como owner, con permiso para crear sus
>   volúmenes, tablas y su pipeline, sin pisar a nadie.
>
> **3. Entitlements del grupo**
> - **Workspace access** (habilita notebooks y pipelines).
> - (Opcional) **Databricks SQL access** para consultar resultados.
>
> **4. Compute**
> - **Serverless habilitado** para notebooks y pipelines (si usan budget policy de
>   serverless, asignar una al grupo). Si no hay serverless, un cluster compartido
>   con *Can Attach To* para el grupo.
>
> Nada más: no se necesitan permisos de administrador de metastore, ni grants a
> nivel de tabla/volumen (los participantes los heredan al crear su propio esquema).
> ¡Gracias!
