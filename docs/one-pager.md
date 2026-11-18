# Workshop Lakeflow SDP + Data Quality

### Construye un pipeline de datos completo en Databricks, de principio a fin, con Genie code

Un taller práctico donde cada participante levanta un pipeline productivo —ingesta, calidad,
modelo dimensional y analítica— **sin escribir el código a mano**. Todo se arma con Genie code
(Databricks Assistant) sobre Lakeflow Spark Declarative Pipelines (SDP), la evolución de DLT.

---

## Para quién es

Ingenieros de datos, analytics engineers y arquitectos que quieran ver, con las manos en el
teclado, cómo Databricks reduce el esfuerzo de construir y mantener pipelines confiables.

**No se requiere experiencia previa con SDP.** Sí ayuda estar cómodo con SQL y conceptos básicos
de datos.

---

## Qué vas a construir

Partiendo de datos crudos de un retailer LATAM ficticio, cada quien arma en su propio espacio:

- **Ingesta bronze** con Auto Loader y columnas de linaje (de dónde vino cada registro).
- **Capa silver** tipada y normalizada, con **expectativas de calidad** declarativas (emails
  válidos, montos no negativos, fechas coherentes, integridad referencial).
- **Capa gold** con dimensiones limpias, un hecho de ventas y KPIs mensuales listos para consumo.
- **Analítica en lenguaje natural** sobre el resultado, con un Genie Space y un dashboard AI/BI.

Y, lo que de verdad diferencia a SDP, los patrones que normalmente cuestan semanas:

| Capacidad | Qué resuelve |
|---|---|
| **CDC / SCD Tipo 2** | Historiza cambios de una dimensión sin escribir `MERGE`. |
| **Cuarentena de datos** | Aísla filas malas para auditar o reprocesar, sin perder nada. |
| **Observabilidad de calidad** | Mide, desde el event log, cuántos registros pasan y fallan cada regla. |
| **Ingesta incremental** | Procesa solo los archivos nuevos, sin reprocesar lo viejo. |
| **Evolución de esquema** | Absorbe columnas nuevas automáticamente, sin romper el pipeline. |
| **Calidad por metadatos** | Define decenas de reglas en una tabla de configuración, no en el código. |

---

## Agenda sugerida

1. **Contexto y setup** — qué es Lakeflow SDP y por qué importa; preparar el entorno.
2. **Generar los datos** — un notebook deja los datos crudos (con "suciedad" intencional) en tu volumen.
3. **Medallion con Genie code** — bronze → silver → gold, paso a paso, con prompts guiados.
4. **Más allá del medallion** — CDC/SCD2, cuarentena, observabilidad, incremental, evolución de esquema y calidad por metadatos.
5. **Cierre analítico** — Genie Space y dashboard AI/BI sobre la capa gold.

**Duración estimada:** media jornada (3–4 horas), según el ritmo del grupo.

---

## Qué te llevas

- La experiencia de construir un pipeline real **con muchísimo menos código** que un enfoque tradicional.
- Un repositorio con todos los notebooks y prompts para repetir el ejercicio con tus propios datos.
- Criterio para decidir cuándo y cómo aplicar SDP en tus casos de uso.

---

## Lo que necesitas (participante)

- Un workspace Databricks con **Unity Catalog** y **compute Serverless**.
- Un catálogo donde tengas permisos para crear esquema y volumen.
- **Genie code / Databricks Assistant** habilitado.

> Cada participante trabaja en su propio esquema aislado, así que varios pueden correr el taller en
> paralelo sobre el mismo catálogo sin pisarse. El admin del cliente tiene una guía dedicada con el
> SQL exacto de preparación.
