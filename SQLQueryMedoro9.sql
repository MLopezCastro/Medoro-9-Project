/*
===========================================================================
🧱 VISTA 1 – ConCubo3Años (versión SQL Server 2022)
===========================================================================

📌 DESCRIPCIÓN:
Esta vista filtra la tabla ConCubo dejando únicamente los registros 
de los últimos 3 años exactos desde hoy (GETDATE()).

Además:
✅ Convierte fechas de texto a datetime
✅ Corrige desfase histórico (-2 días)
✅ Calcula duración en horas
✅ Clasifica duración por tipo de estado
✅ Limpia ID y extrae clave numérica
✅ Convierte fechas a texto plano y formato legible
✅ Conserva columnas relevantes como Turno, Maquinista, etc.

===========================================================================
*/

CREATE OR ALTER VIEW ConCubo3Años AS
WITH DatosParseados AS (
    SELECT *,
        -- 🕓 Conversión robusta de fechas
        TRY_CAST(Inicio AS DATETIME) AS InicioDT,
        TRY_CAST(Fin AS DATETIME) AS FinDT
    FROM ConCubo
    WHERE
        -- 📅 Solo últimos 3 años desde hoy
        TRY_CAST(Inicio AS DATETIME) >= DATEADD(YEAR, -3, CAST(GETDATE() AS DATE))

        -- 🔐 Elimina IDs no numéricos como 'Rotatek 700'
        AND ISNUMERIC(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID))) = 1
),
HorasCalculadas AS (
    SELECT *,
        -- ⏱️ Calcula duración total en horas (float)
        DATEDIFF(SECOND, InicioDT, FinDT) / 3600.0 AS Total_Horas
    FROM DatosParseados
)
SELECT
    -- 🔑 Clave de OT original y limpia
    ID,
    TRY_CAST(SUBSTRING(ID, PATINDEX('%[0-9]%', ID), LEN(ID)) AS INT) AS ID_Limpio,

    Renglon,
    Estado,

    -- 🗓️ Fechas corregidas por desfase de -2 días
    DATEADD(DAY, -2, InicioDT) AS Inicio_Corregido,
    DATEADD(DAY, -2, FinDT) AS Fin_Corregido,

    -- 📄 Fechas en texto legible plano
    CONVERT(VARCHAR(16), DATEADD(DAY, -2, InicioDT), 120) AS Inicio_Legible_Texto,
    CONVERT(VARCHAR(16), DATEADD(DAY, -2, FinDT), 120) AS Fin_Legible_Texto,

    -- 📆 Fecha agrupada (sin hora)
    CONVERT(DATE, DATEADD(DAY, -2, InicioDT)) AS Fecha,

    -- ⏱️ Duración total y discriminada por tipo de estado
    Total_Horas,
    CASE WHEN Estado = 'Producción' THEN Total_Horas ELSE 0 END AS Horas_Produccion,
    CASE WHEN Estado = 'Preparación' THEN Total_Horas ELSE 0 END AS Horas_Preparacion,
    CASE WHEN Estado = 'Maquina Parada' THEN Total_Horas ELSE 0 END AS Horas_Parada,
    CASE WHEN Estado = 'Mantenimiento' THEN Total_Horas ELSE 0 END AS Horas_Mantenimiento,

    -- 📦 Producción buena y mala
    TRY_CAST(CantidadBuenosProducida AS FLOAT) AS CantidadBuenosProducida,
    TRY_CAST(CantidadMalosProducida AS FLOAT) AS CantidadMalosProducida,

    -- 👷‍♂️ Datos de contexto operativo
    Turno,
    Maquinista,
    Operario,
    codproducto,
    motivo

FROM HorasCalculadas;


----------------------------------------------------------------------------------------------------------------------------------

/*
===========================================================================
🧱 VISTA 2 – ConCubo3AñosSec
===========================================================================

📌 DESCRIPCIÓN:
Esta vista toma los datos ya corregidos de `ConCubo3Años` y calcula la 
duración real del evento en horas a partir de `Inicio_Corregido` y 
`Fin_Corregido`. Luego separa esas horas por tipo de estado.

No se modificó ninguna lógica. Solo se actualizó el nombre de la vista
y su fuente para que funcione correctamente en el flujo de Medoro 9.

===========================================================================
*/

CREATE OR ALTER VIEW ConCubo3AñosSec AS
WITH Base AS (
    SELECT *,
        -- ⏱️ Calcula duración real entre fechas corregidas
        DATEDIFF(SECOND, Inicio_Corregido, Fin_Corregido) / 3600.0 AS Duracion_Horas
    FROM ConCubo3Años
)
SELECT
    ID,
    ID_Limpio,
    Renglon,
    Estado,
    Inicio_Corregido,
    Fin_Corregido,
    Inicio_Legible_Texto,
    Fin_Legible_Texto,
    CONVERT(DATE, Inicio_Corregido) AS Fecha,
    Duracion_Horas AS Total_Horas,

    -- ⏱️ Clasificación por tipo de estado
    CASE WHEN Estado = 'Producción' THEN Duracion_Horas ELSE 0 END AS Horas_Produccion,
    CASE WHEN Estado = 'Preparación' THEN Duracion_Horas ELSE 0 END AS Horas_Preparacion,
    CASE WHEN Estado = 'Maquina Parada' THEN Duracion_Horas ELSE 0 END AS Horas_Parada,
    CASE WHEN Estado = 'Mantenimiento' THEN Duracion_Horas ELSE 0 END AS Horas_Mantenimiento,

    -- 📦 Cantidades producidas
    CantidadBuenosProducida,
    CantidadMalosProducida,

    -- 👷‍♂️ Datos operativos
    Turno,
    Maquinista,
    Operario,
    codproducto,
    Motivo

FROM Base;


-------------------------------------------------------------------------------------------------------------------------------
/*
===========================================================================
🧱 VISTA 3 – ConCubo3AñosSecFlag
===========================================================================

📌 DESCRIPCIÓN:
Esta vista detecta los bloques reales de preparación por OT (`ID_Limpio`) y 
máquina (`Renglon`), asignando:

✅ Un número de secuencia para ordenar los eventos por OT  
✅ Un "flag" que marca el comienzo de una nueva preparación  
✅ Una secuencia acumulativa de bloques de preparación por ID  

Esto permite analizar la eficiencia de los procesos, identificar interrupciones 
o preparaciones dobles, y preparar KPIs visuales.

===========================================================================
*/

CREATE OR ALTER VIEW ConCubo3AñosSecFlag AS

-- Primer CTE: agrega número de secuencia por ID y máquina
WITH Base AS (
    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY ID_Limpio, Renglon
            ORDER BY Inicio_Corregido ASC
        ) AS Nro_Secuencia
    FROM ConCubo3AñosSec
),

-- Segundo CTE: marca inicio de bloque de preparación
PrepFlag AS (
    SELECT *,
        CASE 
            WHEN Estado = 'Preparación' AND (
                LAG(Estado) OVER (
                    PARTITION BY ID_Limpio, Renglon 
                    ORDER BY Inicio_Corregido
                ) IS DISTINCT FROM 'Preparación'
            ) THEN 1
            ELSE 0
        END AS FlagPreparacion
    FROM Base
),

-- Tercer CTE: crea secuencia acumulada de bloques de preparación
PrepSecuencia AS (
    SELECT *,
        SUM(FlagPreparacion) OVER (
            PARTITION BY ID_Limpio, Renglon
            ORDER BY Inicio_Corregido
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS SecuenciaPreparacion
    FROM PrepFlag
)

-- Resultado final
SELECT
    ID,
    ID_Limpio,
    Renglon,
    Estado,
    Inicio_Corregido,
    Fin_Corregido,
    Inicio_Legible_Texto,
    Fin_Legible_Texto,
    Fecha,
    Total_Horas,
    Horas_Produccion,
    Horas_Preparacion,
    Horas_Parada,
    Horas_Mantenimiento,
    CantidadBuenosProducida,
    CantidadMalosProducida,
    Turno,
    Maquinista,
    Operario,
    CodProducto,
    Motivo,
    Nro_Secuencia,
    FlagPreparacion,
    SecuenciaPreparacion

FROM PrepSecuencia;


------------------------------------------------------------------------------------------------------------------------------------------

/*
===========================================================================
🧱 VISTA 4 – ConCuboSaccod1Final
===========================================================================

📌 DESCRIPCIÓN:
Vista final del pipeline Medoro 9. Trae toda la información procesada 
desde `ConCubo3AñosSecFlag` y le agrega el `saccod1` proveniente de la 
tabla `TablaVinculadaUNION` mediante un `LEFT JOIN`.

✅ Es la vista lista para usar en Power BI
✅ Agrega contexto físico (saccod1) para análisis técnicos
✅ Ya viene filtrada por últimos 3 años, no necesita filtros adicionales

===========================================================================
*/

CREATE OR ALTER VIEW ConCuboSaccod1Final AS
SELECT
    s.ID,
    s.ID_Limpio,
    s.Renglon,
    s.Estado,
    s.Inicio_Corregido,
    s.Fin_Corregido,
    s.Inicio_Legible_Texto,
    s.Fin_Legible_Texto,
    s.Fecha,

    -- ✅ Las 5 columnas operativas
    s.Turno,
    s.Maquinista,
    s.Operario,
    s.CodProducto,
    s.Motivo,

    -- ⏱️ Tiempos totales y discriminados
    s.Total_Horas,
    s.Horas_Produccion,
    s.Horas_Preparacion,
    s.Horas_Parada,
    s.Horas_Mantenimiento,

    -- 📦 Producción
    s.CantidadBuenosProducida,
    s.CantidadMalosProducida,

    -- 🔢 Secuencia y flags
    s.Nro_Secuencia,
    s.FlagPreparacion,
    s.SecuenciaPreparacion,

    -- 🔩 Única columna del JOIN externo
    VU.saccod1

FROM ConCubo3AñosSecFlag s
LEFT JOIN TablaVinculadaUNION VU
    ON ISNUMERIC(VU.OP) = 1
    AND TRY_CAST(VU.OP AS INT) = s.ID_Limpio;


------------------------------------------------------------------------------------------------------

/*
===========================================================================
🧭 INSTRUCCIÓN PARA USUARIOS QUE YA ESTÁN USANDO UN ARCHIVO .PBIX
===========================================================================

IMPORTANTE: Si ya estás trabajando con un archivo de Power BI que contiene 
medidas, relaciones y visuales conectados a la tabla `ConCuboFinal`, 
NO tenés que rehacer nada.

Solo necesitás actualizar la conexión de esa tabla para que use la nueva 
vista con datos corregidos: `ConCuboSaccod1Final`.

📌 PASOS:

1. Crear en SQL Server 2022 las 4 vistas nuevas de Medoro 9:
   - ConCubo3Años
   - ConCubo3AñosSec
   - ConCubo3AñosSecFlag
   - ConCuboSaccod1Final ✅ (vista final)

2. Abrir tu archivo `.pbix` actual (el que ya tiene visuales funcionando).

3. Ir a "Transform Data" (Editor de Power Query).

4. En el panel izquierdo, seleccionar la tabla llamada `ConCuboFinal`.

5. Abrir el "Advanced Editor".

6. Reemplazar la línea del origen por esta:

   ```powerquery
   = NombreConexionSQL{[Schema="dbo",Item="ConCuboSaccod1Final"]}[Data]

⚠️ IMPORTANTE: Reemplazar NombreConexionSQL por el nombre real
de la conexión que aparece en tu Power BI (ej. Sispro2022 u otro).

Clic en "Done", luego en "Close & Apply".

✅ A partir de ese momento, todos los gráficos, medidas y relaciones
seguirán funcionando igual, pero con los datos nuevos y corregidos
de Medoro 9.

NO se rompe nada, NO se pierden visuales, y NO hace falta rehacer el informe.

===========================================================================
*/