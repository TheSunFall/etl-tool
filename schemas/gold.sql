/* ============================================================
   GOLD AJUSTADO - SAFE SCRIPT
   - Crea esquema gold si no existe.
   - Crea tablas gold materializadas si no existen.
   - Crea procedimientos de carga desde silver.
   - No modifica bronze ni silver hasta que ejecutes los procedimientos.
   ============================================================ */

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'gold')
    EXEC('CREATE SCHEMA gold');
GO

/* ============================================================
   1) TABLAS GOLD
   ============================================================ */

IF OBJECT_ID('gold.INGRESOS_DASHBOARD', 'U') IS NULL
BEGIN
    CREATE TABLE gold.INGRESOS_DASHBOARD
    (
        ANIO                    INT NULL,
        MES                     INT NULL,
        NIVEL_GOBIERNO          NVARCHAR(MAX) NULL,
        SECTOR                  NVARCHAR(MAX) NULL,
        PLIEGO                  NVARCHAR(MAX) NULL,
        EJECUTORA               NVARCHAR(MAX) NULL,
        DEPARTAMENTO            NVARCHAR(MAX) NULL,
        PROVINCIA               NVARCHAR(MAX) NULL,
        DISTRITO                NVARCHAR(MAX) NULL,
        RUBRO                   NVARCHAR(MAX) NULL,
        TIPO_RECURSO            NVARCHAR(MAX) NULL,
        GENERICA                NVARCHAR(MAX) NULL,
        ESPECIFICA              NVARCHAR(MAX) NULL,
        MONTO_PIA               DECIMAL(18,2) NULL,
        MONTO_PIM               DECIMAL(18,2) NULL,
        MONTO_RECAUDADO         DECIMAL(18,2) NULL,
        VARIACION_PIM_PIA       DECIMAL(18,2) NULL,
        SALDO_POR_RECAUDAR      DECIMAL(18,2) NULL,
        PORC_AVANCE_RECAUDACION DECIMAL(18,6) NULL
    );
END;
GO

IF OBJECT_ID('gold.SISMEPRE_DASHBOARD', 'U') IS NULL
BEGIN
    CREATE TABLE gold.SISMEPRE_DASHBOARD
    (
        ANO_APLICACION     INT NULL,
        PERIODO            NVARCHAR(MAX) NULL,
        SEC_EJEC           NVARCHAR(MAX) NULL,
        EJECUTORA_NOMBRE   NVARCHAR(MAX) NULL,
        FORMULARIO_ID      NVARCHAR(MAX) NULL,
        TITULO_FORMULARIO  NVARCHAR(MAX) NULL,
        CLASIFICACION      NVARCHAR(MAX) NULL,
        TIPO_FORMULARIO    NVARCHAR(MAX) NULL,
        PREGUNTA_ID        NVARCHAR(MAX) NULL,
        DESCRIPCION        NVARCHAR(MAX) NULL,
        RESPUESTA_ID       NVARCHAR(MAX) NULL,
        RESPUESTA_TEXTO    NVARCHAR(MAX) NULL,
        RESPUESTA_DECIMAL  NVARCHAR(MAX) NULL,
        RESPUESTA_ENTERO   NVARCHAR(MAX) NULL,
        RESPUESTA_FECHA    NVARCHAR(MAX) NULL
    );
END;
GO

IF OBJECT_ID('gold.RENAMU_DASHBOARD', 'U') IS NULL
BEGIN
    CREATE TABLE gold.RENAMU_DASHBOARD
    (
        ANIO          INT NULL,
        DEPARTAMENTO  NVARCHAR(MAX) NULL,
        PROVINCIA     NVARCHAR(MAX) NULL,
        DISTRITO      NVARCHAR(MAX) NULL,
        TIPOMUNI      INT NULL,
        NOMBRE_CAMPO  NVARCHAR(MAX) NULL,
        DESCRIPCION   NVARCHAR(MAX) NULL,
        VALOR         NVARCHAR(MAX) NULL,
        METADATA      NVARCHAR(MAX) NULL
    );
END;
GO

/* ============================================================
   2) PROCEDIMIENTOS GOLD
   ============================================================ */

CREATE OR ALTER PROCEDURE gold.sp_Gold_Load_Ingresos_Dashboard
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM gold.INGRESOS_DASHBOARD;

    INSERT INTO gold.INGRESOS_DASHBOARD
    (
        ANIO, MES, NIVEL_GOBIERNO, SECTOR, PLIEGO, EJECUTORA,
        DEPARTAMENTO, PROVINCIA, DISTRITO, RUBRO, TIPO_RECURSO,
        GENERICA, ESPECIFICA, MONTO_PIA, MONTO_PIM, MONTO_RECAUDADO,
        VARIACION_PIM_PIA, SALDO_POR_RECAUDAR, PORC_AVANCE_RECAUDACION
    )
    SELECT
        t.ANIO,
        t.MES,
        ng.NIVEL_GOBIERNO_NOMBRE,
        s.SECTOR_NOMBRE,
        p.PLIEGO_NOMBRE,
        e.EJECUTORA_NOMBRE,
        u.DEPARTAMENTO,
        u.PROVINCIA,
        u.DISTRITO,
        r.RUBRO_NOMBRE,
        tr.TIPO_RECURSO_NOMBRE,
        g.GENERICA_NOMBRE,
        esp.ESPECIFICA_NOMBRE,
        SUM(f.MONTO_PIA),
        SUM(f.MONTO_PIM),
        SUM(f.MONTO_RECAUDADO),
        SUM(f.MONTO_PIM) - SUM(f.MONTO_PIA),
        SUM(f.MONTO_PIM) - SUM(f.MONTO_RECAUDADO),
        CASE
            WHEN SUM(f.MONTO_PIM) = 0 THEN 0
            ELSE CAST(SUM(f.MONTO_RECAUDADO) AS DECIMAL(18,6)) / NULLIF(CAST(SUM(f.MONTO_PIM) AS DECIMAL(18,6)), 0)
        END
    FROM silver.FACT_INGRESO_DIARIO f
    INNER JOIN silver.DIM_TIEMPO t ON f.IdTiempo = t.IdTiempo
    INNER JOIN silver.DIM_NIVEL_GOBIERNO ng ON f.IdNivelGobierno = ng.IdNivelGobierno
    INNER JOIN silver.DIM_SECTOR s ON f.IdSector = s.IdSector
    INNER JOIN silver.DIM_PLIEGO p ON f.IdPliego = p.IdPliego
    INNER JOIN silver.DIM_EJECUTORA e ON f.IdEjecutora = e.IdEjecutora
    INNER JOIN silver.DIM_UBIGEO u ON f.IdUbigeo = u.IdUbigeo
    INNER JOIN silver.DIM_RUBRO r ON f.IdRubro = r.IdRubro
    INNER JOIN silver.DIM_TIPO_RECURSO tr ON f.IdTipoRecurso = tr.IdTipoRecurso
    INNER JOIN silver.DIM_GENERICA g ON f.IdGenerica = g.IdGenerica
    INNER JOIN silver.DIM_ESPECIFICA esp ON f.IdEspecifica = esp.IdEspecifica
    GROUP BY
        t.ANIO, t.MES,
        ng.NIVEL_GOBIERNO_NOMBRE,
        s.SECTOR_NOMBRE,
        p.PLIEGO_NOMBRE,
        e.EJECUTORA_NOMBRE,
        u.DEPARTAMENTO,
        u.PROVINCIA,
        u.DISTRITO,
        r.RUBRO_NOMBRE,
        tr.TIPO_RECURSO_NOMBRE,
        g.GENERICA_NOMBRE,
        esp.ESPECIFICA_NOMBRE;
END;
GO

CREATE OR ALTER PROCEDURE gold.sp_Gold_Load_Sismepre_Dashboard
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM gold.SISMEPRE_DASHBOARD;

    INSERT INTO gold.SISMEPRE_DASHBOARD
    (
        ANO_APLICACION, PERIODO, SEC_EJEC, EJECUTORA_NOMBRE,
        FORMULARIO_ID, TITULO_FORMULARIO, CLASIFICACION, TIPO_FORMULARIO,
        PREGUNTA_ID, DESCRIPCION, RESPUESTA_ID, RESPUESTA_TEXTO,
        RESPUESTA_DECIMAL, RESPUESTA_ENTERO, RESPUESTA_FECHA
    )
    SELECT
        aa.ANO_APLICACION,
        f.PERIODO,
        e.SEC_EJEC,
        e.EJECUTORA_NOMBRE,
        form.FORMULARIO_ID,
        form.TITULO,
        form.CLASIFICACION,
        form.TIPO_FORMULARIO,
        preg.PREGUNTA_ID,
        preg.DESCRIPCION,
        f.RESPUESTA_ID,
        f.RESPUESTA_TEXTO,
        f.RESPUESTA_DECIMAL,
        f.RESPUESTA_ENTERO,
        f.RESPUESTA_FECHA
    FROM silver.FACT_FORMULARIO_SISMEPRE f
    INNER JOIN silver.DIM_EJECUTORA e ON f.IdEjecutora = e.IdEjecutora
    INNER JOIN silver.DIM_ANIO_APLICACION aa ON f.IdAnioAplicacion = aa.IdAnioAplicacion
    INNER JOIN silver.DIM_FORMULARIO_SISMEPRE form ON f.IdFormulario = form.IdFormSismepre
    INNER JOIN silver.DIM_PREGUNTA_SISMEPRE preg ON f.IdPregunta = preg.IdPreguntaSismepre;
END;
GO

CREATE OR ALTER PROCEDURE gold.sp_Gold_Load_Renamu_Dashboard
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM gold.RENAMU_DASHBOARD;

    INSERT INTO gold.RENAMU_DASHBOARD
    (ANIO, DEPARTAMENTO, PROVINCIA, DISTRITO, TIPOMUNI, NOMBRE_CAMPO, DESCRIPCION, VALOR, METADATA)
    SELECT
        t.ANIO,
        u.DEPARTAMENTO,
        u.PROVINCIA,
        u.DISTRITO,
        f.TIPOMUNI,
        p.NOMBRE_CAMPO,
        p.DESCRIPCION,
        p.VALOR,
        p.METADATA
    FROM silver.FACT_RENAMU f
    INNER JOIN silver.DIM_TIEMPO t ON f.IdTiempo = t.IdTiempo
    INNER JOIN silver.DIM_UBIGEO u ON f.IdUbigeo = u.IdUbigeo
    INNER JOIN silver.DIM_PREGUNTA_RENAMU p ON f.IdPregunta = p.IdPregunta;
END;
GO

CREATE OR ALTER PROCEDURE gold.sp_Load_Gold_All
AS
BEGIN
    SET NOCOUNT ON;

    EXEC gold.sp_Gold_Load_Ingresos_Dashboard;
    EXEC gold.sp_Gold_Load_Sismepre_Dashboard;
    EXEC gold.sp_Gold_Load_Renamu_Dashboard;
END;
GO

/* ============================================================
   3) VISTAS DE APOYO PARA POWER BI / TABLEAU
   ============================================================ */

CREATE OR ALTER VIEW gold.vw_kpi_ingresos_anual
AS
SELECT
    ANIO,
    SUM(MONTO_PIA) AS MONTO_PIA,
    SUM(MONTO_PIM) AS MONTO_PIM,
    SUM(MONTO_RECAUDADO) AS MONTO_RECAUDADO,
    SUM(VARIACION_PIM_PIA) AS VARIACION_PIM_PIA,
    SUM(SALDO_POR_RECAUDAR) AS SALDO_POR_RECAUDAR,
    CASE
        WHEN SUM(MONTO_PIM) = 0 THEN 0
        ELSE CAST(SUM(MONTO_RECAUDADO) AS DECIMAL(18,6)) / NULLIF(CAST(SUM(MONTO_PIM) AS DECIMAL(18,6)), 0)
    END AS PORC_AVANCE_RECAUDACION
FROM gold.INGRESOS_DASHBOARD
GROUP BY ANIO;
GO

CREATE OR ALTER VIEW gold.vw_ingresos_por_departamento
AS
SELECT
    ANIO,
    DEPARTAMENTO,
    SUM(MONTO_RECAUDADO) AS MONTO_RECAUDADO,
    SUM(MONTO_PIM) AS MONTO_PIM,
    SUM(SALDO_POR_RECAUDAR) AS SALDO_POR_RECAUDAR
FROM gold.INGRESOS_DASHBOARD
GROUP BY ANIO, DEPARTAMENTO;
GO

/* ============================================================
   4) VALIDACIÓN RÁPIDA
   ============================================================ */
SELECT TABLE_SCHEMA, TABLE_NAME
FROM INFORMATION_SCHEMA.TABLES
WHERE TABLE_SCHEMA = 'gold'
ORDER BY TABLE_NAME;
GO

EXEC gold.sp_Gold_Load_Ingresos_Dashboard;
EXEC gold.sp_Gold_Load_Sismepre_Dashboard;
EXEC gold.sp_Gold_Load_Renamu_Dashboard;
GO