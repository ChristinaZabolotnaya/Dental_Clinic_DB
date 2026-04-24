USE [master];
GO

IF DB_ID(N'DentalClinicDW') IS NULL
BEGIN
    CREATE DATABASE [DentalClinicDW];
END;
GO

ALTER DATABASE [DentalClinicDW] SET RECOVERY SIMPLE;
GO

ALTER DATABASE [DentalClinicDW] SET ANSI_NULLS ON;
GO

ALTER DATABASE [DentalClinicDW] SET QUOTED_IDENTIFIER ON;
GO

USE [DentalClinicDW];
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'stg')
    EXEC(N'CREATE SCHEMA stg AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'dw')
    EXEC(N'CREATE SCHEMA dw AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'mart')
    EXEC(N'CREATE SCHEMA mart AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'ctl')
    EXEC(N'CREATE SCHEMA ctl AUTHORIZATION dbo;');
GO

IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'sec')
    EXEC(N'CREATE SCHEMA sec AUTHORIZATION dbo;');
GO

IF OBJECT_ID(N'ctl.ETL_Batch', N'U') IS NOT NULL
    DROP TABLE ctl.ETL_Batch;
GO

CREATE TABLE ctl.ETL_Batch
(
    BatchID              int IDENTITY(1,1) NOT NULL,
    BatchName            nvarchar(200) NOT NULL,
    SourceSystem         nvarchar(100) NOT NULL CONSTRAINT DF_ETL_Batch_SourceSystem DEFAULT (N'CSV'),
    StartedAt            datetime2(0) NOT NULL CONSTRAINT DF_ETL_Batch_StartedAt DEFAULT (sysdatetime()),
    FinishedAt           datetime2(0) NULL,
    Status               nvarchar(30) NOT NULL CONSTRAINT DF_ETL_Batch_Status DEFAULT (N'STARTED'),
    RowsRead             int NOT NULL CONSTRAINT DF_ETL_Batch_RowsRead DEFAULT (0),
    RowsLoaded           int NOT NULL CONSTRAINT DF_ETL_Batch_RowsLoaded DEFAULT (0),
    RowsRejected         int NOT NULL CONSTRAINT DF_ETL_Batch_RowsRejected DEFAULT (0),
    Initiator            sysname NOT NULL CONSTRAINT DF_ETL_Batch_Initiator DEFAULT (suser_sname()),
    CommentText          nvarchar(1000) NULL,
    CONSTRAINT PK_ETL_Batch PRIMARY KEY CLUSTERED (BatchID)
);
GO

IF OBJECT_ID(N'ctl.ETL_Log', N'U') IS NOT NULL
    DROP TABLE ctl.ETL_Log;
GO

CREATE TABLE ctl.ETL_Log
(
    LogID                bigint IDENTITY(1,1) NOT NULL,
    BatchID              int NULL,
    ProcedureName        sysname NOT NULL,
    LogLevel             nvarchar(20) NOT NULL,
    StepName             nvarchar(200) NOT NULL,
    LogMessage           nvarchar(2000) NOT NULL,
    AffectedRows         int NULL,
    CreatedAt            datetime2(0) NOT NULL CONSTRAINT DF_ETL_Log_CreatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_ETL_Log PRIMARY KEY CLUSTERED (LogID),
    CONSTRAINT FK_ETL_Log_Batch FOREIGN KEY (BatchID) REFERENCES ctl.ETL_Batch(BatchID)
);
GO

IF OBJECT_ID(N'ctl.RejectedRows', N'U') IS NOT NULL
    DROP TABLE ctl.RejectedRows;
GO

CREATE TABLE ctl.RejectedRows
(
    RejectedRowID        bigint IDENTITY(1,1) NOT NULL,
    BatchID              int NULL,
    SourceTable          nvarchar(128) NOT NULL,
    BusinessKey          nvarchar(200) NULL,
    RejectionReason      nvarchar(1000) NOT NULL,
    Payload              nvarchar(max) NULL,
    RejectedAt           datetime2(0) NOT NULL CONSTRAINT DF_RejectedRows_RejectedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_RejectedRows PRIMARY KEY CLUSTERED (RejectedRowID),
    CONSTRAINT FK_RejectedRows_Batch FOREIGN KEY (BatchID) REFERENCES ctl.ETL_Batch(BatchID)
);
GO

IF OBJECT_ID(N'ctl.DataQualityIssue', N'U') IS NOT NULL
    DROP TABLE ctl.DataQualityIssue;
GO

CREATE TABLE ctl.DataQualityIssue
(
    IssueID              bigint IDENTITY(1,1) NOT NULL,
    BatchID              int NULL,
    RuleCode             nvarchar(50) NOT NULL,
    EntityName           nvarchar(128) NOT NULL,
    EntityKey            nvarchar(200) NULL,
    Severity             nvarchar(20) NOT NULL,
    IssueDescription     nvarchar(2000) NOT NULL,
    RegisteredAt         datetime2(0) NOT NULL CONSTRAINT DF_DataQualityIssue_RegisteredAt DEFAULT (sysdatetime()),
    IsResolved           bit NOT NULL CONSTRAINT DF_DataQualityIssue_IsResolved DEFAULT (0),
    CONSTRAINT PK_DataQualityIssue PRIMARY KEY CLUSTERED (IssueID),
    CONSTRAINT FK_DataQualityIssue_Batch FOREIGN KEY (BatchID) REFERENCES ctl.ETL_Batch(BatchID)
);
GO

IF OBJECT_ID(N'ctl.ProcessParameter', N'U') IS NOT NULL
    DROP TABLE ctl.ProcessParameter;
GO

CREATE TABLE ctl.ProcessParameter
(
    ParameterName        sysname NOT NULL,
    ParameterValue       nvarchar(4000) NOT NULL,
    ParameterDescription nvarchar(500) NULL,
    UpdatedAt            datetime2(0) NOT NULL CONSTRAINT DF_ProcessParameter_UpdatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_ProcessParameter PRIMARY KEY CLUSTERED (ParameterName)
);
GO

MERGE ctl.ProcessParameter AS tgt
USING
(
    SELECT N'BaseCsvPath' AS ParameterName,
           N'C:\Users\User\Documents\Codex\Dental_CLinic-main\OLAP_DATA' AS ParameterValue,
           N'Katalog s CSV-faylami dlya iskhodnoy zagruzki' AS ParameterDescription
    UNION ALL
    SELECT N'EnableVerboseLogging',
           N'1',
           N'Flag podrobnogo protokolirovaniya ETL-protsessa'
) AS src
ON tgt.ParameterName = src.ParameterName
WHEN MATCHED THEN
    UPDATE
       SET tgt.ParameterValue = src.ParameterValue,
           tgt.ParameterDescription = src.ParameterDescription,
           tgt.UpdatedAt = sysdatetime()
WHEN NOT MATCHED THEN
    INSERT (ParameterName, ParameterValue, ParameterDescription)
    VALUES (src.ParameterName, src.ParameterValue, src.ParameterDescription);
GO

IF OBJECT_ID(N'ctl.usp_WriteLog', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_WriteLog;
GO

CREATE PROCEDURE ctl.usp_WriteLog
    @BatchID       int = NULL,
    @ProcedureName sysname,
    @LogLevel      nvarchar(20),
    @StepName      nvarchar(200),
    @LogMessage    nvarchar(2000),
    @AffectedRows  int = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.ETL_Log
    (
        BatchID,
        ProcedureName,
        LogLevel,
        StepName,
        LogMessage,
        AffectedRows
    )
    VALUES
    (
        @BatchID,
        @ProcedureName,
        @LogLevel,
        @StepName,
        @LogMessage,
        @AffectedRows
    );
END;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'clinic_analyst')
    CREATE ROLE clinic_analyst AUTHORIZATION dbo;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'clinic_etl_operator')
    CREATE ROLE clinic_etl_operator AUTHORIZATION dbo;
GO
