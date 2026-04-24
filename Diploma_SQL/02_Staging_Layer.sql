USE [DentalClinicDW];
GO

IF OBJECT_ID(N'stg.DimPatients_File', N'U') IS NOT NULL
    DROP TABLE stg.DimPatients_File;
GO

CREATE TABLE stg.DimPatients_File
(
    PatientID            int NOT NULL,
    FullName             nvarchar(100) NOT NULL,
    Age                  int NULL,
    Phone                nvarchar(20) NULL
);
GO

IF OBJECT_ID(N'stg.DimPatients_Raw', N'U') IS NOT NULL
    DROP TABLE stg.DimPatients_Raw;
GO

CREATE TABLE stg.DimPatients_Raw
(
    PatientID            int NOT NULL,
    FullName             nvarchar(100) NOT NULL,
    Age                  int NULL,
    Phone                nvarchar(20) NULL,
    SourceFileName       nvarchar(260) NOT NULL CONSTRAINT DF_stg_DimPatients_Raw_SourceFile DEFAULT (N'DimPatients.csv'),
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_stg_DimPatients_Raw_LoadedAt DEFAULT (sysdatetime()),
    BatchID              int NULL
);
GO

IF OBJECT_ID(N'stg.DimDoctors_File', N'U') IS NOT NULL
    DROP TABLE stg.DimDoctors_File;
GO

CREATE TABLE stg.DimDoctors_File
(
    DoctorID             int NOT NULL,
    FullName             nvarchar(100) NOT NULL,
    Specialty            nvarchar(100) NULL
);
GO

IF OBJECT_ID(N'stg.DimDoctors_Raw', N'U') IS NOT NULL
    DROP TABLE stg.DimDoctors_Raw;
GO

CREATE TABLE stg.DimDoctors_Raw
(
    DoctorID             int NOT NULL,
    FullName             nvarchar(100) NOT NULL,
    Specialty            nvarchar(100) NULL,
    SourceFileName       nvarchar(260) NOT NULL CONSTRAINT DF_stg_DimDoctors_Raw_SourceFile DEFAULT (N'DimDoctors.csv'),
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_stg_DimDoctors_Raw_LoadedAt DEFAULT (sysdatetime()),
    BatchID              int NULL
);
GO

IF OBJECT_ID(N'stg.DimDate_File', N'U') IS NOT NULL
    DROP TABLE stg.DimDate_File;
GO

CREATE TABLE stg.DimDate_File
(
    DateID               int NOT NULL,
    [Day]                int NULL,
    [Month]              int NULL,
    [Year]               int NULL,
    WeekDay              nvarchar(20) NULL
);
GO

IF OBJECT_ID(N'stg.DimDate_Raw', N'U') IS NOT NULL
    DROP TABLE stg.DimDate_Raw;
GO

CREATE TABLE stg.DimDate_Raw
(
    DateID               int NOT NULL,
    [Day]                int NULL,
    [Month]              int NULL,
    [Year]               int NULL,
    WeekDay              nvarchar(20) NULL,
    SourceFileName       nvarchar(260) NOT NULL CONSTRAINT DF_stg_DimDate_Raw_SourceFile DEFAULT (N'DimDate.csv'),
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_stg_DimDate_Raw_LoadedAt DEFAULT (sysdatetime()),
    BatchID              int NULL
);
GO

IF OBJECT_ID(N'stg.DimVisitType_File', N'U') IS NOT NULL
    DROP TABLE stg.DimVisitType_File;
GO

CREATE TABLE stg.DimVisitType_File
(
    VisitTypeID          int NOT NULL,
    VisitName            nvarchar(100) NOT NULL,
    VisitCode            nvarchar(50) NOT NULL
);
GO

IF OBJECT_ID(N'stg.DimVisitType_Raw', N'U') IS NOT NULL
    DROP TABLE stg.DimVisitType_Raw;
GO

CREATE TABLE stg.DimVisitType_Raw
(
    VisitTypeID          int NOT NULL,
    VisitName            nvarchar(100) NOT NULL,
    VisitCode            nvarchar(50) NOT NULL,
    SourceFileName       nvarchar(260) NOT NULL CONSTRAINT DF_stg_DimVisitType_Raw_SourceFile DEFAULT (N'DimVisitType.csv'),
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_stg_DimVisitType_Raw_LoadedAt DEFAULT (sysdatetime()),
    BatchID              int NULL
);
GO

IF OBJECT_ID(N'stg.DimDiagnoses_File', N'U') IS NOT NULL
    DROP TABLE stg.DimDiagnoses_File;
GO

CREATE TABLE stg.DimDiagnoses_File
(
    DiagnosisID          int NOT NULL,
    ICDCode              nvarchar(20) NULL,
    DiagnosisName        nvarchar(200) NULL,
    TreatmentMethod      nvarchar(200) NULL
);
GO

IF OBJECT_ID(N'stg.DimDiagnoses_Raw', N'U') IS NOT NULL
    DROP TABLE stg.DimDiagnoses_Raw;
GO

CREATE TABLE stg.DimDiagnoses_Raw
(
    DiagnosisID          int NOT NULL,
    ICDCode              nvarchar(20) NULL,
    DiagnosisName        nvarchar(200) NULL,
    TreatmentMethod      nvarchar(200) NULL,
    SourceFileName       nvarchar(260) NOT NULL CONSTRAINT DF_stg_DimDiagnoses_Raw_SourceFile DEFAULT (N'DimDiagnoses.csv'),
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_stg_DimDiagnoses_Raw_LoadedAt DEFAULT (sysdatetime()),
    BatchID              int NULL
);
GO

IF OBJECT_ID(N'stg.FactVisits_File', N'U') IS NOT NULL
    DROP TABLE stg.FactVisits_File;
GO

CREATE TABLE stg.FactVisits_File
(
    FactID               int NOT NULL,
    DateID               int NULL,
    PatientID            int NULL,
    DoctorID             int NULL,
    DiagnosisID          int NULL,
    VisitTypeID          int NULL,
    ServiceCostText      nvarchar(50) NULL,
    VisitCount           int NULL,
    ProfitText           nvarchar(50) NULL,
    PatientCount         int NULL,
    RepeatRateText       nvarchar(50) NULL,
    AvgTreatmentCostText nvarchar(50) NULL
);
GO

IF OBJECT_ID(N'stg.FactVisits_Raw', N'U') IS NOT NULL
    DROP TABLE stg.FactVisits_Raw;
GO

CREATE TABLE stg.FactVisits_Raw
(
    FactID               int NOT NULL,
    DateID               int NULL,
    PatientID            int NULL,
    DoctorID             int NULL,
    DiagnosisID          int NULL,
    VisitTypeID          int NULL,
    ServiceCostText      nvarchar(50) NULL,
    VisitCount           int NULL,
    ProfitText           nvarchar(50) NULL,
    PatientCount         int NULL,
    RepeatRateText       nvarchar(50) NULL,
    AvgTreatmentCostText nvarchar(50) NULL,
    SourceFileName       nvarchar(260) NOT NULL CONSTRAINT DF_stg_FactVisits_Raw_SourceFile DEFAULT (N'FactVisits.csv'),
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_stg_FactVisits_Raw_LoadedAt DEFAULT (sysdatetime()),
    BatchID              int NULL
);
GO

CREATE OR ALTER VIEW stg.vw_FactVisits_Normalized
AS
SELECT
    FactID,
    DateID,
    PatientID,
    DoctorID,
    DiagnosisID,
    VisitTypeID,
    TRY_CONVERT(decimal(18,2), REPLACE(ServiceCostText, N',', N'.')) AS ServiceCost,
    VisitCount,
    TRY_CONVERT(decimal(18,2), REPLACE(ProfitText, N',', N'.')) AS Profit,
    PatientCount,
    TRY_CONVERT(decimal(9,4), REPLACE(RepeatRateText, N',', N'.')) AS RepeatRate,
    TRY_CONVERT(decimal(18,2), REPLACE(AvgTreatmentCostText, N',', N'.')) AS AvgTreatmentCost,
    SourceFileName,
    LoadedAt,
    BatchID
FROM stg.FactVisits_Raw;
GO

IF OBJECT_ID(N'stg.usp_TruncateRawTables', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_TruncateRawTables;
GO

CREATE PROCEDURE stg.usp_TruncateRawTables
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE stg.FactVisits_File;
    TRUNCATE TABLE stg.DimDiagnoses_File;
    TRUNCATE TABLE stg.DimVisitType_File;
    TRUNCATE TABLE stg.DimDate_File;
    TRUNCATE TABLE stg.DimDoctors_File;
    TRUNCATE TABLE stg.DimPatients_File;
    TRUNCATE TABLE stg.FactVisits_Raw;
    TRUNCATE TABLE stg.DimDiagnoses_Raw;
    TRUNCATE TABLE stg.DimVisitType_Raw;
    TRUNCATE TABLE stg.DimDate_Raw;
    TRUNCATE TABLE stg.DimDoctors_Raw;
    TRUNCATE TABLE stg.DimPatients_Raw;
END;
GO

IF OBJECT_ID(N'stg.usp_BulkLoadCsv', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_BulkLoadCsv;
GO

CREATE PROCEDURE stg.usp_BulkLoadCsv
    @TableName      nvarchar(256),
    @FilePath       nvarchar(4000),
    @FirstRow       int = 2,
    @FieldTerminator nvarchar(20) = N';',
    @RowTerminator  nvarchar(20) = N'0x0a'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Sql nvarchar(max);

    SET @Sql = N'
        BULK INSERT ' + @TableName + N'
        FROM ''' + REPLACE(@FilePath, '''', '''''') + N'''
        WITH
        (
            FIRSTROW = ' + CAST(@FirstRow AS nvarchar(10)) + N',
            FIELDTERMINATOR = ''' + @FieldTerminator + N''',
            ROWTERMINATOR = ' + @RowTerminator + N',
            CODEPAGE = ''65001'',
            TABLOCK,
            KEEPNULLS
        );';

    EXEC sys.sp_executesql @Sql;
END;
GO

IF OBJECT_ID(N'stg.usp_LoadRawFromCsv', N'P') IS NOT NULL
    DROP PROCEDURE stg.usp_LoadRawFromCsv;
GO

CREATE PROCEDURE stg.usp_LoadRawFromCsv
    @BasePath nvarchar(4000) = NULL,
    @BatchID  int = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @ResolvedBasePath nvarchar(4000) =
        COALESCE(@BasePath, (SELECT ParameterValue FROM ctl.ProcessParameter WHERE ParameterName = N'BaseCsvPath'));

    EXEC stg.usp_TruncateRawTables;

    EXEC stg.usp_BulkLoadCsv
        @TableName = N'stg.DimPatients_File',
        @FilePath = CONCAT(@ResolvedBasePath, N'\DimPatients.csv');

    EXEC stg.usp_BulkLoadCsv
        @TableName = N'stg.DimDoctors_File',
        @FilePath = CONCAT(@ResolvedBasePath, N'\DimDoctors.csv');

    EXEC stg.usp_BulkLoadCsv
        @TableName = N'stg.DimDate_File',
        @FilePath = CONCAT(@ResolvedBasePath, N'\DimDate.csv');

    EXEC stg.usp_BulkLoadCsv
        @TableName = N'stg.DimVisitType_File',
        @FilePath = CONCAT(@ResolvedBasePath, N'\DimVisitType.csv');

    EXEC stg.usp_BulkLoadCsv
        @TableName = N'stg.DimDiagnoses_File',
        @FilePath = CONCAT(@ResolvedBasePath, N'\DimDiagnoses.csv');

    EXEC stg.usp_BulkLoadCsv
        @TableName = N'stg.FactVisits_File',
        @FilePath = CONCAT(@ResolvedBasePath, N'\FactVisits.csv');

    INSERT INTO stg.DimPatients_Raw (PatientID, FullName, Age, Phone, BatchID)
    SELECT PatientID, FullName, Age, Phone, @BatchID
    FROM stg.DimPatients_File;

    INSERT INTO stg.DimDoctors_Raw (DoctorID, FullName, Specialty, BatchID)
    SELECT DoctorID, FullName, Specialty, @BatchID
    FROM stg.DimDoctors_File;

    INSERT INTO stg.DimDate_Raw (DateID, [Day], [Month], [Year], WeekDay, BatchID)
    SELECT DateID, [Day], [Month], [Year], WeekDay, @BatchID
    FROM stg.DimDate_File;

    INSERT INTO stg.DimVisitType_Raw (VisitTypeID, VisitName, VisitCode, BatchID)
    SELECT VisitTypeID, VisitName, VisitCode, @BatchID
    FROM stg.DimVisitType_File;

    INSERT INTO stg.DimDiagnoses_Raw (DiagnosisID, ICDCode, DiagnosisName, TreatmentMethod, BatchID)
    SELECT DiagnosisID, ICDCode, DiagnosisName, TreatmentMethod, @BatchID
    FROM stg.DimDiagnoses_File;

    INSERT INTO stg.FactVisits_Raw
    (
        FactID, DateID, PatientID, DoctorID, DiagnosisID, VisitTypeID,
        ServiceCostText, VisitCount, ProfitText, PatientCount, RepeatRateText, AvgTreatmentCostText, BatchID
    )
    SELECT
        FactID, DateID, PatientID, DoctorID, DiagnosisID, VisitTypeID,
        ServiceCostText, VisitCount, ProfitText, PatientCount, RepeatRateText, AvgTreatmentCostText, @BatchID
    FROM stg.FactVisits_File;

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'stg.usp_LoadRawFromCsv',
        @LogLevel = N'INFO',
        @StepName = N'Bulk load',
        @LogMessage = CONCAT(N'CSV files loaded from folder: ', @ResolvedBasePath),
        @AffectedRows =
            (SELECT COUNT(*) FROM stg.DimPatients_Raw)
          + (SELECT COUNT(*) FROM stg.DimDoctors_Raw)
          + (SELECT COUNT(*) FROM stg.DimDate_Raw)
          + (SELECT COUNT(*) FROM stg.DimVisitType_Raw)
          + (SELECT COUNT(*) FROM stg.DimDiagnoses_Raw)
          + (SELECT COUNT(*) FROM stg.FactVisits_Raw);
END;
GO
