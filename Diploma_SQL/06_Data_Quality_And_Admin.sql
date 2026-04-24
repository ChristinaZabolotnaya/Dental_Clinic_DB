USE [DentalClinicDW];
GO

CREATE OR ALTER VIEW ctl.vw_BatchStatus
AS
SELECT
    BatchID,
    BatchName,
    SourceSystem,
    StartedAt,
    FinishedAt,
    Status,
    RowsRead,
    RowsLoaded,
    RowsRejected,
    DATEDIFF(SECOND, StartedAt, FinishedAt) AS DurationSeconds,
    Initiator,
    CommentText
FROM ctl.ETL_Batch;
GO

CREATE OR ALTER VIEW ctl.vw_DataQualitySummary
AS
SELECT
    RuleCode,
    EntityName,
    Severity,
    COUNT(*) AS IssueCount,
    SUM(CASE WHEN IsResolved = 1 THEN 1 ELSE 0 END) AS ResolvedCount,
    SUM(CASE WHEN IsResolved = 0 THEN 1 ELSE 0 END) AS OpenCount,
    MAX(RegisteredAt) AS LastIssueAt
FROM ctl.DataQualityIssue
GROUP BY RuleCode, EntityName, Severity;
GO

CREATE OR ALTER VIEW sec.vw_AccessibleMeasures
AS
SELECT N'ServiceCost' AS MeasureName, N'Vyruchka po uslugam' AS MeasureDescription
UNION ALL SELECT N'Profit', N'Pribyl po vizitam'
UNION ALL SELECT N'VisitCount', N'Kolichestvo vizitov'
UNION ALL SELECT N'PatientCount', N'Kolichestvo patsientov'
UNION ALL SELECT N'RepeatRate', N'Dolya povtornykh vizitov'
UNION ALL SELECT N'AvgTreatmentCost', N'Srednyaya stoimost lecheniya'
UNION ALL SELECT N'MarginPercent', N'Marzhinalnost vizita';
GO

IF OBJECT_ID(N'ctl.usp_RunDataQualityChecks', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_RunDataQualityChecks;
GO

CREATE PROCEDURE ctl.usp_RunDataQualityChecks
    @BatchID int = NULL
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.DataQualityIssue (BatchID, RuleCode, EntityName, EntityKey, Severity, IssueDescription)
    SELECT
        @BatchID,
        N'DATE_WEEKDAY',
        N'dw.DimDate',
        CAST(DateID AS nvarchar(50)),
        N'MEDIUM',
        CONCAT(N'Den nedeli iz iskhodnykh dannykh (', WeekDayName, N') ne sovpadaet s rasschitannoy kalendarnoy datoy.')
    FROM
    (
        SELECT
            DateID,
            WeekDayName,
            FullDate,
            CASE ((DATEDIFF(DAY, '19000101', FullDate) % 7 + 7) % 7)
                WHEN 0 THEN N'ponedelnik'
                WHEN 1 THEN N'vtornik'
                WHEN 2 THEN N'sreda'
                WHEN 3 THEN N'chetverg'
                WHEN 4 THEN N'pyatnitsa'
                WHEN 5 THEN N'subbota'
                WHEN 6 THEN N'voskresene'
            END AS CalculatedWeekDay
        FROM dw.DimDate
        WHERE FullDate IS NOT NULL
    ) q
    WHERE LOWER(ISNULL(WeekDayName, N'')) <> LOWER(ISNULL(CalculatedWeekDay, N''));

    INSERT INTO ctl.DataQualityIssue (BatchID, RuleCode, EntityName, EntityKey, Severity, IssueDescription)
    SELECT
        @BatchID,
        N'PHONE_DUPLICATE',
        N'dw.DimPatient',
        PhoneNormalized,
        N'LOW',
        CONCAT(N'Odin telefon ispolzuetsya neskolkimi patsientami: ', PhoneNormalized)
    FROM dw.DimPatient
    WHERE IsCurrent = 1
      AND NULLIF(PhoneNormalized, N'') IS NOT NULL
    GROUP BY PhoneNormalized
    HAVING COUNT(*) > 1;

    INSERT INTO ctl.DataQualityIssue (BatchID, RuleCode, EntityName, EntityKey, Severity, IssueDescription)
    SELECT
        @BatchID,
        N'NEGATIVE_MARGIN',
        N'dw.FactVisit',
        CAST(FactID AS nvarchar(50)),
        N'MEDIUM',
        N'Marzhinalnost po vizitu otritsatelna, trebuetsya proverka sebestoimosti ili vyruchki.'
    FROM dw.FactVisit
    WHERE MarginPercent < 0;

    INSERT INTO ctl.DataQualityIssue (BatchID, RuleCode, EntityName, EntityKey, Severity, IssueDescription)
    SELECT
        @BatchID,
        N'REPEAT_RATE_RANGE',
        N'dw.FactVisit',
        CAST(FactID AS nvarchar(50)),
        N'HIGH',
        N'Pokazatel RepeatRate nakhoditsya vne diapazona [0;1].'
    FROM dw.FactVisit
    WHERE RepeatRate < 0 OR RepeatRate > 1;

    INSERT INTO ctl.DataQualityIssue (BatchID, RuleCode, EntityName, EntityKey, Severity, IssueDescription)
    SELECT
        @BatchID,
        N'AVG_CHECK_GAP',
        N'dw.FactVisit',
        CAST(FactID AS nvarchar(50)),
        N'LOW',
        N'Srednyaya stoimost lecheniya zametno otlichaetsya ot ServiceCost, trebuetsya biznes-proverka.'
    FROM dw.FactVisit
    WHERE ABS(ServiceCost - AvgTreatmentCost) > 0.01;

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'ctl.usp_RunDataQualityChecks',
        @LogLevel = N'INFO',
        @StepName = N'Quality checks',
        @LogMessage = N'Proverki kachestva dannykh vypolneny',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'ctl.usp_CloseResolvedIssues', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_CloseResolvedIssues;
GO

CREATE PROCEDURE ctl.usp_CloseResolvedIssues
    @RuleCode nvarchar(50) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE ctl.DataQualityIssue
       SET IsResolved = 1
     WHERE IsResolved = 0
       AND (@RuleCode IS NULL OR RuleCode = @RuleCode);
END;
GO

IF OBJECT_ID(N'ctl.usp_RebuildWarehouse', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_RebuildWarehouse;
GO

CREATE PROCEDURE ctl.usp_RebuildWarehouse
    @BasePath nvarchar(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DELETE FROM ctl.DataQualityIssue;
    DELETE FROM ctl.RejectedRows;
    DELETE FROM ctl.ETL_Log;

    DELETE FROM dw.FactVisit;
    DELETE FROM dw.AggregateDoctorMonth;
    DELETE FROM dw.AggregateDiagnosisMonth;
    DELETE FROM dw.DimDiagnosis WHERE DiagnosisKey > 0;
    DELETE FROM dw.DimVisitType WHERE VisitTypeKey > 0;
    DELETE FROM dw.DimDate WHERE DateKey > 0;
    DELETE FROM dw.DimDoctor WHERE DoctorKey > 0;
    DELETE FROM dw.DimPatient WHERE PatientKey > 0;

    EXEC ctl.usp_RunFullLoad @BasePath = @BasePath;
    EXEC ctl.usp_RunDataQualityChecks;
END;
GO

IF OBJECT_ID(N'ctl.usp_DescribeWarehouse', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_DescribeWarehouse;
GO

CREATE PROCEDURE ctl.usp_DescribeWarehouse
AS
BEGIN
    SET NOCOUNT ON;

    SELECT N'dw.DimPatient' AS ObjectName, COUNT(*) AS RowQty FROM dw.DimPatient
    UNION ALL SELECT N'dw.DimDoctor', COUNT(*) FROM dw.DimDoctor
    UNION ALL SELECT N'dw.DimDate', COUNT(*) FROM dw.DimDate
    UNION ALL SELECT N'dw.DimVisitType', COUNT(*) FROM dw.DimVisitType
    UNION ALL SELECT N'dw.DimDiagnosis', COUNT(*) FROM dw.DimDiagnosis
    UNION ALL SELECT N'dw.FactVisit', COUNT(*) FROM dw.FactVisit
    UNION ALL SELECT N'dw.AggregateDoctorMonth', COUNT(*) FROM dw.AggregateDoctorMonth
    UNION ALL SELECT N'dw.AggregateDiagnosisMonth', COUNT(*) FROM dw.AggregateDiagnosisMonth;

    SELECT
        BatchID,
        BatchName,
        Status,
        StartedAt,
        FinishedAt,
        RowsRead,
        RowsLoaded,
        RowsRejected
    FROM ctl.vw_BatchStatus
    ORDER BY BatchID DESC;
END;
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Khranilische paketnoy zagruzki i kontrolya kachestva dannykh dlya OLAP-analitiki stomatologicheskogo kabineta',
    @level0type = N'SCHEMA', @level0name = N'ctl';
GO

EXEC sys.sp_addextendedproperty
    @name = N'MS_Description',
    @value = N'Vitriny dannykh dlya analiticheskikh srezov i sovmestimosti s SSAS-proektom',
    @level0type = N'SCHEMA', @level0name = N'mart';
GO

GRANT SELECT ON ctl.vw_BatchStatus TO clinic_analyst;
GRANT SELECT ON ctl.vw_DataQualitySummary TO clinic_analyst;
GRANT SELECT ON sec.vw_AccessibleMeasures TO clinic_analyst;
GRANT EXECUTE ON ctl.usp_RunDataQualityChecks TO clinic_etl_operator;
GRANT EXECUTE ON ctl.usp_RebuildWarehouse TO clinic_etl_operator;
GRANT EXECUTE ON ctl.usp_DescribeWarehouse TO clinic_analyst;
GO
