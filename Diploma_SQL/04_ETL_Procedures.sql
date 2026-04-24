USE [DentalClinicDW];
GO

IF OBJECT_ID(N'ctl.usp_StartBatch', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_StartBatch;
GO

CREATE PROCEDURE ctl.usp_StartBatch
    @BatchName nvarchar(200),
    @BatchID   int OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.ETL_Batch (BatchName)
    VALUES (@BatchName);

    SET @BatchID = SCOPE_IDENTITY();
END;
GO

IF OBJECT_ID(N'ctl.usp_FinishBatch', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_FinishBatch;
GO

CREATE PROCEDURE ctl.usp_FinishBatch
    @BatchID       int,
    @Status        nvarchar(30),
    @RowsRead      int = NULL,
    @RowsLoaded    int = NULL,
    @RowsRejected  int = NULL,
    @CommentText   nvarchar(1000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE ctl.ETL_Batch
       SET FinishedAt = sysdatetime(),
           Status = @Status,
           RowsRead = COALESCE(@RowsRead, RowsRead),
           RowsLoaded = COALESCE(@RowsLoaded, RowsLoaded),
           RowsRejected = COALESCE(@RowsRejected, RowsRejected),
           CommentText = COALESCE(@CommentText, CommentText)
     WHERE BatchID = @BatchID;
END;
GO

IF OBJECT_ID(N'dw.usp_LoadDimPatient', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_LoadDimPatient;
GO

CREATE PROCEDURE dw.usp_LoadDimPatient
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now datetime2(0) = sysdatetime();

    WITH src AS
    (
        SELECT DISTINCT
            PatientID,
            LTRIM(RTRIM(FullName)) AS FullName,
            Age,
            LTRIM(RTRIM(Phone)) AS Phone,
            CASE
                WHEN Age IS NULL THEN N'Ne opredeleno'
                WHEN Age < 18 THEN N'Do 18 let'
                WHEN Age BETWEEN 18 AND 25 THEN N'18-25'
                WHEN Age BETWEEN 26 AND 35 THEN N'26-35'
                WHEN Age BETWEEN 36 AND 45 THEN N'36-45'
                WHEN Age BETWEEN 46 AND 60 THEN N'46-60'
                ELSE N'60+'
            END AS AgeBand,
            REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Phone)), N' ', N''), N'+', N''), N'-', N'') AS PhoneNormalized
        FROM stg.DimPatients_Raw
    )
    INSERT INTO ctl.RejectedRows (BatchID, SourceTable, BusinessKey, RejectionReason, Payload)
    SELECT
        @BatchID,
        N'stg.DimPatients_Raw',
        CAST(PatientID AS nvarchar(50)),
        N'Nekorrektnye dannye patsienta',
        CONCAT(N'FullName=', COALESCE(FullName, N'<NULL>'), N'; Age=', COALESCE(CAST(Age AS nvarchar(20)), N'<NULL>'))
    FROM src
    WHERE PatientID IS NULL
       OR NULLIF(FullName, N'') IS NULL
       OR (Age IS NOT NULL AND (Age < 0 OR Age > 120));

    ;WITH valid_src AS
    (
        SELECT *
        FROM
        (
            SELECT
                PatientID,
                FullName,
                Age,
                Phone,
                AgeBand,
                PhoneNormalized,
                ROW_NUMBER() OVER (PARTITION BY PatientID ORDER BY PatientID) AS rn
            FROM
            (
                SELECT DISTINCT
                    PatientID,
                    LTRIM(RTRIM(FullName)) AS FullName,
                    Age,
                    LTRIM(RTRIM(Phone)) AS Phone,
                    CASE
                        WHEN Age IS NULL THEN N'Ne opredeleno'
                        WHEN Age < 18 THEN N'Do 18 let'
                        WHEN Age BETWEEN 18 AND 25 THEN N'18-25'
                        WHEN Age BETWEEN 26 AND 35 THEN N'26-35'
                        WHEN Age BETWEEN 36 AND 45 THEN N'36-45'
                        WHEN Age BETWEEN 46 AND 60 THEN N'46-60'
                        ELSE N'60+'
                    END AS AgeBand,
                    REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Phone)), N' ', N''), N'+', N''), N'-', N'') AS PhoneNormalized
                FROM stg.DimPatients_Raw
            ) q
            WHERE PatientID IS NOT NULL
              AND NULLIF(FullName, N'') IS NOT NULL
              AND (Age IS NULL OR (Age BETWEEN 0 AND 120))
        ) d
        WHERE rn = 1
    )
    UPDATE tgt
       SET EffectiveTo = DATEADD(SECOND, -1, @Now),
           IsCurrent = 0
    FROM dw.DimPatient tgt
    JOIN valid_src src
      ON src.PatientID = tgt.PatientID
     AND tgt.IsCurrent = 1
    WHERE ISNULL(tgt.FullName, N'') <> ISNULL(src.FullName, N'')
       OR ISNULL(tgt.Age, -1) <> ISNULL(src.Age, -1)
       OR ISNULL(tgt.PhoneNormalized, N'') <> ISNULL(src.PhoneNormalized, N'');

    ;WITH valid_src AS
    (
        SELECT DISTINCT
            PatientID,
            LTRIM(RTRIM(FullName)) AS FullName,
            Age,
            LTRIM(RTRIM(Phone)) AS Phone,
            CASE
                WHEN Age IS NULL THEN N'Ne opredeleno'
                WHEN Age < 18 THEN N'Do 18 let'
                WHEN Age BETWEEN 18 AND 25 THEN N'18-25'
                WHEN Age BETWEEN 26 AND 35 THEN N'26-35'
                WHEN Age BETWEEN 36 AND 45 THEN N'36-45'
                WHEN Age BETWEEN 46 AND 60 THEN N'46-60'
                ELSE N'60+'
            END AS AgeBand,
            REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(Phone)), N' ', N''), N'+', N''), N'-', N'') AS PhoneNormalized
        FROM stg.DimPatients_Raw
        WHERE PatientID IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(FullName)), N'') IS NOT NULL
          AND (Age IS NULL OR (Age BETWEEN 0 AND 120))
    )
    INSERT INTO dw.DimPatient
    (
        PatientID, FullName, Age, AgeBand, Phone, PhoneNormalized,
        EffectiveFrom, EffectiveTo, IsCurrent, SourceBatchID
    )
    SELECT
        src.PatientID, src.FullName, src.Age, src.AgeBand, src.Phone, src.PhoneNormalized,
        @Now, '99991231', 1, @BatchID
    FROM valid_src src
    OUTER APPLY
    (
        SELECT TOP (1)
            tgt.PatientKey,
            tgt.FullName,
            tgt.Age,
            tgt.PhoneNormalized
        FROM dw.DimPatient tgt
        WHERE tgt.PatientID = src.PatientID
          AND tgt.IsCurrent = 1
        ORDER BY tgt.PatientKey DESC
    ) tgt
    WHERE tgt.PatientKey IS NULL
       OR ISNULL(tgt.FullName, N'') <> ISNULL(src.FullName, N'')
       OR ISNULL(tgt.Age, -1) <> ISNULL(src.Age, -1)
       OR ISNULL(tgt.PhoneNormalized, N'') <> ISNULL(src.PhoneNormalized, N'');

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_LoadDimPatient',
        @LogLevel = N'INFO',
        @StepName = N'SCD2 load',
        @LogMessage = N'Izmerenie patsientov zagruzheno',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'dw.usp_LoadDimDoctor', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_LoadDimDoctor;
GO

CREATE PROCEDURE dw.usp_LoadDimDoctor
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now datetime2(0) = sysdatetime();

    INSERT INTO ctl.RejectedRows (BatchID, SourceTable, BusinessKey, RejectionReason, Payload)
    SELECT
        @BatchID,
        N'stg.DimDoctors_Raw',
        CAST(DoctorID AS nvarchar(50)),
        N'Otsutstvuyut klyuchevye atributy vracha',
        CONCAT(N'FullName=', COALESCE(FullName, N'<NULL>'), N'; Specialty=', COALESCE(Specialty, N'<NULL>'))
    FROM stg.DimDoctors_Raw
    WHERE DoctorID IS NULL
       OR NULLIF(LTRIM(RTRIM(FullName)), N'') IS NULL;

    ;WITH valid_src AS
    (
        SELECT DISTINCT
            DoctorID,
            LTRIM(RTRIM(FullName)) AS FullName,
            LTRIM(RTRIM(Specialty)) AS Specialty,
            CASE
                WHEN Specialty IN (N'Terapevt', N'Khirurg') THEN N'Lechebnyy personal'
                WHEN Specialty IN (N'Ortoped', N'Ortodont') THEN N'Vosstanovitelnaya stomatologiya'
                ELSE N'Prochee'
            END AS SpecialtyGroup
        FROM stg.DimDoctors_Raw
        WHERE DoctorID IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(FullName)), N'') IS NOT NULL
    )
    UPDATE tgt
       SET EffectiveTo = DATEADD(SECOND, -1, @Now),
           IsCurrent = 0
    FROM dw.DimDoctor tgt
    JOIN valid_src src
      ON src.DoctorID = tgt.DoctorID
     AND tgt.IsCurrent = 1
    WHERE ISNULL(tgt.FullName, N'') <> ISNULL(src.FullName, N'')
       OR ISNULL(tgt.Specialty, N'') <> ISNULL(src.Specialty, N'')
       OR ISNULL(tgt.SpecialtyGroup, N'') <> ISNULL(src.SpecialtyGroup, N'');

    ;WITH valid_src AS
    (
        SELECT DISTINCT
            DoctorID,
            LTRIM(RTRIM(FullName)) AS FullName,
            LTRIM(RTRIM(Specialty)) AS Specialty,
            CASE
                WHEN Specialty IN (N'Terapevt', N'Khirurg') THEN N'Lechebnyy personal'
                WHEN Specialty IN (N'Ortoped', N'Ortodont') THEN N'Vosstanovitelnaya stomatologiya'
                ELSE N'Prochee'
            END AS SpecialtyGroup
        FROM stg.DimDoctors_Raw
        WHERE DoctorID IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(FullName)), N'') IS NOT NULL
    )
    INSERT INTO dw.DimDoctor
    (
        DoctorID, FullName, Specialty, SpecialtyGroup,
        EffectiveFrom, EffectiveTo, IsCurrent, SourceBatchID
    )
    SELECT
        src.DoctorID, src.FullName, src.Specialty, src.SpecialtyGroup,
        @Now, '99991231', 1, @BatchID
    FROM valid_src src
    OUTER APPLY
    (
        SELECT TOP (1) *
        FROM dw.DimDoctor tgt
        WHERE tgt.DoctorID = src.DoctorID
          AND tgt.IsCurrent = 1
        ORDER BY tgt.DoctorKey DESC
    ) tgt
    WHERE tgt.DoctorKey IS NULL
       OR ISNULL(tgt.FullName, N'') <> ISNULL(src.FullName, N'')
       OR ISNULL(tgt.Specialty, N'') <> ISNULL(src.Specialty, N'')
       OR ISNULL(tgt.SpecialtyGroup, N'') <> ISNULL(src.SpecialtyGroup, N'');

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_LoadDimDoctor',
        @LogLevel = N'INFO',
        @StepName = N'SCD2 load',
        @LogMessage = N'Izmerenie vrachey zagruzheno',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'dw.usp_LoadDimDate', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_LoadDimDate;
GO

CREATE PROCEDURE dw.usp_LoadDimDate
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.RejectedRows (BatchID, SourceTable, BusinessKey, RejectionReason, Payload)
    SELECT
        @BatchID,
        N'stg.DimDate_Raw',
        CAST(DateID AS nvarchar(50)),
        N'Nekorrektnaya kalendarnaya data',
        CONCAT(N'Day=', COALESCE(CAST([Day] AS nvarchar(10)), N'<NULL>'),
               N'; Month=', COALESCE(CAST([Month] AS nvarchar(10)), N'<NULL>'),
               N'; Year=', COALESCE(CAST([Year] AS nvarchar(10)), N'<NULL>'),
               N'; WeekDay=', COALESCE(WeekDay, N'<NULL>'))
    FROM stg.DimDate_Raw
    WHERE DateID IS NULL
       OR TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2))) IS NULL;

    MERGE dw.DimDate AS tgt
    USING
    (
        SELECT DISTINCT
            DateID,
            TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2))) AS FullDate,
            [Day],
            [Month],
            [Year],
            DATEPART(QUARTER, TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2)))) AS QuarterNumber,
            DATENAME(MONTH, TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2)))) AS MonthName,
            LTRIM(RTRIM(WeekDay)) AS WeekDayName,
            DATEPART(ISO_WEEK, TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2)))) AS WeekOfYear,
            CASE
                WHEN ((DATEDIFF(DAY, '19000101', TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2)))) % 7 + 7) % 7) IN (5, 6)
                    THEN 1
                ELSE 0
            END AS IsWeekend,
            CASE
                WHEN [Month] BETWEEN 9 AND 12 THEN N'Osen'
                WHEN [Month] BETWEEN 1 AND 2 THEN N'Zima'
                WHEN [Month] BETWEEN 3 AND 5 THEN N'Vesna'
                WHEN [Month] BETWEEN 6 AND 8 THEN N'Leto'
                ELSE N'Ne opredeleno'
            END AS AcademicPeriod
        FROM stg.DimDate_Raw
        WHERE DateID IS NOT NULL
          AND TRY_CONVERT(date, CONCAT([Year], RIGHT(CONCAT(N'0', [Month]), 2), RIGHT(CONCAT(N'0', [Day]), 2))) IS NOT NULL
    ) AS src
    ON tgt.DateID = src.DateID
    WHEN MATCHED THEN
        UPDATE
           SET tgt.FullDate = src.FullDate,
               tgt.[Day] = src.[Day],
               tgt.[Month] = src.[Month],
               tgt.[Year] = src.[Year],
               tgt.QuarterNumber = src.QuarterNumber,
               tgt.MonthName = src.MonthName,
               tgt.WeekDayName = src.WeekDayName,
               tgt.WeekOfYear = src.WeekOfYear,
               tgt.IsWeekend = src.IsWeekend,
               tgt.AcademicPeriod = src.AcademicPeriod,
               tgt.SourceBatchID = @BatchID
    WHEN NOT MATCHED THEN
        INSERT
        (
            DateID, FullDate, [Day], [Month], [Year], QuarterNumber,
            MonthName, WeekDayName, WeekOfYear, IsWeekend, AcademicPeriod, SourceBatchID
        )
        VALUES
        (
            src.DateID, src.FullDate, src.[Day], src.[Month], src.[Year], src.QuarterNumber,
            src.MonthName, src.WeekDayName, src.WeekOfYear, src.IsWeekend, src.AcademicPeriod, @BatchID
        );

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_LoadDimDate',
        @LogLevel = N'INFO',
        @StepName = N'MERGE load',
        @LogMessage = N'Kalendarnoe izmerenie zagruzheno',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'dw.usp_LoadDimVisitType', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_LoadDimVisitType;
GO

CREATE PROCEDURE dw.usp_LoadDimVisitType
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dw.DimVisitType AS tgt
    USING
    (
        SELECT DISTINCT
            VisitTypeID,
            LTRIM(RTRIM(VisitName)) AS VisitName,
            LTRIM(RTRIM(VisitCode)) AS VisitCode,
            CASE
                WHEN UPPER(LTRIM(RTRIM(VisitCode))) IN (N'PR', N'PRI') THEN N'Pervichnyy vizit'
                WHEN UPPER(LTRIM(RTRIM(VisitCode))) IN (N'SC', N'SEC') THEN N'Povtornyy vizit'
                ELSE N'Inoy stsenariy'
            END AS VisitCategory
        FROM stg.DimVisitType_Raw
        WHERE VisitTypeID IS NOT NULL
    ) AS src
    ON tgt.VisitTypeID = src.VisitTypeID
    WHEN MATCHED THEN
        UPDATE
           SET tgt.VisitName = src.VisitName,
               tgt.VisitCode = src.VisitCode,
               tgt.VisitCategory = src.VisitCategory,
               tgt.SourceBatchID = @BatchID
    WHEN NOT MATCHED THEN
        INSERT (VisitTypeID, VisitName, VisitCode, VisitCategory, SourceBatchID)
        VALUES (src.VisitTypeID, src.VisitName, src.VisitCode, src.VisitCategory, @BatchID);

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_LoadDimVisitType',
        @LogLevel = N'INFO',
        @StepName = N'MERGE load',
        @LogMessage = N'Izmerenie tipov vizita zagruzheno',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'dw.usp_LoadDimDiagnosis', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_LoadDimDiagnosis;
GO

CREATE PROCEDURE dw.usp_LoadDimDiagnosis
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @Now datetime2(0) = sysdatetime();

    INSERT INTO ctl.RejectedRows (BatchID, SourceTable, BusinessKey, RejectionReason, Payload)
    SELECT
        @BatchID,
        N'stg.DimDiagnoses_Raw',
        CAST(DiagnosisID AS nvarchar(50)),
        N'Ne ukazan diagnoz ili kod MKB',
        CONCAT(N'ICDCode=', COALESCE(ICDCode, N'<NULL>'), N'; DiagnosisName=', COALESCE(DiagnosisName, N'<NULL>'))
    FROM stg.DimDiagnoses_Raw
    WHERE DiagnosisID IS NULL
       OR NULLIF(LTRIM(RTRIM(DiagnosisName)), N'') IS NULL;

    ;WITH valid_src AS
    (
        SELECT DISTINCT
            DiagnosisID,
            LTRIM(RTRIM(ICDCode)) AS ICDCode,
            LTRIM(RTRIM(DiagnosisName)) AS DiagnosisName,
            LTRIM(RTRIM(TreatmentMethod)) AS TreatmentMethod,
            CASE
                WHEN ICDCode LIKE N'K02%' THEN N'Karioznye porazheniya'
                WHEN ICDCode LIKE N'K04%' THEN N'Porazheniya pulpy'
                WHEN ICDCode LIKE N'K05%' THEN N'Parodontologicheskie zabolevaniya'
                WHEN ICDCode LIKE N'K07%' THEN N'Anomalii chelyustey'
                WHEN ICDCode LIKE N'K08%' THEN N'Prochie izmeneniya zubochelyustnoy sistemy'
                WHEN ICDCode LIKE N'K11%' OR ICDCode LIKE N'K12%' OR ICDCode LIKE N'K13%' THEN N'Zabolevaniya slizistoy i slyunnykh zhelez'
                ELSE N'Prochie diagnozy'
            END AS DiagnosisClass
        FROM stg.DimDiagnoses_Raw
        WHERE DiagnosisID IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(DiagnosisName)), N'') IS NOT NULL
    )
    UPDATE tgt
       SET EffectiveTo = DATEADD(SECOND, -1, @Now),
           IsCurrent = 0
    FROM dw.DimDiagnosis tgt
    JOIN valid_src src
      ON src.DiagnosisID = tgt.DiagnosisID
     AND tgt.IsCurrent = 1
    WHERE ISNULL(tgt.ICDCode, N'') <> ISNULL(src.ICDCode, N'')
       OR ISNULL(tgt.DiagnosisName, N'') <> ISNULL(src.DiagnosisName, N'')
       OR ISNULL(tgt.TreatmentMethod, N'') <> ISNULL(src.TreatmentMethod, N'')
       OR ISNULL(tgt.DiagnosisClass, N'') <> ISNULL(src.DiagnosisClass, N'');

    ;WITH valid_src AS
    (
        SELECT DISTINCT
            DiagnosisID,
            LTRIM(RTRIM(ICDCode)) AS ICDCode,
            LTRIM(RTRIM(DiagnosisName)) AS DiagnosisName,
            LTRIM(RTRIM(TreatmentMethod)) AS TreatmentMethod,
            CASE
                WHEN ICDCode LIKE N'K02%' THEN N'Karioznye porazheniya'
                WHEN ICDCode LIKE N'K04%' THEN N'Porazheniya pulpy'
                WHEN ICDCode LIKE N'K05%' THEN N'Parodontologicheskie zabolevaniya'
                WHEN ICDCode LIKE N'K07%' THEN N'Anomalii chelyustey'
                WHEN ICDCode LIKE N'K08%' THEN N'Prochie izmeneniya zubochelyustnoy sistemy'
                WHEN ICDCode LIKE N'K11%' OR ICDCode LIKE N'K12%' OR ICDCode LIKE N'K13%' THEN N'Zabolevaniya slizistoy i slyunnykh zhelez'
                ELSE N'Prochie diagnozy'
            END AS DiagnosisClass
        FROM stg.DimDiagnoses_Raw
        WHERE DiagnosisID IS NOT NULL
          AND NULLIF(LTRIM(RTRIM(DiagnosisName)), N'') IS NOT NULL
    )
    INSERT INTO dw.DimDiagnosis
    (
        DiagnosisID, ICDCode, DiagnosisName, TreatmentMethod, DiagnosisClass,
        EffectiveFrom, EffectiveTo, IsCurrent, SourceBatchID
    )
    SELECT
        src.DiagnosisID, src.ICDCode, src.DiagnosisName, src.TreatmentMethod, src.DiagnosisClass,
        @Now, '99991231', 1, @BatchID
    FROM valid_src src
    OUTER APPLY
    (
        SELECT TOP (1) *
        FROM dw.DimDiagnosis tgt
        WHERE tgt.DiagnosisID = src.DiagnosisID
          AND tgt.IsCurrent = 1
        ORDER BY tgt.DiagnosisKey DESC
    ) tgt
    WHERE tgt.DiagnosisKey IS NULL
       OR ISNULL(tgt.ICDCode, N'') <> ISNULL(src.ICDCode, N'')
       OR ISNULL(tgt.DiagnosisName, N'') <> ISNULL(src.DiagnosisName, N'')
       OR ISNULL(tgt.TreatmentMethod, N'') <> ISNULL(src.TreatmentMethod, N'')
       OR ISNULL(tgt.DiagnosisClass, N'') <> ISNULL(src.DiagnosisClass, N'');

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_LoadDimDiagnosis',
        @LogLevel = N'INFO',
        @StepName = N'SCD2 load',
        @LogMessage = N'Izmerenie diagnozov zagruzheno',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'dw.usp_LoadFactVisit', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_LoadFactVisit;
GO

CREATE PROCEDURE dw.usp_LoadFactVisit
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    INSERT INTO ctl.RejectedRows (BatchID, SourceTable, BusinessKey, RejectionReason, Payload)
    SELECT
        @BatchID,
        N'stg.FactVisits_Raw',
        CAST(FactID AS nvarchar(50)),
        N'Nevozmozhno preobrazovat chislovye pokazateli vizita',
        CONCAT(N'ServiceCost=', COALESCE(ServiceCostText, N'<NULL>'),
               N'; Profit=', COALESCE(ProfitText, N'<NULL>'),
               N'; RepeatRate=', COALESCE(RepeatRateText, N'<NULL>'))
    FROM stg.FactVisits_Raw src
    WHERE FactID IS NULL
       OR NOT EXISTS (SELECT 1 FROM stg.vw_FactVisits_Normalized n WHERE n.FactID = src.FactID AND n.ServiceCost IS NOT NULL AND n.Profit IS NOT NULL AND n.AvgTreatmentCost IS NOT NULL);

    INSERT INTO ctl.DataQualityIssue (BatchID, RuleCode, EntityName, EntityKey, Severity, IssueDescription)
    SELECT
        @BatchID,
        N'FK_MISSING',
        N'FactVisits',
        CAST(n.FactID AS nvarchar(50)),
        N'HIGH',
        CONCAT(N'Otsutstvuet izmerenie dlya fakta. DateID=', n.DateID, N', PatientID=', n.PatientID, N', DoctorID=', n.DoctorID, N', DiagnosisID=', n.DiagnosisID, N', VisitTypeID=', n.VisitTypeID)
    FROM stg.vw_FactVisits_Normalized n
    LEFT JOIN dw.DimDate dd ON dd.DateID = n.DateID
    LEFT JOIN dw.DimPatient dp ON dp.PatientID = n.PatientID AND dp.IsCurrent = 1
    LEFT JOIN dw.DimDoctor dr ON dr.DoctorID = n.DoctorID AND dr.IsCurrent = 1
    LEFT JOIN dw.DimDiagnosis dg ON dg.DiagnosisID = n.DiagnosisID AND dg.IsCurrent = 1
    LEFT JOIN dw.DimVisitType vt ON vt.VisitTypeID = n.VisitTypeID
    WHERE dd.DateKey IS NULL OR dp.PatientKey IS NULL OR dr.DoctorKey IS NULL OR dg.DiagnosisKey IS NULL OR vt.VisitTypeKey IS NULL;

    TRUNCATE TABLE dw.FactVisit;

    INSERT INTO dw.FactVisit
    (
        FactID, DateKey, PatientKey, DoctorKey, DiagnosisKey, VisitTypeKey,
        ServiceCost, VisitCount, Profit, PatientCount, RepeatRate, AvgTreatmentCost, MarginPercent, SourceBatchID
    )
    SELECT
        n.FactID,
        COALESCE(dd.DateKey, 0),
        COALESCE(dp.PatientKey, 0),
        COALESCE(dr.DoctorKey, 0),
        COALESCE(dg.DiagnosisKey, 0),
        COALESCE(vt.VisitTypeKey, 0),
        ISNULL(n.ServiceCost, 0),
        ISNULL(n.VisitCount, 0),
        ISNULL(n.Profit, 0),
        ISNULL(n.PatientCount, 0),
        ISNULL(n.RepeatRate, 0),
        ISNULL(n.AvgTreatmentCost, 0),
        CASE
            WHEN NULLIF(n.ServiceCost, 0) IS NULL THEN 0
            ELSE ROUND((n.Profit / NULLIF(n.ServiceCost, 0)) * 100.0, 4)
        END,
        @BatchID
    FROM stg.vw_FactVisits_Normalized n
    LEFT JOIN dw.DimDate dd
      ON dd.DateID = n.DateID
    LEFT JOIN dw.DimPatient dp
      ON dp.PatientID = n.PatientID
     AND dp.IsCurrent = 1
    LEFT JOIN dw.DimDoctor dr
      ON dr.DoctorID = n.DoctorID
     AND dr.IsCurrent = 1
    LEFT JOIN dw.DimDiagnosis dg
      ON dg.DiagnosisID = n.DiagnosisID
     AND dg.IsCurrent = 1
    LEFT JOIN dw.DimVisitType vt
      ON vt.VisitTypeID = n.VisitTypeID
    WHERE n.FactID IS NOT NULL;

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_LoadFactVisit',
        @LogLevel = N'INFO',
        @StepName = N'Fact load',
        @LogMessage = N'Faktovaya tablitsa vizitov zagruzhena',
        @AffectedRows = @@ROWCOUNT;
END;
GO

IF OBJECT_ID(N'dw.usp_RefreshAggregates', N'P') IS NOT NULL
    DROP PROCEDURE dw.usp_RefreshAggregates;
GO

CREATE PROCEDURE dw.usp_RefreshAggregates
    @BatchID int
AS
BEGIN
    SET NOCOUNT ON;

    TRUNCATE TABLE dw.AggregateDoctorMonth;
    TRUNCATE TABLE dw.AggregateDiagnosisMonth;

    INSERT INTO dw.AggregateDoctorMonth
    (
        [Year], [Month], DoctorKey, VisitQty, RevenueAmount, ProfitAmount,
        AvgRevenuePerVisit, AvgProfitPerVisit
    )
    SELECT
        dd.[Year],
        dd.[Month],
        fv.DoctorKey,
        SUM(fv.VisitCount),
        SUM(fv.ServiceCost),
        SUM(fv.Profit),
        CAST(AVG(CAST(fv.ServiceCost AS decimal(18,2))) AS decimal(18,2)),
        CAST(AVG(CAST(fv.Profit AS decimal(18,2))) AS decimal(18,2))
    FROM dw.FactVisit fv
    JOIN dw.DimDate dd
      ON dd.DateKey = fv.DateKey
    GROUP BY dd.[Year], dd.[Month], fv.DoctorKey;

    INSERT INTO dw.AggregateDiagnosisMonth
    (
        [Year], [Month], DiagnosisKey, VisitQty, RevenueAmount, ProfitAmount, RepeatRateAvg
    )
    SELECT
        dd.[Year],
        dd.[Month],
        fv.DiagnosisKey,
        SUM(fv.VisitCount),
        SUM(fv.ServiceCost),
        SUM(fv.Profit),
        CAST(AVG(CAST(fv.RepeatRate AS decimal(9,4))) AS decimal(9,4))
    FROM dw.FactVisit fv
    JOIN dw.DimDate dd
      ON dd.DateKey = fv.DateKey
    GROUP BY dd.[Year], dd.[Month], fv.DiagnosisKey;

    EXEC ctl.usp_WriteLog
        @BatchID = @BatchID,
        @ProcedureName = N'dw.usp_RefreshAggregates',
        @LogLevel = N'INFO',
        @StepName = N'Aggregate refresh',
        @LogMessage = N'Agregirovannye tablitsy obnovleny',
        @AffectedRows = (SELECT COUNT(*) FROM dw.AggregateDoctorMonth) + (SELECT COUNT(*) FROM dw.AggregateDiagnosisMonth);
END;
GO

IF OBJECT_ID(N'ctl.usp_RunFullLoad', N'P') IS NOT NULL
    DROP PROCEDURE ctl.usp_RunFullLoad;
GO

CREATE PROCEDURE ctl.usp_RunFullLoad
    @BasePath nvarchar(4000) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @BatchID int;
    DECLARE @RowsRead int;
    DECLARE @RowsLoaded int;
    DECLARE @RowsRejected int;

    BEGIN TRY
        EXEC ctl.usp_StartBatch
            @BatchName = N'Polnaya zagruzka stomatologicheskogo OLAP-khranilischa',
            @BatchID = @BatchID OUTPUT;

        EXEC stg.usp_LoadRawFromCsv
            @BasePath = @BasePath,
            @BatchID = @BatchID;

        EXEC dw.usp_LoadDimPatient @BatchID = @BatchID;
        EXEC dw.usp_LoadDimDoctor @BatchID = @BatchID;
        EXEC dw.usp_LoadDimDate @BatchID = @BatchID;
        EXEC dw.usp_LoadDimVisitType @BatchID = @BatchID;
        EXEC dw.usp_LoadDimDiagnosis @BatchID = @BatchID;
        EXEC dw.usp_LoadFactVisit @BatchID = @BatchID;
        EXEC dw.usp_RefreshAggregates @BatchID = @BatchID;

        SET @RowsRead =
              (SELECT COUNT(*) FROM stg.DimPatients_Raw)
            + (SELECT COUNT(*) FROM stg.DimDoctors_Raw)
            + (SELECT COUNT(*) FROM stg.DimDate_Raw)
            + (SELECT COUNT(*) FROM stg.DimVisitType_Raw)
            + (SELECT COUNT(*) FROM stg.DimDiagnoses_Raw)
            + (SELECT COUNT(*) FROM stg.FactVisits_Raw);

        SET @RowsLoaded =
              (SELECT COUNT(*) FROM dw.DimPatient WHERE PatientKey > 0 AND IsCurrent = 1)
            + (SELECT COUNT(*) FROM dw.DimDoctor WHERE DoctorKey > 0 AND IsCurrent = 1)
            + (SELECT COUNT(*) FROM dw.DimDate WHERE DateKey > 0)
            + (SELECT COUNT(*) FROM dw.DimVisitType WHERE VisitTypeKey > 0)
            + (SELECT COUNT(*) FROM dw.DimDiagnosis WHERE DiagnosisKey > 0 AND IsCurrent = 1)
            + (SELECT COUNT(*) FROM dw.FactVisit);

        SET @RowsRejected = (SELECT COUNT(*) FROM ctl.RejectedRows WHERE BatchID = @BatchID);

        EXEC ctl.usp_FinishBatch
            @BatchID = @BatchID,
            @Status = N'SUCCESS',
            @RowsRead = @RowsRead,
            @RowsLoaded = @RowsLoaded,
            @RowsRejected = @RowsRejected,
            @CommentText = N'Polnaya zagruzka zavershena uspeshno';
    END TRY
    BEGIN CATCH
        IF @BatchID IS NOT NULL
        BEGIN
            EXEC ctl.usp_WriteLog
                @BatchID = @BatchID,
                @ProcedureName = N'ctl.usp_RunFullLoad',
                @LogLevel = N'ERROR',
                @StepName = N'Unhandled exception',
                @LogMessage = ERROR_MESSAGE(),
                @AffectedRows = NULL;

            EXEC ctl.usp_FinishBatch
                @BatchID = @BatchID,
                @Status = N'FAILED',
                @CommentText = ERROR_MESSAGE();
        END;

        THROW;
    END CATCH;
END;
GO
