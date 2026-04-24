USE [DentalClinicDW];
GO

CREATE OR ALTER VIEW dbo.DimPatients
AS
SELECT
    PatientID,
    FullName AS [Name],
    Age,
    Phone
FROM dw.DimPatient
WHERE IsCurrent = 1;
GO

CREATE OR ALTER VIEW dbo.DimDoctors
AS
SELECT
    DoctorID,
    FullName AS [Name],
    Specialty
FROM dw.DimDoctor
WHERE IsCurrent = 1;
GO

CREATE OR ALTER VIEW dbo.DimDate
AS
SELECT
    DateID,
    [Day],
    [Month],
    [Year],
    WeekDayName AS WeekDay
FROM dw.DimDate;
GO

CREATE OR ALTER VIEW dbo.DimVisitType
AS
SELECT
    VisitTypeID,
    VisitName,
    VisitCode
FROM dw.DimVisitType;
GO

CREATE OR ALTER VIEW dbo.DimDiagnoses
AS
SELECT
    DiagnosisID,
    ICDCode,
    DiagnosisName,
    TreatmentMethod
FROM dw.DimDiagnosis
WHERE IsCurrent = 1;
GO

CREATE OR ALTER VIEW dbo.FactVisits
AS
SELECT
    fv.FactID,
    dd.DateID,
    dp.PatientID,
    dr.DoctorID,
    dg.DiagnosisID,
    vt.VisitTypeID,
    fv.ServiceCost,
    fv.VisitCount,
    fv.Profit,
    fv.PatientCount,
    fv.RepeatRate,
    fv.AvgTreatmentCost
FROM dw.FactVisit fv
JOIN dw.DimDate dd
  ON dd.DateKey = fv.DateKey
JOIN dw.DimPatient dp
  ON dp.PatientKey = fv.PatientKey
JOIN dw.DimDoctor dr
  ON dr.DoctorKey = fv.DoctorKey
JOIN dw.DimDiagnosis dg
  ON dg.DiagnosisKey = fv.DiagnosisKey
JOIN dw.DimVisitType vt
  ON vt.VisitTypeKey = fv.VisitTypeKey;
GO

CREATE OR ALTER VIEW mart.vw_VisitDetailed
AS
SELECT
    fv.VisitKey,
    fv.FactID,
    dd.FullDate,
    dd.[Year],
    dd.[Month],
    dd.QuarterNumber,
    dd.MonthName,
    dd.WeekDayName,
    dd.IsWeekend,
    dd.AcademicPeriod,
    dp.PatientID,
    dp.FullName AS PatientName,
    dp.Age,
    dp.AgeBand,
    dr.DoctorID,
    dr.FullName AS DoctorName,
    dr.Specialty,
    dr.SpecialtyGroup,
    dg.DiagnosisID,
    dg.ICDCode,
    dg.DiagnosisName,
    dg.DiagnosisClass,
    vt.VisitTypeID,
    vt.VisitName,
    vt.VisitCategory,
    fv.ServiceCost,
    fv.Profit,
    fv.MarginPercent,
    fv.VisitCount,
    fv.PatientCount,
    fv.RepeatRate,
    fv.AvgTreatmentCost
FROM dw.FactVisit fv
JOIN dw.DimDate dd
  ON dd.DateKey = fv.DateKey
JOIN dw.DimPatient dp
  ON dp.PatientKey = fv.PatientKey
JOIN dw.DimDoctor dr
  ON dr.DoctorKey = fv.DoctorKey
JOIN dw.DimDiagnosis dg
  ON dg.DiagnosisKey = fv.DiagnosisKey
JOIN dw.DimVisitType vt
  ON vt.VisitTypeKey = fv.VisitTypeKey;
GO

CREATE OR ALTER VIEW mart.vw_DoctorPerformance
AS
SELECT
    dr.DoctorID,
    dr.FullName AS DoctorName,
    dr.Specialty,
    dr.SpecialtyGroup,
    dd.[Year],
    dd.[Month],
    COUNT_BIG(*) AS RowCountForIndexedUse,
    SUM(fv.VisitCount) AS VisitQty,
    SUM(fv.ServiceCost) AS RevenueAmount,
    SUM(fv.Profit) AS ProfitAmount,
    CAST(AVG(CAST(fv.ServiceCost AS decimal(18,2))) AS decimal(18,2)) AS AvgCheck,
    CAST(AVG(CAST(fv.MarginPercent AS decimal(9,4))) AS decimal(9,4)) AS AvgMarginPercent,
    CAST(AVG(CAST(fv.RepeatRate AS decimal(9,4))) AS decimal(9,4)) AS AvgRepeatRate
FROM dw.FactVisit fv
JOIN dw.DimDoctor dr
  ON dr.DoctorKey = fv.DoctorKey
JOIN dw.DimDate dd
  ON dd.DateKey = fv.DateKey
GROUP BY
    dr.DoctorID,
    dr.FullName,
    dr.Specialty,
    dr.SpecialtyGroup,
    dd.[Year],
    dd.[Month];
GO

CREATE OR ALTER VIEW mart.vw_DiagnosisPerformance
AS
SELECT
    dg.DiagnosisID,
    dg.ICDCode,
    dg.DiagnosisName,
    dg.DiagnosisClass,
    dd.[Year],
    dd.[Month],
    SUM(fv.VisitCount) AS VisitQty,
    SUM(fv.ServiceCost) AS RevenueAmount,
    SUM(fv.Profit) AS ProfitAmount,
    CAST(AVG(CAST(fv.RepeatRate AS decimal(9,4))) AS decimal(9,4)) AS AvgRepeatRate,
    CAST(AVG(CAST(fv.AvgTreatmentCost AS decimal(18,2))) AS decimal(18,2)) AS AvgTreatmentCost
FROM dw.FactVisit fv
JOIN dw.DimDiagnosis dg
  ON dg.DiagnosisKey = fv.DiagnosisKey
JOIN dw.DimDate dd
  ON dd.DateKey = fv.DateKey
GROUP BY
    dg.DiagnosisID,
    dg.ICDCode,
    dg.DiagnosisName,
    dg.DiagnosisClass,
    dd.[Year],
    dd.[Month];
GO

CREATE OR ALTER VIEW mart.vw_PatientSegment
AS
SELECT
    dp.PatientID,
    dp.FullName AS PatientName,
    dp.Age,
    dp.AgeBand,
    COUNT(fv.VisitKey) AS VisitRows,
    SUM(fv.VisitCount) AS VisitQty,
    SUM(fv.ServiceCost) AS RevenueAmount,
    SUM(fv.Profit) AS ProfitAmount,
    MIN(dd.FullDate) AS FirstVisitDate,
    MAX(dd.FullDate) AS LastVisitDate,
    DATEDIFF(DAY, MIN(dd.FullDate), MAX(dd.FullDate)) AS VisitSpanDays,
    CAST(AVG(CAST(fv.RepeatRate AS decimal(9,4))) AS decimal(9,4)) AS AvgRepeatRate
FROM dw.FactVisit fv
JOIN dw.DimPatient dp
  ON dp.PatientKey = fv.PatientKey
JOIN dw.DimDate dd
  ON dd.DateKey = fv.DateKey
GROUP BY
    dp.PatientID,
    dp.FullName,
    dp.Age,
    dp.AgeBand;
GO

CREATE OR ALTER VIEW mart.vw_TimeSeries
AS
SELECT
    dd.FullDate,
    dd.[Year],
    dd.[Month],
    dd.MonthName,
    dd.QuarterNumber,
    SUM(fv.VisitCount) AS VisitQty,
    SUM(fv.ServiceCost) AS RevenueAmount,
    SUM(fv.Profit) AS ProfitAmount,
    CAST(AVG(CAST(fv.AvgTreatmentCost AS decimal(18,2))) AS decimal(18,2)) AS AvgTreatmentCost,
    CAST(AVG(CAST(fv.RepeatRate AS decimal(9,4))) AS decimal(9,4)) AS AvgRepeatRate
FROM dw.FactVisit fv
JOIN dw.DimDate dd
  ON dd.DateKey = fv.DateKey
GROUP BY
    dd.FullDate,
    dd.[Year],
    dd.[Month],
    dd.MonthName,
    dd.QuarterNumber;
GO

IF OBJECT_ID(N'mart.usp_DoctorDashboard', N'P') IS NOT NULL
    DROP PROCEDURE mart.usp_DoctorDashboard;
GO

CREATE PROCEDURE mart.usp_DoctorDashboard
    @Year smallint = NULL,
    @Month tinyint = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        DoctorID,
        DoctorName,
        Specialty,
        SpecialtyGroup,
        [Year],
        [Month],
        VisitQty,
        RevenueAmount,
        ProfitAmount,
        AvgCheck,
        AvgMarginPercent,
        AvgRepeatRate,
        DENSE_RANK() OVER (PARTITION BY [Year], [Month] ORDER BY RevenueAmount DESC) AS RevenueRank
    FROM mart.vw_DoctorPerformance
    WHERE (@Year IS NULL OR [Year] = @Year)
      AND (@Month IS NULL OR [Month] = @Month)
    ORDER BY [Year], [Month], RevenueRank, DoctorName;
END;
GO

IF OBJECT_ID(N'mart.usp_DiagnosisDashboard', N'P') IS NOT NULL
    DROP PROCEDURE mart.usp_DiagnosisDashboard;
GO

CREATE PROCEDURE mart.usp_DiagnosisDashboard
    @DiagnosisClass nvarchar(100) = NULL
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        DiagnosisClass,
        ICDCode,
        DiagnosisName,
        [Year],
        [Month],
        VisitQty,
        RevenueAmount,
        ProfitAmount,
        AvgRepeatRate,
        AvgTreatmentCost,
        SUM(RevenueAmount) OVER (PARTITION BY DiagnosisClass) AS ClassRevenue,
        CAST(100.0 * RevenueAmount / NULLIF(SUM(RevenueAmount) OVER (PARTITION BY DiagnosisClass), 0) AS decimal(9,2)) AS ShareWithinClass
    FROM mart.vw_DiagnosisPerformance
    WHERE (@DiagnosisClass IS NULL OR DiagnosisClass = @DiagnosisClass)
    ORDER BY DiagnosisClass, RevenueAmount DESC, DiagnosisName;
END;
GO

IF OBJECT_ID(N'mart.usp_PatientAgeAnalytics', N'P') IS NOT NULL
    DROP PROCEDURE mart.usp_PatientAgeAnalytics;
GO

CREATE PROCEDURE mart.usp_PatientAgeAnalytics
AS
BEGIN
    SET NOCOUNT ON;

    SELECT
        AgeBand,
        COUNT(*) AS PatientQty,
        SUM(VisitQty) AS VisitQty,
        SUM(RevenueAmount) AS RevenueAmount,
        SUM(ProfitAmount) AS ProfitAmount,
        CAST(AVG(CAST(AvgRepeatRate AS decimal(9,4))) AS decimal(9,4)) AS AvgRepeatRate
    FROM mart.vw_PatientSegment
    GROUP BY AgeBand
    ORDER BY MIN(CASE
        WHEN AgeBand = N'Do 18 let' THEN 1
        WHEN AgeBand = N'18-25' THEN 2
        WHEN AgeBand = N'26-35' THEN 3
        WHEN AgeBand = N'36-45' THEN 4
        WHEN AgeBand = N'46-60' THEN 5
        WHEN AgeBand = N'60+' THEN 6
        ELSE 7
    END);
END;
GO

GRANT SELECT ON dbo.DimPatients TO clinic_analyst;
GRANT SELECT ON dbo.DimDoctors TO clinic_analyst;
GRANT SELECT ON dbo.DimDate TO clinic_analyst;
GRANT SELECT ON dbo.DimVisitType TO clinic_analyst;
GRANT SELECT ON dbo.DimDiagnoses TO clinic_analyst;
GRANT SELECT ON dbo.FactVisits TO clinic_analyst;
GRANT SELECT ON mart.vw_VisitDetailed TO clinic_analyst;
GRANT SELECT ON mart.vw_DoctorPerformance TO clinic_analyst;
GRANT SELECT ON mart.vw_DiagnosisPerformance TO clinic_analyst;
GRANT SELECT ON mart.vw_PatientSegment TO clinic_analyst;
GRANT SELECT ON mart.vw_TimeSeries TO clinic_analyst;
GO

GRANT EXECUTE ON stg.usp_LoadRawFromCsv TO clinic_etl_operator;
GRANT EXECUTE ON ctl.usp_RunFullLoad TO clinic_etl_operator;
GRANT EXECUTE ON mart.usp_DoctorDashboard TO clinic_analyst;
GRANT EXECUTE ON mart.usp_DiagnosisDashboard TO clinic_analyst;
GRANT EXECUTE ON mart.usp_PatientAgeAnalytics TO clinic_analyst;
GO
