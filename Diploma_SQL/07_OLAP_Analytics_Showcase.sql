USE [DentalClinicDW];
GO

/*
    Demonstratsionnyy nabor analiticheskikh zaprosov dlya diplomnoy raboty.
    Skript pokazyvaet operatsii slice, dice, drill-down, roll-up, pivot,
    ranzhirovanie, agregirovanie i sravnitelnyy analiz deyatelnosti stomatologicheskogo kabineta.
*/

DECLARE @ReportYear smallint = NULL;
DECLARE @ReportMonth tinyint = NULL;
DECLARE @TopN int = 5;

-- Q01. Obschie KPI po vsey baze.
SELECT
    COUNT(*) AS FactRowQty,
    SUM(VisitCount) AS TotalVisits,
    SUM(ServiceCost) AS TotalRevenue,
    SUM(Profit) AS TotalProfit,
    AVG(AvgTreatmentCost) AS MeanTreatmentCost,
    AVG(RepeatRate) AS MeanRepeatRate
FROM dw.FactVisit;

-- Q02. Kolichestvo strok izmereniy i faktov.
SELECT N'DimPatient' AS ObjectName, COUNT(*) AS RowQty FROM dw.DimPatient WHERE IsCurrent = 1
UNION ALL
SELECT N'DimDoctor', COUNT(*) FROM dw.DimDoctor WHERE IsCurrent = 1
UNION ALL
SELECT N'DimDate', COUNT(*) FROM dw.DimDate WHERE FullDate IS NOT NULL
UNION ALL
SELECT N'DimVisitType', COUNT(*) FROM dw.DimVisitType WHERE VisitTypeKey > 0
UNION ALL
SELECT N'DimDiagnosis', COUNT(*) FROM dw.DimDiagnosis WHERE IsCurrent = 1
UNION ALL
SELECT N'FactVisit', COUNT(*) FROM dw.FactVisit;

-- Q03. Vyruchka, pribyl i kolichestvo vizitov po godam.
SELECT
    [Year],
    SUM(VisitQty) AS VisitQty,
    SUM(RevenueAmount) AS RevenueAmount,
    SUM(ProfitAmount) AS ProfitAmount
FROM mart.vw_TimeSeries
GROUP BY [Year]
ORDER BY [Year];

-- Q04. Te zhe pokazateli po mesyatsam.
SELECT
    [Year],
    [Month],
    SUM(VisitQty) AS VisitQty,
    SUM(RevenueAmount) AS RevenueAmount,
    SUM(ProfitAmount) AS ProfitAmount
FROM mart.vw_TimeSeries
GROUP BY [Year], [Month]
ORDER BY [Year], [Month];

-- Q05. Sredniy chek po kalendarnym datam.
SELECT
    FullDate,
    RevenueAmount,
    VisitQty,
    CAST(RevenueAmount / NULLIF(VisitQty, 0) AS decimal(18,2)) AS AvgCheck
FROM mart.vw_TimeSeries
ORDER BY FullDate;

-- Q06. Minimalnaya i maksimalnaya data vizitov.
SELECT
    MIN(FullDate) AS MinVisitDate,
    MAX(FullDate) AS MaxVisitDate
FROM mart.vw_TimeSeries;

-- Q07. Vyruchka i pribyl po tipam vizita.
SELECT
    VisitName,
    VisitCategory,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY VisitName, VisitCategory
ORDER BY RevenueAmount DESC;

-- Q08. Raspredelenie vizitov po vozrastnym segmentam patsientov.
SELECT
    AgeBand,
    SUM(VisitCount) AS VisitQty,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
GROUP BY AgeBand
ORDER BY RevenueAmount DESC;

-- Q09. Raspredelenie posescheniy po vrachebnym spetsialnostyam.
SELECT
    Specialty,
    SUM(VisitCount) AS VisitQty,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
GROUP BY Specialty
ORDER BY RevenueAmount DESC;

-- Q10. Raspredelenie vyruchki po klassam diagnozov.
SELECT
    DiagnosisClass,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY DiagnosisClass
ORDER BY RevenueAmount DESC;

-- Q11. Drill-down: god -> kvartal -> mesyats.
SELECT
    [Year],
    QuarterNumber,
    [Month],
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY [Year], QuarterNumber, [Month]
ORDER BY [Year], QuarterNumber, [Month];

-- Q12. Slice po konkretnomu tipu vizita: pervichnye vizity.
SELECT
    FullDate,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
WHERE VisitCategory = N'Pervichnyy vizit'
GROUP BY FullDate
ORDER BY FullDate;

-- Q13. Slice po povtornym vizitam.
SELECT
    FullDate,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
WHERE VisitCategory = N'Povtornyy vizit'
GROUP BY FullDate
ORDER BY FullDate;

-- Q14. Dice: terapevty i ortopedy za osenniy period.
SELECT
    Specialty,
    AcademicPeriod,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
WHERE Specialty IN (N'Terapevt', N'Ortoped')
  AND AcademicPeriod = N'Osen'
GROUP BY Specialty, AcademicPeriod
ORDER BY RevenueAmount DESC;

-- Q15. Profil sezona po klassam diagnozov.
SELECT
    AcademicPeriod,
    DiagnosisClass,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY AcademicPeriod, DiagnosisClass
ORDER BY AcademicPeriod, RevenueAmount DESC;

-- Q16. Dinamika pribyli po dnyam nedeli.
SELECT
    WeekDayName,
    SUM(Profit) AS ProfitAmount,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY WeekDayName
ORDER BY ProfitAmount DESC;

-- Q17. Dolya vykhodnykh i budnikh vizitov.
SELECT
    CASE WHEN IsWeekend = 1 THEN N'Vykhodnye' ELSE N'Budni' END AS DayType,
    SUM(VisitCount) AS VisitQty,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
GROUP BY CASE WHEN IsWeekend = 1 THEN N'Vykhodnye' ELSE N'Budni' END;

-- Q18. Skolzyaschee srednee vyruchki po datam.
SELECT
    FullDate,
    RevenueAmount,
    CAST(AVG(RevenueAmount) OVER (ORDER BY FullDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS decimal(18,2)) AS MovingAvg3
FROM mart.vw_TimeSeries
ORDER BY FullDate;

-- Q19. Nakopitelnaya vyruchka.
SELECT
    FullDate,
    RevenueAmount,
    SUM(RevenueAmount) OVER (ORDER BY FullDate ROWS UNBOUNDED PRECEDING) AS RunningRevenue
FROM mart.vw_TimeSeries
ORDER BY FullDate;

-- Q20. Nakopitelnaya pribyl.
SELECT
    FullDate,
    ProfitAmount,
    SUM(ProfitAmount) OVER (ORDER BY FullDate ROWS UNBOUNDED PRECEDING) AS RunningProfit
FROM mart.vw_TimeSeries
ORDER BY FullDate;

-- Q21. Reyting vrachey po vyruchke.
SELECT
    DoctorName,
    Specialty,
    SUM(RevenueAmount) AS RevenueAmount,
    RANK() OVER (ORDER BY SUM(RevenueAmount) DESC) AS RevenueRank
FROM mart.vw_DoctorPerformance
GROUP BY DoctorName, Specialty
ORDER BY RevenueRank, DoctorName;

-- Q22. Reyting vrachey po pribyli.
SELECT
    DoctorName,
    Specialty,
    SUM(ProfitAmount) AS ProfitAmount,
    RANK() OVER (ORDER BY SUM(ProfitAmount) DESC) AS ProfitRank
FROM mart.vw_DoctorPerformance
GROUP BY DoctorName, Specialty
ORDER BY ProfitRank, DoctorName;

-- Q23. Top-N vrachey po srednemu cheku.
SELECT TOP (@TopN)
    DoctorName,
    Specialty,
    AVG(AvgCheck) AS AvgCheck
FROM mart.vw_DoctorPerformance
GROUP BY DoctorName, Specialty
ORDER BY AvgCheck DESC;

-- Q24. Top-N vrachey po srednemu pokazatelyu povtornosti.
SELECT TOP (@TopN)
    DoctorName,
    Specialty,
    AVG(AvgRepeatRate) AS AvgRepeatRate
FROM mart.vw_DoctorPerformance
GROUP BY DoctorName, Specialty
ORDER BY AvgRepeatRate DESC;

-- Q25. Sravnenie vrachey vnutri spetsialnosti.
SELECT
    Specialty,
    DoctorName,
    SUM(RevenueAmount) AS RevenueAmount,
    DENSE_RANK() OVER (PARTITION BY Specialty ORDER BY SUM(RevenueAmount) DESC) AS RankWithinSpecialty
FROM mart.vw_DoctorPerformance
GROUP BY Specialty, DoctorName
ORDER BY Specialty, RankWithinSpecialty, DoctorName;

-- Q26. Srednyaya marzhinalnost po vracham.
SELECT
    DoctorName,
    Specialty,
    CAST(AVG(AvgMarginPercent) AS decimal(9,4)) AS AvgMarginPercent
FROM mart.vw_DoctorPerformance
GROUP BY DoctorName, Specialty
ORDER BY AvgMarginPercent DESC;

-- Q27. Vyruchka vrachey po mesyatsam.
SELECT
    DoctorName,
    [Year],
    [Month],
    RevenueAmount
FROM mart.vw_DoctorPerformance
ORDER BY DoctorName, [Year], [Month];

-- Q28. Izmenenie vyruchki vracha mesyats k mesyatsu.
SELECT
    DoctorName,
    [Year],
    [Month],
    RevenueAmount,
    RevenueAmount - LAG(RevenueAmount) OVER (PARTITION BY DoctorName ORDER BY [Year], [Month]) AS DeltaToPreviousMonth
FROM mart.vw_DoctorPerformance
ORDER BY DoctorName, [Year], [Month];

-- Q29. Vklad vrachey v obschuyu vyruchku.
SELECT
    DoctorName,
    SUM(RevenueAmount) AS RevenueAmount,
    CAST(100.0 * SUM(RevenueAmount) / NULLIF(SUM(SUM(RevenueAmount)) OVER (), 0) AS decimal(9,2)) AS RevenueSharePercent
FROM mart.vw_DoctorPerformance
GROUP BY DoctorName
ORDER BY RevenueAmount DESC;

-- Q30. Vklad vrachebnykh spetsialnostey v pribyl.
SELECT
    Specialty,
    SUM(ProfitAmount) AS ProfitAmount,
    CAST(100.0 * SUM(ProfitAmount) / NULLIF(SUM(SUM(ProfitAmount)) OVER (), 0) AS decimal(9,2)) AS ProfitSharePercent
FROM mart.vw_DoctorPerformance
GROUP BY Specialty
ORDER BY ProfitAmount DESC;

-- Q31. Samye dokhodnye diagnozy.
SELECT TOP (@TopN)
    DiagnosisName,
    ICDCode,
    SUM(RevenueAmount) AS RevenueAmount
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisName, ICDCode
ORDER BY RevenueAmount DESC;

-- Q32. Samye pribylnye diagnozy.
SELECT TOP (@TopN)
    DiagnosisName,
    ICDCode,
    SUM(ProfitAmount) AS ProfitAmount
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisName, ICDCode
ORDER BY ProfitAmount DESC;

-- Q33. Chastota diagnozov.
SELECT
    DiagnosisClass,
    DiagnosisName,
    SUM(VisitQty) AS VisitQty
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisClass, DiagnosisName
ORDER BY VisitQty DESC, DiagnosisName;

-- Q34. Srednyaya stoimost lecheniya po diagnozam.
SELECT
    DiagnosisName,
    CAST(AVG(AvgTreatmentCost) AS decimal(18,2)) AS AvgTreatmentCost
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisName
ORDER BY AvgTreatmentCost DESC, DiagnosisName;

-- Q35. Srednyaya povtornost po diagnozam.
SELECT
    DiagnosisName,
    CAST(AVG(AvgRepeatRate) AS decimal(9,4)) AS AvgRepeatRate
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisName
ORDER BY AvgRepeatRate DESC, DiagnosisName;

-- Q36. Vklad klassov diagnozov v vyruchku.
SELECT
    DiagnosisClass,
    SUM(RevenueAmount) AS RevenueAmount,
    CAST(100.0 * SUM(RevenueAmount) / NULLIF(SUM(SUM(RevenueAmount)) OVER (), 0) AS decimal(9,2)) AS RevenueSharePercent
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisClass
ORDER BY RevenueAmount DESC;

-- Q37. Vklad klassov diagnozov v kolichestvo vizitov.
SELECT
    DiagnosisClass,
    SUM(VisitQty) AS VisitQty,
    CAST(100.0 * SUM(VisitQty) / NULLIF(SUM(SUM(VisitQty)) OVER (), 0) AS decimal(9,2)) AS VisitSharePercent
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisClass
ORDER BY VisitQty DESC;

-- Q38. Dinamika vybrannogo diagnoza po mesyatsam.
SELECT
    DiagnosisName,
    [Year],
    [Month],
    VisitQty,
    RevenueAmount,
    ProfitAmount
FROM mart.vw_DiagnosisPerformance
WHERE DiagnosisName = (SELECT TOP (1) DiagnosisName FROM mart.vw_DiagnosisPerformance GROUP BY DiagnosisName ORDER BY SUM(RevenueAmount) DESC)
ORDER BY [Year], [Month];

-- Q39. Diagnozy s samoy vysokoy sredney povtornostyu.
SELECT TOP (@TopN)
    DiagnosisName,
    AVG(AvgRepeatRate) AS AvgRepeatRate
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisName
ORDER BY AvgRepeatRate DESC;

-- Q40. Diagnozy s samoy vysokoy sredney pribylyu na vizit.
SELECT TOP (@TopN)
    DiagnosisName,
    CAST(SUM(ProfitAmount) / NULLIF(SUM(VisitQty), 0) AS decimal(18,2)) AS ProfitPerVisit
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisName
ORDER BY ProfitPerVisit DESC;

-- Q41. Top patsientov po vyruchke.
SELECT TOP (@TopN)
    PatientName,
    AgeBand,
    RevenueAmount,
    ProfitAmount,
    VisitQty
FROM mart.vw_PatientSegment
ORDER BY RevenueAmount DESC, PatientName;

-- Q42. Top patsientov po kolichestvu vizitov.
SELECT TOP (@TopN)
    PatientName,
    AgeBand,
    VisitQty,
    RevenueAmount
FROM mart.vw_PatientSegment
ORDER BY VisitQty DESC, PatientName;

-- Q43. Patsienty s maksimalnoy povtornostyu.
SELECT TOP (@TopN)
    PatientName,
    AgeBand,
    AvgRepeatRate
FROM mart.vw_PatientSegment
ORDER BY AvgRepeatRate DESC, PatientName;

-- Q44. Dlitelnost klientskogo tsikla po vozrastnym segmentam.
SELECT
    AgeBand,
    COUNT(*) AS PatientQty,
    AVG(VisitSpanDays * 1.0) AS AvgVisitSpanDays,
    SUM(RevenueAmount) AS RevenueAmount
FROM mart.vw_PatientSegment
GROUP BY AgeBand
ORDER BY AvgVisitSpanDays DESC;

-- Q45. ABC-analiz patsientov po vyruchke.
WITH patient_revenue AS
(
    SELECT
        PatientName,
        RevenueAmount,
        SUM(RevenueAmount) OVER (ORDER BY RevenueAmount DESC ROWS UNBOUNDED PRECEDING) AS RunningRevenue,
        SUM(RevenueAmount) OVER () AS TotalRevenue
    FROM mart.vw_PatientSegment
)
SELECT
    PatientName,
    RevenueAmount,
    CAST(100.0 * RunningRevenue / NULLIF(TotalRevenue, 0) AS decimal(9,2)) AS CumulativeShare,
    CASE
        WHEN 100.0 * RunningRevenue / NULLIF(TotalRevenue, 0) <= 80 THEN N'A'
        WHEN 100.0 * RunningRevenue / NULLIF(TotalRevenue, 0) <= 95 THEN N'B'
        ELSE N'C'
    END AS AbcGroup
FROM patient_revenue
ORDER BY RevenueAmount DESC, PatientName;

-- Q46. Vozrastnye segmenty i sredniy chek.
SELECT
    AgeBand,
    CAST(SUM(RevenueAmount) / NULLIF(SUM(VisitQty), 0) AS decimal(18,2)) AS AvgCheck
FROM mart.vw_PatientSegment
GROUP BY AgeBand
ORDER BY AvgCheck DESC;

-- Q47. Vklad vozrastnykh segmentov v pribyl.
SELECT
    AgeBand,
    SUM(ProfitAmount) AS ProfitAmount,
    CAST(100.0 * SUM(ProfitAmount) / NULLIF(SUM(SUM(ProfitAmount)) OVER (), 0) AS decimal(9,2)) AS ProfitSharePercent
FROM mart.vw_PatientSegment
GROUP BY AgeBand
ORDER BY ProfitAmount DESC;

-- Q48. Patsienty s vysokoy chastotoy vizitov i vysokoy vyruchkoy.
SELECT
    PatientName,
    AgeBand,
    VisitQty,
    RevenueAmount,
    CASE
        WHEN VisitQty >= AVG(VisitQty * 1.0) OVER () AND RevenueAmount >= AVG(RevenueAmount * 1.0) OVER () THEN N'Vysokiy potentsial'
        ELSE N'Standartnyy profil'
    END AS SegmentLabel
FROM mart.vw_PatientSegment
ORDER BY RevenueAmount DESC, VisitQty DESC;

-- Q49. Plotnost vyruchki na 1 den klientskogo tsikla.
SELECT
    PatientName,
    VisitSpanDays,
    RevenueAmount,
    CAST(RevenueAmount / NULLIF(CASE WHEN VisitSpanDays = 0 THEN 1 ELSE VisitSpanDays END, 0) AS decimal(18,2)) AS RevenuePerLifecycleDay
FROM mart.vw_PatientSegment
ORDER BY RevenuePerLifecycleDay DESC, PatientName;

-- Q50. Vozrast patsienta i dokhod po vrachu.
SELECT
    DoctorName,
    AgeBand,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY DoctorName, AgeBand
ORDER BY DoctorName, RevenueAmount DESC;

-- Q51. ROLLUP po vremeni.
SELECT
    [Year],
    [Month],
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
GROUP BY ROLLUP ([Year], [Month])
ORDER BY [Year], [Month];

-- Q52. ROLLUP po spetsialnosti vracha i tipu vizita.
SELECT
    Specialty,
    VisitCategory,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY ROLLUP (Specialty, VisitCategory)
ORDER BY Specialty, VisitCategory;

-- Q53. CUBE po vozrastnomu segmentu i tipu vizita.
SELECT
    AgeBand,
    VisitCategory,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
GROUP BY CUBE (AgeBand, VisitCategory)
ORDER BY AgeBand, VisitCategory;

-- Q54. CUBE po spetsialnosti vracha i klassu diagnoza.
SELECT
    Specialty,
    DiagnosisClass,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY CUBE (Specialty, DiagnosisClass)
ORDER BY Specialty, DiagnosisClass;

-- Q55. GROUPING SETS: vremya, vrach, diagnoz.
SELECT
    [Year],
    DoctorName,
    DiagnosisClass,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount
FROM mart.vw_VisitDetailed
GROUP BY GROUPING SETS
(
    ([Year]),
    (DoctorName),
    (DiagnosisClass),
    ([Year], DoctorName),
    ([Year], DiagnosisClass)
)
ORDER BY [Year], DoctorName, DiagnosisClass;

-- Q56. PIVOT po tipam vizita v razreze vrachey.
WITH src AS
(
    SELECT
        DoctorName,
        VisitCategory,
        ServiceCost
    FROM mart.vw_VisitDetailed
)
SELECT
    DoctorName,
    ISNULL([Pervichnyy vizit], 0) AS PrimaryRevenue,
    ISNULL([Povtornyy vizit], 0) AS SecondaryRevenue,
    ISNULL([Inoy stsenariy], 0) AS OtherRevenue
FROM src
PIVOT
(
    SUM(ServiceCost)
    FOR VisitCategory IN ([Pervichnyy vizit], [Povtornyy vizit], [Inoy stsenariy])
) p
ORDER BY DoctorName;

-- Q57. PIVOT po vozrastnym segmentam i pribyli.
WITH src AS
(
    SELECT
        Specialty,
        AgeBand,
        Profit
    FROM mart.vw_VisitDetailed
)
SELECT
    Specialty,
    ISNULL([Do 18 let], 0) AS Profit_U18,
    ISNULL([18-25], 0) AS Profit_18_25,
    ISNULL([26-35], 0) AS Profit_26_35,
    ISNULL([36-45], 0) AS Profit_36_45,
    ISNULL([46-60], 0) AS Profit_46_60,
    ISNULL([60+], 0) AS Profit_60Plus,
    ISNULL([Ne opredeleno], 0) AS Profit_Unknown
FROM src
PIVOT
(
    SUM(Profit)
    FOR AgeBand IN ([Do 18 let], [18-25], [26-35], [36-45], [46-60], [60+], [Ne opredeleno])
) p
ORDER BY Specialty;

-- Q58. Drill-through k detalyam vracha s maksimalnoy vyruchkoy.
WITH best_doctor AS
(
    SELECT TOP (1) DoctorName
    FROM mart.vw_DoctorPerformance
    GROUP BY DoctorName
    ORDER BY SUM(RevenueAmount) DESC
)
SELECT
    v.FullDate,
    v.DoctorName,
    v.PatientName,
    v.DiagnosisName,
    v.VisitName,
    v.ServiceCost,
    v.Profit
FROM mart.vw_VisitDetailed v
JOIN best_doctor b
  ON b.DoctorName = v.DoctorName
ORDER BY v.FullDate, v.PatientName;

-- Q59. Drill-through k detalyam diagnoza s maksimalnoy vyruchkoy.
WITH best_diag AS
(
    SELECT TOP (1) DiagnosisName
    FROM mart.vw_DiagnosisPerformance
    GROUP BY DiagnosisName
    ORDER BY SUM(RevenueAmount) DESC
)
SELECT
    v.FullDate,
    v.DiagnosisName,
    v.PatientName,
    v.DoctorName,
    v.VisitName,
    v.ServiceCost,
    v.Profit
FROM mart.vw_VisitDetailed v
JOIN best_diag b
  ON b.DiagnosisName = v.DiagnosisName
ORDER BY v.FullDate, v.PatientName;

-- Q60. Drill-across: sravnenie vracha i diagnoza v odnom nabore.
SELECT
    DoctorName,
    DiagnosisClass,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount,
    SUM(VisitCount) AS VisitQty
FROM mart.vw_VisitDetailed
GROUP BY DoctorName, DiagnosisClass
ORDER BY DoctorName, RevenueAmount DESC;

-- Q61. Pareto-analiz po vracham.
WITH doctor_revenue AS
(
    SELECT
        DoctorName,
        SUM(RevenueAmount) AS RevenueAmount
    FROM mart.vw_DoctorPerformance
    GROUP BY DoctorName
),
doctor_pareto AS
(
    SELECT
        DoctorName,
        RevenueAmount,
        SUM(RevenueAmount) OVER (ORDER BY RevenueAmount DESC ROWS UNBOUNDED PRECEDING) AS RunningRevenue,
        SUM(RevenueAmount) OVER () AS TotalRevenue
    FROM doctor_revenue
)
SELECT
    DoctorName,
    RevenueAmount,
    CAST(100.0 * RunningRevenue / NULLIF(TotalRevenue, 0) AS decimal(9,2)) AS CumulativeSharePercent
FROM doctor_pareto
ORDER BY RevenueAmount DESC;

-- Q62. Pareto-analiz po diagnozam.
WITH diagnosis_revenue AS
(
    SELECT
        DiagnosisName,
        SUM(RevenueAmount) AS RevenueAmount
    FROM mart.vw_DiagnosisPerformance
    GROUP BY DiagnosisName
),
diagnosis_pareto AS
(
    SELECT
        DiagnosisName,
        RevenueAmount,
        SUM(RevenueAmount) OVER (ORDER BY RevenueAmount DESC ROWS UNBOUNDED PRECEDING) AS RunningRevenue,
        SUM(RevenueAmount) OVER () AS TotalRevenue
    FROM diagnosis_revenue
)
SELECT
    DiagnosisName,
    RevenueAmount,
    CAST(100.0 * RunningRevenue / NULLIF(TotalRevenue, 0) AS decimal(9,2)) AS CumulativeSharePercent
FROM diagnosis_pareto
ORDER BY RevenueAmount DESC;

-- Q63. Pareto-analiz po vozrastnym segmentam.
WITH age_revenue AS
(
    SELECT
        AgeBand,
        SUM(RevenueAmount) AS RevenueAmount
    FROM mart.vw_PatientSegment
    GROUP BY AgeBand
),
age_pareto AS
(
    SELECT
        AgeBand,
        RevenueAmount,
        SUM(RevenueAmount) OVER (ORDER BY RevenueAmount DESC ROWS UNBOUNDED PRECEDING) AS RunningRevenue,
        SUM(RevenueAmount) OVER () AS TotalRevenue
    FROM age_revenue
)
SELECT
    AgeBand,
    RevenueAmount,
    CAST(100.0 * RunningRevenue / NULLIF(TotalRevenue, 0) AS decimal(9,2)) AS CumulativeSharePercent
FROM age_pareto
ORDER BY RevenueAmount DESC;

-- Q64. Koeffitsient pribylnosti spetsialnosti.
SELECT
    Specialty,
    SUM(ProfitAmount) AS ProfitAmount,
    SUM(RevenueAmount) AS RevenueAmount,
    CAST(100.0 * SUM(ProfitAmount) / NULLIF(SUM(RevenueAmount), 0) AS decimal(9,2)) AS MarginPercent
FROM mart.vw_DoctorPerformance
GROUP BY Specialty
ORDER BY MarginPercent DESC;

-- Q65. Koeffitsient pribylnosti klassa diagnoza.
SELECT
    DiagnosisClass,
    SUM(ProfitAmount) AS ProfitAmount,
    SUM(RevenueAmount) AS RevenueAmount,
    CAST(100.0 * SUM(ProfitAmount) / NULLIF(SUM(RevenueAmount), 0) AS decimal(9,2)) AS MarginPercent
FROM mart.vw_DiagnosisPerformance
GROUP BY DiagnosisClass
ORDER BY MarginPercent DESC;

-- Q66. Indeks dokhodnosti povtornykh vizitov.
SELECT
    VisitCategory,
    SUM(Profit) AS ProfitAmount,
    SUM(ServiceCost) AS RevenueAmount,
    CAST(100.0 * SUM(Profit) / NULLIF(SUM(ServiceCost), 0) AS decimal(9,2)) AS MarginPercent
FROM mart.vw_VisitDetailed
GROUP BY VisitCategory
ORDER BY MarginPercent DESC;

-- Q67. Poisk anomalnykh dney po vyruchke.
WITH day_stats AS
(
    SELECT
        FullDate,
        RevenueAmount,
        AVG(RevenueAmount * 1.0) OVER () AS AvgRevenue,
        STDEV(RevenueAmount * 1.0) OVER () AS StdRevenue
    FROM mart.vw_TimeSeries
)
SELECT
    FullDate,
    RevenueAmount,
    AvgRevenue,
    StdRevenue,
    CASE
        WHEN RevenueAmount > AvgRevenue + StdRevenue THEN N'Vyshe normy'
        WHEN RevenueAmount < AvgRevenue - StdRevenue THEN N'Nizhe normy'
        ELSE N'Norma'
    END AS RevenueBand
FROM day_stats
ORDER BY RevenueAmount DESC;

-- Q68. Poisk anomalnykh dney po pribyli.
WITH day_stats AS
(
    SELECT
        FullDate,
        ProfitAmount,
        AVG(ProfitAmount * 1.0) OVER () AS AvgProfit,
        STDEV(ProfitAmount * 1.0) OVER () AS StdProfit
    FROM mart.vw_TimeSeries
)
SELECT
    FullDate,
    ProfitAmount,
    AvgProfit,
    StdProfit,
    CASE
        WHEN ProfitAmount > AvgProfit + StdProfit THEN N'Vyshe normy'
        WHEN ProfitAmount < AvgProfit - StdProfit THEN N'Nizhe normy'
        ELSE N'Norma'
    END AS ProfitBand
FROM day_stats
ORDER BY ProfitAmount DESC;

-- Q69. Svodnaya upravlencheskaya panel.
SELECT
    (SELECT SUM(VisitCount) FROM dw.FactVisit) AS TotalVisits,
    (SELECT SUM(ServiceCost) FROM dw.FactVisit) AS TotalRevenue,
    (SELECT SUM(Profit) FROM dw.FactVisit) AS TotalProfit,
    (SELECT COUNT(*) FROM dw.DimPatient WHERE IsCurrent = 1 AND PatientKey > 0) AS ActivePatients,
    (SELECT COUNT(*) FROM dw.DimDoctor WHERE IsCurrent = 1 AND DoctorKey > 0) AS ActiveDoctors,
    (SELECT COUNT(*) FROM dw.DimDiagnosis WHERE IsCurrent = 1 AND DiagnosisKey > 0) AS ActiveDiagnoses,
    (SELECT AVG(RepeatRate) FROM dw.FactVisit) AS AvgRepeatRate,
    (SELECT AVG(AvgTreatmentCost) FROM dw.FactVisit) AS AvgTreatmentCost;

-- Q70. Itogovyy mnogomernyy srez: god, vrach, diagnoz, tip vizita.
SELECT
    [Year],
    DoctorName,
    DiagnosisClass,
    VisitCategory,
    SUM(ServiceCost) AS RevenueAmount,
    SUM(Profit) AS ProfitAmount,
    SUM(VisitCount) AS VisitQty,
    CAST(AVG(CAST(MarginPercent AS decimal(9,4))) AS decimal(9,4)) AS AvgMarginPercent
FROM mart.vw_VisitDetailed
GROUP BY [Year], DoctorName, DiagnosisClass, VisitCategory
ORDER BY [Year], DoctorName, DiagnosisClass, VisitCategory;
GO
