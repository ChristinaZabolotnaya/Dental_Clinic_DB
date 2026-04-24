USE [DentalClinicDW];
GO

IF OBJECT_ID(N'dw.FactVisit', N'U') IS NOT NULL
    DROP TABLE dw.FactVisit;
GO

IF OBJECT_ID(N'dw.DimDiagnosis', N'U') IS NOT NULL
    DROP TABLE dw.DimDiagnosis;
GO

IF OBJECT_ID(N'dw.DimVisitType', N'U') IS NOT NULL
    DROP TABLE dw.DimVisitType;
GO

IF OBJECT_ID(N'dw.DimDate', N'U') IS NOT NULL
    DROP TABLE dw.DimDate;
GO

IF OBJECT_ID(N'dw.DimDoctor', N'U') IS NOT NULL
    DROP TABLE dw.DimDoctor;
GO

IF OBJECT_ID(N'dw.DimPatient', N'U') IS NOT NULL
    DROP TABLE dw.DimPatient;
GO

CREATE TABLE dw.DimPatient
(
    PatientKey           int IDENTITY(1,1) NOT NULL,
    PatientID            int NOT NULL,
    FullName             nvarchar(100) NOT NULL,
    Age                  int NULL,
    AgeBand              nvarchar(50) NULL,
    Phone                nvarchar(20) NULL,
    PhoneNormalized      nvarchar(20) NULL,
    EffectiveFrom        datetime2(0) NOT NULL,
    EffectiveTo          datetime2(0) NOT NULL,
    IsCurrent            bit NOT NULL,
    SourceBatchID        int NULL,
    CreatedAt            datetime2(0) NOT NULL CONSTRAINT DF_dw_DimPatient_CreatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_DimPatient PRIMARY KEY CLUSTERED (PatientKey),
    CONSTRAINT UQ_DimPatient_Current UNIQUE NONCLUSTERED (PatientID, EffectiveTo, IsCurrent)
);
GO

CREATE TABLE dw.DimDoctor
(
    DoctorKey            int IDENTITY(1,1) NOT NULL,
    DoctorID             int NOT NULL,
    FullName             nvarchar(100) NOT NULL,
    Specialty            nvarchar(100) NULL,
    SpecialtyGroup       nvarchar(100) NULL,
    EffectiveFrom        datetime2(0) NOT NULL,
    EffectiveTo          datetime2(0) NOT NULL,
    IsCurrent            bit NOT NULL,
    SourceBatchID        int NULL,
    CreatedAt            datetime2(0) NOT NULL CONSTRAINT DF_dw_DimDoctor_CreatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_DimDoctor PRIMARY KEY CLUSTERED (DoctorKey),
    CONSTRAINT UQ_DimDoctor_Current UNIQUE NONCLUSTERED (DoctorID, EffectiveTo, IsCurrent)
);
GO

CREATE TABLE dw.DimDate
(
    DateKey              int IDENTITY(1,1) NOT NULL,
    DateID               int NOT NULL,
    FullDate             date NULL,
    [Day]                tinyint NULL,
    [Month]              tinyint NULL,
    [Year]               smallint NULL,
    QuarterNumber        tinyint NULL,
    MonthName            nvarchar(30) NULL,
    WeekDayName          nvarchar(20) NULL,
    WeekOfYear           tinyint NULL,
    IsWeekend            bit NOT NULL CONSTRAINT DF_dw_DimDate_IsWeekend DEFAULT (0),
    AcademicPeriod       nvarchar(20) NULL,
    SourceBatchID        int NULL,
    CreatedAt            datetime2(0) NOT NULL CONSTRAINT DF_dw_DimDate_CreatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_DimDate PRIMARY KEY CLUSTERED (DateKey),
    CONSTRAINT UQ_DimDate_DateID UNIQUE NONCLUSTERED (DateID)
);
GO

CREATE TABLE dw.DimVisitType
(
    VisitTypeKey         int IDENTITY(1,1) NOT NULL,
    VisitTypeID          int NOT NULL,
    VisitName            nvarchar(100) NOT NULL,
    VisitCode            nvarchar(50) NOT NULL,
    VisitCategory        nvarchar(50) NULL,
    SourceBatchID        int NULL,
    CreatedAt            datetime2(0) NOT NULL CONSTRAINT DF_dw_DimVisitType_CreatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_DimVisitType PRIMARY KEY CLUSTERED (VisitTypeKey),
    CONSTRAINT UQ_DimVisitType_VisitTypeID UNIQUE NONCLUSTERED (VisitTypeID)
);
GO

CREATE TABLE dw.DimDiagnosis
(
    DiagnosisKey         int IDENTITY(1,1) NOT NULL,
    DiagnosisID          int NOT NULL,
    ICDCode              nvarchar(20) NULL,
    DiagnosisName        nvarchar(200) NULL,
    TreatmentMethod      nvarchar(200) NULL,
    DiagnosisClass       nvarchar(100) NULL,
    EffectiveFrom        datetime2(0) NOT NULL,
    EffectiveTo          datetime2(0) NOT NULL,
    IsCurrent            bit NOT NULL,
    SourceBatchID        int NULL,
    CreatedAt            datetime2(0) NOT NULL CONSTRAINT DF_dw_DimDiagnosis_CreatedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_DimDiagnosis PRIMARY KEY CLUSTERED (DiagnosisKey),
    CONSTRAINT UQ_DimDiagnosis_Current UNIQUE NONCLUSTERED (DiagnosisID, EffectiveTo, IsCurrent)
);
GO

CREATE TABLE dw.FactVisit
(
    VisitKey             bigint IDENTITY(1,1) NOT NULL,
    FactID               int NOT NULL,
    DateKey              int NOT NULL,
    PatientKey           int NOT NULL,
    DoctorKey            int NOT NULL,
    DiagnosisKey         int NOT NULL,
    VisitTypeKey         int NOT NULL,
    ServiceCost          decimal(18,2) NOT NULL,
    VisitCount           int NOT NULL,
    Profit               decimal(18,2) NOT NULL,
    PatientCount         int NOT NULL,
    RepeatRate           decimal(9,4) NOT NULL,
    AvgTreatmentCost     decimal(18,2) NOT NULL,
    MarginPercent        decimal(9,4) NULL,
    SourceBatchID        int NULL,
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_dw_FactVisit_LoadedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_FactVisit PRIMARY KEY CLUSTERED (VisitKey),
    CONSTRAINT UQ_FactVisit_FactID UNIQUE NONCLUSTERED (FactID),
    CONSTRAINT FK_FactVisit_DimDate FOREIGN KEY (DateKey) REFERENCES dw.DimDate(DateKey),
    CONSTRAINT FK_FactVisit_DimPatient FOREIGN KEY (PatientKey) REFERENCES dw.DimPatient(PatientKey),
    CONSTRAINT FK_FactVisit_DimDoctor FOREIGN KEY (DoctorKey) REFERENCES dw.DimDoctor(DoctorKey),
    CONSTRAINT FK_FactVisit_DimDiagnosis FOREIGN KEY (DiagnosisKey) REFERENCES dw.DimDiagnosis(DiagnosisKey),
    CONSTRAINT FK_FactVisit_DimVisitType FOREIGN KEY (VisitTypeKey) REFERENCES dw.DimVisitType(VisitTypeKey)
);
GO

IF OBJECT_ID(N'dw.AggregateDoctorMonth', N'U') IS NOT NULL
    DROP TABLE dw.AggregateDoctorMonth;
GO

CREATE TABLE dw.AggregateDoctorMonth
(
    [Year]               smallint NOT NULL,
    [Month]              tinyint NOT NULL,
    DoctorKey            int NOT NULL,
    VisitQty             int NOT NULL,
    RevenueAmount        decimal(18,2) NOT NULL,
    ProfitAmount         decimal(18,2) NOT NULL,
    AvgRevenuePerVisit   decimal(18,2) NOT NULL,
    AvgProfitPerVisit    decimal(18,2) NOT NULL,
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_AggregateDoctorMonth_LoadedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_AggregateDoctorMonth PRIMARY KEY CLUSTERED ([Year], [Month], DoctorKey)
);
GO

IF OBJECT_ID(N'dw.AggregateDiagnosisMonth', N'U') IS NOT NULL
    DROP TABLE dw.AggregateDiagnosisMonth;
GO

CREATE TABLE dw.AggregateDiagnosisMonth
(
    [Year]               smallint NOT NULL,
    [Month]              tinyint NOT NULL,
    DiagnosisKey         int NOT NULL,
    VisitQty             int NOT NULL,
    RevenueAmount        decimal(18,2) NOT NULL,
    ProfitAmount         decimal(18,2) NOT NULL,
    RepeatRateAvg        decimal(9,4) NOT NULL,
    LoadedAt             datetime2(0) NOT NULL CONSTRAINT DF_AggregateDiagnosisMonth_LoadedAt DEFAULT (sysdatetime()),
    CONSTRAINT PK_AggregateDiagnosisMonth PRIMARY KEY CLUSTERED ([Year], [Month], DiagnosisKey)
);
GO

CREATE INDEX IX_DimPatient_BusinessKey
    ON dw.DimPatient (PatientID, IsCurrent)
    INCLUDE (FullName, AgeBand, PhoneNormalized);
GO

CREATE INDEX IX_DimDoctor_BusinessKey
    ON dw.DimDoctor (DoctorID, IsCurrent)
    INCLUDE (FullName, Specialty, SpecialtyGroup);
GO

CREATE INDEX IX_DimDiagnosis_BusinessKey
    ON dw.DimDiagnosis (DiagnosisID, IsCurrent)
    INCLUDE (ICDCode, DiagnosisName, DiagnosisClass);
GO

CREATE INDEX IX_DimDate_FullDate
    ON dw.DimDate (FullDate, [Year], [Month], QuarterNumber);
GO

CREATE INDEX IX_FactVisit_Analysis
    ON dw.FactVisit (DateKey, DoctorKey, DiagnosisKey, VisitTypeKey)
    INCLUDE (ServiceCost, Profit, RepeatRate, AvgTreatmentCost, PatientCount);
GO

CREATE INDEX IX_FactVisit_Patient
    ON dw.FactVisit (PatientKey, DateKey)
    INCLUDE (ServiceCost, Profit, VisitCount);
GO

SET IDENTITY_INSERT dw.DimPatient ON;
INSERT INTO dw.DimPatient
(
    PatientKey, PatientID, FullName, Age, AgeBand, Phone, PhoneNormalized,
    EffectiveFrom, EffectiveTo, IsCurrent, SourceBatchID
)
VALUES
(
    0, 0, N'Neizvestnyy patsient', NULL, N'Ne opredeleno', NULL, NULL,
    '19000101', '99991231', 1, NULL
);
SET IDENTITY_INSERT dw.DimPatient OFF;
GO

SET IDENTITY_INSERT dw.DimDoctor ON;
INSERT INTO dw.DimDoctor
(
    DoctorKey, DoctorID, FullName, Specialty, SpecialtyGroup,
    EffectiveFrom, EffectiveTo, IsCurrent, SourceBatchID
)
VALUES
(
    0, 0, N'Neizvestnyy vrach', N'Ne opredeleno', N'Prochee',
    '19000101', '99991231', 1, NULL
);
SET IDENTITY_INSERT dw.DimDoctor OFF;
GO

SET IDENTITY_INSERT dw.DimDate ON;
INSERT INTO dw.DimDate
(
    DateKey, DateID, FullDate, [Day], [Month], [Year], QuarterNumber,
    MonthName, WeekDayName, WeekOfYear, IsWeekend, AcademicPeriod, SourceBatchID
)
VALUES
(
    0, 0, NULL, NULL, NULL, NULL, NULL,
    N'Ne opredeleno', N'Ne opredeleno', NULL, 0, N'Ne opredeleno', NULL
);
SET IDENTITY_INSERT dw.DimDate OFF;
GO

SET IDENTITY_INSERT dw.DimVisitType ON;
INSERT INTO dw.DimVisitType
(
    VisitTypeKey, VisitTypeID, VisitName, VisitCode, VisitCategory, SourceBatchID
)
VALUES
(
    0, 0, N'Neizvestnyy tip vizita', N'UNK', N'Prochee', NULL
);
SET IDENTITY_INSERT dw.DimVisitType OFF;
GO

SET IDENTITY_INSERT dw.DimDiagnosis ON;
INSERT INTO dw.DimDiagnosis
(
    DiagnosisKey, DiagnosisID, ICDCode, DiagnosisName, TreatmentMethod,
    DiagnosisClass, EffectiveFrom, EffectiveTo, IsCurrent, SourceBatchID
)
VALUES
(
    0, 0, N'UNK', N'Neizvestnyy diagnoz', N'Ne opredelen',
    N'Prochee', '19000101', '99991231', 1, NULL
);
SET IDENTITY_INSERT dw.DimDiagnosis OFF;
GO
