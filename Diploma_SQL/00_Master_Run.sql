/*
    Diplomnyy komplekt SQL-skriptov
    Tema: modelirovanie struktury izmereniy v IAS, postroennykh na printsipakh OLAP,
    dlya analiza deyatelnosti stomatologicheskogo kabineta.

    Skript udobno ispolzovat v rezhime SQLCMD v SSMS.
*/

:setvar RootPath "C:\Users\User\Documents\Codex\Dental_CLinic-main\Diploma_SQL"

PRINT N'Zapusk komplekta razvertyvaniya DentalClinicDW...';
GO

:r $(RootPath)\01_Create_Database_And_Schemas.sql
:r $(RootPath)\02_Staging_Layer.sql
:r $(RootPath)\03_Warehouse_Model.sql
:r $(RootPath)\04_ETL_Procedures.sql
:r $(RootPath)\05_Compatibility_And_Marts.sql
:r $(RootPath)\06_Data_Quality_And_Admin.sql
:r $(RootPath)\07_OLAP_Analytics_Showcase.sql

PRINT N'Komplekt SQL-skriptov vypolnen.';
GO
