-- ============================================
-- Script: Create DataWarehouse Database & Schemas
-- Description: Creates the DataWarehouse database and the 
--              bronze, silver, and gold schemas for ETL layers.
-- Author: Mahmoud
-- ============================================

-- Check if the database exists
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    CREATE DATABASE DataWarehouse;
    PRINT 'Database "DataWarehouse" created successfully.';
END
ELSE
BEGIN
    PRINT 'Database "DataWarehouse" already exists. Skipping creation.';
END
GO

-- Switch to the DataWarehouse database
USE DataWarehouse;
GO

-- Bronze schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('CREATE SCHEMA bronze AUTHORIZATION dbo');
    PRINT 'Schema "bronze" created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema "bronze" already exists. Skipping creation.';
END
GO

-- Silver schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('CREATE SCHEMA silver AUTHORIZATION dbo');
    PRINT 'Schema "silver" created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema "silver" already exists. Skipping creation.';
END
GO

-- Gold schema
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('CREATE SCHEMA gold AUTHORIZATION dbo');
    PRINT 'Schema "gold" created successfully.';
END
ELSE
BEGIN
    PRINT 'Schema "gold" already exists. Skipping creation.';
END
GO
