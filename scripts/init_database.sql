/*
=================================================
CREATE DATABASES AND SCHEMAS
=================================================
Script Purpose:
    This script crate a new database named 'DataWarehouse' after cheking if it already exists.
    If the database exists, its dropped and recreated. Additionally, the script sets up 3 schemas
    within the database: 'bronze','silver','gold'.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if exists and all the data.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
END;
GO

-- Create DataBase 'Warehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
