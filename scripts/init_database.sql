/*
=============================================================
Create Database and Schemas
=============================================================
Script purpose:
	This script creates a new database called 'DWH' after checking if it already exists.
	If the database exists, it is dropped and recreated. Additionaly, the script sets up three schemas
	within the database: 'bronze', 'silver', 'gold'.

WARNING:
	Running this script will drop the entire 'DWH' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DWH')
BEGIN
	ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DWH;
END;
GO

-- Create Database 'DataWarehouse'
CREATE DATABASE DWH;
GO

USE DWH;
GO

-- Create Schemas 
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
