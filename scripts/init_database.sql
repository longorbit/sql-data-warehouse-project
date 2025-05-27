/*
================================================================
Create Database and Schema
================================================================
Script purpose;
    This script creates a new database named 'DataWarehouse' after checking if it already exists.
    if the database exists, it is dropped and recreated. Additionally, the script set up three schemas 
    within the database: 'bronze', 'silver', and 'gold.

WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists.
    All the data in the database will be permanently deleted. Process with caution 
    and ensure you have backups before running this script. 
*/

USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
BEGIN
  ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse; 
END;
GO
  
--create the 'DataWareHouse' database
CREATE DATABASE DataWareHouse;
GO

USE DataWareHouse;
GO

 --create schema 
CREATE SCHEMA bronze;
GO
  
CREATE SCHEMA silver;
GO
  
CREATE SCHEMA gold;
GO
