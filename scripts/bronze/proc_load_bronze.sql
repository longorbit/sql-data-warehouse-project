/*
=========================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze) 
=========================================================
Script Purpose: 
    This stored procedure load data to 'bronze' schema from external CSV files.
    it performs the following actions:
    - Truncate th ebronze table before loading data.
    - Use the 'BULK INSERT' command to load data from csv files to bronze tables.

Parameters:
    None
  This procedure does'nt accept any parameter to return any value.

Usage Example:
    EXEC bronze.load_bronze
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME 
	BEGIN TRY
		PRINT '==============================================='
		PRINT 'Loading Bronze Layer'
		PRINT '==============================================='

		PRINT '-----------------------------------------------'
		PRINT 'Loading CRM Tables'
		PRINT '-----------------------------------------------'

		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATE TABLE: bronze.crm_cust_info'
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>>Inserting data into: bronze.crm_cust_info'
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\software\sql\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'  --cust_info
		WITH(
			FIRSTROW=2
			,FIELDTERMINATOR = ','
			,TABLOCK
		)
		SET @end_time = GETDATE();
	    PRINT '>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds' 
		PRINT '---------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATE TABLE: bronze.crm_prd_info' 
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>>Inserting data into: bronze.prd_cust_info'
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\software\sql\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV' --prd_info
		WITH(
			FIRSTROW=2
			,FIELDTERMINATOR = ','
			,TABLOCK
		)
		SET @end_time = GETDATE();
	    PRINT '>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds' 
		PRINT '---------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATE TABLE: bronze.crm_sales_details' 
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>>Inserting data into: bronze.crm_sales_details'
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\software\sql\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV' --sales_details
		WITH(
			FIRSTROW=2
			,FIELDTERMINATOR = ','
			,TABLOCK
		)
		SET @end_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds'
		PRINT '>>Load Patch Duration: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) as NVARCHAR)+' seconds'
		PRINT '---------------------------------------'

		PRINT '-----------------------------------------------'
		PRINT 'Loading ERP Tables'
		PRINT '-----------------------------------------------'

		SET @batch_start_time = GETDATE();
		SET @start_time = GETDATE();
		PRINT '>>TRUNCATE TABLE: bronze.erp_loc_a101'
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>>Inserting data into: bronze.erp_loc_a101'
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\software\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV' --LOC_A101
		WITH(
			FIRSTROW=2
			,FIELDTERMINATOR = ','
			,TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds' 
		PRINT '---------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATE TABLE: bronze.erp_cust_az12'
		TRUNCATE TABLE bronze.erp_CUST_AZ12

		PRINT '>>Inserting data into: bronze.erp_cust_az12'
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'F:\software\sql\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV' --CUST_AZ12 
		WITH(
			FIRSTROW=2
			,FIELDTERMINATOR = ','
			,TABLOCK
		)
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds' 
		PRINT '---------------------------------------'

		SET @start_time = GETDATE();
		PRINT '>>TRUNCATE TABLE: bronze.erp_px_cat_g1v2' 
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>>Inserting data into: bronze.erp_px_cat_g1v2'
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\software\sql\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV' --PX_CAT_G1V2 
		WITH(
			FIRSTROW=2
			,FIELDTERMINATOR = ','
			,TABLOCK
		)
		SET @end_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT '>>Load Duration: '+CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR)+' seconds' 
		PRINT '>>Load Patch Duration: '+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) as NVARCHAR)+' seconds'
		PRINT '---------------------------------------'

	END TRY
	BEGIN CATCH
		PRINT '==============================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZR LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_MESSAGE() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '==============================================='
	END CATCH	
END;
GO
