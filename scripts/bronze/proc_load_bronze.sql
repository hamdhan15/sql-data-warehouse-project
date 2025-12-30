/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;

	SET @batch_start=GETDATE();
	BEGIN TRY
		PRINT '===================================================================================';
		PRINT 'Loading Bronze Layer' ;
		PRINT '===================================================================================';

		PRINT '-----------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info';
		truncate table bronze.crm_cust_info;

		PRINT '>> Inserting Data into : bronze.crm_cust_info';
		bulk insert bronze.crm_cust_info
		from 'C:\Users\HAMDHAN\Data Engineering practice\sql-dataWarehouse\source_crm\cust_info.csv'
		with
		(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';
		

		SET @start_time= GETDATE();
		PRINT '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting Data into : bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\HAMDHAN\Data Engineering practice\sql-dataWarehouse\source_crm\prd_info.csv'
		WITH
		(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';
		

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting Data into : bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\HAMDHAN\Data Engineering practice\sql-dataWarehouse\source_crm\sales_details.csv'
		WITH
		(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';
		

		PRINT '-----------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table : bronze.erp_CUST_AZ12'; 
		TRUNCATE TABLE bronze.erp_CUST_AZ12;

		PRINT '>> Inserting Data into : bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\HAMDHAN\Data Engineering practice\sql-dataWarehouse\source_erp\CUST_AZ12.csv'
		WITH
		(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';
		

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table : bronze.erp_LOC_A101';
		TRUNCATE TABLE bronze.erp_LOC_A101;

		PRINT '>> Inserting Data into : bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\HAMDHAN\Data Engineering practice\sql-dataWarehouse\source_erp\LOC_A101.csv'
		WITH
		(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';
		

		SET @start_time=GETDATE();
		PRINT '>> Truncating Table : bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT '>> Inserting Data into : bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\HAMDHAN\Data Engineering practice\sql-dataWarehouse\source_erp\PX_CAT_G1V2.csv'
		WITH
		(
		FIRSTROW=2,
		FIELDTERMINATOR=',',
		TABLOCK
		);
		SET @end_time=GETDATE()
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';

		PRINT '============================================================================================';
		SET @batch_end=GETDATE();
		PRINT '>> Loading Bronze Layer is Done'
		PRINT '   - OVERALL TIME TAKEN '+ CAST(DATEDIFF(second,@batch_start,@batch_end) AS NVARCHAR) +' seconds';
		PRINT '============================================================================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==============================================================================='
	END CATCH
END
