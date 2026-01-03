/*
======================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
======================================================================

Script Purpose:
This stored procedure performs the ETL (Extract, Transform, Load) process
to populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
None.
This stored procedure does not accept any parameters or return any values.

Usage Example:
EXEC silver.load_silver;
======================================================================
*/

--EXEC silver.load_silver;

--SQL DATA Warehouse project

--bronze.crm_cst_details -> silver.crm_cst_details
CREATE OR ALTER PROCEDURE silver.load_silver as
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start DATETIME, @batch_end DATETIME;

	SET @batch_start=GETDATE();
	BEGIN TRY
		PRINT '===================================================================================';
		PRINT 'Loading Silver Layer' ;
		PRINT '===================================================================================';

		PRINT '-----------------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT '>> Inserting Data into silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info
		(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_create_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE
		WHEN UPPER(TRIM(cst_marital_status))='S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status))='M' THEN 'Married'
		ELSE 'n/a'
		END AS cst_marital_status,
		CASE 
		WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
		ELSE 'n/a'
		END AS cst_gndr,
		cst_create_date
		FROM
		(SELECT
		*,
		ROW_NUMBER() OVER (PARTITION BY cst_id order by cst_create_date desc) as flag_last
		from
		bronze.crm_cust_info
		where cst_id is not null)t
		WHERE flag_last=1
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';


		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT '>> Inserting into table crm_prd_info '
		INSERT INTO silver.crm_prd_info
		(
		prd_id,
		prd_cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)
		SELECT
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') as prd_cat_id,
		SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE TRIM(UPPER(prd_line))
			WHEN 'R' THEN 'Road'
			WHEN 'M' THEN 'Mountain'
			WHEN 'S' THEN 'Other Sales'
			WHEN 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) as prd_start_dt,
		CAST(LEAD(prd_start_dt) over (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) as prd_end_dt
		FROM
		bronze.crm_prd_info;
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';

		
		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT '>> Inserting into table crm_sales_details '
		INSERT INTO silver.crm_sales_details
		(
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,

		CASE WHEN sls_order_dt < 0 or LEN(sls_order_dt)!=8
		THEN NULL
		ELSE CAST(CAST(sls_order_dt AS VARCHAR)AS DATE) 
		END AS sls_order_dt,

		CASE WHEN sls_ship_dt < 0 or LEN(sls_ship_dt)!=8
		THEN NULL
		ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,

		CASE WHEN sls_due_dt < 0 or LEN(sls_due_dt)!=8
		THEN NULL
		ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,

		CASE WHEN sls_sales<=0 or sls_sales IS NULL or sls_quantity*ABS(sls_price)!=sls_sales
		THEN sls_quantity*ABS(sls_price)
		ELSE sls_sales
		END AS sls_sales,

		sls_quantity,

		CASE WHEN sls_price<=0 or sls_price IS NULL
		THEN sls_sales/sls_quantity
		ELSE sls_price
		END AS sls_price

		FROM
		bronze.crm_sales_details;
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';

		PRINT '-----------------------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.erp_CUST_AZ12'
		TRUNCATE TABLE silver.erp_CUST_AZ12
		PRINT '>> Inserting into table erp_CUST_AZ12 '
		INSERT INTO silver.erp_CUST_AZ12
		(
		cid,
		bdate,
		gender)
		SELECT

		CASE WHEN CID LIKE 'NAS%'
		THEN SUBSTRING(CID,4,LEN(CID))
		ELSE CID
		END AS cid,

		CASE WHEN BDATE > GETDATE()
		THEN NULL
		ELSE BDATE
		END AS bdate,

		CASE WHEN TRIM(GEN)='' OR TRIM(GEN) IS NULL
		THEN 'n/a'
		WHEN TRIM(UPPER(GEN))='F'
		THEN 'Female'
		WHEN TRIM(UPPER(GEN))='M'
		THEN 'Male'
		ELSE GEN
		end as Gender

		FROM
		bronze.erp_CUST_AZ12
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.erp_LOC_A101'
		TRUNCATE TABLE silver.erp_LOC_A101
		PRINT '>> Inserting into table erp_LOC_A101 '
		INSERT INTO silver.erp_LOC_A101
		(
		cid,
		country
		)
		SELECT

		TRIM(REPLACE(CID,'-','')) AS cid,

		CASE WHEN CNTRY ='' OR CNTRY IS NULL
		THEN 'n/a'
		WHEN UPPER(CNTRY)='UNITED STATES' OR UPPER(CNTRY)='US' OR UPPER(CNTRY)='USA'
		THEN 'United States'
		WHEN UPPER(CNTRY)='DE' OR UPPER(CNTRY)='GERMANY'
		THEN 'Germany'
		ELSE CNTRY END AS country

		FROM
		bronze.erp_LOC_A101;
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';


		SET @start_time = GETDATE();

		PRINT '>> Truncating table silver.erp_PX_CAT_G1V2'
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2
		PRINT '>> Inserting into table erp_PX_CAT_G1V2'
		INSERT INTO silver.erp_PX_CAT_G1V2
		(
		id,
		cat,
		subcat,
		maintenance
		)
		SELECT
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		FROM
		bronze.erp_PX_CAT_G1V2
		SET @end_time=GETDATE();
		PRINT '>>  Load Duration : '+ CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' Seconds';
		PRINT '>> ---------------------';

		PRINT '============================================================================================';
		SET @batch_end=GETDATE();
		PRINT '>> Loading Silver Layer is Done'
		PRINT '   - OVERALL TIME TAKEN '+ CAST(DATEDIFF(second,@batch_start,@batch_end) AS NVARCHAR) +' seconds';
		PRINT '============================================================================================';
	END TRY
	BEGIN CATCH
		PRINT '==============================================================================='
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER'
		PRINT 'ERROR MESSAGE'+ERROR_MESSAGE();
		PRINT 'ERROR NUMBER' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT '==============================================================================='
	END CATCH

END
