/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the 'BULK INSERT' command to load data from csv Files to bronze tables.

Parameters:
    NONE.
    This stored procedure does not accept any parameters or return any values.

Usage Example: 
    EXEC bronze.load_bronze;
================================================================================
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS    --Create store procedure for stuff to be used over and over again repeatidly
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY     --Try ... CATCH: SQL runs the TRY block, and if it fails, it runs the CATCH blcok to handle the error
		SET  @batch_start_time = GETDATE();  --Load Whole Bonze LAYER
		PRINT '====================================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================================';

		PRINT '-----------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;     --Make table emtpy first Essentially refreshing table customer info into file database table


		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info     --Inserting file into database and loading it
		FROM 'C:\Users\dkwan\Downloads\SQL\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',   
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------  ';

		--Select COUNT(*) from bronze.crm_cust_info
		--Task: Write SQL BULK Insert to load ALL CSV files into your Bronze Tables
		SET @start_time = GETDATE();									-- Caculating the duration of Loading Tables (Start and END)
		PRINT '>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;    
		PRINT '>> Inserting Data Into: crm_prd_info';
		BULK INSERT bronze.crm_prd_info     
		FROM 'C:\Users\dkwan\Downloads\SQL\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',   
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------  ';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;    
		PRINT '>> Inserting Data Into: crm_sales_details';
		BULK INSERT bronze.crm_sales_details     
		FROM 'C:\Users\dkwan\Downloads\SQL\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',   
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------  ';

		PRINT '-----------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-----------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12;    

		PRINT '>> Inserting Data Into: erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12    
		FROM 'C:\Users\dkwan\Downloads\SQL\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',   
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------  ';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;    

		PRINT '>> Inserting Data Into: erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2    
		FROM 'C:\Users\dkwan\Downloads\SQL\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',   
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------  ';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;    

		PRINT '>> Inserting Data Into: erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101    
		FROM 'C:\Users\dkwan\Downloads\SQL\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',   
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -------------  ';

		SET @batch_end_time = GETDATE();  --Load WHOle Bronze Layer END time
		PRINT '========================================'
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '  -Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as NVARCHAR) + ' seconds';
		PRINT '========================================'
	END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END
