
-- This stored procedure automates the Silver Layer load process in the data warehouse. It performs cleansing, transformation, and standardization of raw Bronze Layer data before storing it in Silver Layer tables.

-- Key steps:

-- Customer Info (crm_cust_info)

-- Deduplicates records using ROW_NUMBER() to keep the latest entry per customer.

-- Cleans names with TRIM.

-- Maps marital status and gender codes to descriptive values.

-- Product Info (crm_prd_info)

-- Extracts category IDs and product keys from raw strings.

-- Replaces null product costs with 0.

-- Maps product line codes to descriptive values.

-- Converts product start dates to DATE.

-- Calculates product end dates using LEAD() (next start date minus one day).

-- Sales Details (crm_sales_details)

-- Cleanses order, ship, and due dates (valid 8-digit integers → DATE, invalid → NULL).

-- Recomputes sales if missing or inconsistent (quantity × price).

-- Corrects invalid prices by deriving from sales ÷ quantity.

-- ERP Customer (erp_cust_az12)

-- Removes NAS prefix from IDs.

-- Nullifies future birthdates.

-- Standardizes gender values.

-- ERP Location (erp_loc_a101)

-- Removes dashes from IDs.

-- Maps country codes to full names, replaces blanks with n/a.

-- ERP Category (erp_px_cat_g1v2)

-- Direct load from Bronze to Silver without transformation.

-- Error handling & logging:

-- Wrapped in TRY…CATCH for error capture.

-- Prints start/end messages and total load duration in seconds.








CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN 
	DECLARE @start_time DATETIME ,@end_time DATETIME;
	BEGIN TRY
		SET @start_time=GETDATE();
		PRINT'========================='
		PRINT'LOADING SILVER LAYER'
		PRINT'=========================='

--loading silver.crm_cust_info
TRUNCATE TABLE silver.crm_cust_info; --deleting all the rows of the table

INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select 
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
	WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
	ELSE 'n/a'
END AS cst_gndr,

cst_create_date 
FROM(
	SELECT *,
	ROW_NUMBER() over (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	where cst_id is not null
)t
WHERE flag_last = 1;




--Loading silver.crm_prd_info
TRUNCATE TABLE silver.crm_prd_info; --deleting all the rows of the table

INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_')as cat_id, --extract category id using prd_ by taking the first 5 values  and replacing '-' with '_'
	SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,  --extracting the new product key using prd_key column and taking out all the character from 7 position to last and storing it in prd_key
	prd_nm,
	ISNULL(prd_cost,0) AS prd_cost,--replacing null values of prd_cost with 0
	CASE
		WHEN UPPER(TRIM(prd_line))='M' then 'Mountain'
		WHEN UPPER(TRIM(prd_line))='R' then 'Road'
		WHEN UPPER(TRIM(prd_line))='S' then 'Other Sales'
		WHEN UPPER(TRIM(prd_line))='T' then 'Touring'
		ELSE 'n/a'
	END AS prd_line,--product line codes to descriptive values
	CAST (prd_start_dt AS DATE)AS prd_start_dt,--2026-03-03 14:33:00  =>      2026-03-03  
	CAST(
		lead(prd_start_dt)over(partition by prd_key order by prd_start_dt)-1
		as date
		) AS prd_end_dt--This code calculates each product’s end date by taking the next product’s start date (using LEAD()), subtracting one day, and casting it to a pure DATE type — defining validity ranges for product records.
	from bronze.crm_prd_info;



	-- loading silver.crm_sales_details
	TRUNCATE TABLE silver.crm_sales_details;--delete all the rows of crm_sales_details
	INSERT INTO silver.crm_sales_details(
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
		WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL --0=>null    2546=>null 20260303=>valid
		ELSE CAST(CAST(sls_order_dt as varchar)as date)--=>20260303=>'20260303' conversion from int to string=>2026-03-03 conversion to date
		END AS sls_order_dt,
		CASE 
		WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL --0=>null    2546=>null 20260303=>valid
		ELSE CAST(CAST(sls_ship_dt as varchar)as date)--=>20260303=>'20260303' conversion from int to string=>2026-03-03 conversion to date
		END AS sls_ship_dt,
	    CASE 
		WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL --0=>null    2546=>null 20260303=>valid
		ELSE CAST(CAST(sls_due_dt as varchar)as date)--=>20260303=>'20260303' conversion from int to string=>2026-03-03 conversion to date
		END AS sls_due_dt,
		CASE
			WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
				THEN sls_quantity*ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales,--calculate sales if value is wrng or null 
		sls_quantity,
		CASE
			when sls_price is null or sls_price<=0
				then sls_sales/nullif(sls_quantity,0)-- calculate  sales price if value is wrong by dividing total sales with total quantity
			else sls_price
		END AS sls_price
	FROM bronze.crm_sales_details;

	--loading silver.erp_cust_az12
	TRUNCATE TABLE silver.erp_cust_az12--remove all the rows 
	INSERT INTO silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))-- remove 'NAS' prefix if present
		ELSE cid
	END AS cid ,
	CASE
		WHEN bdate>getdate() then null
		ELSE bdate
	END AS bdate,--set fututre dates to null
	CASE 
	WHEN UPPER(TRIM(gen)) IN ('F','FEMALE')then 'Female'
	WHEN UPPER(TRIM(gen)) IN ('M','MALE')then 'Male'
	
	ELSE 'n/a'
	END AS gen
	FROM bronze.erp_cust_az12;


	--loading silver.erp_loc_a101
	TRUNCATE TABLE silver.erp_loc_a101;--remove all the rows 
	INSERT INTO silver.erp_loc_a101(
	cid,
	cntry
	) select
	REPLACE(cid,'-','') AS cid,
	CASE
		WHEN TRIM(cntry)='DE' THEN 'GERMANY'
		WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR cntry is NULL THEN 'n/a'

	END AS cntry
	FROM bronze.erp_loc_a101;


	--loading silver.erp_px_cat_g1v2
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	INSERT INTO silver.erp_px_cat_g1v2 (
			id,
			cat,
			subcat,
			maintenance
		)
		SELECT
			id,
			cat,
			subcat,
			maintenance
		FROM bronze.erp_px_cat_g1v2;
		SET @end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='



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

