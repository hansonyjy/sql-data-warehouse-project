/*
===============================================================================
Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Inserts transformed and cleansed data from Bronze into Silver tables.
===============================================================================
*/

-- Loading silver.crm_cust_info
INSERT INTO silver.crm_cust_info(
	cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_material_status,
    cst_gndr,
    cst_create_date)
    
SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	CASE WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
		WHEN UPPER(TRIM(cst_material_status)) = "S" THEN "Single"
		ELSE 'n/a'
	END cst_material_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		WHEN UPPER(TRIM(cst_gndr)) = "F" THEN "Female"
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM (
	SELECT
	*,
	ROW_NUMBER()OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
    WHERE cst_id IS NOT NULL 
		AND cst_id != 0
)t WHERE flag_last = 1;

-- Loading silver.crm_prd_info
INSERT INTO silver.crm_prd_info(
	prd_id,
    cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)

SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-', '_') AS cat_id,
SUBSTRING(prd_key,7) AS prd_key,
prd_nm,
IFNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'n/a'
END prd_line,
CAST(prd_start_dt AS DATE) prd_start_dt,
CAST(DATE_SUB(
    LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
    INTERVAL 1 DAY
	)AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info; 

-- Loading silver.crm_sales_details
INSERT INTO silver.crm_sales_details(
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
CASE WHEN sls_order_dt = 0 OR CHAR_LENGTH(sls_order_dt) !=8 THEN NULL
	ELSE CAST(sls_order_dt AS DATE)
END sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR CHAR_LENGTH(sls_ship_dt) !=8 THEN NULL
	ELSE CAST(sls_ship_dt AS DATE)
END sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR CHAR_LENGTH(sls_due_dt) !=8 THEN NULL
	ELSE CAST(sls_due_dt AS DATE)
END sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
	ELSE sls_sales
END AS sls_sales,
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0 
		THEN sls_sales / NULLIF(sls_quantity,0)
	ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details

--Loading silver.erp_cust_az12
INSERT INTO silver.erp_cust_az12(
	cid,
    bdate,
    gen)
    
SELECT 
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,CHAR_LENGTH(cid))
	ELSE cid
END cid,
CASE WHEN bdate > CURRENT_TIMESTAMP THEN NULL
	ELSE bdate
END bdate,
CASE WHEN REGEXP_LIKE(gen, '^[[:space:]]*f(emale)?[[:space:]]*$', 'i') THEN 'Female'
	WHEN REGEXP_LIKE(gen, '^[[:space:]]*m(ale)?[[:space:]]*$', 'i') THEN 'Male'
    ELSE "n/a"
END AS gen
FROM bronze.erp_cust_az12;

--Loading silver.erp_loc_a101
INSERT INTO silver.erp_loc_a101(
	cid,
    cntry)
    
SELECT
REPLACE(cid, '-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	WHEN TRIM(cntry) IN ("US", "USA") THEN "United States"
    WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
    ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

--Loading silver.erp_px_cat_g1v2
INSERT INTO silver.erp_px_cat_g1v2(
	id,
    cat,
    subcat,
    maintenance)
    
SELECT 
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;
