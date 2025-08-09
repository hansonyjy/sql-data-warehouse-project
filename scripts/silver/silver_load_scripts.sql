/*
===============================================================================
Script Purpose:
    This script performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.

Usage Example:
    EXEC Silver.load_silver;
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
