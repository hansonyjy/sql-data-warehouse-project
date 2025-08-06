/*
===============================================================================
Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This script loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `LOAD DATA INFILE` command to load data from csv Files to bronze tables.
===============================================================================
*/

SET sql_mode = '';

TRUNCATE TABLE bronze.crm_cust_info;
LOAD DATA INFILE '/Users/hansonyang/mysql-imports/source_crm/cust_info.csv'
INTO TABLE bronze.crm_cust_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(cst_id, cst_key, cst_firstname, cst_lastname, cst_material_status, cst_gndr, cst_create_date);

TRUNCATE TABLE bronze.crm_prd_info;
LOAD DATA INFILE '/Users/hansonyang/mysql-imports/source_crm/prd_info.csv'
INTO TABLE bronze.crm_prd_info
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt);

TRUNCATE TABLE bronze.crm_sales_details;
LOAD DATA INFILE '/Users/hansonyang/mysql-imports/source_crm/sales_details.csv'
INTO TABLE bronze.crm_sales_details
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price);

TRUNCATE TABLE bronze.erp_cust_az12;
LOAD DATA INFILE '/Users/hansonyang/mysql-imports/source_erp/cust_az12.csv'
INTO TABLE bronze.erp_cust_az12
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(CID,BDATE,GEN);

TRUNCATE TABLE bronze.erp_loc_a101;
LOAD DATA INFILE '/Users/hansonyang/mysql-imports/source_erp/loc_a101.csv'
INTO TABLE bronze.erp_loc_a101
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(CID,CNTRY);

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
LOAD DATA INFILE '/Users/hansonyang/mysql-imports/source_erp/px_cat_g1v2.csv'
INTO TABLE bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 LINES
(ID,CAT,SUBCAT,MAINTENANCE);
