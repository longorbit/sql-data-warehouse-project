/*
------------------------------------------------------------------------
DDL Script: Create Bromze Tables
------------------------------------------------------------------------
Script Purpose:
  This script create tables in 'bromze' schema, dropping existing tables 
  if they already exist.
  Run this script to re-difine the ddl structure of 'bronze' Tables. 
------------------------------------------------------------------------
*/
IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info
CREATE TABLE bronze.crm_cust_info(
	cut_id INT
	,cut_key NVARCHAR(50)
	,cut_firstname NVARCHAR(50)
	,cut_lastname NVARCHAR(50)
	,cut_material_status NVARCHAR(50)
	,cut_gndr NVARCHAR(50)
	,cut_create_date date			 
);
GO
IF OBJECT_ID ('bronze.crm_prd_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_prd_info
CREATE TABLE bronze.crm_prd_info(
	prd_id INT
	,prd_key NVARCHAR(50)
	,prd_nm NVARCHAR(50)
	,prd_cost INT
	,prd_line NVARCHAR(50)
	,prd_start_dt NVARCHAR(50)
	,prd_end_dt date			 
);
GO
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50)
	,sls_prd_key NVARCHAR(50)
	,sls_cust_id INT
	,sls_order_dt INT
	,sls_ship_dt INT
	,sls_due_dt INT
	,sls_sales INT
	,sls_quantity INT
	,sls_price INT
);
GO
IF OBJECT_ID ('bronze.erp_loc_a101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_a101
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50)
	,cntry NVARCHAR(50)
);
GO
IF OBJECT_ID ('bronze.erp_cust_az12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_az12
CREATE TABLE bronze.erp_cust_az12(
	cid NVARCHAR(50)
	,bdate date
	,gen NVARCHAR(50)
);
GO
IF OBJECT_ID ('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
	DROP TABLE bronze.erp_px_cat_g1v2
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50)
	,cat NVARCHAR(50)
	,subcat NVARCHAR(50)
	,maintenance NVARCHAR(50)
);
GO
		



