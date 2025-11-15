/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

IF OBJECT_ID('bronze.crm_customer_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_customer_info;
GO

create table bronze.crm_customer_info(
customer_id INT,
customer_key NVARCHAR(50),
customer_firstname NVARCHAR(50),
customer_lastname NVARCHAR(50),
customer_marital_status NVARCHAR(50),
customer_gender NVARCHAR(50),
customer_create_date DATE
);

IF OBJECT_ID('bronze.crm_product_info', 'U') IS NOT NULL
    DROP TABLE bronze.crm_product_info;
GO

create table bronze.crm_product_info(
product_id INT,
product_key NVARCHAR(50),
product_name NVARCHAR(50),
product_cost INT,
product_line NVARCHAR(50),
product_start_date DATETIME,
product_end_date DATETIME
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE bronze.crm_sales_details;
GO

create table bronze.crm_sales_details(
sales_order_number NVARCHAR(50),
sales_product_key NVARCHAR(50),
sales_customer_id INT,
sales_order_date INT,
sales_ship_date INT,
sales_due_date INT,
sales_sale INT,
sales_quantity INT,
sales_price INT
);


IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE bronze.erp_loc_a101;
GO

create table bronze.erp_loc_a101 (
country_id    NVARCHAR(50),
country  NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE bronze.erp_cust_az12;
GO

CREATE TABLE bronze.erp_cust_az12 (
customer_id    NVARCHAR(50),
birth_date  DATE,
gender    NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE bronze.erp_px_cat_g1v2;
GO

CREATE TABLE bronze.erp_px_cat_g1v2 (
id           NVARCHAR(50),
category          NVARCHAR(50),
subcategory       NVARCHAR(50),
maintenance  NVARCHAR(50)
);


