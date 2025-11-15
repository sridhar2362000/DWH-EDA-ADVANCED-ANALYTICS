/**************************** Stored Procedure ***********************************/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @Start_time DATETIME, @End_time DATETIME,
			 @Batch_Start_time DATETIME, @Batch_End_time DATETIME;
	BEGIN TRY
	SET @Batch_Start_time = GETDATE();
	PRINT '===================================';
	PRINT 'LOAD SILVER LAYER';
	PRINT '===================================';

	PRINT '===================================';
	PRINT 'LOAD CRM TABLES';
	PRINT '===================================';


SET @Start_time = GETDATE();
print '<< Truncating the CRM customer table >>';
truncate table silver.crm_customer_info;
print '<< Inserting the CRM customer table >>';

insert into silver.crm_customer_info(
customer_id,
customer_key,
customer_firstname,
customer_lastname,
customer_marital_status,
customer_gender,
customer_create_date)

select 
customer_id,
customer_key,

TRIM(customer_firstname) AS customer_firstname,
TRIM(customer_lastname) AS customer_lastname,

CASE WHEN UPPER(TRIM(customer_marital_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(customer_marital_status)) = 'M' THEN 'Married'
	 ELSE 'NA'
END 
customer_marital_status,

CASE WHEN UPPER(TRIM(customer_gender)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(customer_gender)) = 'M' THEN 'Male'
	 ELSE 'NA'
END 
customer_gender,
customer_create_date from(
select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC)
AS flag_set from bronze.crm_customer_info
where customer_id is not null)t where flag_set = 1;

SET @End_time = GETDATE();
PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

SET @Start_time = GETDATE();

print '<< Truncating the CRM product table >>';
truncate table silver.crm_product_info ;
print '<< Inserting the CRM product table >>';

INSERT INTO silver.crm_product_info (
product_id,
category_id,
product_key,
product_name,
product_cost,
product_line,
product_start_date,
product_end_date)
SELECT
product_id,
REPLACE(SUBSTRING(product_key, 1, 5), '-', '_')AS category_id,
SUBSTRING(product_key, 7, LEN(product_key)) AS product_key,
product_name,
ISNULL(product_cost, 0) as product_cost,
CASE WHEN UPPER(TRIM(product_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(product_line)) = 'R' THEN 'Roads'
	 WHEN UPPER(TRIM(product_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(product_line)) = 'T' THEN 'Touring'
	 ELSE 'N/A'
END AS product_line,
CAST (product_start_date AS DATE) AS product_start_date,
CAST(LEAD(product_start_date) OVER (PARTITION BY product_key ORDER BY product_start_date)-1 AS DATE) product_end_date
from bronze.crm_product_info;

SET @End_time = GETDATE();
PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

SET @Start_time = GETDATE();

print '<< Truncating the CRM Sales table >>';
truncate table silver.crm_sales_details;
print '<< Inserting the CRM Sales table >>';

insert into silver.crm_sales_details(
sales_order_number,
sales_product_key,
sales_customer_id,
sales_order_date,
sales_ship_date,
sales_due_date,
sales_sale,
sales_quantity,
sales_price)
select
sales_order_number ,
sales_product_key ,
sales_customer_id ,
CASE WHEN sales_order_date = 0 or len(sales_order_date) != 8 then null
		 ELSE cast(cast(sales_order_date as varchar)as date)
END AS sales_order_date,

CASE WHEN sales_ship_date = 0 or len(sales_ship_date) != 8 then null
		 ELSE cast(cast(sales_ship_date as varchar)as date)
END AS sales_ship_date,

CASE WHEN sales_due_date = 0 or len(sales_due_date) != 8 then null
		 ELSE cast(cast(sales_due_date as varchar)as date)
END AS sales_due_date,

case when sales_sale is null or sales_sale <=0 or sales_sale != sales_quantity * abs(sales_price)
then sales_quantity * abs(sales_price)
else sales_sale
end as sales_sale,

sales_quantity,

case when sales_price is null or sales_price <=0
then sales_sale / nullif(sales_quantity, 0)
else sales_price
end as sales_price
from bronze.crm_sales_details;

SET @End_time = GETDATE();
PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

SET @Start_time = GETDATE();

print '<< Truncating the ERP customer table >>';
truncate table silver.erp_cust_az12;
print '<< Inserting the ERP customer table >>';

insert into silver.erp_cust_az12(
customer_id,
birth_date,
gender)
select 
case when customer_id like 'NAS%' then SUBSTRING(customer_id, 4,len(customer_id))
else customer_id
end customer_id,

case when birth_date > getdate() then null
else birth_date
end as birth_date,

case when  UPPER(TRIM(gender)) in ('F','female','FEMALE')THEN 'Female'
	 when  UPPER(TRIM(gender)) in ('M','male','MALE') THEN 'Male'
	 else 'NA'
end gender
from bronze.erp_cust_az12;

SET @End_time = GETDATE();
PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

SET @Start_time = GETDATE();

print '<< Truncating the ERP Country table >>';
truncate table silver.erp_loc_a101;
print '<< Inserting the ERP Country table >>';

insert into silver.erp_loc_a101(
country_id,
country)
select 
REPLACE(country_id, '-', '') country_id,
CASE WHEN TRIM(country)='DE' THEN 'Germany'
	 WHEN TRIM(country) in ('United States','US','USA') THEN 'USA'
	 WHEN TRIM(country) = '' OR country IS NULL THEN 'NA'
	 ELSE TRIM(country) 
END AS country
from bronze.erp_loc_a101;

SET @End_time = GETDATE();
PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

SET @Start_time = GETDATE();

print '<< Truncating the ERP category table >>';
truncate table silver.erp_px_cat_g1v2;
print '<< Inserting the ERP category table >>';

insert into silver.erp_px_cat_g1v2(
id,
category,
subcategory,
maintenance)
select id,
category,
subcategory,
maintenance
from bronze.erp_px_cat_g1v2;
SET @End_time = GETDATE();
PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

SET @Batch_End_time = GETDATE();
		PRINT '======================================================'
		PRINT 'Loading Silver is completed' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Total load duration ' + CAST(DATEDIFF(SECOND,  @Batch_Start_time, @Batch_End_time )AS NVARCHAR) + ' seconds';
		PRINT '======================================================'

	END TRY
	BEGIN CATCH
		PRINT '======================================================'
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER '
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================'
	END CATCH;
END;

EXEC silver.load_silver;

DROP PROCEDURE silver.load_silver;
