
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @Start_time DATETIME, @End_time DATETIME,
			 @Batch_Start_time DATETIME, @Batch_End_time DATETIME;
	BEGIN TRY
	SET @Batch_Start_time = GETDATE();
	PRINT '===================================';
	PRINT 'LOAD BRONZE LAYER';
	PRINT '===================================';

	PRINT '===================================';
	PRINT 'LOAD CRM TABLES';
	PRINT '===================================';


		SET @Start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_customer_info;

		BULK INSERT bronze.crm_customer_info
		FROM 'C:\Users\rahul\OneDrive\Documents\Sridhar\project-SQL\datasets\source_crm\cust_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';


		/***************************************************************************************************************************/
		
		SET @Start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_product_info;

		BULK INSERT bronze.crm_product_info
		FROM 'C:\Users\rahul\OneDrive\Documents\Sridhar\project-SQL\datasets\source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';
		/***************************************************************************************************************************/

		
		SET @Start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\rahul\OneDrive\Documents\Sridhar\project-SQL\datasets\source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

		/***************************************************************************************************************************/
		
		SET @Start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\rahul\OneDrive\Documents\Sridhar\project-SQL\datasets\source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';

		/***************************************************************************************************************************/
		
		SET @Start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\rahul\OneDrive\Documents\Sridhar\project-SQL\datasets\source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		/***************************************************************************************************************************/

		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\rahul\OneDrive\Documents\Sridhar\project-SQL\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @End_time = GETDATE();
		PRINT 'LOAD DURATION :' + CAST(DATEDIFF(second, @Start_time, @End_time) AS NVARCHAR) + ' SECONDS';
		PRINT '>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>';

		SET @Batch_End_time = GETDATE();
		PRINT '======================================================'
		PRINT 'Loading Bronze is completed' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Total load duration ' + CAST(DATEDIFF(SECOND,  @Batch_Start_time, @Batch_End_time )AS NVARCHAR) + ' seconds';
		PRINT '======================================================'

	END TRY
	BEGIN CATCH
		PRINT '======================================================'
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER '
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '======================================================'
	END CATCH;
END;

--DROP PROCEDURE bronze.load_bronze;
EXEC bronze.load_bronze;


-- CRM Customer info table
select * from bronze.crm_customer_info;

/*checking duplicates and null in customer-id column*/
select customer_id, count(*)
from bronze.crm_customer_info
group by customer_id
having count(*) > 1 or
customer_id is null;

/*now check the high ranked data for the customer id = 29446 using window functions*/
select * from bronze.crm_customer_info;

select * from bronze.crm_customer_info where customer_id = 29466; 

select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC)
AS flag_set from bronze.crm_customer_info where customer_id = 29466;

/*using subquery checking the duplicates for the whole table using customer_id*/

/*select * from(
select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC)
AS flag_set from bronze.crm_customer_info)t where flag_set = 1 and customer_id = 29446;*/

select * from(
select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC)
AS flag_set from bronze.crm_customer_info)t where flag_set = 1;


select * from(
select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC)
AS flag_set from bronze.crm_customer_info)t where flag_set != 1 ;


/*check the unwanted spaces in customer_key column*/
select customer_key from bronze.crm_customer_info
where customer_key != TRIM(customer_key);


/*check the unwanted spaces in each column and cell*/
select 
customer_id,
customer_key,
TRIM(customer_firstname) AS customer_firstname,
TRIM(customer_lastname) AS customer_lastname,
customer_marital_status ,
customer_gender,
customer_create_date from(
select *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY customer_create_date DESC)
AS flag_set from bronze.crm_customer_info
where customer_id is not null)t where flag_set = 1;

/*check the valuse in low cardinality columns like customer_marital_status and customer_gender */

select distinct customer_marital_status from bronze.crm_customer_info;

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

/*Inserting it into the silver.crm_customer_info*/
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

/*****************************************************************************************************/

-- CRM product info table
select * from bronze.crm_product_info;

/*checking duplicates and null in customer-id column*/
select product_id, count(*)
from bronze.crm_product_info
group by product_id
having count(*) > 1 or
product_id is null;

/*Replaced the CO-RF-FR-R92B-58 format to CO_RF comparing with bronze.erp_px_cat_g1v2 table category column*/
select id, category from bronze.erp_px_cat_g1v2;

select product_key,
REPLACE(SUBSTRING(product_key, 1, 5), '-', '_')AS upd_category_id,
SUBSTRING(product_key, 7, LEN(product_key)) AS upd_product_key
from bronze.crm_product_info;

/*comparing the bronze.crm_sales_details table bronze.crm_sales_details sales_product_key
to bronze.crm_product_info table bronze.crm_product_info*/
select sales_product_key from bronze.crm_sales_details;


select product_key,
REPLACE(SUBSTRING(product_key, 1, 5), '-', '_')AS upd_category_id,
SUBSTRING(product_key, 7, LEN(product_key)) AS upd_product_key
from bronze.crm_product_info 
where SUBSTRING(product_key, 7, LEN(product_key)) in (
select sales_product_key from bronze.crm_sales_details 
where sales_product_key like 'BK-R93R%')

/*check the unwanted spaces in customer_key column*/
select product_name from bronze.crm_product_info
where product_name != TRIM(product_name);


/*check the negative costs in customer_key column*/
select product_cost from bronze.crm_product_info
where product_cost < 0 or product_cost is null;

/*making the null values to 0*/
select product_cost, 
ISNULL(product_cost, 0) as upd_product_cost
from bronze.crm_product_info;


/*check the valuse in low cardinality column for product_line */

select product_line,
CASE WHEN UPPER(TRIM(product_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(product_line)) = 'R' THEN 'Roads'
	 WHEN UPPER(TRIM(product_line)) = 'S' THEN 'Other Sales'
	 WHEN UPPER(TRIM(product_line)) = 'T' THEN 'Touring'
	 ELSE 'N/A'
END AS upd_product_line
from bronze.crm_product_info;

/*checking the product_end_date is not earlier than or before the product_start_date*/
select product_start_date, product_end_date 
from bronze.crm_product_info where product_end_date <
product_start_date;


select product_id,
product_key,
product_name,
product_start_date,
product_end_date,
LEAD(product_start_date) OVER (PARTITION BY product_key ORDER BY product_start_date)-1 AS upd_product_enddate_test
from bronze.crm_product_info;


/**/
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

/********************************************************************************************************************************/

-- CRM sales info table
select * from  bronze.crm_sales_details;

/*checking duplicates and null in sales_customer_id and sales_product_key column*/
select sales_product_key from bronze.crm_sales_details
where sales_product_key not in (select product_key from silver.crm_product_info);

select sales_customer_id from bronze.crm_sales_details
where sales_customer_id not in (select sales_customer_id from silver.crm_customer_info);

/*fixing the sales_order_date from int to date datatype*/
select sales_order_date, nullif(sales_order_date, 0) from bronze.crm_sales_details 
where sales_order_date <= 0 
or len(sales_order_date) != 8 
or sales_order_date > 20500101
or sales_order_date < 19000101;

select sales_ship_date, nullif(sales_ship_date, 0) from bronze.crm_sales_details 
where sales_ship_date <= 0 
or len(sales_ship_date) != 8 
or sales_ship_date > 20500101
or sales_ship_date < 19000101;

select sales_due_date, nullif(sales_due_date, 0) from bronze.crm_sales_details 
where sales_due_date <= 0 
or len(sales_due_date) != 8 
or sales_due_date > 20500101
or sales_due_date < 19000101;

select sales_order_date,
CASE WHEN sales_order_date = 0 or len(sales_order_date) != 8 then null
		 ELSE cast(cast(sales_order_date as varchar)as date)
END AS sales_order_date
from bronze.crm_sales_details;

select sales_ship_date,
CASE WHEN sales_ship_date = 0 or len(sales_ship_date) != 8 then null
		 ELSE cast(cast(sales_ship_date as varchar)as date)
END AS sales_ship_date
from bronze.crm_sales_details;

select sales_due_date,
CASE WHEN sales_due_date = 0 or len(sales_due_date) != 8 then null
		 ELSE cast(cast(sales_due_date as varchar)as date)
END AS sales_due_date
from bronze.crm_sales_details;

/*checking the sales_order_date is not earlier than or before the sales_ship_date and sales_due_date*/

select * from  bronze.crm_sales_details where sales_order_date > sales_ship_date
or sales_order_date > sales_due_date;

select distinct
    sales_sale as old_sales_sale,
    sales_quantity,
    sales_price as old_sales_price,
case when sales_sale is null or sales_sale <=0 or sales_sale != sales_quantity * abs(sales_price)
then sales_quantity * abs(sales_price)
else sales_sale
end as sales_sale,

case when sales_price is null or sales_price <=0
then sales_sale / nullif(sales_quantity, 0)
else sales_price
end as sales_price

from bronze.crm_sales_details
where sales_sale IS NULL
   or sales_quantity IS NULL
   or sales_price IS NULL
   or sales_sale <= 0
   or sales_quantity <= 0
   or sales_price <= 0
   or sales_sale != sales_quantity * sales_price
order by sales_sale, sales_quantity, sales_price;

/**/
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

/*******************************************************************************************************/

-- ERP customer info table
select * from bronze.erp_cust_az12;

/*comparing and fixing the customer_id from erp customer table with with silver customer info  */
select
customer_id,
case when customer_id like 'NAS%' then SUBSTRING(customer_id, 4,len(customer_id))
else customer_id
end upd_customer_id
from bronze.erp_cust_az12;

/*checking any data is not matched with silver customer info and erp customer tables*/
select
customer_id
from bronze.erp_cust_az12
where 
case when customer_id like 'NAS%' then SUBSTRING(customer_id, 4,len(customer_id))
else customer_id
end not in (select distinct customer_key from silver.crm_customer_info)

/*Identifying the out of range dates*/
select distinct birth_date from bronze.erp_cust_az12 
where birth_date < '1924-01-01' or birth_date > getdate()

select birth_date,
case when birth_date > getdate() then null
else birth_date
end as upd_birth_date
from bronze.erp_cust_az12;

select gender,
CASE WHEN UPPER(TRIM(gender)) in ('F','female','FEMALE')THEN 'Female'
	 WHEN UPPER(TRIM(gender)) in ('M','male','MALE') THEN 'Male'
	 ELSE 'NA'
END upd_gender
from bronze.erp_cust_az12;

/**/
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

/***********************************************************************************/

-- ERP location info table
select * from bronze.erp_loc_a101;


select country_id from bronze.erp_loc_a101  
select distinct customer_key from silver.crm_customer_info

/*replaced country_id to customer_key format*/
select country_id,
REPLACE(country_id, '-', '') upd_country_id
from bronze.erp_loc_a101 where 
REPLACE(country_id, '-', '') in 
(select customer_key from silver.crm_customer_info)

/*handled blanl or mis-matched country codes*/
select country,
CASE WHEN TRIM(country)='DE' THEN 'Germany'
	 WHEN TRIM(country) in ('United States','US','USA') THEN 'USA'
	 WHEN TRIM(country) = '' OR country IS NULL THEN 'NA'
	 ELSE TRIM(country) 
END AS upd_country
from bronze.erp_loc_a101;

/**/
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

/*******************************************************************************************/

-- ERP category info table
select * from bronze.erp_px_cat_g1v2;

/*check the unwanted spaces*/
select * from bronze.erp_px_cat_g1v2
where category != trim(category) 
or subcategory != trim(subcategory)
or maintenance != trim(maintenance);

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

/*******************************************************************************/