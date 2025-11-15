select * from silver.crm_customer_info;

select customer_id, COUNT(*) from(
select
ci.customer_id,
ci.customer_key,
ci.customer_firstname,
ci.customer_lastname,
ci.customer_marital_status,
ci.customer_gender,
ci.customer_create_date,
ca.birth_date,
ca.gender,
lo.country
from silver.crm_customer_info ci
left join silver.erp_cust_az12 ca
on ci.customer_key = ca.customer_id
left join silver.erp_loc_a101 lo
on ci.customer_key = lo.country_id)t
group by customer_id
having COUNT(*)>1

/*data integration issue*/
 

select distinct
ci.customer_gender,
ca.gender,
case when ci.customer_gender != 'NA' then ci.customer_gender
     else coalesce(ca.gender, 'NA')
end as upd_gender
from silver.crm_customer_info ci
left join silver.erp_cust_az12 ca
on ci.customer_key = ca.customer_id
left join silver.erp_loc_a101 lo
on ci.customer_key = lo.country_id
order by 1,2


IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
CREATE VIEW gold.dim_customers AS
select
row_number() over (order by ci.customer_id) as customer_ID_number,
ci.customer_id,
ci.customer_key,
ci.customer_firstname,
ci.customer_lastname,
lo.country,
ci.customer_marital_status,
case when ci.customer_gender != 'NA' then ci.customer_gender
     else coalesce(ca.gender, 'NA')
end as customer_gender,
ci.customer_create_date,
ca.birth_date
from silver.crm_customer_info ci
left join silver.erp_cust_az12 ca
on ci.customer_key = ca.customer_id
left join silver.erp_loc_a101 lo
on ci.customer_key = lo.country_id;

 /*******************************************************************/

select * from silver.crm_product_info;



select product_key, COUNT(*) from(
SELECT
pn.product_id,
pn.category_id,
pn.product_key,
pn.product_name,
pn.product_cost,
pn.product_line,
pn.product_start_date,
pc.category,
pc.subcategory,
pc.maintenance
FROM silver.crm_product_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.category_id = pc.id
WHERE pn.product_end_date IS NULL)t
group by product_key
having COUNT(*)>1


/**/

IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER (ORDER BY pn.product_start_date, pn.product_key) AS product_keyname, -- Surrogate key
pn.product_id,
pn.product_key,
pn.product_name,
pn.category_id,
pc.category,
pc.subcategory,
pc.maintenance,
pn.product_cost,
pn.product_line,
pn.product_start_date
FROM silver.crm_product_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.category_id = pc.id
WHERE pn.product_end_date IS NULL; -- Filter out all historical data


select * from gold.dim_products;

/********************************************************************************/

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
CREATE VIEW gold.fact_sales AS
SELECT 
sd.sales_order_number,
pr.product_id,
cu.customer_id,
sd.sales_order_date,
sd.sales_ship_date,
sd.sales_due_date,
sd.sales_sale,
sd.sales_quantity,
sd.sales_price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sales_product_key  = pr.product_key
LEFT JOIN gold.dim_customers cu 
    ON sd.sales_customer_id = cu.customer_id

/*SELECT * 
FROM gold.fact_sales f 
LEFT JOIN gold.dim_customers c 
    ON c.customer_key = TRY_CAST(f.customer_id AS INT)
WHERE c.customer_key IS NULL;*/

/*************************************************************************************************************************************************/
