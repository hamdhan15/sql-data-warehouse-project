/*
======================================================================
DDL Script: Create Gold Views
======================================================================

Script Purpose:
This script creates views for the Gold layer in the data warehouse.

The Gold layer represents the final dimension and fact tables (Star Schema).

Each view performs transformations and combines data from the Silver layer
to produce a clean, enriched, and business-ready dataset.

Usage:
- These views can be queried directly for analytics and reporting.
======================================================================
*/

PRINT '======================================================================================'
PRINT ' Create Dimension : gold.dim_customers'
PRINT '======================================================================================'

IF OBJECT_ID ('gold.dim_customers','V') IS NOT NULL
	DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER (ORDER BY ci.cst_id) as customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	CASE WHEN ci.cst_gndr!='n/a' then ci.cst_gndr
	else COALESCE(ca.gender,'n/a')
	end as gender,
	ca.bdate as birth_date,
	ci.cst_marital_status as marital_status,
	lo.country as country,
	ci.cst_create_date as create_date
FROM
silver.crm_cust_info ci
LEFT JOIN silver.erp_CUST_AZ12 ca
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_LOC_A101 lo
on ci.cst_key=lo.cid;
GO


PRINT '======================================================================================'
PRINT ' Create Dimension : gold.dim_products'
PRINT '======================================================================================'

IF OBJECT_ID ('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO

create view gold.dim_products as
select
row_number() over (order by pn.prd_start_dt,pn.prd_id) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.prd_cat_id as category_id,
coalesce(pc.cat,'n/a') as category,
coalesce(pc.subcat,'n/a') as sub_category,
coalesce(pc.maintenance,'n/a') as maintenance,
pn.prd_cost as cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from
silver.crm_prd_info pn
left join silver.erp_PX_CAT_G1V2 pc
on pc.id=pn.prd_cat_id
where pn.prd_end_dt is null
GO


PRINT '======================================================================================'
PRINT ' Create Dimension : gold.fact_sales'
PRINT '======================================================================================'

IF OBJECT_ID ('gold.fact_sales','V') IS NOT NULL
	DROP VIEW gold.fact_sales;
GO


CREATE VIEW gold.fact_sales AS
select
ss.sls_ord_num as sales_order_number,
p.product_key as product_key,
c.customer_key as customer_key,
ss.sls_order_dt as order_date,
ss.sls_ship_dt as ship_date,
ss.sls_due_dt as due_date,
ss.sls_sales as sales,
ss.sls_quantity as quantity,
ss.sls_price as price
from
silver.crm_sales_details ss
left join gold.dim_customers c
on c.customer_id=ss.sls_cust_id 
left join gold.dim_products p
on p.product_number= ss.sls_prd_key
GO
