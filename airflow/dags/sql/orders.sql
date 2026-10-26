DROP TABLE IF EXISTS sales;
CREATE TABLE sales AS
SELECT DISTINCT
order_id,
CAST(order_date AS TIMESTAMP)          AS order_date,
COALESCE(customer_name, 'Unknown')     AS customer_name,
COALESCE(product, 'Unknown')           AS product,
quantity,
price,
COALESCE(region, 'Unknown'),
(quantity * price)                       AS total_sale
FROM raw_sales