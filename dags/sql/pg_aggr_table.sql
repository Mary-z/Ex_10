DROP TABLE IF EXISTS aggr_table;
CREATE TABLE aggr_table AS
    SELECT product_id, region, count(sale_id) as count_sales, sum(sale_amount) as sum_sales, ROUND(AVG(sale_amount)::numeric, 1) AS average_sale_amount
    FROM sales GROUP BY region, product_id;
