-- How many stores does the business have and in which countries?

SELECT
	CASE
		WHEN country_code = 'DE' THEN 'Germany'
		WHEN country_code = 'US' THEN 'America'
		WHEN country_code = 'GB' THEN 'United Kingdom'
		ELSE 'Online'
	END AS country,
count(*) AS total_no_stores
FROM dim_store_details 
GROUP BY country_code;

-- Which locations currently have the most stores? (Top 7)

SELECT locality, count(*) AS total_no_stores FROM dim_store_details
GROUP BY locality
ORDER BY total_no_stores DESC 
LIMIT 7;

-- Which months produced the largest amount of sales? (Top 6)

SELECT ROUND(SUM(dp.product_price * ot.product_quantity)::numeric, 2) AS total_sales, ddt.month FROM orders_table AS ot
JOIN dim_date_times AS ddt
ON ot.date_uuid = ddt.date_uuid
JOIN dim_products AS dp
ON ot.product_code = dp.product_code
GROUP BY ddt.month 
ORDER BY total_sales DESC
LIMIT 6;

-- How many sales are coming from online?

SELECT COUNT(*) AS number_of_sales, SUM(product_quantity) AS product_quantity_count,  
	CASE
		WHEN store_type = 'Web Portal' THEN 'Web'
		ELSE 'Offline'
	END AS location
FROM orders_table as ot
JOIN dim_store_details AS dsd
ON ot.store_code = dsd.store_code
GROUP BY location
ORDER BY number_of_sales;

-- What percentage of sales come through each type of store?

SELECT
	ROUND(SUM(product_quantity * dp.product_price)::numeric, 2) AS total_sales,
	dsd.store_type, 
	ROUND(COUNT(*) / CAST((SELECT COUNT(*) FROM orders_table) AS NUMERIC), 2) * 100 as "percentage_total(%)"
FROM orders_table as ot
JOIN dim_store_details AS dsd
ON ot.store_code = dsd.store_code
JOIN dim_products AS dp
ON ot.product_code = dp.product_code
GROUP BY dsd.store_type;

-- Which month in each year produced the highest cost of sales?

SELECT
	ROUND(SUM(product_quantity * dp.product_price)::numeric, 2) AS total_sales, ddt.year, ddt.month
FROM orders_table as ot
JOIN dim_products AS dp
ON ot.product_code = dp.product_code
JOIN dim_date_times as ddt
ON ot.date_uuid = ddt.date_uuid
GROUP BY ddt.year, ddt.month
ORDER BY total_sales DESC
LIMIT 10; 

-- What is our staff headcount in each country?

SELECT SUM(staff_numbers) AS total_staff_numbers, country_code
FROM dim_store_details
GROUP BY country_code
ORDER BY total_staff_numbers DESC;

-- Which German Store type is selling the most

SELECT ROUND(SUM(product_quantity * dp.product_price)::numeric, 2) AS total_sales, dsd.store_type, dsd.country_code
FROM dim_store_details as dsd
JOIN orders_table AS ot
ON dsd.store_code = ot.store_code
JOIN dim_products AS dp
ON ot.product_code = dp.product_code
WHERE country_code LIKE 'DE'
GROUP BY dsd.store_type, dsd.country_code
ORDER BY total_sales;

-- How quickly is the company making sales?    

WITH cte1 AS (
	SELECT
		CONCAT(
			year,
			'-',
			month,
			'-',
			day,
			' ',
			timestamp
			)::timestamp(3) AS true_timestamp,
		year
	FROM dim_date_times
	ORDER BY true_timestamp DESC
	),
	cte2 AS (
	SELECT 
		true_timestamp, year, LEAD(true_timestamp, 1) OVER (ORDER BY true_timestamp DESC) AS interval
		FROM cte1
		)
SELECT year, AVG((true_timestamp - interval)) as actual_time_taken
FROM cte2
GROUP BY year
ORDER BY actual_time_taken DESC
LIMIT 5;