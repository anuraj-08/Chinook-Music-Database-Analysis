use chinook;
-- 1 
-- To check duplicates, we use the primary key. For example:
select
    customer_id,
    count(*)
from customer
group by customer_id
having count(*) > 1;

-- To check for missing values, for example in the invoice table:
SELECT * 
FROM invoice 
WHERE invoice_id IS NULL 
   OR customer_id IS NULL 
   OR invoice_date IS NULL 
   OR billing_address IS NULL 
   OR billing_city IS NULL 
   OR billing_state IS NULL 
   OR billing_country IS NULL 
   OR billing_postal_code IS NULL 
   OR total IS NULL;

-- 2 Find the top-selling tracks and top artist in the USA and identify their most famous genres.
SELECT 
    t.name AS track,
    at.name AS artist,
    g.name AS genre,
    SUM(quantity) AS total_count,
    SUM(quantity * il.unit_price) AS total_revenue,
    RANK() OVER(ORDER BY SUM(quantity * il.unit_price) DESC,SUM(quantity) DESC) AS RNK
FROM invoice_line il 
JOIN track t 
    ON il.track_id = t.track_id
JOIN genre g 
    ON g.genre_id = t.genre_id
JOIN invoice i 
    ON i.invoice_id = il.invoice_id
JOIN album a 
    ON a.album_id = t.album_id
JOIN artist at 
    ON at.artist_id = a.artist_id
WHERE billing_country = 'USA'
GROUP BY t.name, g.name, at.name 
ORDER BY total_count DESC, total_revenue DESC 
LIMIT 5;


-- 3 What is the customer demographic breakdown (age, gender, location) of Chinook's customer base?
SELECT 
    country,
    COALESCE(state, "N/A") AS state,
    city,
    COUNT(*) AS total_customers
FROM customer
GROUP BY country, state, city 
ORDER BY total_customers desc ,country;

-- 4 Calculate the total revenue and number of invoices for each country, state, and city
SELECT 
    billing_country AS country,
    billing_state AS state,
    billing_city AS city,
    COUNT(invoice_id) AS number_of_invoices,
    SUM(total) AS total_revenue
FROM invoice
GROUP BY billing_country, billing_state, billing_city
ORDER BY total_revenue DESC;

-- 5 Find the top 5 customers by total revenue in each country
WITH cust_total_rev AS (
    SELECT
        i.customer_id,
        CONCAT(first_name, " ", last_name) AS name,
        billing_country AS country,
        SUM(total) AS total_revenue,
        DENSE_RANK() OVER (PARTITION BY country ORDER BY SUM(total) DESC) AS rnk
    FROM invoice i
    JOIN customer c
        ON i.customer_id = c.customer_id
    GROUP BY customer_id, billing_country, CONCAT(first_name, " ", last_name)
)
SELECT
    name,
    country,
    total_revenue,
    rnk
FROM cust_total_rev
WHERE rnk <= 5;

-- 6 Identify the top-selling track for each customer
WITH cust_qnty_sum AS (
    SELECT
        CONCAT(first_name, " ", last_name) AS customer_name,
        t.name AS track_name,
        SUM(il.quantity) AS sum,
        ROW_NUMBER() OVER (PARTITION BY CONCAT(first_name, " ", last_name) ORDER BY SUM(il.quantity) DESC) AS rnk
    FROM customer c
    JOIN invoice i 
        ON c.customer_id = i.customer_id
    JOIN invoice_line il 
        ON i.invoice_id = il.invoice_id
    JOIN track t 
        ON il.track_id = t.track_id
    GROUP BY CONCAT(first_name, " ", last_name), t.name
)

SELECT
    customer_name,
    track_name,
    sum AS total_quantity
FROM cust_qnty_sum
WHERE rnk = 1
ORDER BY total_quantity DESC;

-- 7 Are there any patterns or trends in customer purchasing behavior
WITH cust_trend_format AS (
    SELECT
        customer_id,
        DATE(invoice_date) AS date,
        DATE(LAG(invoice_date) OVER (PARTITION BY customer_id ORDER BY invoice_date)) AS prev_date,
        TIMESTAMPDIFF(DAY, LAG(invoice_date) OVER (PARTITION BY customer_id ORDER BY invoice_date), invoice_date) AS date_dif,
        total,
        billing_country
    FROM invoice i
)

SELECT
    customer_id,
    ROUND(AVG(date_dif), 2) AS frequency_of_buying,
    ROUND(AVG(total), 2) AS avg_order_value
FROM cust_trend_format
GROUP BY customer_id;

--  8.	What is the customer churn rate?
WITH churned_cust AS 
(
	SELECT
		customer_id
	FROM invoice i 
	GROUP BY customer_id
	HAVING MAX(invoice_date) < DATE_SUB((SELECT MAX(invoice_date) FROM invoice), INTERVAL 6 MONTH)
)
SELECT ROUND(100*(SELECT COUNT(*) FROM churned_cust)/(SELECT COUNT(DISTINCT customer_id) FROM customer),2) AS churn_rate;

-- 9 Calculate the percentage of total sales contributed by each genre in the USA and identify the best-selling genres and artists.
-- View Creation
CREATE VIEW usa_genre_sales AS
SELECT 
	g.genre_id,
    i.billing_country AS country,
    g.name AS genre,
	ROUND(100 * SUM(il.unit_price * il.quantity) / (SELECT SUM(unit_price * quantity) FROM invoice_line il_sub JOIN invoice i_sub ON il_sub.invoice_id = i_sub.invoice_id WHERE i_sub.billing_country='USA'), 2) AS total_sales_per,
    SUM(il.unit_price * il.quantity) AS total_sales,
    RANK() OVER(ORDER BY SUM(il.unit_price * il.quantity) DESC) AS rnk
FROM invoice i
JOIN invoice_line il 
    ON i.invoice_id = il.invoice_id
JOIN track t 
    ON il.track_id = t.track_id
JOIN genre g 
    ON t.genre_id = g.genre_id
WHERE i.billing_country = 'USA'
GROUP BY i.billing_country, g.name, g.genre_id
ORDER BY i.billing_country, total_sales DESC;

-- 1st part of question : total sales contributed by each genre in the USA 
SELECT * FROM usa_genre_sales ;

-- 2n part of question :  identify the best-selling genres and artists.
WITH artist_rnk_by_revenue AS 
(
	SELECT
		ar.artist_id,
		ar.name AS artist_name,
		g.genre_id,
        g.name AS genre_name,
		SUM(il.unit_price * il.quantity) AS revenue,
		ROW_NUMBER() OVER(PARTITION BY g.genre_id ORDER BY SUM(il.unit_price * il.quantity) DESC) AS rnk
	FROM invoice_line il
	JOIN track t 
		ON il.track_id = t.track_id
	JOIN album al 
		ON t.album_id = al.album_id
	JOIN artist ar 
		ON al.artist_id = ar.artist_id
	JOIN genre g 
	ON t.genre_id = g.genre_id
	GROUP BY ar.artist_id, ar.name, g.genre_id, g.name
)
SELECT
	artist_name,
    genre_name,
    revenue,
    rnk
FROM artist_rnk_by_revenue
WHERE rnk <= 3 AND genre_id IN (SELECT genre_id FROM usa_genre_sales WHERE rnk <= 3);



-- 10 Find customers who have purchased tracks from at least 3 different genres 
SELECT 
    CONCAT(c.first_name , " ", c.last_name) AS customer_name,
    COUNT(DISTINCT t.genre_id) AS num_tracks
FROM customer c
JOIN invoice i 
    ON c.customer_id = i.customer_id
JOIN invoice_line il 
    ON i.invoice_id = il.invoice_id
JOIN track t 
    ON il.track_id = t.track_id
GROUP BY i.customer_id, CONCAT(c.first_name , " ", c.last_name)
HAVING COUNT(DISTINCT t.genre_id) > 2
ORDER BY num_tracks DESC;

-- 11.	Rank genres based on their sales performance in the USA
SELECT 
	genre,
    total_sales,
    rnk AS genre_rank
FROM usa_genre_sales
ORDER BY rnk;

-- 12 Identify customers who have not made a purchase in the last 3 months

SELECT
	c.customer_id,
	CONCAT(first_name," ",last_name) AS name,
    DATE(recent_date) AS last_purchase_date
FROM
(SELECT
	customer_id,
	MAX(invoice_date) AS recent_date 
FROM invoice
GROUP BY customer_id) t
JOIN customer c 
ON t.customer_id = c.customer_id
WHERE TIMESTAMPDIFF(MONTH, recent_date, (SELECT MAX(invoice_date) FROM invoice)) >= 3
ORDER BY last_purchase_date DESC;


-- ---------------------------------------------------------------------------------------------------------


-- subjections questions 

-- 1.	Recommend the three albums from the new record label that should be prioritised for advertising and promotion in the USA based on genre sales analysis.
SELECT
	genre_name,
	album_name,
    total_sales,
    rnk AS rank_by_sales
FROM
(SELECT
    g.genre_id,
    g.name AS genre_name,
    a.title AS album_name,
    SUM(il.quantity * il.unit_price) AS total_sales,
    DENSE_RANK() OVER(PARTITION BY genre_id ORDER BY SUM(il.quantity * il.unit_price) DESC) AS rnk
FROM track t
JOIN genre g 
    ON t.genre_id = g.genre_id
JOIN album a
    ON t.album_id = a.album_id
JOIN invoice_line il 
    ON il.track_id = t.track_id
JOIN invoice i 
    ON i.invoice_id = il.invoice_id
WHERE i.billing_country = 'USA' 
  AND g.genre_id IN (SELECT DISTINCT genre_id FROM usa_genre_sales WHERE rnk <= 3)
GROUP BY g.genre_id, g.name, a.title
ORDER BY genre_id, total_sales DESC) AS t
WHERE rnk IN (1, 2);

-- 2 Determine the top-selling genres in countries other than the USA and identify any commonalities or differences.
WITH top_genre_per_country AS 
(
    SELECT
        billing_country AS country,
        g.genre_id,
        g.name,
        SUM(il.quantity * il.unit_price) AS total_sales,
        DENSE_RANK() OVER(PARTITION BY billing_country ORDER BY SUM(il.quantity * il.unit_price) DESC) AS rankk
    FROM track t 
    JOIN genre g 
        ON t.genre_id = g.genre_id
    JOIN invoice_line il 
        ON il.track_id = t.track_id
    JOIN invoice i 
        ON i.invoice_id = il.invoice_id
    WHERE billing_country != 'USA'
    GROUP BY billing_country, g.genre_id , g.name
    ORDER BY billing_country
)
SELECT 
    country,
    name AS genre_name,
    rankk AS rnk
FROM top_genre_per_country
WHERE rankk <= 2;


-- 3.	Customer Purchasing Behavior Analysis: How do the purchasing habits (frequency, basket size, spending amount) of long-term customers differ from those of new customers? What insights can these patterns provide about customer loyalty and retention strategies?
WITH data_min_date AS
(
    SELECT
        *,
        (SELECT MIN(invoice_date) FROM invoice WHERE customer_id = o.customer_id) AS min_date
    FROM invoice o 
    ORDER BY customer_id, invoice_date
),
cust_data_with_status AS 
(
    SELECT
        customer_id,
        billing_country,
        total,
        YEAR(min_date) AS year,
        CASE
            WHEN YEAR(min_date) <= 2017 THEN 'old customer'
            ELSE 'new customer'
        END AS status
    FROM data_min_date
),
cust_agg_data AS
(
    SELECT 
        status,
        COUNT(DISTINCT customer_id) AS total_customer,
        SUM(total) AS total_spent,
        ROUND(SUM(total) / COUNT(DISTINCT customer_id), 2) AS avg_spending
    FROM cust_data_with_status
    GROUP BY status
),
cust_order_frequency AS 
(
    SELECT
        status,
        round(AVG(frequency),2) AS avg_orders_per_customer
    FROM 
    (
        SELECT
            customer_id,
            status,
            COUNT(*) AS frequency
        FROM cust_data_with_status 
        GROUP BY customer_id, status
    ) AS t
    GROUP BY status
),
basket_size_intr AS 
(
    SELECT
        i.customer_id,
        i.invoice_id,
        status,
        COUNT(*) AS basket_size
    FROM invoice i
    JOIN invoice_line il
        ON i.invoice_id = il.invoice_id
    JOIN cust_data_with_status cs 
        ON cs.customer_id = i.customer_id 
    GROUP BY i.customer_id, i.invoice_id, status
    ORDER BY i.customer_id, i.invoice_id
),
basket_size_agg AS
(
    SELECT 
        status,
        ROUND(AVG(basket_size), 2) AS avg_basket_size
    FROM basket_size_intr
    GROUP BY status
)

-- final query
SELECT
	cd.status,
    cd.total_customer,
    cd.total_spent,
    cd.avg_spending,
    cf.avg_orders_per_customer,
    ba.avg_basket_size
FROM cust_agg_data cd 	
JOIN cust_order_frequency cf 
ON cd.status = cf.status
JOIN basket_size_agg ba 
ON cd.status = ba.status;


-- 4.	Product Affinity Analysis: Which music genres, artists, or albums are frequently purchased together by customers? How can this information guide product recommendations and cross-selling initiatives?

SELECT
    g1.name AS genre_1,
    g2.name AS genre_2,
    COUNT(*) AS paired_purchase_count
FROM invoice_line i1
JOIN invoice_line i2
    ON i1.invoice_id = i2.invoice_id 
   AND i1.invoice_line_id < i2.invoice_line_id -- Ensures unique pairing
JOIN track t1
    ON t1.track_id = i1.track_id 
JOIN track t2 
    ON t2.track_id = i2.track_id
JOIN genre g1
    ON t1.genre_id = g1.genre_id
JOIN genre g2 
    ON t2.genre_id = g2.genre_id
WHERE g1.genre_id < g2.genre_id -- Ensures a consistent pair order
GROUP BY g1.name, g2.name
ORDER BY paired_purchase_count DESC;

SELECT
    ar1.name AS artist_1,
    ar2.name AS artist_2,
    COUNT(*) AS paired_purchase_count
FROM invoice_line i1
JOIN invoice_line i2
    ON i1.invoice_id = i2.invoice_id 
   AND i1.invoice_line_id < i2.invoice_line_id -- Ensures unique pairing
JOIN track t1 
    ON i1.track_id = t1.track_id
JOIN track t2 
    ON i2.track_id = t2.track_id
JOIN album al1 
    ON t1.album_id = al1.album_id
JOIN album al2 
    ON t2.album_id = al2.album_id
JOIN artist ar1 
    ON al1.artist_id = ar1.artist_id
JOIN artist ar2 
    ON al2.artist_id = ar2.artist_id
WHERE ar1.artist_id < ar2.artist_id -- Ensures a consistent pair order
GROUP BY ar1.name, ar2.name
ORDER BY paired_purchase_count DESC;

SELECT
    al1.title AS album_1,
    al2.title AS album_2,
    COUNT(*) AS paired_purchase_count
FROM invoice_line i1
JOIN invoice_line i2
    ON i1.invoice_id = i2.invoice_id
   AND i1.invoice_line_id < i2.invoice_line_id -- Ensures unique pairing
JOIN track t1 
    ON i1.track_id = t1.track_id
JOIN track t2 
    ON i2.track_id = t2.track_id
JOIN album al1 
    ON t1.album_id = al1.album_id
JOIN album al2 
    ON t2.album_id = al2.album_id
WHERE al1.album_id < al2.album_id -- Ensures a consistent pair order
GROUP BY al1.title, al2.title
ORDER BY paired_purchase_count DESC;


-- 5.	Regional Market Analysis: Do customer purchasing behaviors and churn rates vary across different geographic regions or store locations? How might these correlate with local demographic or economic factors?
WITH cust_lastest_purchase_date AS 
(
	SELECT
		customer_id,
		billing_country,
		MAX(invoice_date) AS last_purchase_date
	FROM invoice
	GROUP BY customer_id, billing_country
),
churned_cust_count AS
(
	SELECT
		billing_country AS region,
		COUNT(DISTINCT customer_id) AS total_customer,
		SUM(CASE WHEN TIMESTAMPDIFF(MONTH, last_purchase_date, (SELECT MAX(invoice_date) FROM invoice)) >= 6 THEN 1 ELSE 0 END) AS churned_cust
	FROM cust_lastest_purchase_date
	GROUP BY billing_country
)

SELECT
	region,
    total_customer,
    churned_cust,
    ROUND(100.0 * churned_cust / total_customer, 2) AS churn_rate
FROM churned_cust_count
ORDER BY churn_rate DESC;

SELECT
    i.billing_country AS country,
    COUNT(DISTINCT i.invoice_id) AS total_transactions,
    COUNT(DISTINCT i.customer_id) AS total_customers,
    -- Calculates Average Sales per Customer (Total Revenue / Total Customers)
    ROUND(SUM(il.quantity * il.unit_price) / COUNT(DISTINCT i.customer_id), 2) AS avg_sales_per_customer,
    -- Calculates Average Basket Size (Total Items / Total Transactions)
    ROUND(COUNT(il.invoice_line_id) / COUNT(DISTINCT i.invoice_id), 2) AS avg_basket_size
FROM invoice i
JOIN invoice_line il 
    ON i.invoice_id = il.invoice_id
GROUP BY i.billing_country
ORDER BY country;

-- 6.	Customer Risk Profiling: Based on customer profiles (age, gender, location, purchase history), which customer segments are more likely to churn or pose a higher risk of reduced spending? What factors contribute to this risk?
-- Per customer purchasing behavior and risk segmentation
WITH cust_spending AS (
    SELECT
        i.customer_id,
        i.billing_country,
        SUM(i.total) AS total_spent,
        ROUND(SUM(i.total) / COUNT(DISTINCT i.invoice_id), 2) AS avg_spending,
        MAX(invoice_date) AS last_purchase_date
    FROM invoice i
    GROUP BY i.customer_id, i.billing_country
),
cust_frequency AS (
    SELECT
        customer_id,
        COUNT(DISTINCT invoice_id) AS frequency
    FROM invoice
    GROUP BY customer_id
),
basket_size_intr AS (
    SELECT
        i.customer_id,
        i.invoice_id,
        COUNT(il.invoice_line_id) AS basket_size
    FROM invoice i
    JOIN invoice_line il
        ON i.invoice_id = il.invoice_id
    GROUP BY i.customer_id, i.invoice_id
),
basket_size_agg AS (
    SELECT
        customer_id,
        ROUND(AVG(basket_size)) AS avg_basket_size
    FROM basket_size_intr
    GROUP BY customer_id
)
SELECT
    cs.customer_id,
    cs.billing_country,
    cs.total_spent,
    cs.avg_spending,
    cf.frequency AS total_purchases,
    ba.avg_basket_size,
    cs.last_purchase_date,
    TIMESTAMPDIFF(DAY, cs.last_purchase_date, (SELECT MAX(invoice_date) FROM invoice)) AS days_since_last_purchase,
    CASE
        -- Priority 1: High Risk (Recency - Inactive for 6+ months)
        WHEN TIMESTAMPDIFF(DAY, cs.last_purchase_date, (SELECT MAX(invoice_date) FROM invoice)) > 180 THEN 'High Risk (Inactive)'
        -- Priority 2: Medium Risk (Frequency - Low purchases)
        WHEN cf.frequency <= 2 THEN 'Medium Risk (Low Frequency)'
        -- Priority 3: Low Spender (Monetary - Below overall average)
        WHEN cs.total_spent < (SELECT AVG(total_spent) FROM cust_spending) THEN 'Low Spender'
        ELSE 'Low Risk'
    END AS risk_segment
FROM cust_spending cs
JOIN cust_frequency cf
    ON cs.customer_id = cf.customer_id
JOIN basket_size_agg ba
    ON cs.customer_id = ba.customer_id
ORDER BY risk_segment;

-- 7 Customer Lifetime Value Modeling: How can you leverage customer data (tenure, purchase history, engagement) to predict the lifetime value of different customer segments? This could inform targeted marketing and loyalty program strategies. Can you observe any common characteristics or purchase patterns among customers who have stopped purchasing?

WITH cust_spending AS (
    -- Calculate key monetary and recency metrics per customer
    SELECT
        i.customer_id,
        i.billing_country,
        SUM(i.total) AS total_spent,
        ROUND(SUM(i.total) / COUNT(DISTINCT i.invoice_id), 2) AS avg_spending,
        MAX(i.invoice_date) AS last_purchase_date
    FROM invoice i
    GROUP BY i.customer_id, i.billing_country
),
cust_frequency AS (
    -- Calculate total number of purchases (Frequency)
    SELECT
        customer_id,
        COUNT(DISTINCT invoice_id) AS total_purchases
    FROM invoice
    GROUP BY customer_id
),
basket_size_intr AS (
    -- Calculate basket size for each individual invoice
    SELECT
        i.customer_id,
        i.invoice_id,
        COUNT(il.invoice_line_id) AS basket_size
    FROM invoice i
    JOIN invoice_line il
        ON i.invoice_id = il.invoice_id
    GROUP BY i.customer_id, i.invoice_id
),
basket_size_agg AS (
    -- Calculate the average basket size per customer
    SELECT
        customer_id,
        ROUND(AVG(basket_size), 2) AS avg_basket_size
    FROM basket_size_intr
    GROUP BY customer_id
),
-- Final CLV table with calculated metrics and segmentation
customer_lifetime_value AS (
    SELECT
        cs.customer_id,
        cs.billing_country,
        cs.total_spent,
        cs.avg_spending,
        cf.total_purchases,
        ba.avg_basket_size,
        cs.last_purchase_date,
        -- Recency: Days since last purchase
        TIMESTAMPDIFF(DAY, cs.last_purchase_date, (SELECT MAX(invoice_date) FROM invoice)) AS days_since_last_purchase,
        
        -- 1. Simple Value Segmentation (CLV Proxy)
        CASE
            WHEN cs.avg_spending < 3 AND cs.total_spent < 60 THEN 'Low Value'
            WHEN cs.avg_spending < 6 AND cs.total_spent < 80 THEN 'Medium Value'
            ELSE 'High Value'
        END AS customer_segment,
        
        -- 2. Activity Flag (Churn Indicator)
        CASE
            WHEN TIMESTAMPDIFF(DAY, cs.last_purchase_date, (SELECT MAX(invoice_date) FROM invoice)) <= 180 THEN 'Active'
            ELSE 'Inactive'
        END AS activity_status,
        
        -- 3. Basket Behavior (Purchase Depth)
        CASE
            WHEN ba.avg_basket_size < 2 THEN 'Small Basket Shopper'
            WHEN ba.avg_basket_size BETWEEN 2 AND 5 THEN 'Medium Basket Shopper'
            ELSE 'Bulk Buyer'
        END AS basket_behavior,
        
        -- 4. Frequency Tag (Commitment Level)
        CASE
            WHEN cf.total_purchases < 8 THEN 'Not Frequent Buyer'
            ELSE 'Frequent Buyer'
        END AS purchase_behavior
    FROM cust_spending cs
    JOIN cust_frequency cf 
        ON cs.customer_id = cf.customer_id
    JOIN basket_size_agg ba 
        ON cs.customer_id = ba.customer_id
)
SELECT * FROM customer_lifetime_value
ORDER BY total_spent DESC;

-- 10 How can you alter the "Albums" table to add a new column named "ReleaseYear" of type INTEGER to store the release year of each album?

ALTER TABLE album
ADD COLUMN ReleaseYear INT(4);


-- 11 Chinook is interested in understanding the purchasing behavior of customers based on their geographical location. They want to know the average total amount spent by customers from each country, along with the number of customers and the average number of tracks purchased per customer. Write an SQL query to provide this information.
WITH CustomerMetrics AS (
    -- Step 1: Calculate Total Tracks and Total Spending per individual Customer
    SELECT
        i.customer_id,
        i.billing_country,
        -- Total spent is calculated by summing the line item prices (robust)
        SUM(il.unit_price * il.quantity) AS total_spent_by_customer,
        -- Total number of tracks purchased by the customer
        COUNT(il.track_id) AS total_tracks_purchased
    FROM invoice i
    JOIN invoice_line il
        ON i.invoice_id = il.invoice_id
    GROUP BY i.customer_id, i.billing_country
)

-- Step 2: Aggregate the customer metrics by Country
SELECT
    billing_country,
    COUNT(customer_id) AS number_of_customers,
    -- Average of the 'total_spent_by_customer' for all customers in that country
    AVG(total_spent_by_customer) AS average_total_amount_spent,
    -- Average of the 'total_tracks_purchased' for all customers in that country
    AVG(total_tracks_purchased) AS average_tracks_purchased_per_customer
FROM CustomerMetrics
GROUP BY billing_country
ORDER BY number_of_customers DESC, average_total_amount_spent DESC;
 