/* =========================================================
PROJECT: Coffee Market Expansion Analytics
AUTHOR: Shivam Kumar
DIALECT: MySQL 8.0+
PURPOSE: Advanced retail expansion analysis
========================================================= */

/* =========================================================
SECTION 0: DATA GOVERNANCE SCORECARD
========================================================= */
-- Q0.1: Duplicate Checks
SELECT sale_id, COUNT(*) AS duplicate_count
FROM sales
GROUP BY sale_id
HAVING COUNT(*) > 1;

-- Q0.2: Completeness Checks
SELECT COUNT(*) AS null_revenue_rows
FROM sales
WHERE total IS NULL;


/* =========================================================
STAGING LAYER: DECLARATIVE VIEWS
Replaces legacy TEMPORARY TABLE architecture for dbt-style modeling
========================================================= */

CREATE OR REPLACE VIEW vw_sales_enriched AS
SELECT 
    s.sale_id, 
    s.sale_date, 
    s.customer_id, 
    c.customer_name, 
    c.city_id, 
    ci.city_name, 
    ci.population,
    ci.estimated_rent,
    s.product_id, 
    p.product_name, 
    p.price,
    s.total, 
    s.rating
FROM sales s
JOIN customers c ON s.customer_id = c.customer_id
JOIN city ci ON c.city_id = ci.city_id
JOIN products p ON s.product_id = p.product_id;


/* =========================================================
SECTION 1: CITY OPERATING BENCHMARK
========================================================= */
-- Q1. What is the baseline operating performance and scale of each city?
CREATE OR REPLACE VIEW vw_city_baseline AS
SELECT 
    city_id, 
    city_name, 
    population, 
    estimated_rent,
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(total) AS total_revenue,
    AVG(total) AS avg_order_value,
    SUM(total) / NULLIF(COUNT(DISTINCT customer_id), 0) AS revenue_per_customer,
    SUM(total) / NULLIF(estimated_rent, 0) AS revenue_to_rent_ratio,
    AVG(rating) AS avg_rating,
    (COUNT(DISTINCT customer_id) / NULLIF(population, 0)) * 100000 AS customers_per_100k_residents
FROM vw_sales_enriched
GROUP BY city_id, city_name, population, estimated_rent;


/* =========================================================
SECTION 2: MOMENTUM DIAGNOSTICS
========================================================= */
-- Q2. How is revenue momentum changing in the most recent 3 months versus the prior 3 months?
CREATE OR REPLACE VIEW vw_city_momentum AS
WITH max_date AS (
    SELECT MAX(sale_date) AS md FROM vw_sales_enriched
),
city_sales AS (
    SELECT 
        city_id,
        SUM(CASE WHEN sale_date BETWEEN DATE_SUB((SELECT md FROM max_date), INTERVAL 3 MONTH) AND (SELECT md FROM max_date) THEN total ELSE 0 END) AS recent_3m_revenue,
        SUM(CASE WHEN sale_date BETWEEN DATE_SUB((SELECT md FROM max_date), INTERVAL 6 MONTH) AND DATE_SUB((SELECT md FROM max_date), INTERVAL 3 MONTH) THEN total ELSE 0 END) AS prior_3m_revenue
    FROM vw_sales_enriched
    GROUP BY city_id
)
SELECT 
    city_id, 
    recent_3m_revenue, 
    prior_3m_revenue,
    ROUND(100.0 * (recent_3m_revenue - prior_3m_revenue) / NULLIF(prior_3m_revenue, 0), 2) AS recent_vs_prior_3m_revenue_pct
FROM city_sales;


/* =========================================================
SECTION 3: COHORT RETENTION
========================================================= */
-- Q3. What is the 30-day (M1) retention rate for new customers in each city?
CREATE OR REPLACE VIEW vw_city_retention AS
WITH first_purchase AS (
    SELECT customer_id, city_id, MIN(sale_date) AS first_date 
    FROM vw_sales_enriched 
    GROUP BY customer_id, city_id
),
retention_flags AS (
    SELECT 
        f.customer_id, 
        f.city_id,
        MAX(CASE WHEN s.sale_date BETWEEN DATE_ADD(f.first_date, INTERVAL 1 DAY) AND DATE_ADD(f.first_date, INTERVAL 30 DAY) THEN 1 ELSE 0 END) AS m1_retained
    FROM first_purchase f
    LEFT JOIN vw_sales_enriched s ON f.customer_id = s.customer_id AND s.sale_date > f.first_date
    GROUP BY f.customer_id, f.city_id
)
SELECT 
    city_id, 
    ROUND(100.0 * SUM(m1_retained) / COUNT(customer_id), 2) AS m1_retention_pct
FROM retention_flags
GROUP BY city_id;


/* =========================================================
SECTION 4: RFM CUSTOMER QUALITY
========================================================= */
-- Q4. What percentage of each city's customer base are high-value 'Champions' versus 'At Risk'?
CREATE OR REPLACE VIEW vw_city_rfm_mix AS
WITH customer_rfm AS (
    SELECT 
        customer_id, 
        city_id,
        DATEDIFF((SELECT MAX(sale_date) FROM vw_sales_enriched), MAX(sale_date)) AS recency,
        COUNT(DISTINCT sale_id) AS frequency,
        SUM(total) AS monetary
    FROM vw_sales_enriched
    GROUP BY customer_id, city_id
),
scored AS (
    SELECT 
        customer_id, 
        city_id,
        NTILE(5) OVER(ORDER BY recency DESC) AS r_score,
        NTILE(5) OVER(ORDER BY frequency ASC) AS f_score,
        NTILE(5) OVER(ORDER BY monetary ASC) AS m_score
    FROM customer_rfm
),
segments AS (
    SELECT 
        customer_id, 
        city_id,
        CASE 
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champion'
            WHEN r_score <= 2 AND m_score >= 4 THEN 'At Risk'
            ELSE 'Other' 
        END AS segment
    FROM scored
)
SELECT 
    city_id,
    ROUND(100.0 * SUM(CASE WHEN segment = 'Champion' THEN 1 ELSE 0 END) / COUNT(customer_id), 2) AS champion_pct,
    ROUND(100.0 * SUM(CASE WHEN segment = 'At Risk' THEN 1 ELSE 0 END) / COUNT(customer_id), 2) AS at_risk_pct
FROM segments
GROUP BY city_id;


/* =========================================================
SECTION 5: REVENUE CONCENTRATION RISK
========================================================= */
-- Q5. How reliant is each city on its top 10% of customers (Revenue Concentration Risk)?
CREATE OR REPLACE VIEW vw_city_concentration AS
WITH cust_rev AS (
    SELECT customer_id, city_id, SUM(total) AS rev 
    FROM vw_sales_enriched 
    GROUP BY customer_id, city_id
),
ranked AS (
    SELECT 
        customer_id, 
        city_id, 
        rev,
        PERCENT_RANK() OVER(PARTITION BY city_id ORDER BY rev ASC) AS pct_rank
    FROM cust_rev
)
SELECT 
    city_id,
    ROUND(100.0 * SUM(CASE WHEN pct_rank >= 0.90 THEN rev ELSE 0 END) / SUM(rev), 2) AS top_10pct_customer_revenue_share
FROM ranked
GROUP BY city_id;


/* =========================================================
SECTION 6: PORTFOLIO MIX
========================================================= */
-- Q6. What percentage of total revenue comes from premium or high-margin products in each city?
CREATE OR REPLACE VIEW vw_city_portfolio AS
SELECT 
    city_id,
    ROUND(100.0 * SUM(CASE WHEN product_name LIKE '%Premium%' OR product_name LIKE '%Beans%' THEN total ELSE 0 END) / SUM(total), 2) AS premium_mix_pct
FROM vw_sales_enriched
GROUP BY city_id;


/* =========================================================
SECTION 7: HERO SKU RANKING
========================================================= */
-- Q7. What are the top-selling hero products (SKUs) in each city?
SELECT 
    city_name, 
    product_name, 
    SUM(total) AS revenue,
    RANK() OVER(PARTITION BY city_name ORDER BY SUM(total) DESC) AS sku_rank
FROM vw_sales_enriched
GROUP BY city_name, product_name
ORDER BY city_name, sku_rank;


/* =========================================================
SECTION 8: MARKET BASKET AFFINITY
========================================================= */
-- Q8. Which product pairs are most frequently purchased together across all transactions?
WITH basket_pairs AS (
    SELECT 
        a.product_name AS product_1, 
        b.product_name AS product_2, 
        COUNT(DISTINCT a.sale_id) AS times_bought_together
    FROM vw_sales_enriched a
    JOIN vw_sales_enriched b 
      ON a.sale_id = b.sale_id 
     AND a.product_id < b.product_id
    GROUP BY a.product_name, b.product_name
)
SELECT * FROM basket_pairs 
ORDER BY times_bought_together DESC 
LIMIT 20;


/* =========================================================
SECTION 9: WHITESPACE SCAN
========================================================= */
-- Q9. Which massive population centers have the lowest current market penetration (Whitespace)?
SELECT 
    city_name, 
    population, 
    total_customers,
    customers_per_100k_residents
FROM vw_city_baseline
WHERE population > 10000000 AND customers_per_100k_residents < 0.50
ORDER BY population DESC;

-- Insight: Large population centers with low current penetration represent prime digital-first whitespace before fixed capex.


/* =========================================================
SECTION 13: EXECUTIVE EXPANSION SCORECARD
Outputs the raw normalized scores (Percentiles) so the business
can dynamically weight demand, retention, economics, and risk 
in a BI environment.
========================================================= */
-- Q13. What is the final, normalized expansion scorecard ranking cities by weighted potential and risk?
WITH base_scores AS (
    SELECT
        cb.city_id,
        cb.city_name,
        cb.total_revenue,
        cb.total_customers,
        cb.avg_order_value,
        cb.revenue_per_customer,
        cb.revenue_to_rent_ratio,
        cb.avg_rating,
        cb.customers_per_100k_residents,
        COALESCE(cr.m1_retention_pct, 0) AS m1_retention_pct,
        COALESCE(rfm.champion_pct, 0) AS champion_pct,
        COALESCE(rfm.at_risk_pct, 0) AS at_risk_pct,
        COALESCE(cc.top_10pct_customer_revenue_share, 100) AS top_10pct_customer_revenue_share,
        COALESCE(cm.recent_vs_prior_3m_revenue_pct, -100) AS recent_vs_prior_3m_revenue_pct,
        COALESCE(cp.premium_mix_pct, 0) AS premium_mix_pct,
        cb.population,
        ROUND(PERCENT_RANK() OVER (ORDER BY cb.total_revenue) * 100, 2) AS total_revenue_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY cb.revenue_to_rent_ratio) * 100, 2) AS revenue_to_rent_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY COALESCE(cr.m1_retention_pct, 0)) * 100, 2) AS retention_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY COALESCE(rfm.champion_pct, 0)) * 100, 2) AS champion_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY cb.avg_rating) * 100, 2) AS rating_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY cb.population) * 100, 2) AS population_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY COALESCE(cm.recent_vs_prior_3m_revenue_pct, -100)) * 100, 2) AS momentum_score,
        ROUND(PERCENT_RANK() OVER (ORDER BY COALESCE(cp.premium_mix_pct, 0)) * 100, 2) AS premium_score,
        ROUND((1 - PERCENT_RANK() OVER (ORDER BY COALESCE(cc.top_10pct_customer_revenue_share, 100))) * 100, 2) AS concentration_score,
        ROUND((1 - PERCENT_RANK() OVER (ORDER BY COALESCE(rfm.at_risk_pct, 100))) * 100, 2) AS risk_score
    FROM vw_city_baseline cb
    LEFT JOIN vw_city_retention cr
        ON cb.city_id = cr.city_id
    LEFT JOIN vw_city_rfm_mix rfm
        ON cb.city_id = rfm.city_id
    LEFT JOIN vw_city_concentration cc
        ON cb.city_id = cc.city_id
    LEFT JOIN vw_city_momentum cm
        ON cb.city_id = cm.city_id
    LEFT JOIN vw_city_portfolio cp
        ON cb.city_id = cp.city_id
)
SELECT
    city_name,
    total_revenue,
    revenue_to_rent_ratio,
    m1_retention_pct,
    recent_vs_prior_3m_revenue_pct,
    -- Outputting raw percentile scores for BI parameterization instead of hardcoded SQL weights
    total_revenue_score,
    revenue_to_rent_score,
    retention_score,
    champion_score,
    momentum_score,
    concentration_score,
    risk_score,
    -- Simple baseline average of scores for quick sorting
    ROUND((total_revenue_score + revenue_to_rent_score + retention_score + champion_score + momentum_score + concentration_score) / 6.0, 2) AS unweighted_baseline_score
FROM base_scores
ORDER BY unweighted_baseline_score DESC;

-- Insight: Chennai, Pune, and Bangalore consistently rank in the top quartile across multiple unweighted dimensions.
-- Recommendation: Export these percentiles into a BI tool where stakeholders can adjust the weights dynamically (e.g., heavily weighting retention vs. scale).


/* =========================================================
SECTION 10: TIME-TO-SECOND-PURCHASE (T2SP)
========================================================= */
-- Q10. For repeat customers, how many days does it take to make their second purchase?
CREATE OR REPLACE VIEW vw_time_to_second_purchase AS
WITH customer_orders AS (
    SELECT 
        customer_id, 
        sale_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY sale_date ASC) as purchase_sequence
    FROM sales
),
first_two_purchases AS (
    SELECT 
        customer_id,
        MAX(CASE WHEN purchase_sequence = 1 THEN sale_date END) AS first_purchase_date,
        MAX(CASE WHEN purchase_sequence = 2 THEN sale_date END) AS second_purchase_date
    FROM customer_orders
    GROUP BY customer_id
    HAVING MAX(CASE WHEN purchase_sequence = 2 THEN sale_date END) IS NOT NULL
),
t2sp_calc AS (
    SELECT 
        customer_id,
        DATEDIFF(second_purchase_date, first_purchase_date) AS days_to_second_purchase
    FROM first_two_purchases
)
SELECT 
    CASE 
        WHEN days_to_second_purchase <= 7 THEN '1. Within 1 Week'
        WHEN days_to_second_purchase <= 14 THEN '2. 1-2 Weeks'
        WHEN days_to_second_purchase <= 30 THEN '3. 2-4 Weeks'
        ELSE '4. 30+ Days'
    END AS t2sp_bucket,
    COUNT(customer_id) AS customer_count,
    ROUND(100.0 * COUNT(customer_id) / SUM(COUNT(customer_id)) OVER(), 2) AS pct_of_repeat_customers
FROM t2sp_calc
GROUP BY t2sp_bucket
ORDER BY t2sp_bucket;

-- Insight: Knowing the exact window in which most customers return for their second purchase helps optimize the timing of automated retention emails.


/* =========================================================
SECTION 11: MARKET BASKET ATTACHMENT RATE
========================================================= */
-- Q11. When a customer buys a "hero" item, what percentage of the time do they attach a secondary item?
CREATE OR REPLACE VIEW vw_attachment_rate AS
WITH basket_totals AS (
    SELECT product_name, COUNT(DISTINCT sale_id) as total_sales
    FROM vw_sales_enriched
    GROUP BY product_name
),
basket_pairs AS (
    SELECT 
        a.product_name AS primary_product, 
        b.product_name AS secondary_product, 
        COUNT(DISTINCT a.sale_id) AS times_bought_together
    FROM vw_sales_enriched a
    JOIN vw_sales_enriched b 
      ON a.sale_id = b.sale_id 
     AND a.product_id != b.product_id
    GROUP BY a.product_name, b.product_name
)
SELECT 
    bp.primary_product,
    bp.secondary_product,
    bp.times_bought_together,
    bt.total_sales AS primary_product_total_sales,
    ROUND(100.0 * bp.times_bought_together / bt.total_sales, 2) AS attachment_rate_pct
FROM basket_pairs bp
JOIN basket_totals bt ON bp.primary_product = bt.product_name
ORDER BY bt.total_sales DESC, attachment_rate_pct DESC
LIMIT 20;

-- Insight: Attachment rate is more actionable than raw co-occurrence. It tells us exactly how effective a primary product is at driving sales for a secondary product.


/* =========================================================
SECTION 12: LIFETIME VALUE (CLTV) DECILES
========================================================= */
-- Q12. How does the average lifetime value differ between the top 10% of customers and the bottom 50%?
CREATE OR REPLACE VIEW vw_cltv_deciles AS
WITH customer_cltv AS (
    SELECT 
        customer_id, 
        city_name,
        SUM(total) AS lifetime_value
    FROM vw_sales_enriched
    GROUP BY customer_id, city_name
),
deciled_customers AS (
    SELECT 
        customer_id,
        city_name,
        lifetime_value,
        NTILE(10) OVER (ORDER BY lifetime_value DESC) as cltv_decile
    FROM customer_cltv
)
SELECT 
    cltv_decile,
    COUNT(customer_id) AS customers_in_decile,
    ROUND(MIN(lifetime_value), 2) AS min_value,
    ROUND(MAX(lifetime_value), 2) AS max_value,
    ROUND(AVG(lifetime_value), 2) AS avg_cltv,
    ROUND(SUM(lifetime_value), 2) AS total_segment_revenue,
    ROUND(100.0 * SUM(lifetime_value) / SUM(SUM(lifetime_value)) OVER(), 2) AS revenue_share_pct
FROM deciled_customers
GROUP BY cltv_decile
ORDER BY cltv_decile ASC;

-- Insight: Highlights how heavily skewed customer value is. The top 10% (Decile 1) often generates vastly more revenue than the bottom 50% combined.


/* ======================================================
FINAL BUSINESS INSIGHTS
========================================================= */

-- Bangalore and Chennai are the strongest near-term expansion
-- candidates because they combine scale, retention, customer quality, and healthier
-- expansion economics. Pune remains a high-value market, but its recent slowdown means
-- the priority is to defend demand before accelerating new capex. Mumbai and Delhi are
-- still strategically attractive whitespace cities, but current penetration is too shallow
-- to justify aggressive fixed-cost rollout without more demand proof. Lower-stickiness
-- cities such as Surat, Indore, and Nagpur should be improved operationally before store
-- expansion is considered. Across markets, the launch assortment should remain coffee-led,
-- anchored by hero SKUs such as Cold Brew Coffee Pack and Coffee Beans, with premium
-- products layered in where customer quality and mix support higher ticket sizes.
-- Furthermore, targeting first-time buyers within the peak "Time-to-Second-Purchase" window 
-- (typically 1-2 weeks) and leveraging high Attachment Rates (e.g., pairing pastries with 
-- Cold Brew) will be critical levers to maximize Customer Lifetime Value, especially among 
-- the top 10% of customers who drive the vast majority of network revenue.

/* ======================================================
FINAL RECOMMENDATION
========================================================= */

-- Follow a phased expansion strategy: expand first in Bangalore and
-- Chennai, defend and re-accelerate Pune, build Mumbai and Delhi through digital-first
-- and low-capex channels, and delay store investment in weaker repeat markets until
-- retention and customer depth improve. This keeps capital allocation disciplined and
-- aligns expansion with proven demand quality, not population alone. Optimize product 
-- bundles using empirically proven attachment rates, and deploy automated marketing sequences 
-- timed specifically to the median Time-to-Second-Purchase.
