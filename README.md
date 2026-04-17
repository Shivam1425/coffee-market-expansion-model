# Coffee Market Expansion Analytics

**Author:** Shivam Kumar  
**SQL Dialect:** MySQL 8.0+  

## Overview

This project analyzes the sales performance of a coffee retail brand across 14 cities to determine the optimal strategy for market expansion. Rather than simply ranking cities by total revenue, I built a SQL-based decision engine that evaluates multiple dimensions: retention rates, unit economics (revenue-to-rent ratio), customer quality (RFM segmentation), and whitespace potential (population vs. penetration). 

The goal was to mathematically separate markets into four strategic tiers: **scale now**, **defend and deepen**, **develop digitally first**, or **fix before investing**.

## The Data
- **Timeframe:** Jan 2023 - Oct 2024
- **Scale:** 14 Cities, 10,388 transactions, 497 unique customers
- **Total Revenue Analyzed:** 6.07M

## Key Analytical Findings

1. **Chennai is the most balanced expansion market.** It leads the final expansion score with strong revenue (`944k`), high M1 retention (`66.67%`), the best average rating (`4.52`), and a very strong champion-customer mix.
2. **Pune is the monetization leader, but it is cooling.** It delivers the highest revenue (`1.26M`), best rent efficiency (`82.24x` revenue-to-rent), and strongest M1 retention (`80.77%`), but recent 3-month revenue is down `22.42%` versus the prior 3 months.
3. **Mumbai and Delhi are whitespace markets, not immediate store bets.** Their addressable population is huge, but current customer penetration is still shallow. They are better suited to digital-first demand building before an aggressive fixed-cost rollout.
4. **The Pareto principle is alive and well.** The top 10% of customers (Decile 1) generate a vastly disproportionate share of lifetime revenue.
5. **The critical retention window is highly specific.** Time-to-Second-Purchase (T2SP) analysis shows exactly when first-time buyers are most likely to return, providing a precise timeline for automated lifecycle marketing.

## SQL Techniques Used

- **CTEs & Declarative Views:** Used `CREATE OR REPLACE VIEW` to build a clean, modular semantic layer instead of relying on messy temporary tables.
- **Window Functions:** Heavy use of `ROW_NUMBER()`, `DENSE_RANK()`, `NTILE()`, and `PERCENT_RANK()` for cohort analysis and RFM deciling.
- **Cross Joins & Self Joins:** Executed market basket analysis to calculate Support, Confidence, and Lift for product pairs.

## Engineering & Performance Optimization Notes

While writing this analysis, I made specific design choices to balance analytical depth with database performance:

*   **Dynamic Views vs. Materialized Tables:** In this repository, the analysis relies heavily on `CREATE OR REPLACE VIEW`. While this is great for keeping the code modular, views like `vw_city_momentum` and `vw_city_rfm_mix` recalculate complex aggregations and Window Functions on the fly. In a real-world production environment with millions of rows, I would use an ETL tool (like `dbt`) to materialize these views into physical tables on a nightly schedule to avoid crushing the database compute resources.
*   **Indexing Strategy:** To ensure the self-joins required for the Market Basket Affinity (Section 8) run efficiently, the `schema.sql` file includes composite indexes on `(customer_id, sale_date, product_id)`. Without these indexes, the `CROSS JOIN` operations would result in full table scans and exponential query times.

## BI Dashboard Integration (Semantic Layer)

This SQL script is designed to act as the **Semantic Layer** for a BI tool like Power BI or Tableau. 
Instead of forcing Power BI to perform complex DAX calculations for RFM segmentation or Cohort Retention, the heavy lifting is done in the database. The final view, `vw_city_baseline`, acts as a pre-aggregated Fact Table that can be imported directly into Power BI, ensuring the dashboard loads instantly and the business logic remains version-controlled in SQL.

## How to Run

1. Execute `schema.sql` in MySQL 8.0+ to build the tables and indexes.
2. Load the source CSV files from the `data/` folder into the tables.
3. Run `analysis.sql` from top to bottom.
