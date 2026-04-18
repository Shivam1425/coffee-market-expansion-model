# Coffee Market Expansion Analytics

**Author:** Shivam Kumar  
**SQL Dialect:** MySQL 8.0+

## What This Project Is About

I had a dataset for a coffee brand selling across 14 Indian cities and the obvious question was "which city should we expand into next?" The easy answer is to rank cities by total revenue and pick the top one. But that felt lazy — a city can have high revenue and still be a bad expansion bet if its customers aren't coming back, or if the rent is eating all the margin.

So I built a scoring engine in SQL that evaluates cities across multiple dimensions at once — retention, unit economics, customer quality, and market penetration — and outputs a single ranked decision with an action label for each city.

## The Dataset

- **Timeframe:** Jan 2023 – Oct 2024
- **Scale:** 14 cities, 10,388 transactions, 497 unique customers
- **Total Revenue Analyzed:** ₹6.07M

## What I Found

1. **Chennai, not Pune, is the best expansion bet.** Pune has the highest revenue (₹1.26M) but its momentum is cooling — revenue dropped 22.42% in the last 3 months vs the prior 3 months. Chennai has ₹944k in revenue, the best avg rating in the dataset (4.52), 66.67% M1 retention, and is still growing.

2. **Mumbai and Delhi look attractive on paper but aren't ready for stores.** The population is massive but current customer penetration is very shallow (under 0.50 customers per 100k residents). A store rollout there before building digital demand would be capital destruction.

3. **The top 10% of customers drive a disproportionate share of revenue in every city.** Protecting these people should be the first priority — not acquisition.

4. **The second-purchase window is very specific.** Most repeat customers come back within 1–2 weeks of their first purchase. If you're not running a re-engagement campaign in that window, you're losing them.

**Sample output — Final Expansion Scorecard (Section 13):**

| city_name | total_revenue | m1_retention_pct | revenue_to_rent_ratio | expansion_decision |
|---|---|---|---|---|
| Chennai | 944,000 | 66.67% | 61.3x | Scale Now |
| Bangalore | 876,000 | 58.33% | 47.8x | Scale Now |
| Pune | 1,260,000 | 80.77% | 82.24x | Defend & Deepen |
| Mumbai | 1,180,000 | 41.25% | 35.2x | Digital-First Build |
| Delhi | 990,000 | 38.40% | 29.7x | Digital-First Build |
| Surat | 312,000 | 27.10% | 18.4x | Fix Before Investing |

Pune has the highest revenue and best rent efficiency but its momentum is cooling (−22.42% recent 3-month trend), so it drops to "Defend & Deepen" instead of "Scale Now". Chennai edges it out for top expansion priority.

## What I Actually Built in SQL

The core of the project is a layered view architecture. Instead of writing one massive query, I built each analytical layer as a `CREATE OR REPLACE VIEW` so they can be queried independently:

- `vw_sales_enriched` — joins all four tables into one clean row-level view
- `vw_city_baseline` — per-city revenue, AOV, rent ratio, customer density
- `vw_city_momentum` — recent 3-month vs prior 3-month revenue trend
- `vw_city_retention` — M1 cohort retention rate per city
- `vw_city_rfm_mix` — what % of each city's customers are Champions vs At Risk
- `vw_city_concentration` — how much revenue comes from just the top 10% of customers

The final query (Section 13) joins all these views together, runs `PERCENT_RANK()` across every dimension, applies a weighted formula, and outputs a ranked scorecard with expansion labels.

## Technical Things Worth Noting

**Why views and not temp tables?** Views keep each step readable and independently queryable. The tradeoff is that complex views like `vw_city_rfm_mix` recalculate everything from scratch on each query call. On this 10k-row dataset it's fine — but at production scale I'd schedule these as nightly materialized tables using something like dbt.

**The market basket self-join (Section 8)** was the trickiest query to get right. To find product pairs bought together in the same customer-day basket, I had to join the basket table to itself with `a.product_id < b.product_id`. That condition is what prevents (A→B) and (B→A) from showing up as two separate pairs. Without it, every pair was double-counted.

**The RFM scoring direction:** I scored recency as `6 - NTILE(5) OVER (ORDER BY recency ASC)` — the inversion is important. Lower recency days = more recent buyer = higher score. I got this backwards on my first attempt and Champions were all showing up as At Risk.

## Challenges

The hardest part was building the final expansion scorecard. I had 14 cities and 8–10 metrics per city, and I needed to make them comparable. Raw values don't work because revenue is in millions and retention is in percentages — you can't add those. The solution was `PERCENT_RANK()` — converting everything into a 0–100 percentile score first, then applying weights. Once I understood that, the rest fell into place.

## How to Run

1. Run `schema.sql` in MySQL 8.0+ to create the tables and indexes.
2. Load the CSV files from `data/` into the tables.
3. Run `analysis.sql` top to bottom. The views build first, then the analysis queries use them.
