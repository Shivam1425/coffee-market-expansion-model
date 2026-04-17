# Coffee Market Expansion Analytics

**Author:** Shivam Kumar  
**SQL Dialect:** MySQL 8.0+  
**Project Type:** Advanced retail expansion analysis

## Executive Summary

This project evaluates where a coffee retail brand should scale next by combining demand, retention, customer quality, unit economics, portfolio mix, whitespace potential, and concentration risk. Instead of ranking markets on revenue alone, the SQL package builds a city-by-city decision engine that separates markets to **scale now**, **defend and deepen**, **develop digitally first**, or **fix before investing**.

## Dataset Snapshot

- Date range: `2023-01-01` to `2024-10-01`
- Cities: `14`
- Customers: `497`
- Products: `28`
- Sales rows: `10,388`
- Total revenue: `6.07M`
- Average order value: `584.35`
- Average rating: `3.99`
- Data quality status: `0` duplicate sale IDs, `0` price mismatches, `0` missing ratings

## What The Analysis Solves

The SQL file answers five executive questions:

1. Which cities are genuinely expansion-ready once we account for retention, economics, and customer quality?
2. Which markets look large on population but are still too shallow or too fragile for store capex?
3. Which products and portfolio mixes should anchor new-city launches?
4. Where is the business overexposed to narrow customer groups or softening momentum?
5. What is the optimal Time-to-Second-Purchase (T2SP) window, and how skewed is the Customer Lifetime Value (CLTV) distribution?

## Headline Findings

- **Chennai is the most balanced expansion market.** It leads the final expansion score with strong revenue (`944k`), high M1 retention (`66.67%`), the best average rating (`4.52`), and a very strong champion-customer mix.
- **Pune is the monetization leader, but it is cooling.** It delivers the highest revenue (`1.26M`), best rent efficiency (`82.24x` revenue-to-rent), and strongest M1 retention (`80.77%`), but recent 3-month revenue is down `22.42%` versus the prior 3 months.
- **Bangalore is the cleanest scale-now profile.** It combines `860k` revenue, a `66.67%` champion share, premium-friendly demand, and positive recent momentum (`+10.35%`).
- **Mumbai and Delhi are whitespace markets, not immediate store bets.** Their addressable population is huge, but current customer penetration is still shallow, so they are better suited to digital-first demand building before aggressive fixed-cost rollout.
- **The business is decisively coffee-led.** Core coffee contributes `56.87%` of total revenue, followed by merch and gifts (`20.82%`). Cold Brew Coffee Pack is the top SKU overall at `1.19M`.
- **Hero products are stable, but organic bundling is weak.** Cold Brew Coffee Pack and Coffee Beans dominate city-level product rankings, yet most same-day basket pairs show low support and lift below `1.0`. Bundles should be designed and tested, not assumed.
- **The Pareto principle is alive and well.** The top 10% of customers (Decile 1) generate a vastly disproportionate share of lifetime revenue, making targeted VIP retention far more lucrative than blanket discounting.
- **The critical retention window is highly specific.** Time-to-Second-Purchase (T2SP) analysis shows exactly when first-time buyers are most likely to return, providing a precise timeline for automated lifecycle marketing.

## Analytical Framework In `analysis.sql`

The project is organized as a reusable MySQL 8 analysis package using declarative views (`CREATE OR REPLACE VIEW`):

- **Staging layer:** Clean, enriched views for sales, customer metrics, and city-month metrics
- **Data governance scorecard:** Duplicate, mismatch, and completeness checks
- **City operating benchmark:** Revenue, AOV, customer productivity, rating, penetration, and rent efficiency
- **Momentum diagnostics:** Recent 3-month vs prior 3-month revenue and customer movement
- **Cohort retention:** M1, M2, and M3 retention by city
- **RFM customer quality:** Champion, loyal, developing, and at-risk high-value mix by market
- **Revenue concentration risk:** Dependence on top 10% and 20% of customers
- **Portfolio mix:** Core coffee, premiumization, subscription, merch, and equipment mix
- **Hero SKU ranking:** Top products by city for launch assortment planning
- **Market basket affinity:** Support, confidence, and lift using same-customer same-day baskets
- **Whitespace scan:** High-population, low-penetration markets for digital-first development
- **Time-to-Second-Purchase (T2SP):** Cohort tracking for behavioral retention windows
- **Market Basket Attachment Rate:** Quantifying the exact pull of "hero" items on secondary SKUs
- **Lifetime Value (CLTV) Deciles:** Statistical bucketing to prove the Pareto distribution
- **Executive scorecard:** Default weighted expansion model with recommended action labels, plus BI-ready percentile diagnostics

## Strategic Recommendation

The current data supports a three-tier expansion view:

- **Scale now:** `Bangalore`
- **Scale, but arrest softening:** `Chennai`, `Pune`
- **Digital-first whitespace build:** `Mumbai`, `Delhi`
- **Fix retention before capex:** weaker repeat markets such as `Surat`, `Indore`, `Nagpur`, and similar low-stickiness cities

The practical rollout implication is simple: use Chennai and Bangalore as the operating blueprint, protect Pune before momentum weakens further, and treat Mumbai and Delhi as funnel-development markets until penetration and unit economics improve.

## Sample Output: City Expansion Scorecard

| city_name | total_revenue | m1_retention_pct | revenue_to_rent_ratio | expansion_decision |
|-----------|---------------|------------------|-----------------------|--------------------|
| Chennai   | 944,000       | 66.67%           | 45.2x                 | Scale Now          |
| Pune      | 1,260,000     | 80.77%           | 82.2x                 | Defend & Deepen    |

## Files

- `analysis.sql` - Advanced MySQL 8 analytical workflow
- `schema.sql` - MySQL 8 schema and indexing strategy
- `data/` - Source CSV files
- `assets/` - ERD and schema visuals

## SQL Skills Demonstrated

- CTE pipelines
- Declarative Views (`CREATE OR REPLACE VIEW`)
- Window functions: `ROW_NUMBER()`, `DENSE_RANK()`, `NTILE()`, `PERCENT_RANK()`
- Cohort and retention analysis
- RFM segmentation
- Concentration and portfolio diagnostics
- Composite market scoring
- Decision-ready business commentary in SQL

## How To Use

1. Create the schema from `schema.sql` in MySQL 8.0+.
2. Load the CSV files into the matching tables.
3. Run `analysis.sql` top to bottom. It uses declarative views, so downstream queries can reference upstream logic dynamically.
4. Use the final executive scorecard as the presentation-ready market recommendation output.
