# Genie space: "Sahra Retail Analyst"

Genie lets business users ask questions in plain English. It writes SQL against the Gold tables you give it.

## 1. Tables to add
- `workspace.sahra_gold.sales_enriched`
- `workspace.sahra_gold.daily_store_sales`
- `workspace.sahra_gold.monthly_category_sales`
- `workspace.sahra_gold.product_performance`
- `workspace.sahra_gold.customer_rfm`
- `workspace.sahra_gold.dim_date`

## 2. General instructions (paste into the Instructions box)
```
You are the retail analyst for Sahra Retail, a UAE supermarket and electronics chain.
- All money is in AED and excludes VAT. Round money to whole dirhams.
- Revenue = net_amount (or net_revenue) for COMPLETED orders only (is_revenue = true).
- Gross margin % = gross_margin / net_revenue.
- The weekend in the UAE is Saturday and Sunday (dim_date.is_weekend).
- Use dim_date.retail_event for Ramadan, Eid, White Friday, Dubai Shopping Festival questions.
- "Online" is a channel and also a store (ST12). Physical stores are Hypermarket or Express.
- Customer segments come from customer_rfm.segment. Tier history: use loyalty_tier_at_order in sales_enriched.
- When asked about "last month" use the latest month present in the data, not today's date.
```

## 3. Example SQL (add as trusted example queries)
**Question:** What was revenue per day during Ramadan compared with regular days?
```sql
SELECT d.retail_event, round(sum(s.net_revenue) / count(DISTINCT s.order_date)) AS revenue_per_day
FROM workspace.sahra_gold.daily_store_sales s
JOIN workspace.sahra_gold.dim_date d ON s.order_date = d.date
WHERE d.retail_event IN ('Ramadan', 'Regular')
GROUP BY 1;
```
**Question:** Which stores have the highest gross margin %?
```sql
SELECT store_name, round(sum(gross_margin) / sum(net_revenue) * 100, 1) AS margin_pct
FROM workspace.sahra_gold.daily_store_sales
GROUP BY 1 ORDER BY 2 DESC;
```

## 4. Questions to try
1. What were total revenue and gross margin in 2025?
2. Show monthly revenue by category as a chart.
3. Which 5 products sold the most during White Friday?
4. How many Champions customers live in Abu Dhabi?
5. Compare average basket size on weekends and weekdays.
6. Which emirate grew fastest from H1 to H2?
7. How much revenue came from Gold-tier customers at the time of purchase?

## 5. Connect it to Claude (paid workspaces only)
Databricks Free Edition does not offer managed MCP servers. On a paid workspace, the Genie space
is exposed at `https://<workspace-host>/api/2.0/mcp/genie/<genie-space-id>` and can be added to
Claude as a remote MCP server so you can ask the same questions from Claude.

---
Author: Imran Sheikh · [LinkedIn](https://www.linkedin.com/in/imranazhar/)
