#!/usr/bin/env python3
"""
Stay.AI Dashboard Data Refresh Script
======================================
Queries BigQuery for fresh retention, LTV, product retention, and revenue data,
then updates the embedded data in stay_ai_dashboard.html.

Requirements:
  pip install google-cloud-bigquery

Usage:
  python refresh_dashboard.py                          # default: same folder
  python refresh_dashboard.py --dashboard /path/to/stay_ai_dashboard.html
  python refresh_dashboard.py --project happy-aging-466917

Authentication:
  - Set GOOGLE_APPLICATION_CREDENTIALS to your service account JSON key, OR
  - Run `gcloud auth application-default login` first
"""

import argparse
import re
import os
import sys
from datetime import datetime

try:
    from google.cloud import bigquery
except ImportError:
    print("ERROR: google-cloud-bigquery is not installed.")
    print("Run: pip install google-cloud-bigquery")
    sys.exit(1)


# ââ Config ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ
PROJECT_ID = "happy-aging-466917"
DATASET = "stay_ai_subscriptions"

# Product name standardization mapping (used in all queries)
PRODUCT_CASE = """
    CASE
      WHEN LOWER(pt) LIKE '%longevity shot%' AND LOWER(pt) NOT LIKE '%glow duo%'
           AND LOWER(pt) NOT LIKE '%advanced%' AND LOWER(pt) NOT LIKE '%essential%'
           AND LOWER(pt) NOT LIKE '%booster ritual%' THEN 'Longevity Shots'
      WHEN LOWER(pt) LIKE '%glow shot%' OR LOWER(pt) LIKE '%glow shots%' THEN 'Glow Shots'
      WHEN LOWER(pt) LIKE '%hydro burn%' OR LOWER(pt) LIKE '%molecular hydrogen%' THEN 'Hydro Burn'
      WHEN LOWER(pt) LIKE '%happiest gut%' OR LOWER(pt) LIKE '%electrolytes%' THEN 'Happiest Gut'
      WHEN LOWER(pt) LIKE '%advanced longevity%' OR LOWER(pt) LIKE '%women%longevity%beauty%' THEN 'NAD+ Advanced longevity formula'
      WHEN pt = 'Calm Shot' THEN 'Calm Shots'
      WHEN pt = 'Cap Happy Aging Gift' THEN 'Happy Aging Cap'
      WHEN LOWER(pt) = 'lean muscle formula' THEN 'Lean Muscle Formula'
      WHEN LOWER(pt) = 'liposomal curcumin' THEN 'Liposomal Curcumin'
      WHEN LOWER(pt) LIKE '%liposomal sleep%' THEN 'Liposomal Sleep Blend'
      ELSE pt
    END
"""

# ââ Queries âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

Q_RETENTION = f"""
WITH cohort_base AS (
  SELECT
    id AS subscription_id,
    FORMAT_TIMESTAMP('%Y-%m', PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', createdAt)) AS cohort_month,
    DATE_TRUNC(DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', createdAt)), MONTH) AS cohort_date,
    CASE WHEN cancelledAt IS NOT NULL AND cancelledAt != ''
      THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', cancelledAt) ELSE NULL END AS cancelled_ts
  FROM `{PROJECT_ID}.{DATASET}.subscriptions`
  WHERE createdAt IS NOT NULL AND price > 0
),
months AS (SELECT month_offset FROM UNNEST(GENERATE_ARRAY(0, 24)) AS month_offset),
retention AS (
  SELECT
    s.cohort_month, m.month_offset,
    COUNT(DISTINCT s.subscription_id) AS total_in_cohort,
    COUNT(DISTINCT CASE
      WHEN (s.cancelled_ts IS NULL
        OR DATE(s.cancelled_ts) > LAST_DAY(DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH), MONTH))
      THEN s.subscription_id END) AS active_subscribers
  FROM cohort_base s CROSS JOIN months m
  WHERE DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH) <= CURRENT_DATE()
  GROUP BY 1, 2
)
SELECT
  cohort_month || '|' || CAST(month_offset AS STRING) || '|' ||
  CAST(total_in_cohort AS STRING) || '|' || CAST(active_subscribers AS STRING) || '|' ||
  CAST(ROUND(SAFE_DIVIDE(active_subscribers, total_in_cohort) * 100, 1) AS STRING) AS line
FROM retention
ORDER BY cohort_month, month_offset
"""

Q_LTV = f"""
WITH sub_base AS (
  SELECT customerId, id AS subscription_id,
    CAST(subscriptionId AS STRING) AS subscription_id_str,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', createdAt) AS created_ts,
    FORMAT_TIMESTAMP('%Y-%m', PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', createdAt)) AS cohort_month,
    DATE_TRUNC(DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', createdAt)), MONTH) AS cohort_date,
    price AS subscription_price, status,
    CASE WHEN cancelledAt IS NOT NULL AND cancelledAt != ''
      THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', cancelledAt) ELSE NULL END AS cancelled_ts,
    completedOrdersCount
  FROM `{PROJECT_ID}.{DATASET}.subscriptions`
  WHERE createdAt IS NOT NULL AND price > 0
),
cohort_sizes AS (
  SELECT cohort_month, COUNT(DISTINCT subscription_id) AS cohort_size FROM sub_base GROUP BY 1
),
months AS (SELECT month_offset FROM UNNEST(GENERATE_ARRAY(0, 24)) AS month_offset),
retention_info AS (
  SELECT s.cohort_month, s.cohort_date, m.month_offset,
    COUNT(DISTINCT s.subscription_id) AS total_in_cohort,
    COUNT(DISTINCT CASE
      WHEN (s.cancelled_ts IS NULL
        OR DATE(s.cancelled_ts) > LAST_DAY(DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH), MONTH))
      THEN s.subscription_id END) AS active_subscribers,
    COUNT(DISTINCT CASE
      WHEN s.cancelled_ts IS NOT NULL
        AND DATE(s.cancelled_ts) > LAST_DAY(DATE_ADD(s.cohort_date, INTERVAL (m.month_offset - 1) MONTH), MONTH)
        AND DATE(s.cancelled_ts) <= LAST_DAY(DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH), MONTH)
      THEN s.subscription_id END) AS cancelled_count
  FROM sub_base s CROSS JOIN months m
  WHERE DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH) <= CURRENT_DATE()
  GROUP BY 1, 2, 3
),
orders_with_cohort AS (
  SELECT o.subscriptionId AS order_subscription_id, o.totalPrice,
    PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', o.createdAt) AS order_date, s.cohort_month, s.cohort_date,
    s.subscription_id, s.customerId,
    DATE_DIFF(DATE_TRUNC(DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', o.createdAt)), MONTH), s.cohort_date, MONTH) AS month_offset
  FROM `{PROJECT_ID}.{DATASET}.subs_orders` o
  INNER JOIN sub_base s
    ON CAST(o.subscription_Id_clean AS STRING) = s.subscription_id_str
       OR o.subscriptionId = s.subscription_id_str
  WHERE o.totalPrice > 0 AND o.createdAt IS NOT NULL
),
ltv_monthly AS (
  SELECT cohort_month, cohort_date, month_offset,
    COUNT(DISTINCT subscription_id) AS subscriptions_with_orders,
    SUM(totalPrice) AS total_revenue, COUNT(*) AS order_count
  FROM orders_with_cohort WHERE month_offset >= 0 GROUP BY 1, 2, 3
)
SELECT
  l.cohort_month || '|' || CAST(l.month_offset AS STRING) || '|' ||
  CAST(cs.cohort_size AS STRING) || '|' ||
  CAST(r.active_subscribers AS STRING) || '|' ||
  CAST(r.cancelled_count AS STRING) || '|' ||
  CAST(l.subscriptions_with_orders AS STRING) || '|' ||
  CAST(l.order_count AS STRING) || '|' ||
  CAST(ROUND(l.total_revenue, 2) AS STRING) || '|' ||
  CAST(ROUND(SUM(l.total_revenue) OVER (PARTITION BY l.cohort_month ORDER BY l.month_offset), 2) AS STRING) || '|' ||
  CAST(ROUND(SAFE_DIVIDE(SUM(l.total_revenue) OVER (PARTITION BY l.cohort_month ORDER BY l.month_offset), cs.cohort_size), 2) AS STRING) || '|' ||
  CAST(ROUND(SAFE_DIVIDE(l.total_revenue, l.order_count), 2) AS STRING) AS line
FROM ltv_monthly l
JOIN cohort_sizes cs ON l.cohort_month = cs.cohort_month
LEFT JOIN retention_info r ON l.cohort_month = r.cohort_month AND l.month_offset = r.month_offset
ORDER BY l.cohort_month, l.month_offset
"""

Q_PRODUCT_RETENTION = f"""
WITH sub_base AS (
  SELECT
    s.id AS subscription_id,
    DATE_TRUNC(DATE(PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', s.createdAt)), MONTH) AS cohort_date,
    s.status,
    CASE WHEN s.cancelledAt IS NOT NULL AND s.cancelledAt != ''
      THEN PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*SZ', s.cancelledAt) ELSE NULL END AS cancelled_ts,
    {PRODUCT_CASE} AS product_name
  FROM `{PROJECT_ID}.{DATASET}.subscriptions` s,
  UNNEST(JSON_EXTRACT_ARRAY(s.lineItems)) AS li
  CROSS JOIN UNNEST([JSON_VALUE(li, '$.produchtTitle')]) AS pt
  WHERE s.createdAt IS NOT NULL
    AND s.price > 0
    AND s.lineItems IS NOT NULL
    AND JSON_VALUE(li, '$.isOneTime') = 'false'
    AND JSON_VALUE(li, '$.productTitle') IS NOT NULL
),
months AS (SELECT month_offset FROM UNNEST(GENERATE_ARRAY(0, 24)) AS month_offset),
product_retention AS (
  SELECT
    s.product_name, m.month_offset,
    COUNT(DISTINCT s.subscription_id) AS cohort_total,
    COUNT(DISTINCT CASE
      WHEN (s.cancelled_ts IS NULL
        OR DATE(s.cancelled_ts) > LAST_DAY(DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH), MONTH))
      THEN s.subscription_id END) AS active_subscribers
  FROM sub_base s CROSS JOIN months m
  WHERE DATE_ADD(s.cohort_date, INTERVAL m.month_offset MONTH) <= CURRENT_DATE()
  GROUP BY 1, 2
)
SELECT
  product_name || '|' || CAST(month_offset AS STRING) || '|' ||
  CAST(cohort_total AS STRING) || '|' || CAST(active_subscribers AS STRING) || '|' ||
  CAST(ROUND(SAFE_DIVIDE(active_subscribers, cohort_total) * 100, 1) AS STRING) AS line
FROM product_retention
WHERE cohort_total >= 30
ORDER BY product_name, month_offset
"""

Q_REVENUE = f"""
WITH sub_products AS (
  SELECT DISTINCT
    s.id AS subscription_id,
    {PRODUCT_CASE} AS product_name
  FROM `{PROJECT_ID}.{DATASET}.subscriptions` s,
  UNNEST(JSON_EXTRACT_ARRAY(s.lineItems)) AS li
  CROSS JOIN UNNEST([JSON_VALUE(li, '$.productTitle')]) AS pt
  WHERE s.lineItems IS NOT NULL
    AND JSON_VALUE(li, '$.isOneTime') = 'false'
    AND JSON_VALUE(li, '$.productTitle') IS NOT NULL
    AND s.price > 0
),
agg AS (
  SELECT
    sp.product_name,
    COUNT(DISTINCT sp.subscription_id) AS total_subs,
    COUNT(o.id) AS total_orders,
    ROUND(COALESCE(SUM(SAFE_CAST(o.total AS FLOAT64)), 0), 2) AS total_revenue,
    ROUND(COALESCE(AVG(SAFE_CAST(o.total AS FLOAT64)), 0), 2) AS avg_order_value,
    ROUND(COALESCE(SUM(SAFE_CAST(o.total AS FLOAT64)), 0) / NULLIF(COUNT(DISTINCT sp.subscription_id), 0), 2) AS revenue_per_sub
  FROM sub_products sp
  LEFT JOIN `{PROJECT_ID}.{DATASET}.subs_orders` o
    ON CAST(sp.subscription_id AS STRING) = CAST(o.subscriptionId AS STRING)
  GROUP BY 1
)
SELECT
  product_name || '|' || CAST(total_subs AS STRING) || '|' ||
  CAST(total_orders AS STRING) || '|' || CAST(total_revenue AS STRING) || '|' ||
  CAST(avg_order_value AS STRING) || '|' || CAST(revenue_per_sub AS STRING) AS line
FROM agg
ORDER BY total_revenue DESC
"""


# ââ Helpers âââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def run_query(client, query, label):
    """Run a BigQuery query and return pipe-delimited lines."""
    print(f"  Querying {label}...", end=" ", flush=True)
    result = client.query(query).result()
    lines = [row.line for row in result if row.line]
    print(f"{len(lines)} rows")
    return "\n".join(lines)


def replace_data_block(html, var_name, new_data):
    """Replace a const varName = `...` block with new data."""
    pattern = re.compile(
        r"(const " + re.escape(var_name) + r" = `)([\s\S]*?)(`)",
        re.MULTILINE
    )
    match = pattern.search(html)
    if not match:
        print(f"  WARNING: Could not find '{var_name}' in HTML!")
        return html
    return pattern.sub(r"\g<1>" + new_data + r"\3", html)


def update_date_stamp(html):
    """Update the 'Updated on MM/DD/YYYY' date in the header."""
    today = datetime.now().strftime("%m/%d/%Y")
    return re.sub(
        r"Updated on \d{2}/\d{2}/\d{4}",
        f"Updated on {today}",
        html
    )


# ââ Main ââââââââââââââââââââââââââââââââââââââââââââââââââââââââââââ

def main():
    parser = argparse.ArgumentParser(description="Refresh Stay.AI Dashboard data from BigQuery")
    parser.add_argument("--dashboard", default=os.path.join(os.path.dirname(__file__), "index.html"),
                        help="Path to the dashboard HTML file")
    parser.add_argument("--project", default=PROJECT_ID, help="GCP project ID")
    parser.add_argument("--dry-run", action="store_true", help="Query data but don't write the file")
    args = parser.parse_args()

    dashboard_path = args.dashboard
    if not os.path.exists(dashboard_path):
        print(f"ERROR: Dashboard file not found: {dashboard_path}")
        sys.exit(1)

    print(f"Stay.AI Dashboard Refresh")
    print(f"{'=' * 40}")
    print(f"Dashboard: {dashboard_path}")
    print(f"Project:   {args.project}")
    print()

    # Initialize BigQuery client
    client = bigquery.Client(project=args.project)
    print("Connected to BigQuery")
    print()

    # Run all 4 queries
    print("Running queries...")
    retention_data = run_query(client, Q_RETENTION, "Retention by Cohort")
    ltv_data = run_query(client, Q_LTV, "LTV & ARPU")
    product_data = run_query(client, Q_PRODUCT_RETENTION, "Retention by Product")
    revenue_data = run_query(client, Q_REVENUE, "Revenue by Product")
    print()

    if args.dry_run:
        print("DRY RUN â data queried but file not updated.")
        print(f"  Retention: {len(retention_data.splitlines())} rows")
        print(f"  LTV:       {len(ltv_data.splitlines())} rows")
        print(f"  Product:   {len(product_data.splitlines())} rows")
        print(f"  Revenue:   {len(revenue_data.splitlines())} rows")
        return

    # Read existing HTML
    with open(dashboard_path, "r", encoding="utf-8") as f:
        html = f.read()

    original_size = len(html)

    # Replace data blocks
    print("Updating dashboard...")
    html = replace_data_block(html, "retentionRaw", retention_data)
    html = replace_data_block(html, "ltvRaw", ltv_data)
    html = replace_data_block(html, "productRaw", product_data)
    html = replace_data_block(html, "revenueRaw", revenue_data)
    html = update_date_stamp(html)

    # Write updated HTML
    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html)

    new_size = len(html)
    print()
    print(f"Done! Dashboard updated successfully.")
    print(f"  File size: {original_size/1024:.1f} KB -> {new_size/1024:.1f} KB")
    print(f"  Date:      {datetime.now().strftime('%m/%d/%Y %H:%M')}")


if __name__ == "__main__":
    main()
