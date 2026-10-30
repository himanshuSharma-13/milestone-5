# Simple interview notes

Use these as discussion points. The CSV and code are a demo, not evidence that production controls are operating.

## A short explanation of the code

“I kept every incoming row in bronze. I put invalid and duplicate rows in rejects with a reason, and kept clean unique orders in silver. I used dbt in Snowflake to create daily sales and run basic tests. The example has 8 raw rows, 4 clean rows, and 4 rejected rows. Three clean rows count toward sales because one was cancelled.”

## 1. First 24 hours and ownership

Ask the sales owner for the approved definition of sales and the reporting deadline. Ask the operational and marketplace owners for source schemas, delivery times, and known defects. Ask security who may see customer and payment data. Inventory the source tables/files and the Databricks and Snowflake targets. Record risks, assumptions, issues, and dependencies in a small RAID log. Prioritize wrong sales totals and exposed sensitive fields before catalog polish.

Suggested owners: sales leader = business definition and sign-off; source system owner = corrections; data engineer = pipeline; security owner = access; data steward = exceptions and metadata.

## 2. Profile and choose critical fields

Check missing values, duplicate order IDs, invalid dates and amounts, allowed statuses/currencies, delivery age, and whether customer/product IDs match reference tables. Critical fields are order ID, customer ID, order date, amount, currency, status, and product ID because they affect joins and sales. Names, emails, phone numbers, addresses, and payment references are sensitive; they are deliberately absent from this demo.

## 3–7. Layers, quality, and recovery

Bronze holds the original rows; silver holds valid orders; rejects holds failed rows and reasons. The notebook overwrites the three demo tables, so a replay of the same file has stable results. The rules here block rows with duplicate IDs, missing IDs, future/invalid dates, nonpositive quantity, negative/invalid amount, unknown currency/status, or a shipped order without a shipment date. A real project should agree rule owners, thresholds, severity, and an exception review process. Correct bad source data and rerun; never edit silver silently.

For 10 million rows, I would backfill history once, then ingest changes using a source update timestamp or CDC and a saved watermark. I would advance the watermark only after the load succeeds. Stable source record IDs and a `MERGE` into bronze would prevent replay duplicates. Late changes would be accepted within an agreed lookback period. These are production design choices, **not implemented in this demo**.

## 8. Databricks–Snowflake reconciliation

Compare silver and Snowflake counts and amount totals by order date and currency. Then inspect rejected rows, duplicate IDs, load logs, and timing. For a mismatch of 1,000,000 versus 995,000, first locate the dates with missing rows, then identify the exact order IDs. Do not call the report complete until counts and totals agree or the business accepts a documented exception. This demo's dbt reconciliation test compares the seeded silver data with daily sales in Snowflake; it does not compare live Databricks and Snowflake tables.

## 9–10. Customer identity and schema change

Use a trusted customer ID where possible, then reviewed email matches for marketplace data. Do not automatically merge uncertain matches; a steward reviews them and records the decision. If the source adds a column, preserve it in bronze and review whether downstream models need it. If amount becomes text, try a safe conversion and reject nonnumeric values. Alert the source owner before changing the contract.

## 11–13. Security, glossary, and Purview

Analysts should get only curated reporting tables. Customer contact and payment fields should have restricted roles, masking or secure views, and access reviews. Define “Gross Sales” with the sales owner; record the approved formula and changes. A Purview-style catalog entry would include asset name, owner, description, classification, lineage (`orders.csv → bronze → silver → Snowflake → daily_sales`), and quality status. This repo is a documentation mock-up, not a Purview connection.

## 14–16. Monitoring, GenAI, and readiness

Before the 7 a.m. reporting deadline, check source freshness, row counts, reject rate, pipeline status, and reconciliation. If a threshold fails, mark the report incomplete and notify the owner. GenAI may draft definitions or suggest classifications only from approved metadata; a human reviews and approves them, and raw customer/payment data should not go into prompts. If AI suggests a wrong “Active Customer” definition, reject it and correct the catalog entry. Readiness rating for this repo: **demo only**. Production work includes automated transfer, CDC, access controls, monitoring, ownership sign-off, and load testing.
