# Retail orders: small Databricks + Snowflake + dbt demo

This is a deliberately small interview project based on the retail order assignment. It uses **8 made-up order rows**. There is no personal information, data generator, scheduler, cloud credential, or production claim.

## What to explain in an interview

```text
orders.csv -> Databricks bronze (all 8 rows)
           -> Databricks silver (4 valid, unique orders)
           -> Databricks rejects (4 rows with a reason)
           -> Snowflake orders_silver (same 4 rows, loaded by dbt seed for this demo)
           -> dbt daily_sales (3 reportable orders)
           -> dbt tests + reconciliation test
```

Bronze keeps the input. Silver keeps the first row for each order ID and applies basic rules. Rejects keeps bad rows and an error reason. The dbt seed is a **static copy of the expected silver output**, so the Snowflake/dbt portion runs without a Databricks-to-Snowflake connector. In a real deployment, replace the seed with a scheduled export/load and reconcile the two systems after each run.

## Files

| File | Purpose |
| --- | --- |
| `data/orders.csv` | Eight hand-written synthetic source rows |
| `databricks/orders_pipeline.py` | One Databricks notebook: bronze, silver, rejects |
| `seeds/orders_silver.csv` | Four hand-written valid rows for the Snowflake demo |
| `models/daily_sales.sql` | Simple daily reporting model |
| `models/schema.yml` | Basic dbt tests, definitions, and owner/classification labels |
| `tests/*.sql` | Valid-value and sales-total checks |
| `security/reporting_access.sql` | Small reporting-role example |
| `docs/interview_notes.md` | Short interview answers |

## Run the Databricks part

1. In a personal Databricks workspace, upload `data/orders.csv` to a Unity Catalog volume or other readable path.
2. Import `databricks/orders_pipeline.py` as a Python notebook.
3. Set the notebook widget `input_path` to the uploaded CSV path and run all cells.
4. Inspect `retail_demo.orders_bronze`, `retail_demo.orders_silver`, and `retail_demo.orders_rejects` in the current catalog.

The notebook overwrites these three **demo** tables on each run. Re-running the same file gives the same result; it does not append duplicates. It prints the expected counts: bronze 8, silver 4, rejects 4.

The notebook also checks that every bronze row reached either silver or rejects, and that silver has no repeated order IDs. Code comments explain each step.

## Run the Snowflake + dbt part

1. Create a small Snowflake database/schema and a user or role that can create tables there.
2. Install `dbt-snowflake` in your own Python environment.
3. Copy `profiles.yml.example` to `~/.dbt/profiles.yml`. Fill in the Snowflake account, user, authentication, warehouse, database, schema, and role using environment variables. Do not commit credentials.
4. From this folder, run:

```bash
dbt debug
dbt seed
dbt run
dbt test
```

Expected `daily_sales`:

| order_date | currency | order_count | gross_sales |
| --- | --- | ---: | ---: |
| 2026-10-20 | INR | 2 | 180.00 |
| 2026-10-21 | INR | 1 | 50.00 |

`CANCELLED` is kept in silver but excluded from sales. `gross_sales` means the sum of order amounts for `PLACED` and `SHIPPED` orders in this demo. The business owner would approve the real definition.

## The three controls to discuss

1. **Data quality and testing:** Databricks rejects bad rows with a reason. dbt checks required and unique IDs, valid status/currency, positive quantity, nonnegative amount, dates, and daily sales totals.
2. **Governance:** `models/schema.yml` records the meaning, owner roles, and classification of the silver data and sales table. A real sales owner must approve the sales definition.
3. **Security:** The sample contains no names, emails, addresses, or payment references. `daily_sales` exposes only aggregates. If you want to demonstrate a Snowflake reporting role, adapt and run `security/reporting_access.sql`; it grants read access only to `daily_sales`.

## Scope and limitations

The assignment asks for much more than this demo: CDC at 10 million rows, automated cross-system transfer, customer identity resolution, Purview integration, production security, SLAs, and GenAI controls. Those need real systems, ownership decisions, and operational evidence. This project demonstrates the core flow and basic tests; [interview notes](docs/interview_notes.md) give simple answers for the remaining topics.
