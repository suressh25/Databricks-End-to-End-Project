# Databricks End-to-End Data Engineering Project

This project implements an end-to-end Azure data engineering workflow on Databricks using ADLS Gen2, PySpark, Delta Lake, Unity Catalog, and a Lakeflow Spark Declarative Pipeline for the gold product dimension. Based on the project assets and the reference architecture image, the flow is designed as: GitHub -> Databricks -> Azure Data Lake Gen2 -> Bronze -> Silver -> Gold/star schema -> warehouse/reporting consumption in Power BI.

The implementation combines notebook-driven ingestion and transformation with curated Delta tables and dimensional modeling. Raw source data lands in ADLS Gen2, is processed into bronze and silver layers, and is then promoted into gold dimension and fact tables for analytics.

## Architecture summary

The reference architecture in [Architecture.jpeg](#file-451142891229490) maps closely to the codebase:

* Azure services provide the cloud foundation.
* GitHub acts as the source-control entry point for project code.
* Databricks performs ingestion, transformation, and dimensional modeling.
* ADLS Gen2 is used as the storage layer across source, bronze, silver, and gold zones.
* PySpark notebooks implement ETL logic.
* A Lakeflow Spark Declarative Pipeline powers the gold products dimension. The image labels this as "Delta Live Tables", but the current product name is Lakeflow Spark Declarative Pipelines.
* Gold data is modeled for warehouse-style analytics and downstream reporting in Power BI.
* The diagram also indicates a security layer around Azure identity and secret management, which fits the Azure deployment model even though those configurations are not stored in this project folder.

## Project structure

Project folder: `/Users/suresh25off@outlook.com/Databricks-End-to-End-Project`

Assets discovered in this project:

* [parameters](#notebook-3575224262120890)
* [Bronze Layer](#notebook-3575224262120891)
* [Silver_Customers](#notebook-3575224262120892)
* [Silver_Products](#notebook-3575224262120884)
* [Silver_Orders](#notebook-3575224262120885)
* [Silver_Regions](#notebook-3575224262120888)
* [Gold_Customers](#notebook-3575224262120889)
* [Gold Products](#notebook-3575224262120886)
* [Gold Orders](#notebook-3575224262120887)
* [Gold Products](#pipeline-5865c2b0-b568-43c7-8665-5ecda0069c29)
* [Architecture.jpeg](#file-451142891229490)

## End-to-end data flow

### 1. Parameter-driven ingestion control
The [parameters](#notebook-3575224262120890) notebook defines a dynamic dataset list and publishes it through `dbutils.jobs.taskValues`.

Datasets configured in code:

* `orders`
* `customers`
* `products`

This notebook is intended to support orchestration, especially when the bronze ingestion notebook is executed multiple times with different input values.

### 2. Bronze layer ingestion
The [Bronze Layer](#notebook-3575224262120891) notebook performs raw ingestion from ADLS Gen2 source storage into the bronze zone.

Key implementation details:

* Uses a widget named `file_name` to dynamically select the dataset.
* Reads data with Auto Loader via `cloudFiles` in streaming mode.
* Source format is Parquet.
* Reads from `abfss://source@databricksetestorage.dfs.core.windows.net/{file_name}`.
* Stores schema and checkpoint metadata in the bronze container.
* Writes raw data to `abfss://bronze@databricksetestorage.dfs.core.windows.net/{file_name}`.
* Uses `trigger(once=True)` to process data like a bounded micro-batch ingestion job.

This establishes the raw landing layer for downstream processing.

### 3. Silver layer standardization and cleansing
The silver notebooks consume bronze data, clean and enrich it, then write Delta outputs to the silver layer and register Unity Catalog tables.

#### [Silver_Customers](#notebook-3575224262120892)
Responsibilities:

* Reads bronze customer Parquet files.
* Drops `_rescued_data`.
* Extracts email domain into a `domains` column.
* Derives `fullname` from first and last names.
* Writes Delta output to `abfss://silver@databricksetestorage.dfs.core.windows.net/customers`.
* Registers [databricks_cat.silver.customers_silver](#table).

Business value:

* Produces a cleaner customer dimension source.
* Adds lightweight enrichment useful for profiling and downstream analytics.

#### [Silver_Orders](#notebook-3575224262120885)
Responsibilities:

* Reads bronze orders data.
* Renames and drops rescued-data fields.
* Converts `order_date` to timestamp.
* Derives `year` from order date.
* Demonstrates ranking logic with `dense_rank`, `rank`, and `row_number` window functions.
* Includes a small OOP example wrapping ranking logic in a class.
* Writes Delta output to `abfss://silver@databricksetestorage.dfs.core.windows.net/orders`.
* Registers [databricks_cat.silver.orders_silver](#table).

Business value:

* Produces a typed and analytics-ready order dataset.
* Prepares order data for gold fact-table construction.

#### [Silver_Products](#notebook-3575224262120884)
Responsibilities:

* Reads bronze products data.
* Drops `_rescued_data`.
* Attempts to define reusable SQL and Python functions for product transformations:
  * `databricks_cat.bronze.discount_func`
  * `databricks_cat.bronze.upper_func`
* Applies a discounted price transformation.
* Writes Delta output to `abfss://silver@databricksetestorage.dfs.core.windows.net/products`.
* Registers [databricks_cat.silver.products_silver](#table).

Current implementation note:

* Several cells in this notebook are currently in an error state. The intended design is clear, but this notebook likely needs debugging before a clean end-to-end rerun.

#### [Silver_Regions](#notebook-3575224262120888)
Responsibilities:

* Reads region data from a bronze table source.
* Drops `_rescued_data`.
* Writes Delta output to `abfss://silver@databricksetestorage.dfs.core.windows.net/regions`.
* Registers [databricks_cat.silver.regions_silver](#table).

This notebook extends the model with a supporting geographic dimension source.

### 4. Gold layer dimensional modeling
The gold layer builds analytics-ready dimensions and facts.

#### [Gold_Customers](#notebook-3575224262120889)
This notebook creates and maintains the customer dimension.

Key steps:

* Reads from [databricks_cat.silver.customers_silver](#table).
* Uses `init_load_flag` to distinguish first-time load versus incremental maintenance.
* Removes duplicate `customer_id` values.
* Splits records into new versus existing customers by joining to the current gold table.
* Preserves `create_date` for existing rows and refreshes `update_date`.
* Generates surrogate keys using `monotonically_increasing_id()` plus the current max key.
* Unions old and new records into a final dimension dataset.
* Uses Delta Lake merge logic to implement SCD Type 1 behavior.
* Writes or merges into [databricks_cat.gold.DimCustomers](#table).

Result:

* A curated customer dimension with surrogate keys and audit timestamps.

#### [Gold Products](#notebook-3575224262120886)
This notebook is the source for the [Gold Products](#pipeline-5865c2b0-b568-43c7-8665-5ecda0069c29) Lakeflow Spark Declarative Pipeline.

Key steps:

* Imports `dlt` and defines data quality expectations.
* Reads [databricks_cat.silver.products_silver](#table) as a streaming source.
* Creates a staged streaming table with rules:
  * `product_id IS NOT NULL`
  * `product_name IS NOT NULL`
  * `price > 0`
* Creates a DLT view over the staged data.
* Creates a target streaming table named `DimProducts`.
* Applies changes with `stored_as_scd_type=2` using `product_id` as the business key.

Pipeline details:

* Pipeline name: [Gold Products](#pipeline-5865c2b0-b568-43c7-8665-5ecda0069c29)
* Catalog/schema: `databricks_cat.gold`
* Compute mode: serverless with Photon enabled
* Recent updates: latest runs completed successfully

Managed gold datasets identified from the pipeline:

* `databricks_cat.gold.dimproducts_stage`
* `databricks_cat.gold.dimproducts`
* `dimproducts_view`

Result:

* A product dimension managed through declarative pipeline logic with SCD Type 2 history and expectation-based quality checks.

#### [Gold Orders](#notebook-3575224262120887)
This notebook creates the fact table for orders.

Key steps:

* Reads from [databricks_cat.silver.orders_silver](#table).
* Reads gold dimensions for customers and products.
* Joins silver order data to the customer and product dimensions.
* Replaces natural keys with dimensional keys.
* Uses Delta Lake merge logic for upsert behavior.
* Creates or updates [databricks_cat.gold.FactOrders](#table).

Result:

* A fact table aligned to a star-schema model with foreign keys to gold dimensions.

## Target analytical model

From the notebooks and the architecture image, the intended warehouse model is a star schema:

* Dimension tables:
  * [databricks_cat.gold.DimCustomers](#table)
  * [databricks_cat.gold.dimproducts](#table)
  * likely region-related dimensions as the model evolves
* Fact table:
  * [databricks_cat.gold.FactOrders](#table)

This supports BI workloads such as sales analysis by customer, product, time, and region.

## Storage layout

The project consistently uses ADLS Gen2 containers through ABFSS paths:

* Source: `abfss://source@databricksetestorage.dfs.core.windows.net/`
* Bronze: `abfss://bronze@databricksetestorage.dfs.core.windows.net/`
* Silver: `abfss://silver@databricksetestorage.dfs.core.windows.net/`
* Gold: `abfss://gold@databricksetestorage.dfs.core.windows.net/`

This clearly separates raw, cleansed, and curated data zones.

## Unity Catalog objects referenced

The codebase references these core objects:

* [databricks_cat.silver.customers_silver](#table)
* [databricks_cat.silver.orders_silver](#table)
* [databricks_cat.silver.products_silver](#table)
* [databricks_cat.silver.regions_silver](#table)
* [databricks_cat.gold.DimCustomers](#table)
* [databricks_cat.gold.dimproducts](#table)
* [databricks_cat.gold.dimproducts_stage](#table)
* [databricks_cat.gold.FactOrders](#table)
* `databricks_cat.bronze.discount_func`
* `databricks_cat.bronze.upper_func`

## Suggested execution order

For a fresh run, the logical order is:

1. Run [parameters](#notebook-3575224262120890) if using task-based orchestration.
2. Run [Bronze Layer](#notebook-3575224262120891) once for each input dataset.
3. Run [Silver_Customers](#notebook-3575224262120892).
4. Run [Silver_Orders](#notebook-3575224262120885).
5. Run [Silver_Products](#notebook-3575224262120884) after resolving current notebook errors.
6. Run [Silver_Regions](#notebook-3575224262120888) if regions are part of the model.
7. Run [Gold_Customers](#notebook-3575224262120889).
8. Run the [Gold Products](#pipeline-5865c2b0-b568-43c7-8665-5ecda0069c29) pipeline.
9. Run [Gold Orders](#notebook-3575224262120887).
10. Expose gold tables to warehouse/reporting tools such as Power BI.

## Project strengths

* Clear bronze, silver, and gold layer separation.
* Uses Azure-native ADLS Gen2 paths throughout.
* Mixes notebook engineering patterns with declarative pipeline processing.
* Implements dimensional modeling with dimensions and facts.
* Includes SCD handling in the gold layer.
* Aligns well with an end-to-end Azure + Databricks reporting architecture.

## Current gaps and improvement opportunities

* [Silver_Products](#notebook-3575224262120884) contains execution errors and should be stabilized.
* There is no job definition in this folder to orchestrate the full workflow automatically.
* The bronze ingestion currently covers `orders`, `customers`, and `products`; `regions` appears to be sourced differently and may need to be documented or standardized further.
* Security, CI/CD, and Power BI deployment are represented in the architecture but not implemented as project files here.
* Surrogate key generation using `monotonically_increasing_id()` is workable for learning projects, but production pipelines often use tighter key-management patterns.

## Summary

This repository is a solid end-to-end Azure Databricks data engineering project that demonstrates ingestion, transformation, Delta storage, dimensional modeling, and declarative pipeline processing. The overall solution builds raw-to-curated data products in ADLS Gen2, promotes them into gold dimensions and facts, and aligns the final model with warehouse-style analytics and Power BI reporting.
