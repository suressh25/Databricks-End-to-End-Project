# Databricks End-to-End Data Engineering Project

A comprehensive Azure data engineering implementation on Databricks, demonstrating a complete medallion architecture (bronze, silver, gold layers) with Azure Data Lake Storage Gen2, PySpark, Delta Lake, Unity Catalog, and Lakeflow Spark Declarative Pipelines.

## 📋 Overview

This project implements an end-to-end ETL/ELT workflow that ingests raw data into a bronze layer, cleanses and standardizes it in the silver layer, and builds analytics-ready dimensional models in the gold layer. The architecture follows best practices for scalable, maintainable data engineering on the Databricks platform.

## 🏗️ Architecture

```
Azure ADLS Gen2 (Source)
        ↓
   Bronze Layer (Raw)
        ↓
   Silver Layer (Cleansed)
   ├── Customers
   ├── Orders
   ├── Products
   └── Regions
        ↓
   Gold Layer (Curated)
   ├── Dimension Tables
   └── Fact Tables
        ↓
   Power BI / Analytics
```

**Key Components:**
- **Azure Services**: Cloud infrastructure and identity management
- **ADLS Gen2**: Multi-zone storage (source, bronze, silver, gold)
- **Databricks**: Compute and orchestration
- **PySpark**: ETL logic and transformations
- **Delta Lake**: ACID compliance and versioning
- **Unity Catalog**: Data governance and lineage
- **Lakeflow Spark**: Declarative pipeline processing for gold products
- **Power BI**: Business intelligence and reporting

## 📁 Project Structure

```
Databricks-End-to-End-Project/
├── 01_parameters.ipynb           # Parameter-driven ingestion control
├── 02_bronze_layer.ipynb         # Raw data ingestion
├── 03_silver_customers.ipynb     # Customer dimension cleaning
├── 04_silver_orders.ipynb        # Order fact table preparation
├── 05_silver_products.ipynb      # Product dimension processing
├── 06_silver_regions.ipynb       # Geographic dimension
├── 07_gold_customers.ipynb       # Customer dimension (SCD Type 1)
├── 08_gold_orders.ipynb          # Order fact table
├── 09_gold_products.ipynb        # Product dimension (DLT/SCD Type 2)
├── architecture.jpeg             # Architecture diagram
└── README.md
```

## 🔄 Data Flow & Transformations

### 1. Parameter-Driven Ingestion Control
Defines dynamic dataset list and publishes through job task values for flexible orchestration.

**Configured Datasets:**
- `orders`
- `customers`
- `products`

### 2. Bronze Layer – Raw Ingestion
Ingests raw data from ADLS Gen2 source into the bronze zone using Auto Loader.

**Key Details:**
- Auto Loader with streaming mode (`cloudFiles`)
- Parquet source format
- Single trigger for bounded batch processing
- Schema and checkpoint metadata management
- Input: `abfss://source@databricksetestorage.dfs.core.windows.net/{file_name}`
- Output: `abfss://bronze@databricksetestorage.dfs.core.windows.net/{file_name}`

### 3. Silver Layer – Standardization & Cleansing

#### Silver_Customers
- Cleans customer records
- Extracts email domains
- Derives full names
- Output: `databricks_cat.silver.customers_silver`

#### Silver_Orders
- Standardizes order records
- Converts timestamps
- Derives temporal attributes (year)
- Demonstrates window functions (rank, dense_rank, row_number)
- Output: `databricks_cat.silver.orders_silver`

#### Silver_Products
- Cleans product data
- Applies discount transformations
- Defines reusable SQL/Python functions
- Output: `databricks_cat.silver.products_silver`
- *Note: May contain errors requiring debugging*

#### Silver_Regions
- Processes geographic dimensions
- Output: `databricks_cat.silver.regions_silver`

### 4. Gold Layer – Dimensional Modeling

#### DimCustomers (SCD Type 1)
- Removes duplicates
- Distinguishes new vs. existing records
- Preserves creation date, updates change date
- Generates surrogate keys
- Merge-based upsert logic
- Output: `databricks_cat.gold.DimCustomers`

#### DimProducts (DLT with SCD Type 2)
- Declarative pipeline with data quality expectations
- Streaming table with validation rules:
  - `product_id NOT NULL`
  - `product_name NOT NULL`
  - `price > 0`
- Tracks historical changes
- Output: `databricks_cat.gold.dimproducts`

#### FactOrders
- Joins orders with customer and product dimensions
- Replaces natural keys with dimensional foreign keys
- Star-schema optimized
- Merge-based upsert logic
- Output: `databricks_cat.gold.FactOrders`

## 🎯 Target Analytical Model

**Star Schema:**

**Dimensions:**
- `DimCustomers`
- `DimProducts`
- `DimRegions` (planned)

**Facts:**
- `FactOrders`

**Use Cases:**
- Sales analysis by customer, product, time, and region
- Customer profitability analysis
- Product performance tracking
- Trend analysis and forecasting

## 💾 Storage Layout

| Layer | Path |
|-------|------|
| Source | `abfss://source@databricksetestorage.dfs.core.windows.net/` |
| Bronze | `abfss://bronze@databricksetestorage.dfs.core.windows.net/` |
| Silver | `abfss://silver@databricksetestorage.dfs.core.windows.net/` |
| Gold | `abfss://gold@databricksetestorage.dfs.core.windows.net/` |

## 🗄️ Unity Catalog Objects

### Silver Layer Tables
- `databricks_cat.silver.customers_silver`
- `databricks_cat.silver.orders_silver`
- `databricks_cat.silver.products_silver`
- `databricks_cat.silver.regions_silver`

### Gold Layer Tables
- `databricks_cat.gold.DimCustomers`
- `databricks_cat.gold.dimproducts`
- `databricks_cat.gold.dimproducts_stage`
- `databricks_cat.gold.FactOrders`

### Functions
- `databricks_cat.bronze.discount_func`
- `databricks_cat.bronze.upper_func`

## 🚀 Execution Order

For a fresh end-to-end run:

1. **Parameters** – Set up dynamic configuration
2. **Bronze Layer** – Ingest each dataset (orders, customers, products)
3. **Silver Customers** – Clean and enrich customer data
4. **Silver Orders** – Standardize and prepare orders
5. **Silver Products** – Process products (after debugging)
6. **Silver Regions** – Process geographic data
7. **Gold Customers** – Build customer dimension
8. **Gold Products** – Run DLT pipeline
9. **Gold Orders** – Build fact table
10. **Power BI** – Connect and visualize

## ✅ Project Strengths

- ✓ Clear medallion architecture (bronze → silver → gold)
- ✓ Leverages Azure-native ADLS Gen2
- ✓ Combines notebook and declarative pipeline patterns
- ✓ Implements dimensional modeling with dimensions and facts
- ✓ Includes SCD Type 1 and Type 2 handling
- ✓ Production-ready star schema design
- ✓ Data quality validation (DLT expectations)

## ⚠️ Known Issues & Improvement Opportunities

| Item | Status | Notes |
|------|--------|-------|
| Silver_Products notebook | ⚠️ Errors | Requires debugging before full run |
| Job orchestration | ❌ Missing | No automated workflow definition |
| Regions ingestion | ⚠️ Documented | May need standardization |
| Surrogate keys | ✓ Working | Uses `monotonically_increasing_id()` |
| Key management | 📋 Todo | Production pipelines need tighter patterns |
| Security/CI-CD | 📋 Todo | Architecture defined, not yet implemented |
| Power BI integration | 📋 Todo | Deployment scripts/documentation needed |

## 🛠️ Tech Stack

| Component | Technology |
|-----------|-----------|
| Cloud | Microsoft Azure |
| Data Warehouse | Databricks |
| Storage | ADLS Gen2 |
| Processing | PySpark / SQL |
| Format | Delta Lake / Parquet |
| Governance | Unity Catalog |
| Pipelines | Lakeflow Spark Declarative |
| Analytics | Power BI |

## 📚 Getting Started

### Prerequisites
- Databricks workspace with admin access
- ADLS Gen2 storage account
- Service principal credentials
- Unity Catalog enabled

### Setup Steps
1. Clone this repository
2. Configure ADLS Gen2 credentials in Databricks secrets
3. Import notebooks into Databricks workspace
4. Execute notebooks in order (see Execution Order section)
5. Validate Unity Catalog objects
6. Connect Power BI to gold tables

### Configuration
Update storage paths in notebooks:
```python
source_path = "abfss://source@{your_storage}.dfs.core.windows.net/"
bronze_path = "abfss://bronze@{your_storage}.dfs.core.windows.net/"
silver_path = "abfss://silver@{your_storage}.dfs.core.windows.net/"
gold_path = "abfss://gold@{your_storage}.dfs.core.windows.net/"
```

## 📖 Documentation

- See individual notebooks for detailed implementation notes
- Refer to `architecture.jpeg` for visual overview
- Check Unity Catalog for table lineage and metadata

## 🤝 Contributing

Contributions welcome! Please:
1. Fork the repository
2. Create a feature branch
3. Commit improvements
4. Submit a pull request

## 📝 License

[Specify your license here]

## 👤 Author

[Your Name/Organization]

## 📧 Support

For issues, questions, or suggestions, please open a GitHub issue.

---

**Last Updated:** June 2026  
**Project Status:** Active Development
