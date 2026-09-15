#  EchoChain — Circular Economy & Lifecycle Analytics
EchoChain: Circular Economy and Secondary Market Lifecycle Analytics using Scrapy, Databricks, PySpark, and Power BI.

##  Project Overview

**EchoChain** is a circular economy analytics project designed to analyze the lifecycle of manufactured products and identify opportunities for **refurbishment, resale, reuse, and secondary-market recovery**.

The project combines internal manufacturing data with secondary-market product listings to understand product lifecycle, depreciation, and circularity opportunities.

The pipeline uses **Scrapy, Databricks Delta Lake, PySpark, and Microsoft Power BI** to collect, process, analyze, and visualize the data.

---

##  Problem Statement

Traditional manufacturing analytics mainly focus on the primary product lifecycle and do not provide enough visibility into what happens after products leave the primary market.

Secondary-market listings contain valuable information about:

- Product resale value
- Product condition
- Depreciation
- Refurbishment opportunities
- Reuse potential
- Secondary-market demand

However, this information is often unstructured and difficult to connect with internal manufacturing data.

**EchoChain** addresses this problem by combining secondary-market data with internal product information and applying data engineering and analytics techniques to support circular economy decisions.

---

##  Objectives

The main objectives of EchoChain are:

- Collect secondary-market product data using web scraping.
- Process and organize raw data using Databricks.
- Build a Bronze–Silver–Gold Delta Lake architecture.
- Match secondary-market listings with internal product/SKU information.
- Apply fuzzy matching techniques using PySpark.
- Calculate product depreciation and circularity metrics.
- Identify products with higher refurbishment and resale potential.
- Build an executive Power BI dashboard for business insights.

---

##  Project Architecture

```text
Secondary Market Websites
          ↓
       Scrapy
          ↓
     Raw Dataset
          ↓
   Databricks / Delta Lake
          ↓
       Bronze Layer
          ↓
       Silver Layer
          ↓
        PySpark
          ↓
    Fuzzy Matching
          ↓
        Gold Layer
          ↓
   Circularity Metrics
          ↓
       Power BI
          ↓
   Business Insights
