# Databricks notebook source
# MAGIC %md
# MAGIC ECHOCHAIN **CIRCULAR ECONOMY AND SECONDARY MARKET LIFECYCLE ANALYTIC**

# COMMAND ----------

# Read the BOM table
bom_df = spark.table("workspace.default.bom_data")

# Show the data
display(bom_df)

# COMMAND ----------

# Read all three EchoChain tables

bom_df = spark.table("workspace.default.bom_data")
warranty_df = spark.table("workspace.default.warranty_data")
secondary_df = spark.table("workspace.default.secondary_market_data")

# Display row counts
print("BOM rows:", bom_df.count())
print("Warranty rows:", warranty_df.count())
print("Secondary Market rows:", secondary_df.count())

# COMMAND ----------

# Display the Warranty data
display(warranty_df)

# COMMAND ----------

# Display the Secondary Market data
display(secondary_df)

# COMMAND ----------

# Check the structure and data types of all three datasets

print("===== BOM DATA =====")
bom_df.printSchema()

print("\n===== WARRANTY DATA =====")
warranty_df.printSchema()

print("\n===== SECONDARY MARKET DATA =====")
secondary_df.printSchema()

# COMMAND ----------

### Check missing values

from pyspark.sql.functions import col, sum

def check_missing(df, name):
    print(f"\n===== {name} =====")
    df.select([
        sum(col(c).isNull().cast("int")).alias(c)
        for c in df.columns
    ]).show()

check_missing(bom_df, "BOM DATA")
check_missing(warranty_df, "WARRANTY DATA")
check_missing(secondary_df, "SECONDARY MARKET DATA")

# COMMAND ----------

print("BOM duplicate rows:", bom_df.count() - bom_df.dropDuplicates().count())
print("Warranty duplicate rows:", warranty_df.count() - warranty_df.dropDuplicates().count())
print("Secondary Market duplicate rows:", secondary_df.count() - secondary_df.dropDuplicates().count())

# COMMAND ----------

from pyspark.sql.functions import col, trim, upper, to_date

# =========================
# 1. Clean BOM data
# =========================

bom_clean = (
    bom_df
    .withColumn("SKU", upper(trim(col("SKU"))))
    .withColumn("Product_Name", trim(col("Product_Name")))
    .withColumn("Component_ID", upper(trim(col("Component_ID"))))
    .withColumn("Component_Name", trim(col("Component_Name")))
    .withColumn("Component_Category", trim(col("Component_Category")))
    .withColumn("Component_Cost", col("Component_Cost").cast("double"))
    .withColumn("Material_Type", trim(col("Material_Type")))
    .withColumn("Reusable", trim(col("Reusable")))
)

# =========================
# 2. Clean Warranty data
# =========================

warranty_clean = (
    warranty_df
    .withColumn("Warranty_ID", upper(trim(col("Warranty_ID"))))
    .withColumn("SKU", upper(trim(col("SKU"))))
    .withColumn("Component_ID", upper(trim(col("Component_ID"))))
    .withColumn("Component_Name", trim(col("Component_Name")))
    .withColumn("Failure_Type", trim(col("Failure_Type")))
    .withColumn("Claim_Date", to_date(col("Claim_Date")))
    .withColumn("Claim_Status", trim(col("Claim_Status")))
    .withColumn("Repair_Cost", col("Repair_Cost").cast("double"))
    .withColumn("Replacement_Required", trim(col("Replacement_Required")))
)

# =========================
# 3. Clean Secondary Market data
# =========================

secondary_clean = (
    secondary_df
    .withColumn("Listing_ID", upper(trim(col("Listing_ID"))))
    .withColumn("Listing_Title", trim(col("Listing_Title")))
    .withColumn("Product_Name", trim(col("Product_Name")))
    .withColumn("Listed_Price", col("Listed_Price").cast("double"))
    .withColumn("Currency", upper(trim(col("Currency"))))
    .withColumn("Condition", trim(col("Condition")))
    .withColumn("Seller_Type", trim(col("Seller_Type")))
    .withColumn("Listing_Date", to_date(col("Listing_Date")))
    .withColumn("Marketplace", trim(col("Marketplace")))
    .withColumn("Location", trim(col("Location")))
)

print("Cleaning completed successfully.")

# COMMAND ----------

### verify the cleaning data

print("===== CLEANED BOM =====")
bom_clean.printSchema()

print("\n===== CLEANED WARRANTY =====")
warranty_clean.printSchema()

print("\n===== CLEANED SECONDARY MARKET =====")
secondary_clean.printSchema()

# COMMAND ----------

# Save cleaned data as Silver tables

bom_clean.write.mode("overwrite").saveAsTable(
    "workspace.default.silver_bom"
)

warranty_clean.write.mode("overwrite").saveAsTable(
    "workspace.default.silver_warranty"
)

secondary_clean.write.mode("overwrite").saveAsTable(
    "workspace.default.silver_secondary_market"
)

print("Silver tables created successfully.")

# COMMAND ----------

# Verify Silver tables

print("Silver BOM rows:", spark.table("workspace.default.silver_bom").count())
print("Silver Warranty rows:", spark.table("workspace.default.silver_warranty").count())
print("Silver Secondary Market rows:", spark.table("workspace.default.silver_secondary_market").count())

# COMMAND ----------

# Inspect marketplace product titles

display(
    secondary_clean.select(
        "Listing_ID",
        "Listing_Title",
        "Product_Name"
    )
)

# COMMAND ----------

### Now let's implement the fuzzy matching in PySpark
from pyspark.sql.functions import col, lower, regexp_replace, trim

# Prepare marketplace titles for matching
secondary_match = (
    secondary_clean
    .withColumn(
        "Title_Clean",
        lower(
            regexp_replace(
                col("Listing_Title"),
                "[^a-zA-Z0-9 ]",
                " "
            )
        )
    )
)

display(
    secondary_match.select(
        "Listing_ID",
        "Listing_Title",
        "Title_Clean"
    )
)

# COMMAND ----------

# Create internal SKU reference table

sku_reference = (
    bom_clean
    .select("SKU", "Product_Name")
    .dropDuplicates()
)

display(sku_reference)

# COMMAND ----------

### run the fuzzy match
from pyspark.sql.functions import col, lower, levenshtein, row_number
from pyspark.sql.window import Window

# Prepare marketplace titles
marketplace = (
    secondary_match
    .select(
        "Listing_ID",
        "Listing_Title",
        "Title_Clean"
    )
)

# Prepare internal product names
products = (
    sku_reference
    .withColumn(
        "Product_Clean",
        lower(col("Product_Name"))
    )
)

# Compare every marketplace title with every internal product
matches = (
    marketplace
    .crossJoin(products)
    .withColumn(
        "Distance",
        levenshtein(
            col("Title_Clean"),
            col("Product_Clean")
        )
    )
)

# Select the closest product for each marketplace listing
window_spec = Window.partitionBy("Listing_ID").orderBy(col("Distance"))

fuzzy_matches = (
    matches
    .withColumn(
        "Rank",
        row_number().over(window_spec)
    )
    .filter(col("Rank") == 1)
    .select(
        "Listing_ID",
        "Listing_Title",
        "SKU",
        "Product_Name",
        "Distance"
    )
    .orderBy("Listing_ID")
)

display(fuzzy_matches)

# COMMAND ----------

from pyspark.sql.functions import when

fuzzy_matches_corrected = (
    fuzzy_matches
    .withColumn(
        "Matched_SKU",
        when(
            col("Listing_Title").rlike("(?i)5420"),
            "DELL5420"
        )
        .when(
            col("Listing_Title").rlike("(?i)840\\s*G7"),
            "HP840G7"
        )
        .when(
            col("Listing_Title").rlike("(?i)T14"),
            "LENOVO_T14"
        )
        .otherwise(col("SKU"))
    )
)

display(
    fuzzy_matches_corrected.select(
        "Listing_ID",
        "Listing_Title",
        "SKU",
        "Matched_SKU",
        "Distance"
    ).orderBy("Listing_ID")
)

# COMMAND ----------

# Create cleaned marketplace data with matched SKU

secondary_matched = (
    secondary_match
    .join(
        fuzzy_matches_corrected.select(
            "Listing_ID",
            "Matched_SKU"
        ),
        on="Listing_ID",
        how="left"
    )
)

display(
    secondary_matched.select(
        "Listing_ID",
        "Listing_Title",
        "Matched_SKU",
        "Listed_Price",
        "Condition",
        "Seller_Type",
        "Listing_Date",
        "Marketplace",
        "Location"
    ).orderBy("Listing_ID")
)

# COMMAND ----------

# Join marketplace listings with BOM components

gold_marketplace_bom = (
    secondary_matched
    .join(
        bom_clean,
        secondary_matched["Matched_SKU"] == bom_clean["SKU"],
        how="left"
    )
    .select(
        secondary_matched["Listing_ID"],
        secondary_matched["Listing_Title"],
        secondary_matched["Matched_SKU"],
        secondary_matched["Listed_Price"],
        secondary_matched["Currency"],
        secondary_matched["Condition"],
        secondary_matched["Seller_Type"],
        secondary_matched["Listing_Date"],
        secondary_matched["Marketplace"],
        secondary_matched["Location"],
        bom_clean["Component_ID"],
        bom_clean["Component_Name"],
        bom_clean["Component_Category"],
        bom_clean["Component_Cost"],
        bom_clean["Material_Type"],
        bom_clean["Reusable"]
    )
)

display(gold_marketplace_bom)

# COMMAND ----------

### verify the gold marketplace
print("Gold Marketplace + BOM rows:", gold_marketplace_bom.count())

# COMMAND ----------

print("Unique listings:", gold_marketplace_bom.select("Listing_ID").distinct().count())
print("Unique SKUs:", gold_marketplace_bom.select("Matched_SKU").distinct().count())
print("Unique components:", gold_marketplace_bom.select("Component_ID").distinct().count())

# COMMAND ----------

# Join warranty information with marketplace + BOM data

gold_warranty = (
    gold_marketplace_bom
    .join(
        warranty_clean,
        gold_marketplace_bom["Component_ID"] == warranty_clean["Component_ID"],
        how="left"
    )
    .select(
        gold_marketplace_bom["Listing_ID"],
        gold_marketplace_bom["Listing_Title"],
        gold_marketplace_bom["Matched_SKU"],
        gold_marketplace_bom["Listed_Price"],
        gold_marketplace_bom["Condition"],
        gold_marketplace_bom["Seller_Type"],
        gold_marketplace_bom["Listing_Date"],
        gold_marketplace_bom["Marketplace"],
        gold_marketplace_bom["Location"],
        gold_marketplace_bom["Component_ID"],
        gold_marketplace_bom["Component_Name"],
        gold_marketplace_bom["Component_Category"],
        gold_marketplace_bom["Component_Cost"],
        gold_marketplace_bom["Material_Type"],
        gold_marketplace_bom["Reusable"],
        warranty_clean["Warranty_ID"],
        warranty_clean["Failure_Type"],
        warranty_clean["Claim_Date"],
        warranty_clean["Claim_Status"],
        warranty_clean["Repair_Cost"],
        warranty_clean["Replacement_Required"]
    )
)

display(gold_warranty)

# COMMAND ----------

print("Gold rows after warranty join:", gold_warranty.count())
print("Unique listings:", gold_warranty.select("Listing_ID").distinct().count())
print("Unique components:", gold_warranty.select("Component_ID").distinct().count())
print("Warranty claims:", gold_warranty.select("Warranty_ID").distinct().count())

# COMMAND ----------

# Check warranty records and identify the 16 claims

display(
    warranty_clean
    .select(
        "Warranty_ID",
        "SKU",
        "Component_ID",
        "Component_Name",
        "Failure_Type",
        "Claim_Date",
        "Repair_Cost",
        "Replacement_Required"
    )
    .orderBy("Warranty_ID")
)

# COMMAND ----------

### create a component-level warrent summary
from pyspark.sql.functions import count, sum, max, when

component_warranty_summary = (
    warranty_clean
    .groupBy(
        "SKU",
        "Component_ID"
    )
    .agg(
        count("Warranty_ID").alias("Warranty_Claim_Count"),
        sum("Repair_Cost").alias("Total_Repair_Cost"),
        max(
            when(col("Replacement_Required") == "Yes", 1).otherwise(0)
        ).alias("Replacement_Required_Flag")
    )
)

display(
    component_warranty_summary
    .orderBy("SKU", "Component_ID")
)

# COMMAND ----------

# Create component-level Gold dataset

gold_component = (
    bom_clean
    .join(
        component_warranty_summary,
        on=["SKU", "Component_ID"],
        how="left"
    )
    .fillna({
        "Warranty_Claim_Count": 0,
        "Total_Repair_Cost": 0,
        "Replacement_Required_Flag": 0
    })
)

display(
    gold_component
    .select(
        "SKU",
        "Component_ID",
        "Component_Name",
        "Component_Category",
        "Component_Cost",
        "Material_Type",
        "Reusable",
        "Warranty_Claim_Count",
        "Total_Repair_Cost",
        "Replacement_Required_Flag"
    )
    .orderBy("SKU", "Component_ID")
)

# COMMAND ----------

from pyspark.sql.functions import count, sum, when, round

product_circularity = (
    gold_component
    .groupBy("SKU")
    .agg(
        count("Component_ID").alias("Total_Components"),

        sum(
            when(col("Reusable") == "Yes", 1).otherwise(0)
        ).alias("Reusable_Components"),

        sum("Component_Cost").alias("Total_Component_Cost"),

        sum(
            when(col("Reusable") == "Yes", col("Component_Cost"))
            .otherwise(0)
        ).alias("Reusable_Component_Cost"),

        sum("Warranty_Claim_Count").alias("Total_Warranty_Claims"),

        sum("Total_Repair_Cost").alias("Total_Repair_Cost")
    )
    .withColumn(
        "Circularity_Score",
        round(
            col("Reusable_Components") /
            col("Total_Components") * 100,
            2
        )
    )
    .orderBy("SKU")
)

display(product_circularity)

# COMMAND ----------

from pyspark.sql.functions import avg, min, max, count, round

secondary_summary = (
    secondary_matched
    .groupBy("Matched_SKU")
    .agg(
        count("Listing_ID").alias("Listing_Count"),

        round(avg("Listed_Price"), 2)
        .alias("Average_Resale_Price"),

        round(min("Listed_Price"), 2)
        .alias("Minimum_Resale_Price"),

        round(max("Listed_Price"), 2)
        .alias("Maximum_Resale_Price"),

        round(
            avg(
                when(
                    col("Condition") == "Used",
                    col("Listed_Price")
                )
            ),
            2
        ).alias("Average_Used_Price"),

        round(
            avg(
                when(
                    col("Condition") == "Refurbished",
                    col("Listed_Price")
                )
            ),
            2
        ).alias("Average_Refurbished_Price")
    )
    .orderBy("Matched_SKU")
)

display(secondary_summary)

# COMMAND ----------

from pyspark.sql.functions import col, round

product_analysis = (
    product_circularity
    .join(
        secondary_summary,
        product_circularity["SKU"] == secondary_summary["Matched_SKU"],
        how="left"
    )
    .drop("Matched_SKU")
    .withColumn(
        "Resale_vs_Component_Cost_Ratio",
        round(
            col("Average_Resale_Price") /
            col("Total_Component_Cost") * 100,
            2
        )
    )
    .withColumn(
        "Cost_Based_Depreciation_Percent",
        round(
            (
                col("Total_Component_Cost") -
                col("Average_Resale_Price")
            ) /
            col("Total_Component_Cost") * 100,
            2
        )
    )
    .orderBy("SKU")
)

display(product_analysis)

# COMMAND ----------

product_analysis.write.mode("overwrite").saveAsTable(
    "workspace.default.gold_product_analytics"
)

print("Gold product analytics table created successfully.")

# COMMAND ----------

gold_component.write.mode("overwrite").saveAsTable(
    "workspace.default.gold_component_lifecycle"
)

print("Gold component lifecycle table created successfully.")

# COMMAND ----------

print("Gold Product Analytics rows:",
      spark.table("workspace.default.gold_product_analytics").count())

print("Gold Component Lifecycle rows:",
      spark.table("workspace.default.gold_component_lifecycle").count())

# COMMAND ----------

