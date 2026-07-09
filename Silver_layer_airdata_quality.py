## This file is use to clean data and load into silver layer
## 1. Remove special characters from column names
## 2. Replace nulls  with -200
## 3. convert string datatype into decimals and relace "," to "."
## 4. convert data time
## 5. Derive necessary columns
## 6. Check data in silver layer tables 

%python
# ====================== SILVER LAYER ======================
from pyspark.sql.functions import *
from pyspark.sql.types import *

print("=== Starting Silver Layer Processing ===")

# ---------------------- 1. AIR QUALITY ----------------------
air_bronze = spark.table("bronze_air_quality")

# Clean column names (remove special characters and spaces)
air_silver = air_bronze.select(
    [col(c).alias(c.replace("(", "_").replace(")", "").replace(".", "_")) for c in air_bronze.columns]
)

# Replace -200 with null (this dataset uses -200 for missing values)
air_silver = air_silver.replace(-200, None)

# Convert string columns with comma decimal separators to numeric
for col_name in ["CO_GT", "C6H6_GT", "T", "RH", "AH"]:
    air_silver = air_silver.withColumn(
        col_name,
        regexp_replace(col(col_name), ",", ".").cast("double")
    )

# Combine Date and Time into proper timestamp
air_silver = air_silver.withColumn(
    "timestamp",
    to_timestamp(concat(date_format(col("Date"), "yyyy-MM-dd"), lit(" "), col("Time")), "yyyy-MM-dd HH.mm.ss")
).drop("Date", "Time")

# Create useful derived columns
air_silver = air_silver.withColumn(
    "CO_Level",
    when(col("CO_GT") > 10, "Hazardous")
    .when(col("CO_GT") > 4, "Unhealthy")
    .when(col("CO_GT") > 2, "Moderate")
    .otherwise("Good")
)

air_silver = air_silver.withColumn(
    "has_high_pollution",
    when(col("CO_GT") > 5, 1).otherwise(0)
)

print("✅ Air Quality Silver processing done")

# ---------------------- 2. WATER QUALITY ----------------------
water_bronze = spark.table("bronze_water_quality")

# Clean column names
water_silver = water_bronze.select(
    [col(c).alias(c.replace(" ", "_").replace(".", "_")) for c in water_bronze.columns]
)

# Example: Create Water Quality categories (customize based on your columns)
# Assuming columns like: pH, Turbidity, Dissolved_Oxygen, etc.
water_silver = water_silver.withColumn(
    "pH_Category",
    when(col("pH") < 6.5, "Acidic")
    .when(col("pH") > 8.5, "Alkaline")
    .otherwise("Normal")
)

water_silver = water_silver.withColumn(
    "Water_Quality_Index",
    when((col("pH") >= 6.5) & (col("pH") <= 8.5) & (col("Turbidity__NTU") < 5), "Good")
    .when((col("pH") >= 6) & (col("pH") <= 9) & (col("Turbidity__NTU") < 10), "Fair")
    .otherwise("Poor")
)

print("✅ Water Quality Silver processing done")

# ---------------------- SAVE SILVER TABLES ----------------------
air_silver.write.format("delta").mode("overwrite").saveAsTable("silver_air_quality")
water_silver.write.format("delta").mode("overwrite").saveAsTable("silver_water_quality")


# Check Silver tables
print("Silver Air Quality:")
spark.table("silver_air_quality").printSchema()

print("\nSilver Water Quality:")
spark.table("silver_water_quality").printSchema()

# Quick row count
print(f"\nSilver Air rows: {spark.table('silver_air_quality').count()}")
print(f"Silver Water rows: {spark.table('silver_water_quality').count()}")
print("\n✅ Silver Layer completed successfully!")
print("Tables created: silver_air_quality, silver_water_quality")
