## This file loads data of sir quality and water quality into gold layer tables
## Derived summarized columns for semantic layer

%python
# ====================== GOLD LAYER ======================
from pyspark.sql.functions import *

print("=== Building Gold Layer ===")

# ---------------------- 1. DAILY AIR QUALITY SUMMARY ----------------------
gold_daily_air = spark.table("silver_air_quality") \
    .withColumn("date", to_date(col("timestamp"))) \
    .groupBy("date") \
    .agg(
        round(avg("CO_GT"), 2).alias("avg_CO"),
        round(max("CO_GT"), 2).alias("max_CO"),
        round(avg("PT08_S1_CO"), 2).alias("avg_Sensor_CO"),
        sum("has_high_pollution").alias("high_pollution_hours"),
        count("*").alias("total_readings")
    ) \
    .withColumn(
        "pollution_status",
        when(col("high_pollution_hours") >= 6, "Critical")
        .when(col("high_pollution_hours") >= 3, "Warning")
        .otherwise("Normal")
    ) \
    .orderBy("date")

# Save Gold Table
gold_daily_air.write.format("delta").mode("overwrite").saveAsTable("gold_daily_air_summary")

print("✅ Created: gold_daily_air_summary")

# ---------------------- 2. DAILY WATER QUALITY SUMMARY ----------------------
gold_daily_water = spark.table("silver_water_quality") \
    .withColumn("date", to_date(col("Timestamp"))) \
    .groupBy("date") \
    .agg(
        round(avg("pH"), 2).alias("avg_pH"),
        round(avg("Turbidity__NTU"), 2).alias("avg_Turbidity"),
        round(avg("DO__mg/L"), 2).alias("avg_DO"),
        count("*").alias("total_readings")
    ) \
    .withColumn(
        "water_quality_status",
        when((col("avg_pH") >= 6.5) & (col("avg_pH") <= 8.5) & (col("avg_Turbidity") < 5), "Good")
        .when((col("avg_pH") >= 6) & (col("avg_pH") <= 9), "Fair")
        .otherwise("Poor")
    ) \
    .orderBy("date")

gold_daily_water.write.format("delta").mode("overwrite").saveAsTable("gold_daily_water_summary")

print("✅ Created: gold_daily_water_summary")

# ---------------------- 3. HIGH POLLUTION ALERTS (Optional but useful) ----------------------
gold_high_pollution = gold_daily_air.filter(col("high_pollution_hours") >= 3)

gold_high_pollution.write.format("delta").mode("overwrite").saveAsTable("gold_high_pollution_alerts")

print("✅ Created: gold_high_pollution_alerts")

print("\n🎉 Gold Layer completed successfully!")
