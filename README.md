# Databricks_Process_files_from_Kaggle
In this project, two csv files are downloaded from kaggle(through API) into databricks volume . cleaned data and aggregated data for further analysis

1. Data is loaded into databricks volume using kaggle API
   1. Loginto your kaggle account and create a Token
   2. Create a volme in databricks workspace
   3. Import files from Kaggle using API
  
3. Ingest data into bronze layer
     
4. Clean and transform data in silver layer.
   
     1.Clean column names that has special characters ike . and (. replace it with _)
   
    2. Replace -200 with Nulls
       
    3. Derive new columns based on existing data
     
    4.  Combine data and time to form a new value.
      
    5. Change columns data with has "," instead of ". " ( Examples 2,6 into 2.6)
   
6. Aggregated data is loaded into gold layer
