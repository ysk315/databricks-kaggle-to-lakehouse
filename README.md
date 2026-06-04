# Databricks_Process_files_from_Kaggle
In this project, two csv files are downloaded from kaggle(through API) into databricks volume . cleaned data and aggregated data for further analysis

1. Data is loaded into databricks volume using kaggle API
   A. Loginto your kaggle account and create a Token
   B. Create a volme in databricks workspace
   C.Improt those files from Kaggle using API 
3. Ingested data into bronze layer
     
4. Cleaned and transformed data in silver layer
    A.Clean column names that has special characters ike . and (. replace it with _
    B. Replace -200 with Nulls
    C. Derive new columns based on existing data
    D.  Combine data and time to form a new value.
    E. Change columns data with has , instead of . ( Examples 2,6 into 2.6)
6. Aggregated data is loaded into gold layer
