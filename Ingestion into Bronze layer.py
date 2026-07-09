##  1) Intall Kaggle and authenticate Kaggle API
##  2)  Create Unity catalog volume in databricks and download datasets from kaggle
%python
%pip install kaggle

# ====================== KAGGLE AUTHENTICATION (FIXED) ======================
import os

# Paste your full token here (starts with KGAT_)
os.environ['KAGGLE_API_TOKEN'] = ' ENTER YOUR API TOKEN HERE '

from kaggle.api.kaggle_api_extended import KaggleApi

api = KaggleApi()
api.authenticate()

print("✅ Kaggle API authenticated successfully!")

# Create volume and download datasets
import os

# Create Unity Catalog volume for data storage
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.default.environmental_data")

data_path = "/Volumes/workspace/default/environmental_data"
os.chdir(data_path)

# Download datasets
api.dataset_download_files('fedesoriano/air-quality-data-set', path='.', unzip=True)
print("✅ Air Quality dataset downloaded")

api.dataset_download_files('ziya07/water-quality-and-pollution-monitoring-dataset', path='.', unzip=True)

display(dbutils.fs.ls("/Volumes/workspace/default/environmental_data"))
print("✅ Water Quality dataset downloaded")
