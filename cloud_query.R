library(arrow)
library(dplyr)
library(keyring)

# 0. Securely Setup Credentials - Uncomment and run ONCE then re-comment out
# key_set(service = "backblaze", username = "key_id")
# key_set(service = "backblaze", username = "app_key")

# 1. Define your Backblaze B2 credentials and S3 endpoint
key_id = key_get(service = "backblaze", username = "key_id")
app_key = key_get(service = "backblaze", username = "app_key")
b2_endpoint = "s3.us-east-005.backblazeb2.com"
bucket_name = "cdpr-parquet"
data_path = "parquet-exports"

# 2. Connect to the Backblaze B2 bucket using S3-compatible file system
b2_fs = S3FileSystem$create(
  access_key = key_id,
  secret_key = app_key,
  endpoint_override = b2_endpoint
)

# 3. Point Arrow to the Hive-partitioned directory root
dataset_uri = paste0(bucket_name, "/", data_path)
ds = open_dataset(b2_fs$path(dataset_uri), format = "parquet")

# 4. Explore data
# 4.1 Print column names and data types
print(ds$schema)

# 4.2 How man rows are there in the database?
# 132 million rows, nrow() will run slowly
# nrow(ds)

# 4.3 Explore column values - chem_code in this case
ds |> select(chem_code) |> distinct() |> collect()

# 5. Execute the query
query_result = ds %>%
  filter(year == 2020 & chem_code %in% c("1601","458")) %>%
  collect()  # Pulls ONLY the filtered data down into your R data.frame memory
