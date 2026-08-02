# Load the packages used for HTTP requests and JSON handling.
library(httr)
library(jsonlite)
library(digest)

# Backblaze B2 credentials for the target account.
key_id = "00544d7932c50750000000003"
app_key = "K005F42mZ0WeMUbyvK5KHyftAmMzxrA"

# Step 1: Authorize the account and retrieve the API endpoint and token.
auth = GET(
  "https://api.backblazeb2.com/b2api/v2/b2_authorize_account",
  authenticate(key_id, app_key),
  add_headers(Accept = "application/json")
)

# Check the status of the authorization request and handle errors.
auth_text = content(auth, as = "text", encoding = "UTF-8")

# Stop early if authorization fails so the error response is visible.
if (status_code(auth) != 200) {
  message("Backblaze auth failed with status ", status_code(auth))
  cat(auth_text, "\n")
  stop("Check your Backblaze key ID and application key.")
}

# Parse the authorization response into a list for later use.
auth_data = fromJSON(auth_text, simplifyDataFrame = FALSE)

# Print the authorization details for debugging.
cat("Auth response:\n")
print(auth_data)

# Step 2: Request a temporary upload URL for the target bucket.
bucket_id = "9484ddf7f913c20c95f00715"

upload_url = POST(
  paste0(auth_data$apiUrl, "/b2api/v2/b2_get_upload_url"),
  body = list(bucketId = bucket_id),
  encode = "json",
  add_headers(Authorization = auth_data$authorizationToken)
)

# Parse the upload URL response.
upload_data = content(upload_url, as = "parsed", type = "application/json")

# Step 3: Upload each new local file to the Backblaze B2 bucket using the temporary upload URL.

# Gather local files under the export folder.
local_files = list.files(
  "cdpr_parquet_export",
  pattern = "\\.parquet$",
  recursive = TRUE,
  full.names = FALSE
)

# Prefix inside the bucket for the uploaded parquet objects.
prefix = ""

# Convert local file paths to the object names used in Backblaze.
local_keys = paste0("parquet-exports/", gsub("\\\\", "/", local_files))

# List files already present in the bucket. Start without a prefix so we can inspect the real object names.
list_res = POST(
  paste0(auth_data$apiUrl, "/b2api/v2/b2_list_file_names"),
  body = list(
    bucketId = bucket_id,
    prefix = prefix,
    startFileName = "",
    maxFileCount = 1000
  ),
  encode = "json",
  add_headers(Authorization = auth_data$authorizationToken)
)

# Check the status of the list request and handle errors.
if (status_code(list_res) != 200) {
  message("Could not list files in the bucket. Status: ", status_code(list_res))
  cat(content(list_res, as = "text", encoding = "UTF-8"), "\n")
  stop("Check the bucket ID and authorization token.")
}

list_data = content(list_res, as = "parsed", type = "application/json")
remote_keys = vapply(list_data$files, function(x) x$fileName, character(1))

cat("Bucket object names:\n")
print(remote_keys)

# If the bucket contains objects under a different path, update prefix above and rerun.
# Determine which local files still need to be uploaded.
to_upload = sort(setdiff(local_keys, remote_keys))

cat("Local parquet files:", length(local_keys), "\n")
cat("Files already in bucket:", length(remote_keys), "\n")
cat("Files to upload:", length(to_upload), "\n")
print(to_upload)

# Upload each missing file to Backblaze B2.
for (key in to_upload) {
  rel_path = sub("^parquet-exports/", "", key)
  local_path = file.path("cdpr_parquet_export", rel_path)

  # Read the local file bytes.
  file_bytes = readBin(local_path, what = "raw", n = file.info(local_path)$size)

  upload_req = POST(
    upload_data$uploadUrl,
    body = file_bytes,
    add_headers(
      Authorization = upload_data$authorizationToken,
      `X-Bz-File-Name` = key,
      `Content-Type` = "application/octet-stream",
      `X-Bz-Content-Sha1` = digest::digest(file_bytes, algo = "sha1", serialize = FALSE)
    )
  )

  if (status_code(upload_req) != 200) {
    message("Upload failed for ", key)
    cat(content(upload_req, as = "text", encoding = "UTF-8"), "\n")
  } else {
    message("Uploaded: ", key)
  }
}

