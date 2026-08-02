# DEVELOPMENT NOTES:
#     - This script is designed to export data from a local DuckDB database to Parquet files, partitioned by year.
#     - It checks for existing Parquet files to avoid overwriting data and only exports missing years.
#     - The script does not partition by chem_code to prevent crashes due to over-partitioning

library(duckdb)
library(DBI)

# Connect to your local heavy database
con_local = dbConnect(duckdb(), dbdir = "cdpr_combined.duckdb")

# Create a local directory to hold the partitioned files
export_dir = normalizePath("cdpr_parquet_export", winslash = "/", mustWork = F) # Use forward slashes for DuckDB

# use absolute path to avoid ambiguity in DuckDB
if(dir.exists(export_dir)) {
  message(sprintf("Directory '%s' already exists. Will not overwrite existing data.", export_dir))
} else {
  message(sprintf("Creating directory '%s' for partitioned export.", export_dir))
  dir.create(export_dir, recursive = TRUE)
}

tables = dbListTables(con_local)
cols = sort(dbListFields(con_local, tables[1]))

# --- MEMORY & OUT-OF-CORE OPTIMIZATIONS ---

# 6 GB is too large to process completely on my computer's RAM
# 
# 1. Create ONE local project folder for temporary memory spilling
temp_path = normalizePath("duckdb_temp", mustWork = FALSE, winslash = "/") # Use forward slashes for DuckDB

if(dir.exists(temp_path)) {
  message(sprintf("Directory '%s' already exists. Will not overwrite existing data.", temp_path))
} else {
  message(sprintf("Creating directory '%s' for temporary memory spilling.", temp_path))
  dir.create(temp_path, recursive = TRUE)
}

# 2. Force an absolute path so DuckDB doesn't lose track of the directory
dbExecute(con_local, sprintf("SET temp_directory = '%s';", temp_path))

# 3. CRITICAL: Allow DuckDB to re-order rows to massively drop RAM usage
dbExecute(con_local, "SET preserve_insertion_order = false;")

# 4. Restrict threads slightly to lower simultaneous data chunk memory overhead
parallel::detectCores(logical = FALSE) # 8 cores on my machine
dbExecute(con_local, "SET threads = 2;")

# 5. Limit DuckDB's memory footprint to trigger smooth spilling to disk
dbExecute(con_local, "SET max_memory = '4GB';")

# --- CONDITIONAL EXPORT PROCESS ---

# which years are in the duckdb? Based on cdpr_combined.duckdb
yr_db = dbGetQuery(con_local, "SELECT DISTINCT year FROM cdpr_combined")[[1]]

# which years do we have? Based on actual parquet files in cdpr_parquet_export
parquet_files = list.files(path = export_dir, recursive = TRUE, pattern = "\\.parquet$", full.names = FALSE)

if (length(parquet_files) > 0) {
  yr_have = unique(gsub("^year=", "", dirname(parquet_files)))
} else {
  yr_have = character(0)
}

# which years do we need to export and partition?
yr_need = sort(setdiff(yr_db, yr_have))

# Export and partition the data - by year only. 
# NOTE: Changed partitions to (year) to avoid over-partitioning crash.
if (length(yr_need) == 0) {
  message("All years are already up to date in 'cdpr_parquet_export'. Nothing to do!")
} else {
  message("Found new data. Exporting missing years: ", paste(yr_need, collapse = ", "))
  
  for (yr in yr_need) {
    yr_literal = dbQuoteString(con_local, as.character(yr)) # quote the year to avoid SQL injection issues
    n_rows = dbGetQuery(con_local, sprintf("SELECT COUNT(*) AS n_rows FROM cdpr_combined WHERE year = %s", yr_literal))[[1]]
    message(sprintf("Exporting year %s (%s row(s))", yr, n_rows))

    sql = sprintf(
      "COPY (SELECT * FROM cdpr_combined WHERE year = %s) TO '%s' (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE_OR_IGNORE TRUE);",
      yr_literal, export_dir
    )
    dbExecute(con_local, sql)
  }

  message("Incremental export complete!")
}

dbDisconnect(con_local)


# library(arrow)

# ds = open_dataset("cdpr_parquet_export", partitioning = hive_partition())
# ds |> dplyr::group_by(year) |> dplyr::summarise(n = dplyr::n())
