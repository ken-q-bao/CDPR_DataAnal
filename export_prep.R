library(duckdb)
library(DBI)

# Connect to your local heavy database
con_local = dbConnect(duckdb(), dbdir = "cdpr_combined.duckdb")

# Create a local directory to hold the partitioned files
if(exists("cdpr_parquet_export")) {
  message("Directory 'cdpr_parquet_export' already exists. Will not overwrite existing data.")
} else {
  message("Creating directory 'cdpr_parquet_export' for partitioned export.")
  dir.create("cdpr_parquet_export")
}

tables = dbListTables(con_local)
cols = sort(dbListFields(con_local, tables[1]))

# --- MEMORY & OUT-OF-CORE OPTIMIZATIONS ---

# 6 GB is too large to process completely on my computer's RAM
# 
# 1. Create ONE local project folder for temporary memory spilling
if(exists("duckdb_temp")) {
  message("Directory 'duckdb_temp' already exists. Will not overwrite existing data.")
} else {
  message("Creating directory 'duckdb_temp' for temporary memory spilling.")
  dir.create("duckdb_temp", showWarnings = FALSE)
}


# 2. Force an absolute path so DuckDB doesn't lose track of the directory
project_temp_path = file.path(getwd(), "duckdb_temp")
dbExecute(con_local, sprintf("SET temp_directory = '%s';", project_temp_path))

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

# which years do we have? Based on files in cdpr_parquet_export
existing_folders = list.dirs(path = "cdpr_parquet_export", full.names = F, recursive = F)
yr_have = gsub("year=", "", existing_folders)

# which years do we need to export and partition?
yr_need = setdiff(yr_db, yr_have)

# Export and partition the data. 
# NOTE: Changed partitions to (year, county_code) to avoid over-partitioning crash.
if (length(yr_need) == 0) {
  message("All years are already up to date in 'cdpr_parquet_export'. Nothing to do!")
} else {
  message("Found new data. Exporting missing years: ", paste(yr_need, collapse = ", "))
  
  for (yr in yr_need) {
  dbExecute(con_local, sprintf("
    COPY (
      SELECT * FROM cdpr_combined WHERE year = %s
    )
    TO 'cdpr_parquet_export'
    (FORMAT PARQUET, PARTITION_BY (year), OVERWRITE TRUE);
  ", yr))
}

  message("Incremental export complete!")
}

dbDisconnect(con_local)
