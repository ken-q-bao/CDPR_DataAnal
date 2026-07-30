library(duckdb)
library(DBI)

# Connect to your local heavy database
con_local = dbConnect(duckdb(shared_home = F), dbdir = "cdpr_combined.duckdb")

# Create a local directory to hold the partitioned files
dir.create("cdpr_parquet_export")

tables = dbListTables(con_local)
cols = sort(dbListFields(con_local, tables[1]))

# --- MEMORY & OUT-OF-CORE OPTIMIZATIONS ---
# 6 GB is too large to process completely on my computer's RAM
# 
# 1. Create ONE local project folder for temporary memory spilling
dir.create("duckdb_temp", showWarnings = FALSE)

# 2. Force an absolute path so DuckDB doesn't lose track of the directory
project_temp_path = file.path(getwd(), "duckdb_temp")
dbExecute(con_local, sprintf("SET temp_directory = '%s';", project_temp_path))

# 3. CRITICAL: Allow DuckDB to re-order rows to massively drop RAM usage
dbExecute(con_local, "SET preserve_insertion_order = false;")

# 4. Restrict threads slightly to lower simultaneous data chunk memory overhead
dbExecute(con_local, "SET threads = 1;")

# 5. Limit DuckDB's memory footprint to trigger smooth spilling to disk
dbExecute(con_local, "SET max_memory = '8GB';")

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
  
  # Format the missing years into a SQL-friendly list (e.g., "2025, 2026")
  yr_list = paste(yr_need, collapse = ", ")
  
  # Build a query that ONLY selects the missing years
  # CRITICAL: Change OVERWRITE TRUE to OVERWRITE FALSE (or remove it) so DuckDB 
  # safely adds the new year folders without wiping out your existing years!
  export_query <- sprintf("
    COPY (SELECT * FROM cdpr_combined WHERE year IN (%s)) 
    TO 'cdpr_parquet_export' 
    (FORMAT PARQUET, PARTITION_BY (year, chem_code), OVERWRITE TRUE);
  ", yr_list)
  
  # Execute the targeted export
  dbExecute(con_local, export_query)
  message("Incremental export complete!")
}

dbDisconnect(con_local)