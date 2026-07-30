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

# --- EXPORT PROCESS ---

# Export and partition the data. 
# NOTE: Changed partitions to (year, county_code) to avoid over-partitioning crash.
dbExecute(con_local, "
  COPY (SELECT * FROM cdpr_combined) 
  TO 'cdpr_parquet_export' 
  (FORMAT PARQUET, PARTITION_BY (year, chem_code), OVERWRITE TRUE);
")

dbDisconnect(con_local)