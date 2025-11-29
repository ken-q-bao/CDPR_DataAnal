library(DBI)
library(duckdb)
library(progress)   # for a nice progress bar

con_raw = dbConnect(duckdb(), "cdpr_rawdata.duckdb")
tables_raw = dbListTables(con_raw)
dbDisconnect(con_raw)

con_combined = dbConnect(duckdb(), "cdpr_combined.duckdb")

# create list of all tables in combined database
tables_com = dbListTables(con_combined)

# create tracking table if it doesn't exist
if (length(tables_com) == 0) {
  dbExecute(con_combined, "CREATE TABLE track_tables (tables TEXT);")
}

# attach raw database
dbExecute(con_combined, "ATTACH 'cdpr_rawdata.duckdb' AS raw;")

# get all unique column names across all raw tables
all_cols <- character()

for (t in tables_raw) {
  cols <- dbGetQuery(con_combined, sprintf("
    PRAGMA table_info('raw.%s');
  ", t))$name
  
  all_cols <- union(all_cols, cols)
}

# keep a stable order
all_cols <- sort(all_cols)

# initialize progress bar
pb = progress_bar$new(
  format = "Processing tables [:bar] :percent (:current/:total)",
  total = length(tables_raw),
  clear = FALSE, width = 60
)

for (t in tables_raw) {
  old_tables = dbGetQuery(con_combined, "SELECT tables FROM track_tables;")$tables
  
  if (length(old_tables) == 0) {

    # first time setup: create combined table with all columns
    cols_def <- paste(sprintf("%s TEXT", all_cols), collapse = ", ")

    dbExecute(con_combined, sprintf("
      CREATE TABLE cdpr_combined (%s);
    ", cols_def))
    
    ####### data insertion
    # get columns of source table
    cols_source <- dbGetQuery(con_combined, sprintf("PRAGMA table_info('raw.%s');", t))$name

    # build select statement with NULLs for missing columns
    select_list <- sapply(all_cols, function(c) {
      if (c %in% cols_source) {
        c
      } else {
        sprintf("NULL AS %s", c)
      }
    })

    # build final select SQL
    select_sql <- paste(select_list, collapse = ", ")

    # insert data into combined table
    dbExecute(con_combined, sprintf("
      INSERT INTO cdpr_combined (%s)
      SELECT %s FROM raw.%s;
    ", paste(all_cols, collapse = ", "), select_sql, t))

    dbExecute(con_combined, sprintf("INSERT INTO track_tables (tables) VALUES ('%s');", t))
    message("processed: ", t)
    
  } else if (t %in% old_tables) {
    message("skipped: ", t)
    
  } else {
    # insert new table name into tracking table
    dbExecute(con_combined, sprintf("INSERT INTO track_tables (tables) VALUES ('%s');", t))

    # get columns of source table
    cols_source <- dbGetQuery(con_combined, sprintf("PRAGMA table_info('raw.%s');", t))$name

    # build select statement with NULLs for missing columns
    select_list <- sapply(all_cols, function(c) {
      if (c %in% cols_source) {
        c
      } else {
        sprintf("NULL AS %s", c)
      }
    })
    
    # build final select SQL
    select_sql <- paste(select_list, collapse = ", ")

    # insert data into combined table
    dbExecute(con_combined, sprintf("
      INSERT INTO cdpr_combined (%s)
      SELECT %s FROM raw.%s;
    ", paste(all_cols, collapse = ", "), select_sql, t))
        message("processed: ", t)
  }
  
  # update progress bar
  pb$tick()
}

dbDisconnect(con_combined)
