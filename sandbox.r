library(DBI)
library(duckdb)

con = dbConnect(duckdb(), "cdpr_combined.duckdb")

# list all tables in the database
tables = dbListTables(con)

# get tracking table entries
trk_tbls = dbGetQuery(con, "SELECT tables FROM track_tables;")$tables

# print all column names inside 'cdpr_combined' table
colnames <- dbGetQuery(con, "PRAGMA table_info('cdpr_combined')")$name

# example query: get all records for a specific chemical code between certain years
chem_val  <- "1601" # paraquat dichloride
start_yr  <- 1980   # keep these as integers
end_yr    <- 2021

sql <- sqlInterpolate(con, "
  SELECT *
  FROM cdpr_combined
  WHERE (chem_code = ?chem_val OR chemical_code = ?chem_val)
    AND CAST(\"year\" AS INTEGER) BETWEEN ?start_yr AND ?end_yr;
", chem_val = chem_val, start_yr = start_yr, end_yr = end_yr)

tbl <- dbGetQuery(con, sql)

dbDisconnect(con)

times = dplyr::select(tbl, applic_time, year)
hist(as.numeric(dplyr::filter(times, year == 2021)$applic_time))
