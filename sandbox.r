library(DBI)
library(duckdb)

con = dbConnect(duckdb(), "cdpr_combined.duckdb")

# list all tables in the database
tables = dbListTables(con)

# get tracking table entries
trk_tbls = dbGetQuery(con, "SELECT tables FROM track_tables;")$tables

# print all column names inside 'cdpr_combined' table
colnames <- dbGetQuery(con, "PRAGMA table_info('cdpr_combined')")$name

# example query: get all records from years 2018-2022
comb_tbl = dbGetQuery(con, "SELECT * 
                            FROM cdpr_combined
                            WHERE CAST(\"year\" AS INTEGER) BETWEEN 2020 AND 2021;")

# Now run your interpolated query
chem_val  <- "1601"
start_yr  <- 2019   # keep these as integers
end_yr    <- 2021

sql <- sqlInterpolate(con, "
  SELECT *
  FROM cdpr_combined
  WHERE (chem_code = ?chem_val OR chemical_code = ?chem_val)
    AND CAST(\"year\" AS INTEGER) BETWEEN ?start_yr AND ?end_yr;
", chem_val = chem_val, start_yr = start_yr, end_yr = end_yr)

tbl <- dbGetQuery(con, sql)

test_qry = "SELECT *
            FROM cdpr_combined
            WHERE CAST(\"year\" AS INTEGER) = 2021;"

test = dbGetQuery(con, test_qry)

test = dbGetQuery(con, "SELECT DISTINCT year FROM cdpr_combined;")
dbDisconnect(con)
