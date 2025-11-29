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
                            WHERE CAST(\"year\" AS INTEGER) BETWEEN 2017 AND 2021;")



dbDisconnect(con)

tmpcon = dbConnect(duckdb(), "cdpr_rawdata.duckdb")

test = dbGetQuery(tmpcon, "SELECT * FROM udc18_01;")

dbDisconnect(tmpcon)
