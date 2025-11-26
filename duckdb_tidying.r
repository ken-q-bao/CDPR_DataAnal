library(duckdb)

con = dbConnect(duckdb::duckdb(), "cdpr_rawdata.duckdb")

# get current tables within cdpr_rawdata.duckdb
current_db_files = dbListTables(con)

# define the table union query
query = paste0(
    "SELECT * FROM ",
    current_db_files,
    "\nUNION ALL BY NAME"
)

dbDisconnect(con)
