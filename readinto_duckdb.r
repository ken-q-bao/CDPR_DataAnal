library(duckdb)

# get list of all .txt files in cdpr_data_unzip folder
files = list.files(path = "cdpr_data_unzip", pattern = ".txt", full.names = TRUE)
files_basenames = gsub("\\.txt$", "", basename(files))

# create list of queries to load each .txt file into duckdb
queries = lapply(files, function(f) {
  table_name = tools::file_path_sans_ext(basename(f))
  qry = sprintf("CREATE TABLE %s AS SELECT * FROM read_text('%s');", 
                table_name, f)
  return(qry)
})

con = dbConnect(duckdb::duckdb(), "cdpr_rawdata.duckdb")

current_db_files = dbListTables(con)
# only run queries for tables that do not already exist in the database
## filter queries to only include those for tables that do not already exist in the database
queries = queries[!files_basenames %in% current_db_files]

lapply(queries, function(q) {
  dbExecute(con, q)
})



dbDisconnect(con)
