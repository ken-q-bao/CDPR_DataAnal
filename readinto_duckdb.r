library(duckdb)
library(readr)

# get list of all .txt files in cdpr_data_unzip folder
files = list.files(path = "cdpr_data_unzip", pattern = ".txt", full.names = TRUE)
files_basenames = gsub("\\.txt$", "", basename(files))

#################### ALTERNATIVE SLOWER APPROACH ########################
# # first read in the .txt files onto memory 1 at a time
# # then write to duckdb
# # utilize vectorization (lapply) to loop through files

# # define function to read in .txt file and write to duckdb
# read_and_write_to_duckdb = function(file_path, con) {
#   table_name = tools::file_path_sans_ext(basename(file_path))
#   data = read_table(
#     file_path,
#     col_names = TRUE
#   )
#   dbWriteTable(con, table_name, data)
# }

# con = dbConnect(duckdb::duckdb(), "cdpr_rawdata.duckdb")

# # get current tables within cdpr_rawdata.duckdb
# current_db_files = dbListTables(con)

# # keep only those files that are not already in the database
# files_to_load = files[!files_basenames %in% current_db_files]

# lapply(files_to_load, function(files) {
#   read_and_write_to_duckdb(files, con)
# })
# dbDisconnect(con)
##########################################################

#################### FASTER APPROACH ##########################
# create list of queries to load each .txt file into duckdb
queries = lapply(files, function(f) {
  table_name = tools::file_path_sans_ext(basename(f))
  qry = sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto('%s', header=TRUE, all_varchar=TRUE);", 
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
