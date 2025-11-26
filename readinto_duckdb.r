# Purpose: Read in all .txt files from cdpr_data_unzip folder into cdpr_rawdata.duckdb
# Date: June 12, 2024
# Details: reads in .txt files mostly as-is, with all columns as varchar to avoid type issues.
#          Uses duckdb's read_csv_auto function for faster reading and writing to duckdb.
#          skips files that are already in the duckdb database to avoid duplication.
#          If initial run, total run time is about 3 hrs.
#          Total file size of cdpr_rawdata.duckdb is about 5.66 GB.
library(duckdb)
library(readr)

# get list of all .txt files in cdpr_data_unzip folder
files = list.files(path = "cdpr_data_unzip", pattern = ".txt", full.names = TRUE)
files_basenames = gsub("\\.txt$", "", basename(files))

#################### FASTER APPROACH ##########################
# create list of queries to load each .txt file into duckdb
queries = lapply(files, function(f) {
  table_name = tools::file_path_sans_ext(basename(f))
  qry = sprintf("CREATE TABLE %s AS SELECT * FROM read_csv_auto( 
                                                      '%s', 
                                                      header=TRUE, 
                                                      all_varchar=TRUE, 
                                                      strict_mode=FALSE,
                                                      max_line_size = 8000000);", 
                table_name, f)
  return(qry)
})

con = dbConnect(duckdb::duckdb(), "cdpr_rawdata.duckdb")

# only run queries for tables that do not already exist in the database
# get current tables within cdpr_rawdata.duckdb
current_db_files = dbListTables(con)

## filter queries to only include those for tables that do not already exist in the database
queries = queries[!files_basenames %in% current_db_files]

lapply(queries, function(q) {
  dbExecute(con, q)
})
dbDisconnect(con)

#################### ALTERNATIVE SLOWER APPROACH ########################
# # first read in the .txt files onto memory 1 at a time
# # then write to duckdb
# # utilize vectorization (lapply) to loop through files
# 
# test = read_delim(
#   "cdpr_data_unzip/pur80.txt", 
#   delim = "\t",
#   col_names = TRUE
# )
# 
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