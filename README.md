# CDPR Data Analysis

This repository contains data analysis and research for CDPR (California Department of Pesticide Regulation).

## Project Structure

- `webscrape.r` scrapes the CAL PUR data website to download each year's zip files
- `unzip.r` unzips all of the folders obtained via `webscrape.r`.
- `readinto_duckdb.r` adds all unzipped .txt data files into duckdb on an as-is basis.
- `duckdb_tidying.r` tidy's up the data by combining everything into a single data frame into another duckdb.

## Getting Started
For first time users:
1. First run `webscrape.r` to obtain all of CAL PUR data for your chosen years. Downloaded zip files are contained in "cdpr_data_zip" folder. 
2. Then run `unzip.r` to unzip all of the folders from "cdpr_data_zip". This will only unzip the relevant content. If the directory "cdpr_data_unzip" does not exist, it will create it and unzip all files. If the "cdpr_data_unzip" does exist, it will unzip only the files that are not already in "cdpr_data_unzip".
3. Then run `readinto_duckdb.r` to add all of the unzipped .txt data files into a duckdb database. It will also avoid duplication of files that already exist.
4. Then run `duckdb_tidying.r` to tidy up the resulting duckdb data.

After running the four files in the correct order, users will have a final local database of about 6.85 gb that contains ALL available years of CA PUR data. Users can of course, adjust the desired years if desired in the `webscrape.r` file.

### R

Use the R Interactive terminal in VS Code.

## Notes

- The project uses a local Python virtual environment in the `test/` directory
- Interactive variable explorer available via Python Interactive Window in VS Code
