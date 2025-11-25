library(stringr)
library(utils)

# specify directory to unzip files to
dir = "cdpr_data_unzip/"
# if dir does not exist, create it
if(!dir.exists(dir)){
  dir.create(dir)
}

# get list of unzipped files
existing_files = list.files(path = "cdpr_data_unzip", pattern = ".txt")

# get list of all zip folders in cdpr_data_zip
zip_filenames = list.files(path = "cdpr_data_zip", pattern = ".zip")

# create list of all files contained in each zip folder
all_txtfiles = lapply(zip_filenames, function(x) unzip(paste0("cdpr_data_zip/", x), list = TRUE))

# find all filenames to be extracted for each zip folder
## pur1990 and after, data is saved as "udc"
## pur1989 and before, data is saved as "pur"
pattern89 = "^pur[0-9]{2}\\.txt"  # files for years 1989 and before
pattern90 = "^udc[0-9]{2}_[0-9]{2}\\.txt$"

## keep only txt files that match desired patterns
select_txtfiles = lapply(all_txtfiles, function(df) {
  df$Name[str_detect(df$Name, pattern89) | str_detect(df$Name, pattern90)]
})

# keep files in select_txtfiles that are not in existing_files
files_to_extract = lapply(select_txtfiles, function(files) {
  files[!(basename(files) %in% existing_files)]
})

# extract only files that are not already unzipped
n = length(unlist(files_to_extract))
pb = txtProgressBar(min = 0, max = n, style = 3)

for(i in 1:length(zip_filenames)){
  if(length(files_to_extract[[i]]) > 0){
    unzip(zipfile = paste0("cdpr_data_zip/", zip_filenames[i]),
          files = files_to_extract[[i]],
          exdir = "cdpr_data_unzip/")
  }
  setTxtProgressBar(pb, i)
}
close(pb)
