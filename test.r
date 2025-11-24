# purpose is to auto download missing data from CDPR
## linked here: https://files.cdpr.ca.gov/pub/outgoing/pur_archives/
## downloads zip files to cdpr_data_zip folder
library(rvest)
library(stringr)

# define url and parse the html of webpage to be scraped
url = "https://files.cdpr.ca.gov/pub/outgoing/pur_archives/"
webpage = read_html(url)

# links are contained within <a> tags in HTML
## use html_nodes("a") to extract all <a> nodes
linknodes = webpage %>%
  html_nodes("a")

# actual URLs of the zip files are contained in the "href" attribute of the <a> nodes
urls_data = linknodes %>%
  html_attr("href")

# subset to only those links that contain the PUR data
urls_data = str_subset(urls_data, "/pub/outgoing/pur_archives/pur")

# get only file names from URLs
filenames = basename(urls_data)

# list existing files in cdpr_data_zip folder
existing_files = list.files("cdpr_data_zip/")

# if an item in filenames is not in existing_files, download it
options(timeout = max(300, getOption("timeout")))
for(i in 1:length(filenames)){
  if(!(filenames[i] %in% existing_files)){
    download.file(url = paste0(url, filenames[i]),
                  destfile = paste0("cdpr_data_zip/", filenames[i]), mode = "wb")
  }
}

