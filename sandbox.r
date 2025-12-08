library(DBI)
library(dplyr)
library(duckdb)
library(ggplot2)
library(sf)
library(stringr)

con = dbConnect(duckdb(), "cdpr_combined.duckdb")

# list all tables in the database
tables = dbListTables(con)

# get tracking table entries
trk_tbls = dbGetQuery(con, "SELECT tables FROM track_tables;")$tables

# print all column names inside 'cdpr_combined' table
colnames <- dbGetQuery(con, "PRAGMA table_info('cdpr_combined')")$name

# example query: get all records for a specific chemical code between certain years
chem_val  <- c("1601","458") # paraquat dichloride
start_yr  <- 1980   # keep these as integers
end_yr    <- 2021

sql <- sqlInterpolate(con, "
  SELECT *
  FROM cdpr_combined
  WHERE (chem_code IN ?chem_val OR chemical_code IN ?chem_val)
    AND CAST(\"year\" AS INTEGER) BETWEEN ?start_yr AND ?end_yr;
", chem_val = chem_val, start_yr = start_yr, end_yr = end_yr)

tbl <- dbGetQuery(con, sql)

dbDisconnect(con)

######################## set up - prep attributes to append to spatial ########################

######## township info
# Number of the township in the Public Land Survey System
# where the application occurred. Must be combined with
# BASE_LN_MER and TSHIP_DIR to determine the unique
# township.

######### section info
# Each township may be divided into a maximum of 36
# sections. Must be used in combination with meridian,
# township, and range to identify the unique section.

# the plss key string is
# <MERIDIAN>-<TOWNSHIP2><TSHIP_DIR>-<RANGE2><RANGE_DIR>-<SECTION2>-<COUNTY_CD>
para_df = select(
  tbl, 
  chem_code, 
  chemname,
  chemical_code, 
  chemical_no,  
  year, 
  county_cd,
  county,
  base_ln_mer, 
  tship_dir,
  township,
  range_dir,
  range,
  section,
  acre_planted,
  acre_treated,
  applic_cnt,
  applic_dt,
  applic_time,
  lbs_chm_used
) |>
  mutate(
    base_ln_mer = toupper(str_trim(base_ln_mer)),
    tship_dir   = toupper(str_trim(tship_dir)),
    range_dir   = toupper(str_trim(range_dir)),
    township    = if_else(is.na(township) | township == "", NA_character_, as.character(as.integer(township))),
    range       = if_else(is.na(range) | range == "", NA_character_, as.character(as.integer(range))),
    section     = if_else(is.na(section) | section == "", NA_character_, as.character(as.integer(section))),
    county_cd   = str_pad(as.character(county_cd), width = 2, pad = "0"),
    township    = str_pad(township, width = 2, pad = "0"),
    range       = str_pad(range, width = 2, pad = "0"),
    section     = str_pad(section, width = 2, pad = "0"),
    plss_key    = ifelse(  
      if_any(c(base_ln_mer, township, tship_dir, range, range_dir, section, county_cd), is.na),
      NA,
      paste0(base_ln_mer, "-", township, tship_dir, "-", range, range_dir, "-", section, "-", county_cd)
    )
  )


################## download spatial data ########################################

# spatial data downloaded from https://gis.data.ca.gov/maps/2230fc8527f5440e8221d69080d1d687/about
section = st_read(dsn = "shapefiles/cdpr_plsnet.gdb") |>
  st_make_valid() |>
  mutate(
    SECTION = as.integer(SECTION)
  )

section = st_read("shapefiles/cdpr_plsnet.geojson") |>
  st_make_valid() |>
  mutate(area_m2 = as.numeric(st_area(geometry)))

st_layers("shapefiles/cdpr_plsnet.gdb")

ggplot() +
  geom_sf(data = head(filter(section, area_m2<2.6e+6),10000), fill =NA,color="grey60", linewidth=.05, size = .01)

# the plss key string is
# <MERIDIAN>-<TOWNSHIP2><TSHIP_DIR>-<RANGE2><RANGE_DIR>-<SECTION2>-<COUNTY_CD>
plss_df = section |>
  mutate(
    BASE_LN_ME  = toupper(str_trim(BASE_LN_ME)),   # or MERIDIAN field name in your PLSS
    TOWNSHIP    = toupper(str_trim(TOWNSHIP)),
    RANGE       = toupper(str_trim(RANGE)),
    SECTION     = str_pad(as.character(as.integer(SECTION)), width = 2, pad = "0"),
    COUNTY_CD   = str_pad(as.character(COUNTY_CD), width = 2, pad = "0"),
    plss_key = ifelse(
      if_any(c(BASE_LN_ME, TOWNSHIP, RANGE, SECTION, COUNTY_CD), is.na),
      NA,
      paste0(BASE_LN_ME, "-", TOWNSHIP, "-", RANGE, "-", SECTION, "-", COUNTY_CD)
    )
  )
# join application data with section shapefile by section number

joined_df = plss_df |>
  dplyr::left_join(para_df, by = "plss_key")

pd = filter(joined_df, year == "2021")

ggplot() +
  geom_sf(data = section, fill = "transparent", color = "grey", linewidth = .05) +
  geom_sf(data = pd, aes(fill = as.numeric(acre_treated)), color = NA) +
  scale_fill_viridis_c(option = "plasma", na.value = "grey90") +
  theme_minimal() +
  labs(
    title = "Pesticide Application Times in California (2021)",
    fill = "Application Time (minutes)"
  )

sort(unique(as.integer(section$SECTION)))
sort(unique(as.integer(tbl$section)))
