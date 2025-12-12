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

# Quote each value safely, then coerce to character
chem_list <- paste(
  vapply(chem_val, function(x) as.character(DBI::dbQuoteLiteral(con, x)), ""),
  collapse = ", "
)

sql <- sprintf("
  SELECT *
  FROM cdpr_combined
  WHERE (chem_code IN (%s) OR chemical_code IN (%s))
    AND CAST(year AS INTEGER) BETWEEN %d AND %d;
", chem_list, chem_list, start_yr, end_yr)

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
base = tigris::counties(state = "CA", year = 2023, cb = TRUE)

# Compute centroids of county polygons
base_centroids <- st_centroid(base)

# If multipolygons exist, st_centroid may place labels outside.
# st_point_on_surface() ensures the point lies inside the polygon:
base_centroids <- st_point_on_surface(base)

plot = ggplot() +
  geom_sf(data = base, fill = "transparent", color = "black", linewidth = .05) +
  geom_sf(data = pd, aes(fill = as.numeric(applic_time)), color = NA) +
  geom_sf_text(data = base_centroids, aes(label = NAME), size = 2, color = "black") +
  scale_fill_viridis_c(option = "plasma", na.value = "grey90") +
  theme_minimal() +
  labs(
    title = "Paraquat Application Times in California (2021)",
    fill = "Application Time (HH:MM)"
  )

ggsave("paraquat_apptiming_2021.jpg", plot, dpi = 1000)

plot = ggplot() +
  geom_sf(data = base, fill = "transparent", color = "black", linewidth = .05) +
  geom_sf(data = pd, aes(fill = as.numeric(acre_treated)), color = NA) +
  geom_sf_text(data = base_centroids, aes(label = NAME), size = 2, color = "black") +
  scale_fill_viridis_c(option = "plasma", na.value = "grey90") +
  theme_minimal() +
  labs(
    title = "Paraquat Acres Ttreated in California (2021)",
    fill = "Acres Treated"
  )
ggsave("parquat_acres_treated_2021.jpg", plot, dpi = 1000)


