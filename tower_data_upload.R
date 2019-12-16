# README

# UPDATE 2018-06-05. I changed the workflow slightly such that data, which all
# come in as characters, are changed to numeric in R instead of in the database
# temporary table - this is more conducive to any error checking we might want
# to do. Specific to this update, the rain gauge at LDP was vandalized. Because
# we do not know when this happened, I changed all precip. data since when the
# data were last downloaded (2017-12-19 11:30:00) to NA. We likely lost some
# good data with that approach but that is preferable to housing any erroneous
# data.

# Ryan's Ruby script for uploading 10-m tower data to the database has become
# unworkable owing to it performing actions on every row in the database, not
# just the new data to upload - translating to hours of runtime, which will only
# increase as the size of the database continues to grow. Below is a quick
# workflow to address data upload in R. Note that I am using POSTGRES' ON
# CONFLICT clause to avoid uploading duplicate data.

# I constructed this workflow to upload the December 2018 data downloads. I had
# forgotten, however, that this database is horribly structured - i.e., wide,
# not long. I am under the gun for the CAP renewal at the time of this writing
# but at a later date when there are fewer deadlines, the database needs to be
# restructured to a proper format (e.g., long not wide, and with a code for site
# instead of separate tables), and a new upload script(s) to match the new
# structure created.


# libraries ---------------------------------------------------------------

library(tidyverse)
library(RPostgreSQL)


# connections -------------------------------------------------------------

source('~/Documents/localSettings/pg_prod.R')
source('~/Documents/localSettings/pg_local.R')

pg <- pg_prod
pg <- pg_local


# variable metadata -------------------------------------------------------

# get analyses data
analyses_metadata <- dbGetQuery(pg, "SELECT * FROM lter120.variables;")


# identify data files -----------------------------------------------------

dataFiles <- list.files(path = '~/Desktop/Loggernet_downloads/',
                        pattern = 'dbg',
                        recursive = FALSE,
                        full.names = TRUE,
                        ignore.case = TRUE)

dataFiles <- list.files(path = '~/Desktop/Loggernet_downloads/',
                        pattern = 'ldp',
                        recursive = FALSE,
                        full.names = TRUE,
                        ignore.case = TRUE)


# harvest data ------------------------------------------------------------

harvest_tower_data <- function(datafile) {
  
  newdata <- read_csv(datafile, skip = 1) %>% 
    slice(-c(1:2)) %>% 
    set_names(tolower(names(.)))
  
  newdata <- newdata %>% 
    mutate(
      timestamp = as.POSIXct(timestamp, format = "%Y-%m-%d %H:%M:%S"),
      airtc_avg = as.numeric(airtc_avg),
      rh = as.numeric(rh),
      slrkw_avg = as.numeric(slrkw_avg),
      slrmj_tot = as.numeric(slrmj_tot),
      ws_ms_avg = as.numeric(ws_ms_avg),
      winddir = as.numeric(winddir),
      rain_mm_tot = as.numeric(rain_mm_tot)
    )
  
  return(newdata)
}

towerDataList <- map(.x = dataFiles,
                     .f = harvest_tower_data)

towerDataBound <- bind_rows(towerDataList)


# quality control ---------------------------------------------------------

# check to see if there are any duplicates
nrow(towerDataBound %>% group_by(timestamp) %>% filter(n() > 1))

# if so, what to do, purge them?
# newdata %>% distinct(TIMESTAMP, .keep_all = T) # purge duplicates

# have a quick look at the data

towerDataBound %>% 
  select(-timestamp, -record) %>% 
  gather(key = analyte, value = value) %>% 
  mutate(value = replace(value, value == 'NAN', NA)) %>% 
  mutate(value = as.numeric(value)) %>% 
  group_by(analyte) %>% 
  summarise(
    min = min(value, na.rm = T),
    max = max(value, na.rm = T)
  )

towerDataBound %>% 
  ggplot(aes(x = timestamp, y = rain_mm_tot)) +
  geom_point()

towerDataBound %>% 
  ggplot(aes(x = timestamp, y = airtc_avg)) +
  geom_point()


# write to temporary table ------------------------------------------------

if (dbExistsTable(pg, c('lter120', 'temp_data_table'))) dbRemoveTable(pg, c('lter120', 'temp_data_table')) # make sure tbl does not exist
dbWriteTable(pg, c('lter120', 'temp_data_table'), value = towerDataBound, row.names = F)

dbExecute(pg, '
          ALTER TABLE lter120.temp_data_table
            ALTER COLUMN "timestamp" TYPE timestamp without time zone USING "timestamp"::timestamp without time zone,
            ALTER COLUMN record TYPE numeric USING record::numeric;')

# changed workflow such that most column types are changed to numeric in R above

# ALTER COLUMN airtc_avg TYPE numeric USING airtc_avg::numeric,
# ALTER COLUMN rh TYPE numeric USING rh::numeric,
# ALTER COLUMN slrkw_avg TYPE numeric USING slrkw_avg::numeric,
# ALTER COLUMN slrmj_tot TYPE numeric USING slrmj_tot::numeric, 
# ALTER COLUMN ws_ms_avg TYPE numeric USING ws_ms_avg::numeric,
# ALTER COLUMN winddir TYPE numeric USING winddir::numeric,
# ALTER COLUMN rain_mm_tot TYPE numeric USING rain_mm_tot::numeric;')


# LDP insert --------------------------------------------------------------

dbExecute(pg,'
INSERT INTO lter120.ldp_data(
  "timestamp",
  record,
  airtc_avg,
  rh,
  slrkw_avg,
  slrmj_tot, 
  ws_ms_avg,
  wind_dir,
  rain_mm_tot
)
(
  SELECT 
    "timestamp",
    record,
    airtc_avg,
    rh,
    slrkw_avg,
    slrmj_tot,
    ws_ms_avg, 
    winddir,
    rain_mm_tot
  FROM lter120.temp_data_table
)
ON CONFLICT ON CONSTRAINT timestamp_unique DO NOTHING;')

# clean up
if (dbExistsTable(pg, c('lter120', 'temp_data_table'))) dbRemoveTable(pg, c('lter120', 'temp_data_table')) # make sure tbl does not exist


# DBG insert --------------------------------------------------------------

dbExecute(pg,'
INSERT INTO lter120.dbg_data(
  "timestamp",
  record,
  airtc_avg,
  rh,
  slrkw_avg,
  slrmj_tot, 
  ws_ms_avg,
  wind_dir,
  rain_mm_tot
)
(
  SELECT 
    "timestamp",
    record,
    airtc_avg,
    rh,
    slrkw_avg,
    slrmj_tot,
    ws_ms_avg, 
    winddir,
    rain_mm_tot
  FROM lter120.temp_data_table
)
ON CONFLICT ON CONSTRAINT timestamps_to_be_unique DO NOTHING;')

# clean up
if (dbExistsTable(pg, c('lter120', 'temp_data_table'))) dbRemoveTable(pg, c('lter120', 'temp_data_table')) # make sure tbl does not exist
