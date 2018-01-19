# README

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
library(purrr)
library(RPostgreSQL)


# connections -------------------------------------------------------------

source('~/Documents/localSettings/pg_prod.R')
source('~/Documents/localSettings/pg_local.R')

pg <- pg_prod
pg <- pg_local


# variable metadata -------------------------------------------------------

# get analyses data
analyses_metadata <- dbGetQuery(pg, "SELECT * FROM lter120.variables;")


# new data ----------------------------------------------------------------

newdata <- read_csv('~/localRepos/lter120tower-ruby/LDP_CR1000_TenMin_20171220.dat', skip = 1) %>% 
  slice(-c(1:2)) %>% 
  set_names(tolower(names(.)))

newdata <- read_csv('~/localRepos/lter120tower-ruby/DBG_CR1000_TenMin_20171220.dat', skip = 1) %>% 
  slice(-c(1:2)) %>% 
  set_names(tolower(names(.)))

# check to see if there are any duplicates
nrow(newdata %>% group_by(timestamp) %>% filter(n() > 1))

# if so, what to do, purge them?
# newdata %>% distinct(TIMESTAMP, .keep_all = T) # purge duplicates

# have a quick look at the data
newdata %>% 
  select(-timestamp, -record) %>% 
  gather(key = analyte, value = value) %>% 
  mutate(value = replace(value, value == 'NAN', NA)) %>% 
  mutate(value = as.numeric(value)) %>% 
  group_by(analyte) %>% 
  summarise(
    min = min(value, na.rm = T),
    max = max(value, na.rm = T)
  )

if (dbExistsTable(pg, c('lter120', 'temp_data_table'))) dbRemoveTable(pg, c('lter120', 'temp_data_table')) # make sure tbl does not exist
dbWriteTable(pg, c('lter120', 'temp_data_table'), value = newdata, row.names = F)

dbExecute(pg, '
          ALTER TABLE lter120.temp_data_table
            ALTER COLUMN "timestamp" TYPE timestamp without time zone USING "timestamp"::timestamp without time zone,
            ALTER COLUMN record TYPE numeric USING record::numeric,
            ALTER COLUMN airtc_avg TYPE numeric USING airtc_avg::numeric,
            ALTER COLUMN rh TYPE numeric USING rh::numeric,
            ALTER COLUMN slrkw_avg TYPE numeric USING slrkw_avg::numeric,
            ALTER COLUMN slrmj_tot TYPE numeric USING slrmj_tot::numeric, 
            ALTER COLUMN ws_ms_avg TYPE numeric USING ws_ms_avg::numeric,
            ALTER COLUMN winddir TYPE numeric USING winddir::numeric,
            ALTER COLUMN rain_mm_tot TYPE numeric USING rain_mm_tot::numeric;')


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
