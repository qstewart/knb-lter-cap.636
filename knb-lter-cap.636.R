
# README ------------------------------------------------------------------

# Just noticed when publishing version 3 that the spatial coverage includes only
# DBG - need to add coverage for LDP as well at the next upload.
#
# Recall that this version 3 is the dataset that I had inexplicable problems
# loading into PASTA, and, ultimately, Duane had to do it for me:
#
# An entity file is missing from the data repository: entityDir: /pasta/data1;
# packageId: knb-lter-cap.636.3; entity id: 0f3c24ead461ea09a0f18695eb0d9a94


# libraries ---------------------------------------------------------------
library(EML)
library(RPostgreSQL)
library(RMySQL)
library(tidyverse)
library(tools)
library(readxl)
library(aws.s3)
library(capeml)
  

# reml-helper-functions ---------------------------------------------------
source('~/localRepos/reml-helper-tools/writeAttributesFn.R')
source('~/localRepos/reml-helper-tools/createDataTableFromFileFn.R')
source('~/localRepos/reml-helper-tools/createKMLFn.R')
source('~/localRepos/reml-helper-tools/address_publisher_contact_language_rights.R')
source('~/localRepos/reml-helper-tools/createOtherEntityFn.R')
source('~/localRepos/reml-helper-tools/createPeople.R')
source('~/localRepos/reml-helper-tools/createFactorsDataframe.R')
  

# connections -------------------------------------------------------------

# Amazon
source('~/Documents/localSettings/aws.s3')
  
# postgres
source('~/Documents/localSettings/pg_prod.R')
source('~/Documents/localSettings/pg_local.R')
  
pg <- pg_prod
pg <- pg_local

# mysql
source('~/Documents/localSettings/mysql_prod.R')
prod <- mysql_prod

# dataset details to set first ----
projectid <- 636
packageIdent <- 'knb-lter-cap.636.3'
pubDate <- '2017-05-18'


# data entity -------------------------------------------------------------

# LDP ----

tower_data_ldp <- dbGetQuery(pg,
'SELECT
  "timestamp",
  airtc_avg,
  rh, slrkw_avg,
  slrmj_tot,
  ws_ms_avg,
  wind_dir,
  rain_mm_tot
FROM lter120.ldp_data;')

tower_data_ldp <- tower_data_ldp %>%
  mutate(site_code = "Lost Dutchman State Park (LDP)") %>%
  select(site_code, timestamp:rain_mm_tot) %>%
  arrange(timestamp)

# tower_data_ldp[,tower_data_ldp == ''] <- NA # stymied by the date col
# tower_data_ldp[, -2][tower_data_ldp[, -2] == ''] <- NA # this migtht work but not sure it is needed


writeAttributes(tower_data_ldp) # write data frame attributes to a csv in current dir to edit metadata
tower_data_ldp_desc <- 'Micrometeoroligical data from a CAP LTER weather station located at the Lost Dutchman State Park, AZ. Data are 10-min averages of measurments collected at 5-second intervals.'
tower_data_ldp_DT <- createDTFF(dfname = tower_data_ldp,
                                description = tower_data_ldp_desc,
                                dateRangeField = timestamp)

# !!!!!!!!!!!!!
#
# CAREFUL WITH ATTRS - AT THE TIME OF THIS WRITING, MULTIPLE MISSING VALUE CODES ARE NOT SUPPORTED FROM THE TEMPLATE, SO NAs and NaNs NEED TO BE ADDRESSED BY HAND (AS DO NAs GENERALLY EVEN WHEN ALONE)
#
# !!!!!!!!!!!!!

# DBG ----

tower_data_dbg <- dbGetQuery(pg,
'SELECT
  "timestamp",
  airtc_avg,
  rh, slrkw_avg,
  slrmj_tot,
  ws_ms_avg,
  wind_dir,
  rain_mm_tot
FROM lter120.dbg_data;')

tower_data_dbg <- tower_data_dbg %>%
  mutate(site_code = "Desert Botanical Garden (DBG)") %>%
  select(site_code, timestamp:rain_mm_tot) %>% 
  arrange(timestamp)

writeAttributes(tower_data_dbg) # write data frame attributes to a csv in current dir to edit metadata
tower_data_dbg_desc <- "Micrometeoroligical data from a CAP LTER weather station located near the Desert Botanical Garden in Papago Park, AZ. Data are 10-min averages of measurments collected at 5-second intervals."

# create data table based on metadata provided in the companion csv
# use createdataTableFn() if attributes and classes are to be passed directly
tower_data_dbg_DT <- createDTFF(dfname = tower_data_dbg,
                                description = tower_data_dbg_desc,
                                dateRangeField = timestamp)

# !!!!!!!!!!!!!
#
# CAREFUL WITH ATTRS - AT THE TIME OF THIS WRITING, MULTIPLE MISSING VALUE CODES ARE NOT SUPPORTED FROM THE TEMPLATE, SO NAs and NaNs NEED TO BE ADDRESSED BY HAND (AS DO NAs GENERALLY EVEN WHEN ALONE)
#
# !!!!!!!!!!!!!

# title and abstract ----
# screwed up the title again in version 2 (2010-2016, next time change to ongoing... as below)
title <- 'CAP LTER weather stations at Papago Park and Lost Dutchman State Park in the greater Phoenix metropolitan area, ongoing since 2010'

# abstract from file or directly as text
# abstract <- as(set_TextType("abstract_as_md_file.md"), "abstract") 
abstract <- 'The CAP LTER maintains two 10-m micrometeorological stations in the greater Phoenix metropolitan area, including at Lost Dutchman State Park and near the Desert Botanical Garden at Papago Park. The local terrain at both sites is flat or gently sloping Sonoran desert, and the vegetation canopy consists of patchy coverage of desert shrubs and trees. The dominant vegetation species include bursage (Ambrosia deltoidea) and creosote bush (Larrea tridentata), while minor species include palo verde (Parkinsonia microphylla) and saguaro cactus (Carnegiea gigantea). Wind speed and direction, incoming solar radiation, air temperature, relative humidity, and precipitation (see table below) have been monitored nearly continuously since the fall of 2010. Each variable is measured every 5 seconds and the average (or total for precipitation and total solar radiation) is saved to a data logger every 10 minutes.'


# people ----

nancyGrimm <- addCreator('n', 'grimm')
danChilders <- addCreator('d', 'childers')
johnAllen <- addCreator('jonathan', 'allen')
sharonHall <- addCreator('s', 'hall')
jasonKaye <- addCreator('j', 'kaye')

creators <- c(as(johnAllen, 'creator'),
              as(nancyGrimm, 'creator'),
              as(sharonHall, 'creator'),
              as(jasonKaye, 'creator'),
              as(danChilders, 'creator'))


stevanEarl <- addMetadataProvider('s', 'earl')
metadataProvider <-c(as(stevanEarl, 'metadataProvider'))


# keywords ----

# CAP IRTs for reference: https://sustainability.asu.edu/caplter/research/
# be sure to include these as appropriate

keywordSet <-
  c(new("keywordSet",
        keywordThesaurus = "LTER controlled vocabulary",
        keyword =  c("weather",
                     "urban",
                     "temperature",
                     "solar radiation",
                     "pyranometers",
                     "precipitation",
                     "relative humidity",
                     "wind",
                     "wind direction",
                     "wind speed",
                     "climate")),
    new("keywordSet",
        keywordThesaurus = "LTER core areas",
        keyword =  c("disturbance patterns",
                     "climate and heat")),
    new("keywordSet",
        keywordThesaurus = "Creator Defined Keyword Set",
        keyword =  c("lost dutchman state park",
                     "papago park",
                     "desert botanical garden")),
    new("keywordSet",
        keywordThesaurus = "CAPLTER Keyword Set List",
        keyword =  c("cap lter",
                     "cap",
                     "caplter",
                     "central arizona phoenix long term ecological research",
                     "arizona",
                     "az",
                     "arid land"))
  )

# methods and coverages ----
methods <- set_methods("636_tower_data_methods.md")

# begin date will never change, but pull max date from data
ldp_max_date <- dbGetQuery(pg, "SELECT MAX(timestamp) AS date FROM lter120.ldp_data;")
dbg_max_date <- dbGetQuery(pg, "SELECT MAX(timestamp) AS date FROM lter120.dbg_data;")
enddate <- max(ldp_max_date$date, dbg_max_date$date) 
enddate <- as.character(enddate, format = "%Y-%m-%d")

begindate <- "2006-05-10"
geographicDescription <- "CAP LTER study area"
coverage <- set_coverage(begin = begindate,
                         end = enddate,
                         geographicDescription = geographicDescription,
                         west = -111.9476, east =  -111.9415,
                         north = +33.4612, south = +33.4554)

# construct the dataset ----

# address, publisher, contact, and rights come from a sourced file

# XML DISTRUBUTION
  xml_url <- new("online",
                 onlineDescription = "CAPLTER Metadata URL",
                 url = paste0("https://sustainability.asu.edu/caplter/data/data-catalog/view/", packageIdent, "/xml/"))
metadata_dist <- new("distribution",
                 online = xml_url)

# DATASET
dataset <- new("dataset",
               title = title,
               creator = creators,
               pubDate = pubDate,
               metadataProvider = metadataProvider,
               # associatedParty = associatedParty,
               intellectualRights = rights,
               abstract = abstract,
               keywordSet = keywordSet,
               coverage = coverage,
               contact = contact,
               methods = methods,
               distribution = metadata_dist,
               dataTable = c(tower_data_dbg_DT,
                             tower_data_ldp_DT))
               # otherEntity = c(core_arthropod_locations)) # if other entity is relevant

# ls(pattern= "_DT") # can help to pull out DTs

# construct the eml ----

# ACCESS
allow_cap <- new("allow",
                 principal = "uid=CAP,o=LTER,dc=ecoinformatics,dc=org",
                 permission = "all")
allow_public <- new("allow",
                    principal = "public",
                    permission = "read")
lter_access <- new("access",
                   authSystem = "knb",
                   order = "allowFirst",
                   scope = "document",
                   allow = c(allow_cap,
                             allow_public))

# CUSTOM UNITS
# standardUnits <- get_unitList()
# unique(standardUnits$unitTypes$id) # unique unit types

custom_units <- rbind(
  data.frame(id = "kilowattPerMeterSquared",
             unitType = "irradiance",
             parentSI = "wattPerMeterSquared",
             multiplierToSI = "1000",
             description = "average amount of energy per square meter of surface during the observation period"),
  data.frame(id = "megajoulePerMeterSquared",
             parentSI = "joulePerMeterSquared",
             unitType = "irradiance",
             multiplierToSI = "1000000",
             description = "total amount of energy per square meter of surface during the observation period"))
unitList <- set_unitList(custom_units)

eml <- new("eml",
           packageId = packageIdent,
           scope = "system",
           system = "knb",
           access = lter_access,
           dataset = dataset,
           additionalMetadata = as(unitList, "additionalMetadata"))

# write the xml to file ----
# write_eml(eml, "knb-lter-cap.636.2.xml")
write_eml(tower_data_ldp_DT, "tower_data_ldp.xml")
write_eml(tower_data_dbg_DT, "tower_data_dbg.xml")
