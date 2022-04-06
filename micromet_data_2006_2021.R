#' @title subset all micromet data into approximately decade intervals
#'
#' @description The workflow detailed here is designed to subset the full
#' record of CAP micrometeorological data into approximately decadal intervals
#' through 2020. Publishing the full record of observations as a single file is
#' becoming onerous owing to the large and ever growing amount of data. Unless
#' the data structure or content of data prior to 2021 is altered, the code
#' here should not have to be run again. Rather, the data files have already
#' been created, are published in EDI and stored in AWS so can be referenced
#' rather than regenerated.
#'
#' @note The 2006-2010 and 2011-2020 data files are accessible through AWS; the
#' corresponding metadata are in this repository as
#' `micromet_data_2006_2010_DT.xml` and `micromet_data_2011_2020_DT.xml`. The
#' metadata files are harvested in the Rmd workflow. As the metadata files
#' point to the datafiles in AWS.

## 2006:2010

# check for duplicates

micromet_data |>
  dplyr::filter(timestamp < "2011-01-01") |>
  dplyr::count(site_code, timestamp, replicate) |>
  dplyr::filter(n > 1)

# subset data

micromet_data_2006_2010 <- micromet_data |>
  dplyr::filter(timestamp < "2011-01-01") |>
  dplyr::mutate(
    replicate = dplyr::case_when(
      is.na(replicate) ~ as.integer(1),
    )
  )

try({
  capeml::write_attributes(micromet_data_2006_2010, overwrite = FALSE)
  capeml::write_factors(micromet_data_2006_2010, overwrite = FALSE)
})

micromet_data_2006_2010_desc <- "Micrometeorological data from the CAP LTER weather stations located at the Desert Botanical Garden and Lost Dutchman State Park, AZ, USA sites. Data are 10-min averages of measurements collected at 5-second intervals. Data collected 2006 through 2010."

micromet_data_2006_2010_DT <- capeml::create_dataTable(
  dfname         = micromet_data_2006_2010,
  description    = micromet_data_2006_2010_desc,
  dateRangeField = "timestamp",
  overwrite      = TRUE
)

EML::write_eml(
  eml  = micromet_data_2006_2010_DT,
  file = "micromet_data_2006_2010_DT.xml"
)

# alpha <- EML::read_eml("micromet_data_2006_2010_DT.xml")


## 2011:2020

# check for duplicates

micromet_data |>
  dplyr::filter(timestamp >= "2011-01-01" & timestamp < "2021-01-01") |>
  dplyr::count(site_code, timestamp, replicate) |>
  dplyr::filter(n > 1)

# subset data

micromet_data_2011_2020 <- micromet_data |>
  dplyr::filter(timestamp >= "2011-01-01" & timestamp < "2021-01-01") |>
  dplyr::mutate(
    replicate = dplyr::case_when(
      is.na(replicate) ~ as.integer(1),
      TRUE ~ replicate
      )
  )


try({
  capeml::write_attributes(micromet_data_2011_2020, overwrite = FALSE)
  capeml::write_factors(micromet_data_2011_2020, overwrite = FALSE)
})

micromet_data_2011_2020_desc <- "Micrometeorological data from the CAP LTER weather stations located at the Desert Botanical Garden and Lost Dutchman State Park, AZ, USA sites. Data are 10-min averages of measurements collected at 5-second intervals. Data collected 2011 through 2020."

micromet_data_2011_2020_DT <- capeml::create_dataTable(
  dfname         = micromet_data_2011_2020,
  description    = micromet_data_2011_2020_desc,
  dateRangeField = "timestamp",
  overwrite      = TRUE
)

EML::write_eml(
  eml  = micromet_data_2011_2020_DT,
  file = "micromet_data_2011_2020_DT.xml"
)
