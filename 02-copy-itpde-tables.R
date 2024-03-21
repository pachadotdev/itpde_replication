library(archive)
library(readr)
library(dplyr)
library(tidyr)
library(purrr)
library(RPostgres)

url <- "https://www.usitc.gov/data/gravity/itpd_e/itpd_e_r02.zip"
finp <- "inp/"
zip <- gsub(".*/", finp, url)

con <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "itpde_replication",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD")
)

if (!"usitc_trade" %in% dbListTables(con)) {
  # I needed to download from the browser or it doesn't work
  # https://www.usitc.gov/data/gravity/itpd_e/itpd_e_r02.zip
  if (!file.exists(paste0(finp, "/ITPD_E_R02.rds"))) {
    if (!length(list.files(finp, pattern = "ITPD_E_R02\\.csv")) == 1) {
      archive_extract(zip, dir = finp)
    }

    # this table is to compare replication values later
    # this file is super heavy and takes a lot of time to read
    # it crashed pandas :(
    trade <- read_csv(
      paste0(finp, "/ITPD_E_R02.csv"),
      col_types = cols(
        year = col_integer(),
        industry_id = col_integer(),
        trade = col_double()
      )
    )

    # reduce weight a bit using factors
    trade <- trade %>%
      mutate_if(is.character, as.factor)

    gc()

    saveRDS(trade, paste0(finp, "/ITPD_E_R02.rds"))

    try(file.remove(paste0(finp, "/ITPD_E_R02.csv")))

    gc()
  } else {
    trade <- readRDS(paste0(finp, "/ITPD_E_R02.rds"))
  }

  # this table is to avoid writing special country codes that we won't need
  country_names <- trade %>%
    select(
      country_iso3 = exporter_iso3,
      country_dynamic_code = exporter_dynamic_code,
      country_name = exporter_name
    ) %>%
    distinct() %>%
    mutate_if(is.factor, as.character) %>%
    bind_rows(
      trade %>%
        select(
          country_iso3 = importer_iso3,
          country_dynamic_code = importer_dynamic_code,
          country_name = importer_name
        ) %>%
        distinct() %>%
        mutate_if(is.factor, as.character)
    ) %>%
    distinct() %>%
    arrange(country_iso3)

  dbWriteTable(con, "usitc_country_codes", country_names, overwrite = T)

  trade <- trade %>%
    select(-exporter_name, -importer_name)

  # just for references
  industry_names <- trade %>%
    select(industry_id, industry_descr) %>%
    distinct() %>%
    mutate_if(is.factor, as.character)

  dbWriteTable(con, "usitc_industry_names", industry_names, overwrite = T)

  trade <- trade %>%
    select(-industry_descr)

  sector_names <- trade %>%
    select(broad_sector) %>%
    distinct() %>%
    mutate_if(is.factor, as.character) %>%
    arrange(broad_sector) %>%
    mutate(broad_sector_id = row_number()) %>%
    select(broad_sector_id, broad_sector)

  dbWriteTable(con, "usitc_sector_names", sector_names, overwrite = T)

  trade <- trade %>%
    mutate(
      broad_sector_id = case_when(
        broad_sector == "Agriculture" ~ 1L,
        broad_sector == "Manufacturing" ~ 2L,
        broad_sector == "Mining and Energy" ~ 3L,
        broad_sector == "Services" ~ 4L
      )
    ) %>%
    select(-broad_sector)

  trade <- trade %>%
    select(
      year,
      exporter_iso3, importer_iso3,
      exporter_dynamic_code, importer_dynamic_code,
      broad_sector_id, industry_id,
      flag_mirror, flag_zero,
      trade
    )

  trade <- trade %>%
    mutate(flag_mirror = as.integer(flag_mirror))

  trade <- trade %>%
    group_by(year) %>%
    nest()

  gc()

  # drop usitc_trade if exists
  if ("usitc_trade" %in% dbListTables(con)) {
    dbRemoveTable(con, "usitc_trade")
  }

  # copy year by year
  map(
    1986:2020,
    function(y) {
      message(y)

      d <- trade %>%
        filter(year == y) %>%
        unnest(data) %>%
        mutate_if(is.factor, as.character)

      dbWriteTable(con, "usitc_trade", d, overwrite = F, append = T)
    }
  )

  rm(trade, country_names, industry_names, sector_names)
  gc()
}

dbDisconnect(con)
