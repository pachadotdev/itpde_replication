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

if (!file.exists(zip)) {
  try(download.file(url, zip, method = "wget", quiet = T))
}

if (!length(list.files(finp, pattern = "ITPD_E_R02\\.csv")) == 1) {
  archive_extract(zip, dir = finp)
}

if (!"usitc_trade" %in% dbListTables(con2)) {
  # this table is to compare replication values later
  trade <- read_csv(paste0(finp, "/ITPD_E_R02.csv"))

  # this table is to avoid writing special country codes that we won't need
  country_names <- trade %>%
    select(country_iso3 = exporter_iso3) %>%
    distinct() %>%
    bind_rows(
      trade %>%
        select(country_iso3 = importer_iso3) %>%
        distinct()
    ) %>%
    distinct() %>%
    arrange(country_iso3)

  # just for references
  industry_names <- trade %>%
    select(industry_id, industry_descr) %>%
    distinct()

  dbWriteTable(con, "usitc_country_codes", country_names)

  dbWriteTable(con, "usitc_industry_names", industry_names)

  sector_names <- trade %>%
    select(broad_sector) %>%
    distinct() %>%
    arrange(broad_sector) %>%
    mutate(broad_sector_id = row_number()) %>%
    select(broad_sector_id, broad_sector)

  sector_names_2 <- trade %>%
    select(broad_sector) %>%
    inner_join(sector_names)

  trade <- trade %>%
    select(-broad_sector, -industry_descr)

  trade <- trade %>%
    bind_cols(sector_names_2) %>%
    select(year, exporter_iso3, importer_iso3, broad_sector_id, broad_sector_id,
           industry_id, trade, flag_mirror, flag_zero)

  trade <- trade %>%
    group_by(year) %>%
    nest()

  gc()

  map(
    1986:2020,
    function(y) {
      message(y)

      d <- trade %>%
        filter(year == y) %>%
        unnest(data)

      dbWriteTable(con, "usitc_trade", d, overwrite = F, append = T)
    }
  )

  rm(trade, sector_names, sector_names_2); gc()
}

dbDisconnect(con)
