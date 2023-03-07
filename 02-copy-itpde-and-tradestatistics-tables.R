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

if (!"usitc_trade" %in% dbListTables(con)) {
  # this table is to compare replication values later
  trade <- read_csv(
    paste0(finp, "/ITPD_E_R02.csv"),
    col_types = cols(
      year = col_integer(),
      industry_id = col_integer(),
      trade = col_double(),
      exporter_dynamic_code = col_skip(),
      exporter_name = col_skip(),
      importer_dynamic_code = col_skip(),
      importer_name = col_skip()
    )
  )

  gc()

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

  dbSendQuery(
    con,
    "CREATE TABLE usitc_country_codes (
	    country_iso3 bpchar(3) NULL
    )"
  )

  dbSendQuery(
    con,
    "CREATE TABLE usitc_industry_names (
      industry_id int4 NULL,
      industry_descr varchar(255) NULL
    )"
  )

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
    select(year, exporter_iso3, importer_iso3, broad_sector_id,
           industry_id, trade, flag_mirror, flag_zero)

  trade <- trade %>%
    mutate(
      year = as.integer(year),
      broad_sector_id = as.integer(broad_sector_id),
      industry_id = as.integer(industry_id)
    )

  trade <- trade %>%
    group_by(year) %>%
    nest()

  gc()

  dbSendQuery(
    con,
    "CREATE TABLE usitc_trade (
    	year int4 NULL,
    	exporter_iso3 bpbpchar(3) NULL,
    	importer_iso3 bpchar(3) NULL,
    	broad_sector_id int4 NULL,
    	industry_id int4 NULL,
    	trade float8 NULL,
    	flag_mirror int4 NULL,
    	flag_zero bpchar(1) NULL
    )"
  )

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
