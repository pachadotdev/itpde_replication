library(uncomtrademisc)
library(readr)
library(janitor)
library(dplyr)
library(duckdb)
library(DBI)
library(tidyr)
library(arrow)
library(usitcgravity)

# Download UN COMTRADE data ----

dir_comtrade_data <- "inp/hs-rev1992/"
if (!dir.exists(dir_comtrade_data)) {
  data_downloading(
    subset_years = 1988:2020, arrow = T, token = 1, dataset = 1,
    remove_old_files = 1, parallel = 2, subdir = "inp")
}

# Download HS92 (H0) to ISIC3 codes ----

url_isic3_hs92 <- "http://wits.worldbank.org/data/public/concordance/Concordance_H0_to_I3.zip"
zip_isic3_hs92 <- "inp/Concordance_H0_to_I3.zip"
csv_isic3_hs92 <- "inp/hs92_to_isic3.csv"

if (!file.exists(csv_isic3_hs92)) {
  download.file(url_isic3_hs92, zip_isic3_hs92)
  unzip(zip_isic3_hs92, exdir = "inp")
  file.rename("inp/JobID-6_Concordance_H0_to_I3.CSV", csv_isic3_hs92)
}

# Download ISIC to ITPD-E codes ----

url_isic_id <- "https://www.usitc.gov/data/gravity/itpde_concordances/itpd_e_r02_ff_isic.csv"
csv_isic_id <- "inp/isic_to_itpde.csv"

if (!file.exists(csv_isic_id)) {
  download.file(url_isic_id, csv_isic_id)
}

# Download ISIC3 letter codes ----

url_isic3 <- "https://unstats.un.org/unsd/classifications/Econ/Download/In%20Text/ISIC_Rev_3_english_structure.txt"
txt_isic3 <- "inp/isic3.txt"

if (!file.exists(txt_isic3)) {
  download.file(url_isic3, txt_isic3)
}

# Read codes ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

# ‘Forestry’ corresponds to industry code A02, “Forestry and Logging”,
# in ISIC revisions 3 and 4. ‘Fishing’ corresponds to industry code A03, “Fishing and
# Aquaculture”, in ISIC revision 4 and industry code B05 in ISIC revision 3.

# I only have HS92 to ISIC3 table => I use ISIC3

if (!"fishing_forestry_isic3_to_itpde" %in% dbListTables(con)) {
  isic3_to_itpde <- read_csv(csv_isic_id) %>%
    clean_names() %>%
    select(industry_id = itpd_id, isic3) %>%
    mutate_if(is.double, as.integer)

  dbWriteTable(con, "fishing_forestry_isic3_to_itpde", isic3_to_itpde, overwrite = T)
}

if (!"fishing_forestry_isic3_to_hs92" %in% dbListTables(con)) {
  hs92_to_isic3 <- read_csv(csv_isic3_hs92) %>%
    clean_names() %>%
    select(hs92 = hs_1988_92_product_code,
           isic3 = isic_revision_3_product_code) %>%
    mutate_if(is.double, as.integer)

  # replace multiple spaces to remove descriptions
  isic3 <- read_delim(txt_isic3, delim = "\t") %>%
    clean_names() %>%
    mutate(code_description = gsub("\\s+.*", "", code_description)) %>%
    mutate(
      l = substr(code_description, 1, 2),
      l = ifelse(l %in% LETTERS, l, NA)
    ) %>%
    fill(l) %>%
    filter(code_description != l) %>%
    mutate(l = paste0(l, substr(code_description, 1, 2))) %>%
    filter(nchar(code_description) == 4L) %>%
    rename(isic3_industry_code = l, isic3 = code_description)

  hs92_to_isic3 <- hs92_to_isic3 %>%
    left_join(isic3)

  dbWriteTable(con, "fishing_forestry_isic3_to_hs92", hs92_to_isic3, overwrite = T)
}

dbDisconnect(con, shutdown = T)

# Import raw trade data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"fishing_forestry_country_names" %in% dbListTables(con)) {
  con2 <- usitcgravity_connect()

  country_names <- tbl(con2, "country_names") %>%
    select(country_iso3) %>%
    collect()

  dbDisconnect(con2, shutdown = T)

  dbWriteTable(con, "fishing_forestry_country_names", country_names, overwrite = T)
}

if (!"fishing_forestry_comtrade_trade_raw" %in% dbListTables(con)) {
  # Tidy trade data ----
  con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

  a02 <- tbl(con, "fishing_forestry_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("A02")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  b05 <- tbl(con, "fishing_forestry_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("B05")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  # ADD MAP HERE FOR THE DIFFERENT YEARS
  d_imp_a02 <- open_dataset(
    sources = paste0(dir_comtrade_data, "parquet/6/import"),
    partitioning = schema(
      year = int32(), reporter_iso = string())
  ) %>%
    filter(year == y) %>%
    filter(!(partner_iso %in% c("all","wld"))) %>%
    filter(commodity_code %in% a02) %>%
    select(year, reporter_iso3 = reporter_iso,
           partner_iso3 = partner_iso,
           commodity_code,
           trade_value_usd_imp = trade_value_usd,
           trade_value_tonnes_imp = netweight_kg) %>%
    collect() %>%
    mutate(trade_value_tonnes_imp = trade_value_tonnes_imp * 1000) %>%
    group_by(year, reporter_iso3, partner_iso3, commodity_code) %>%
    summarise_if(is.double, sum, na.rm = T) %>%
    ungroup()

  d_exp_a02 <- open_dataset(
    sources = paste0(dir_comtrade_data, "parquet/6/import"),
    partitioning = schema(
      year = int32(), reporter_iso = string())
  ) %>%
    filter(year == y) %>%
    filter(!(partner_iso %in% c("all","wld"))) %>%
    filter(commodity_code %in% a02) %>%
    select(year, reporter_iso3 = reporter_iso,
           partner_iso3 = partner_iso,
           commodity_code,
           trade_value_usd_exp = trade_value_usd,
           trade_value_tonnes_exp = netweight_kg) %>%
    collect() %>%
    mutate(trade_value_tonnes_exp = trade_value_tonnes_exp * 1000) %>%
    group_by(year, reporter_iso3, partner_iso3, commodity_code) %>%
    summarise_if(is.double, sum, na.rm = T) %>%
    ungroup()

  d_imp_a02 <- d_imp_a02 %>%
    full_join(d_exp_a02) %>%
    select(-commodity_code) %>%
    mutate(industry_id = 27L) %>%
    rename(
      export_value_usd = trade_value_usd_exp,
      export_quantity_tonnes = trade_value_tonnes_exp,

      import_value_usd = trade_value_usd_imp,
      import_quantity_tonnes = trade_value_tonnes_imp
    )

  rm(d_exp_a02)

  d_imp_b05 <- open_dataset(
    sources = paste0(dir_comtrade_data, "parquet/6/import"),
    partitioning = schema(
      year = int32(), reporter_iso = string())
  ) %>%
    filter(year == y) %>%
    filter(!(partner_iso %in% c("all","wld"))) %>%
    filter(commodity_code %in% b05) %>%
    select(year, reporter_iso3 = reporter_iso,
           partner_iso3 = partner_iso,
           commodity_code,
           trade_value_usd_imp = trade_value_usd,
           trade_value_tonnes_imp = netweight_kg) %>%
    collect() %>%
    mutate(trade_value_tonnes_imp = trade_value_tonnes_imp * 1000) %>%
    group_by(year, reporter_iso3, partner_iso3, commodity_code) %>%
    summarise_if(is.double, sum, na.rm = T) %>%
    ungroup()

  d_exp_b05 <- open_dataset(
    sources = paste0(dir_comtrade_data, "parquet/6/import"),
    partitioning = schema(
      year = int32(), reporter_iso = string())
  ) %>%
    filter(year == y) %>%
    filter(!(partner_iso %in% c("all","wld"))) %>%
    filter(commodity_code %in% b05) %>%
    select(year, reporter_iso3 = reporter_iso,
           partner_iso3 = partner_iso,
           commodity_code,
           trade_value_usd_exp = trade_value_usd,
           trade_value_tonnes_exp = netweight_kg) %>%
    collect() %>%
    mutate(trade_value_tonnes_exp = trade_value_tonnes_exp * 1000) %>%
    group_by(year, reporter_iso3, partner_iso3, commodity_code) %>%
    summarise_if(is.double, sum, na.rm = T) %>%
    ungroup()

  d_imp_b05 <- d_imp_b05 %>%
    full_join(d_exp_b05) %>%
    select(-commodity_code) %>%
    mutate(industry_id = 28L) %>%
    rename(
      export_value_usd = trade_value_usd_exp,
      export_quantity_tonnes = trade_value_tonnes_exp,

      import_value_usd = trade_value_usd_imp,
      import_quantity_tonnes = trade_value_tonnes_imp
    )

  rm(d_exp_b05)

  d_imp <- bind_rows(d_imp_a02, d_imp_b05)
  rm(d_imp_a02, d_imp_b05)

  d_imp <- d_imp %>%
    mutate(
      reporter_iso3 = toupper(case_when(
        reporter_iso3 == "e-490" ~ "twn",
        reporter_iso3 %in% c("drc", "zar") ~ "cod",
        reporter_iso3 == "rom" ~ "rou",
        TRUE ~ reporter_iso3
      )),
      partner_iso3 = toupper(case_when(
        partner_iso3 == "e-490" ~ "twn",
        partner_iso3 %in% c("drc", "zar") ~ "cod",
        partner_iso3 == "rom" ~ "rou",
        TRUE ~ partner_iso3
      ))
    )

  d_imp <- d_imp %>%
    inner_join(
      tbl(con, "fishing_forestry_country_names") %>%
        collect() %>%
        distinct() %>%
        rename(reporter_iso3 = country_iso3)
    ) %>%
    inner_join(
      tbl(con, "fishing_forestry_country_names") %>%
        collect() %>%
        distinct() %>%
        rename(partner_iso3 = country_iso3)
    ) %>%
    group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
    summarise_if(is.numeric, sum, na.rm = T)

  dbWriteTable(bla bla)
  dbDisconnect(con, shutdown = T)
}
