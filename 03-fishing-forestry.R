library(uncomtrademisc)
library(readr)
library(janitor)
library(dplyr)
library(duckdb)
library(DBI)
library(tidyr)
library(arrow)
library(usitcgravity)
library(purrr)
library(readxl)

# Download UN COMTRADE trade data ----

dir_comtrade_data <- "inp/hs-rev1992/"

if (!dir.exists(dir_comtrade_data)) {
  data_downloading(
    subset_years = 1988:2020, arrow = T, token = 1, dataset = 1,
    remove_old_files = 1, parallel = 2, subdir = "inp")
}

# Download UN Data production data ----

# the file was downloaded from the browser and points to
# http://data.un.org/Handlers/DownloadHandler.ashx?DataFilter=group_code:206;fiscal_year:1988,1989,1990,1991,1992,1993,1994,1995,1996,1997,1998,1999,2000,2001,2002,2003,2004,2005,2006,2007,2008,2009,2010,2011,2012,2013,2014,2015,2016,2017,2018,2019,2020;sub_item_code:22,23&DataMartId=SNA&Format=psv&c=2,3,5,6,8,9,10,11,12,13,14,15&s=_cr_engNameOrderBy:asc,fiscal_year:desc,_grSbIt_code:asc
# see image for http://data.un.org/Data.aspx?d=SNA&f=group_code%3a206
# (inp/undata_forestry_and_fishing_production.png)
# the file is inp/undata_forestry_and_fishing_production.zip

zip_production_isic4 <- "inp/undata_forestry_and_fishing_production_isic4.zip"
txt_production_isic4 <- "inp/undata_forestry_and_fishing_production_isic4.txt"

if (!file.exists(txt_production_isic4)) {
  unzip(zip_production_isic4, exdir = "inp")
  file.rename("inp/UNdata_Export_20221104_165440293.txt", txt_production_isic4)
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

# Download GDPs to convert currencies ----

url_gdp_local <- "https://unstats.un.org/unsd/amaapi/api/file/1"
url_gdp_usd <- "https://unstats.un.org/unsd/amaapi/api/file/2"

xlsx_gdp_local <- "inp/unstats_gdp_local_current_prices.xlsx"
xlsx_gdp_usd <- "inp/unstats_gdp_usd_current_prices.xlsx"

if (!file.exists(xlsx_gdp_local)) {
  download.file(url_gdp_local, xlsx_gdp_local)
}

if (!file.exists(xlsx_gdp_usd)) {
  download.file(url_gdp_usd, xlsx_gdp_usd)
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

  dbDisconnect(con, shutdown = T)

  map(
    1988:2020,
    function(y) {
      message(y)

      con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

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

      dbWriteTable(con, "fishing_forestry_comtrade_trade_raw", d_imp,
                   append = T)

      dbDisconnect(con, shutdown = T)
    }
  )
}

# Import raw production data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"fishing_forestry_undata_production_raw" %in% dbListTables(con)) {
  d_prod <- read_delim(txt_production_isic4,
                       delim = "|",
                       escape_double = FALSE,
                       col_types = cols(`Sub Group` = col_character(),
                                        Year = col_character(), Series = col_character(),
                                        `SNA system` = col_character(), Value = col_character(),
                                        `Value Footnotes` = col_character()),
                       trim_ws = TRUE, n_max = 25987) %>%
    clean_names()

  d_prod_footnotes <- read_delim(txt_production_isic4,
                          delim = "|",
                          escape_double = FALSE,
                          trim_ws = TRUE, skip = 25988) %>%
    clean_names()

  unique(d_prod$sub_item)
  unique(d_prod$value_footnotes)

  d_prod <- d_prod %>%
    mutate(
      year = as.integer(year),
      sna_system = as.integer(sna_system),
      series = as.integer(series),
      value = as.double(value)
    )

  dbWriteTable(con, "fishing_forestry_undata_production_raw", d_prod, append = T)
  dbWriteTable(con, "fishing_forestry_undata_production_raw_footnotes", d_prod_footnotes, append = T)

  dbDisconnect(con, shutdown = T)
}

# Import GDP data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"fishing_forestry_gdp_unstats" %in% dbListTables(con)) {
  gdp_local <- read_excel(xlsx_gdp_local, skip = 2) %>%
    pivot_longer(`1970`:`2020`, names_to = "year", values_to = "gdp_local_currency") %>%
    clean_names()

  gdp_usd <- read_excel(xlsx_gdp_usd, skip = 2) %>%
    pivot_longer(`1970`:`2020`, names_to = "year", values_to = "gdp_usd") %>%
    clean_names()

  gdp_usd <- gdp_usd %>%
    inner_join(gdp_local)

  gdp_usd <- gdp_usd %>%
    select(year, country_id, country, indicator_name, currency, gdp_local_currency, gdp_usd) %>%
    mutate(year = as.integer(year))

  dbWriteTable(con, "fishing_forestry_gdp_unstats", gdp_usd, append = T)

  dbDisconnect(con, shutdown = T)
}

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"fishing_forestry_country_iso3_codes" %in% dbListTables(con)) {
  d_iso_codes <- open_dataset(
    sources = paste0(dir_comtrade_data, "parquet/6/import"),
    partitioning = schema(
      year = int32(), reporter_iso = string())
  )

  d_iso_codes_2 <- map_df(
    1988:2020,
    function(y) {
      message(y)
      d_iso_codes %>%
        filter(year == y) %>%
        select(reporter_iso, reporter_code, reporter) %>%
        distinct() %>%
        collect()
    }
  )

  d_iso_codes_2 <- d_iso_codes_2 %>%
    distinct() %>%
    arrange(reporter_code) %>%
    select(country_iso3 = reporter_iso, country_id = reporter_code,
           country = reporter)

  d_iso_codes_2 <- d_iso_codes_2 %>%
    mutate(
      country_iso3 = toupper(country_iso3),
      country = toupper(country)
    )

  rm(d_iso_codes_2)
  gc()

  dbWriteTable(con, "fishing_forestry_country_iso3_codes", d_iso_codes_2, overwrite = T)

  dbDisconnect(con, shutdown = T)
}

# Tidy data (combine trade and production in same final table) ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"fishing_forestry_comtrade_trade_tidy" %in% dbListTables(con)) {
  ## Tidy production -----

  ### Get raw production and convert all currencies to USD ----

  d_prod <- tbl(con, "fishing_forestry_undata_production_raw") %>%
    filter(item == "Output, at basic prices") %>%
    select(year, country = country_or_area, sub_item, production_local_currency = value) %>%
    mutate(country = toupper(country)) %>%
    left_join(
      tbl(con, "fishing_forestry_gdp_unstats") %>%
        filter(indicator_name == "Gross Domestic Product (GDP)") %>%
        mutate(
          constant = gdp_usd / gdp_local_currency,
          year = as.integer(year),
          country = toupper(ifelse(
            country == "U.R. of Tanzania: Mainland" , "Tanzania - Mainland", country
          ))
        ) %>%
        filter(year %in% 1988:2020) %>%
        select(year, country, country_id, constant)
    ) %>%
    left_join(
      tbl(con, "fishing_forestry_country_iso3_codes")
    ) %>%
    # select(year, country_iso3, production_local_currency, constant) %>%
    mutate(production_usd = production_local_currency * constant) %>%
    select(-constant, -production_local_currency) %>%
    collect()

  ### Fix ISO-3 codes ----

  d_prod <- d_prod %>%
    mutate(
      country_iso3 = case_when(
        country == "BOSNIA AND HERZEGOVINA" ~ "BIH", # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
        country == "BRITISH VIRGIN ISLANDS" ~ "VGB", # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
        country == "CAYMAN ISLANDS" ~ "CYM", # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
        country == "FRANCE" ~ "FRA",
        country == "INDIA" ~ "IND",
        country == "IRAN (ISLAMIC REPUBLIC OF)" ~ "IRN", # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
        country == "ITALY" ~ "IND",
        country == "NORWAY" ~ "NOR",
        country == "TANZANIA - MAINLAND" ~ "TZA",
        country == "UNITED STATES" ~ "USA",
        country == "ZANZIBAR" ~ "EAZ", # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3,
        TRUE ~ country_iso3
      )
    )

  d_prod %>%
    filter(is.na(country_iso3)) %>%
    select(country, country_iso3) %>%
    distinct()

  # A02 ISIC 4 = ITPDE INDUSTRY 27
  # A03 ISIC 4 = ITPDE INDUSTRY 28

  ### Add industry ID -----

  d_prod <- d_prod %>%
    mutate(
      industry_id = case_when(
        sub_item == "Forestry and logging (02)" ~ 27L,
        sub_item == "Fishing and aquaculture (03)" ~ 28L
      )
    ) %>%
    select(year, country_iso3, industry_id, production_usd)

  d_prod <- d_prod %>%
    arrange(country_iso3, year)

  ### Add flags ----

  d_prod <- d_prod %>%
    rename(exporter_iso3 = country_iso3) %>%
    mutate(importer_iso3 = exporter_iso3) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, production_int_usd = production_usd) %>%
    full_join(
      tbl(con, "fishing_forestry_comtrade_trade_raw") %>%
        select(year, exporter_iso3 = partner_iso3, industry_id, import_value_usd) %>%
        group_by(year, exporter_iso3, industry_id) %>%
        summarise(total_exports = sum(import_value_usd, na.rm = T)) %>%
        collect()
    ) %>%
    mutate(trade = production_int_usd - total_exports) %>%
    filter(trade >= 0) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, trade)

  d_prod <- d_prod %>%
    mutate(
      flag_mirror = 0L,
      flag_zero = case_when(
        trade > 0 ~ "p",
        trade == 0 ~ "r"
      ),
      flag_flow = "d"
    )

  ## Trade data ----

  ### Get imports/exports ----

  d_trade <- tbl(con, "fishing_forestry_comtrade_trade_raw") %>%
    select(year, exporter_iso3 = partner_iso3, importer_iso3 = reporter_iso3,
           industry_id, import_value_usd, export_value_usd) %>%
    group_by(year, exporter_iso3, importer_iso3, industry_id) %>%
    summarise(
      import_value_usd = sum(import_value_usd, na.rm = T),
      export_value_usd = sum(export_value_usd, na.rm = T)
    ) %>%
    collect()

  d_trade <- d_trade %>%
    arrange(exporter_iso3, year)

  ### Add flags -----

  d_trade <- d_trade %>%
    mutate(
      trade = case_when(
        import_value_usd == 0 ~ export_value_usd,
        TRUE ~ import_value_usd
      ),
      flag_mirror = case_when(
        trade == import_value_usd ~ 0L,
        TRUE ~ 1L
      ),
      flag_zero = case_when(
        trade > 0 ~ "p",
        trade == 0 ~ "r" # I still need to add "flag = u" later
      ),
      flag_flow = "i"
    )

  ## Combine tables ----

  d_trade <- d_trade %>%
    select(year, exporter_iso3, importer_iso3, industry_id, trade, flag_mirror, flag_zero, flag_flow) %>%
    bind_rows(
      d_prod %>%
        select(year, exporter_iso3, importer_iso3, industry_id, trade, flag_mirror, flag_zero, flag_flow)
    )

  dbWriteTable(con, "fishing_forestry_comtrade_trade_tidy", d_trade, overwrite = T)

  dbDisconnect(con, shutdown = T)
}
