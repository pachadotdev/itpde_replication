library(readr)
library(readxl)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)
library(RPostgres)

# Download UN COMTRADE trade data ----

# this shall create a separate SQL database
# remotes::install_github("tradestatistics/uncomtrademisc")
# uncomtrademisc::data_downloading(postgres = T, token = T, dataset = 1, remove_old_files = 1, subset_years = 1988:2020, parallel = 2)

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
# Import raw trade data ----

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

con <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "itpde_replication",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD")
)

# ‘Forestry’ corresponds to industry code A02, “Forestry and Logging”,
# in ISIC revisions 3 and 4. ‘Fishing’ corresponds to industry code A03, “Fishing and
# Aquaculture”, in ISIC revision 4 and industry code B05 in ISIC revision 3.

# I only have HS92 to ISIC3 table => I use ISIC3

if (!"usitc_isic3_to_itpde" %in% dbListTables(con)) {
  isic3_to_itpde <- read_csv(csv_isic_id) %>%
    clean_names() %>%
    select(industry_id = itpd_id, isic3) %>%
    mutate_if(is.double, as.integer)

  dbWriteTable(con, "usitc_isic3_to_itpde", isic3_to_itpde, overwrite = T)
}

if (!"wb_isic3_to_hs92" %in% dbListTables(con)) {
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

  dbWriteTable(con, "wb_isic3_to_hs92", hs92_to_isic3, overwrite = T)
}

# Import raw trade data ----

if (!"uncomtrade_imports" %in% dbListTables(con)) {
  con2 <- dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade_commodities",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )

  a02 <- tbl(con, "wb_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("A02")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  b05 <- tbl(con, "wb_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("B05")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  map(
    1988:2020,
    function(y) {
      message(y)

      d <- tbl(con2, "hs_rev1992_tf_import_al_6") %>%
        filter(year == y) %>%
        filter(!(partner_iso %in% c("all","wld"))) %>%
        filter(commodity_code %in% c(a02, b05)) %>%
        collect()

      dbWriteTable(con, "uncomtrade_imports", d, append = T)
    }
  )

  rm(a02, b05)
  dbDisconnect(con2)
  rm(con2)
}

if (!"uncomtrade_exports" %in% dbListTables(con)) {
  con2 <- dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade_commodities",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )

  a02 <- tbl(con, "wb_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("A02")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  b05 <- tbl(con, "wb_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("B05")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  map(
    1988:2020,
    function(y) {
      message(y)

      d <- tbl(con2, "hs_rev1992_tf_export_al_6") %>%
        filter(year == y) %>%
        filter(!(partner_iso %in% c("all","wld"))) %>%
        filter(commodity_code %in% c(a02, b05)) %>%
        collect()

      dbWriteTable(con, "uncomtrade_exports", d, append = T)
    }
  )

  rm(a02, b05)
  dbDisconnect(con2)
  rm(con2)
}

if (!"uncomtrade_trade" %in% dbListTables(con)) {
  a02 <- tbl(con, "wb_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("A02")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  b05 <- tbl(con, "wb_isic3_to_hs92") %>%
    filter(isic3_industry_code %in% c("B05")) %>%
    distinct(hs92) %>%
    collect() %>%
    pull()

  map(
    1988:2020,
    function(y) {
      message(y)

      d_imp_a02 <- tbl(con, "uncomtrade_imports") %>%
        filter(year == y) %>%
        filter(commodity_code %in% a02) %>%
        select(year, importer_iso3 = reporter_iso,
               exporter_iso3 = partner_iso,
               commodity_code,
               import_value_usd = trade_value_usd) %>%
        collect() %>%
        group_by(year, importer_iso3, exporter_iso3, commodity_code) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d_exp_a02 <- tbl(con, "uncomtrade_exports") %>%
        filter(year == y) %>%
        filter(commodity_code %in% a02) %>%
        select(year, exporter_iso3 = reporter_iso,
               importer_iso3 = partner_iso,
               commodity_code,
               export_value_usd = trade_value_usd) %>%
        collect() %>%
        group_by(year, importer_iso3, exporter_iso3, commodity_code) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d_imp_a02 <- d_imp_a02 %>%
        full_join(
          d_exp_a02 %>%
            select(-year),
          by = c("importer_iso3",
                 "exporter_iso3",
                 "commodity_code")
        ) %>%
        select(-commodity_code) %>%
        mutate(
          industry_id = 27L,
          year = y
        )

      rm(d_exp_a02)

      d_imp_b05 <- tbl(con, "uncomtrade_imports") %>%
        filter(year == y) %>%
        filter(commodity_code %in% b05) %>%
        select(year, importer_iso3 = reporter_iso,
               exporter_iso3 = partner_iso,
               commodity_code,
               import_value_usd = trade_value_usd) %>%
        collect() %>%
        group_by(year, importer_iso3, exporter_iso3, commodity_code) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d_exp_b05 <- tbl(con, "uncomtrade_exports") %>%
        filter(year == y) %>%
        filter(commodity_code %in% b05) %>%
        select(year, importer_iso3 = reporter_iso,
               exporter_iso3 = partner_iso,
               commodity_code,
               export_value_usd = trade_value_usd) %>%
        collect() %>%
        group_by(year, importer_iso3, exporter_iso3, commodity_code) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d_imp_b05 <- d_imp_b05 %>%
        full_join(
          d_exp_b05 %>%
            select(-year),
          by = c("importer_iso3",
                 "exporter_iso3",
                 "commodity_code")
        ) %>%
        select(-commodity_code) %>%
        mutate(
          industry_id = 28L,
          year = y
        )

      rm(d_exp_b05)

      d_imp <- bind_rows(d_imp_a02, d_imp_b05)
      rm(d_imp_a02, d_imp_b05)

      d_imp <- d_imp %>%
        mutate(
          importer_iso3 = toupper(case_when(
            importer_iso3 == "e-490" ~ "twn",
            importer_iso3 %in% c("drc", "zar") ~ "cod",
            importer_iso3 == "rom" ~ "rou",
            TRUE ~ importer_iso3
          )),
          exporter_iso3 = toupper(case_when(
            exporter_iso3 == "e-490" ~ "twn",
            exporter_iso3 %in% c("drc", "zar") ~ "cod",
            exporter_iso3 == "rom" ~ "rou",
            TRUE ~ exporter_iso3
          ))
        )

      d_imp <- d_imp %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            collect() %>%
            distinct() %>%
            rename(importer_iso3 = country_iso3)
        ) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            collect() %>%
            distinct() %>%
            rename(exporter_iso3 = country_iso3)
        ) %>%
        group_by(year, importer_iso3, exporter_iso3, industry_id) %>%
        summarise_if(is.numeric, sum, na.rm = T)

      dbWriteTable(con, "uncomtrade_trade", d_imp, append = T)
    }
  )
}

# Import raw production data ----

if (!"undata_production" %in% dbListTables(con)) {
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

  # unique(d_prod$sub_item)
  # unique(d_prod$value_footnotes)

  d_prod <- d_prod %>%
    mutate(
      year = as.integer(year),
      sna_system = as.integer(sna_system),
      series = as.integer(series),
      value = as.double(value)
    )

  dbWriteTable(con, "undata_production", d_prod, append = T)
  dbWriteTable(con, "undata_production_footnotes", d_prod_footnotes, append = T)
}

# Import country names ----

if (!"uncomtrade_country_codes" %in% dbListTables(con)) {
  con2 <- dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade_commodities",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )

  d_iso_codes <- map_df(
    1988:2020,
    function(y) {
      message(y)
      tbl(con2, "hs_rev1992_tf_import_al_6") %>%
        filter(year == y) %>%
        select(reporter_iso, reporter_code, reporter) %>%
        distinct() %>%
        collect()
    }
  )

  d_iso_codes <- d_iso_codes %>%
    distinct() %>%
    arrange(reporter_code) %>%
    select(country_iso3 = reporter_iso, country_id = reporter_code,
           country = reporter)

  d_iso_codes <- d_iso_codes %>%
    mutate(
      country_iso3 = toupper(country_iso3),
      country = toupper(country)
    )

  gc()

  dbWriteTable(con, "uncomtrade_country_codes", d_iso_codes, overwrite = T)

  dbDisconnect(con2)
}

# Tidy data (combine trade and production in same final table) ----

if (!"uncomtrade_trade_tidy" %in% dbListTables(con)) {
  ## Trade data ----

  d_trade <- tbl(con, "uncomtrade_trade") %>%
    select(year, importer_iso3, exporter_iso3,
           industry_id, import_value_usd, export_value_usd) %>%
    group_by(year, exporter_iso3, importer_iso3, industry_id) %>%
    summarise(
      import_value_usd = sum(import_value_usd, na.rm = T),
      export_value_usd = sum(export_value_usd, na.rm = T)
    ) %>%
    collect()

  d_trade <- d_trade %>%
    arrange(exporter_iso3, year)

  d_trade <- d_trade %>%
    mutate(
      import_value_usd = import_value_usd / 1000000,
      export_value_usd = export_value_usd / 1000000
    )

  d_trade %>%
    filter(is.na(import_value_usd))

  d_trade %>%
    filter(is.na(export_value_usd))

  d_trade <- d_trade %>%
    # mutate_if(is.double, function(x) ifelse(is.na(x), 0, x)) %>%
    mutate(
      trade = case_when(
        import_value_usd == 0 ~ export_value_usd,
        TRUE ~ import_value_usd
      ),
      trade_flag_code = case_when(
        is.na(trade) ~ 1L, # 1 means trade = NA
        import_value_usd == 0 ~ 2L, # 2 means trade = exports
        import_value_usd > 0 ~ 3L # 3 means trade = imports
      ),
      trade = case_when(
        is.na(trade) ~ 0,
        TRUE ~ trade
      )
    )

  d_trade <- d_trade %>%
    select(-import_value_usd, -export_value_usd)

  ## Production -----

  ### Copy GDP table to SQL ----

  if (!"unstats_gdp" %in% dbListTables(con)) {
    gdp_local <- read_excel(xlsx_gdp_local, skip = 2) %>%
      pivot_longer(`1970`:`2020`, names_to = "year", values_to = "gdp_local_currency") %>%
      clean_names()

    gdp_usd <- read_excel(xlsx_gdp_usd, skip = 2) %>%
      pivot_longer(`1970`:`2020`, names_to = "year", values_to = "gdp_usd") %>%
      clean_names()

    gdp_usd <- gdp_usd %>%
      full_join(gdp_local)

    gdp_usd <- gdp_usd %>%
      select(year, country_id, country, indicator_name, currency, gdp_local_currency, gdp_usd) %>%
      mutate(
        year = as.integer(year),
        gdp_rate = gdp_usd / gdp_local_currency,
      ) %>%
      filter(indicator_name == "Gross Domestic Product (GDP)") %>%
      select(-indicator_name)

    dbWriteTable(con, "unstats_gdp", gdp_usd, overwrite = T)

    rm(gdp_local, gdp_usd)
  }

  ### Get raw production and convert all currencies to USD ----

  d_prod <- tbl(con, "undata_production") %>%
    filter(item == "Output, at basic prices") %>%
    select(year, country = country_or_area, sub_item, production_local_currency = value) %>%
    mutate(country = toupper(country)) %>%
    left_join(
      tbl(con, "unstats_gdp") %>%
        mutate(
          year = as.integer(year),
          country = toupper(ifelse(
            country == "U.R. of Tanzania: Mainland" , "Tanzania - Mainland", country
          ))
        ) %>%
        filter(year %in% 1988:2020) %>%
        select(year, country, country_id, gdp_rate)
    ) %>%
    left_join(
      tbl(con, "uncomtrade_country_codes")
    ) %>%
    mutate(production_usd = production_local_currency * gdp_rate) %>%
    select(-gdp_rate, -production_local_currency) %>%
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

  d_prod <- d_prod %>%
    rename(exporter_iso3 = country_iso3) %>%
    mutate(
      importer_iso3 = exporter_iso3,
      production_usd = production_usd / 1000000
    ) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, production_usd) %>%
    full_join(
      d_trade %>%
        select(year, exporter_iso3, industry_id, trade) %>%
        group_by(year, exporter_iso3, industry_id) %>%
        summarise(total_exports = sum(trade, na.rm = T)) %>%
        collect()
    ) %>%
    mutate(
      trade_flag_code = case_when(
        is.na(production_usd) ~ 4L # 4 means production = NA
      ),
      production_usd = case_when(
        is.na(production_usd) ~ 0,
        TRUE ~ production_usd
      ),

      trade_flag_code = case_when(
        trade_flag_code == 4L & is.na(total_exports) ~ 5L, # 5 means exports = NA and production = NA
        trade_flag_code == 4L & !is.na(total_exports) ~ 6L, # 6 means exports = NA and production != NA"
        TRUE ~ trade_flag_code
      ),
      total_exports = case_when(
        is.na(total_exports) ~ 0,
        TRUE ~ total_exports
      ),

      trade = production_usd - total_exports,
      trade_flag_code = case_when(
        trade < 0 ~ 7L, # 7 means production - trade < 0
        TRUE ~ trade_flag_code
      ),
      trade = case_when(
        trade < 0 ~ 0,
        TRUE ~ trade
      )
    ) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, trade, trade_flag_code)

  ## Combine tables ----

  d_trade <- d_trade %>%
    select(year, exporter_iso3, importer_iso3, industry_id, trade, trade_flag_code) %>%
    bind_rows(
      d_prod %>%
        select(year, exporter_iso3, importer_iso3, industry_id, trade, trade_flag_code)
    )

  rm(d_prod)

  dbWriteTable(con, "uncomtrade_trade_tidy", d_trade, overwrite = T)
}
