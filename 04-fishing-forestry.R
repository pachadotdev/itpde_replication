library(readr)
library(readxl)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(RPostgres)

# Download UN COMTRADE trade data ----

# this shall create a separate SQL database
# remotes::install_github("tradestatistics/uncomtrademisc")
# uncomtrademisc::data_downloading(postgres = T, token = T, dataset = 1, remove_old_files = 1, subset_years = 1988:2020, parallel = 2)

# Download UN Data production data ----

# the file was downloaded from the browser and points to
# http://data.un.org/Data.aspx?q=Output%2c+gross+value+added+and+fixed+assets+by+industries+at+current+prices&d=SNA&f=group_code%3a206
# (inp/undata_forestry_and_fishing_production.png
# the file is inp/undata_forestry_and_fishing_production.zip

zip_production_isic4 <- "inp/undata_forestry_and_fishing_production.zip"
txt_production_isic4 <- "inp/undata_forestry_and_fishing_production.csv"

if (!file.exists(txt_production_isic4)) {
  unzip(zip_production_isic4, exdir = "inp")
  file.rename("inp/UNdata_Export_20230123_215204713.csv", txt_production_isic4)
}

# Download IMF exchange rate ----

# The series corresponds to "Domestic Currency per U.S. Dollar, Period Average, Rate"
# the file was downloaded from the browser and points to
# https://data.imf.org/?sk=4c514d48-b6ba-49ed-8ab9-52b0c1a0179b&sId=1409151240976
# (inp/imf_fishing_forestry_exchange_rate_1.png and
# inp/imf_fishing_forestry_exchange_rate_2.png)
# the file is inp/imf_fishing_forestry_exchange_rate.zip

zip_exchange_rate <- "inp/imf_fishing_forestry_exchange_rate.zip"
csv_exchange_rate <- "inp/imf_fishing_forestry_exchange_rate.csv"

if (!file.exists(csv_exchange_rate)) {
  unzip(zip_exchange_rate, exdir = "inp")
  file.rename("inp/IFS_01-23-2023 21-11-09-50_timeSeries.csv", csv_exchange_rate)
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

  dbSendQuery(
    con,
    "CREATE TABLE usitc_isic3_to_itpde (
    	industry_id int4 NULL,
    	isic3 char(4) NULL
    )"
  )

  dbWriteTable(con, "usitc_isic3_to_itpde", isic3_to_itpde, overwrite = F, append = T)
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

  dbSendQuery(
    con,
    "CREATE TABLE wb_isic3_to_hs92 (
    	hs92 char(6) NULL,
    	isic3 char(4) NULL,
    	isic3_industry_code char(3) NULL
    )"
  )

  dbWriteTable(con, "wb_isic3_to_hs92", hs92_to_isic3, overwrite = F, append = T)
}

# Import raw trade data ----

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

if ("uncomtrade_imports" %in% dbListTables(con)) {
  n_uncomtrade_imports <- tbl(con, "uncomtrade_imports") %>%
    filter(commodity_code %in% c(a02, b05)) %>%
    count() %>%
    collect() %>%
    pull() %>%
    as.numeric()
} else {
  n_uncomtrade_imports <- 0
}

if ("uncomtrade_exports" %in% dbListTables(con)) {
  n_uncomtrade_exports <- tbl(con, "uncomtrade_exports") %>%
    filter(commodity_code %in% c(a02, b05)) %>%
    count() %>%
    collect() %>%
    pull() %>%
    as.numeric()
} else {
  n_uncomtrade_exports <- 0
}

if (n_uncomtrade_imports == 0) {
  con2 <- dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade_commodities",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )

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

  dbDisconnect(con2)
  rm(con2)
}

if (n_uncomtrade_exports == 0) {
  con2 <- dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade_commodities",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )

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

  dbDisconnect(con2)
  rm(con2)
}

if (!"uncomtrade_trade" %in% dbListTables(con)) {
  dbSendQuery(
    con,
    "CREATE TABLE uncomtrade_trade (
    	year int4 NULL,
    	importer_iso3 text NULL,
    	exporter_iso3 text NULL,
    	industry_id int4 NULL,
    	import_value_usd float8 NULL,
    	export_value_usd float8 NULL
    )"
  )

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

rm(a02, b05)

# Import raw production data ----

if (!"undata_production" %in% dbListTables(con)) {
  d_prod <- read_csv(txt_production_isic4,
                       col_types = cols(`Sub Group` = col_character(),
                                        Year = col_character(),
                                        Series = col_character(),
                                        `SNA system` = col_character(),
                                        Value = col_character(),
                                        `Value Footnotes` = col_character()),
                       trim_ws = TRUE, n_max = 3477) %>%
    clean_names()

  d_prod_footnotes <- read_csv(txt_production_isic4, trim_ws = TRUE, skip = 3478) %>%
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

  d_prod <- d_prod %>%
    select(-c(sna93_table_code, sub_group, item, sna93_item_code, series, sna_system, fiscal_year_type))

  dbSendQuery(
    con,
    "CREATE TABLE undata_production (
    	country_or_area text NULL,
    	sub_item text NULL,
    	year int4 NULL,
    	currency text NULL,
    	value float8 NULL,
    	value_footnotes text NULL
    )"
  )

  dbSendQuery(
    con,
    "CREATE TABLE undata_production_footnotes (
    	footnote_seq_id int4 NULL,
    	footnote text NULL
    )"
  )

  dbWriteTable(con, "undata_production", d_prod, append = T)

  dbWriteTable(con, "undata_production_footnotes", d_prod_footnotes, append = T)
}

# Import exchange rate ----

if (!"imf_exchange_rate" %in% dbListTables(con)) {
  exchange_rate <- read_csv(
      "inp/imf_fishing_forestry_exchange_rate.csv",
      col_types = cols(
        `1986` = col_double(),
        `1987` = col_double(),
        `1988` = col_double(),
        `1989` = col_double(),
        `1990` = col_double(),
        `1991` = col_double(),
        `1992` = col_double(),
        `1993` = col_double(),
        `1994` = col_double(),
        `1995` = col_double(),
        `1996` = col_double(),
        `1997` = col_double(),
        `1998` = col_double(),
        `1999` = col_double(),
        `2000` = col_double(),
        `2001` = col_double(),
        `2002` = col_double(),
        `2003` = col_double(),
        `2004` = col_double(),
        `2005` = col_double(),
        `2006` = col_double(),
        `2007` = col_double(),
        `2008` = col_double(),
        `2009` = col_double(),
        `2010` = col_double(),
        `2011` = col_double(),
        `2012` = col_double(),
        `2013` = col_double(),
        `2014` = col_double(),
        `2015` = col_double(),
        `2016` = col_double(),
        `2017` = col_double(),
        `2018` = col_double(),
        `2019` = col_double(),
        `2020` = col_double()
      )
    ) %>%
    clean_names()

  exchange_rate <- exchange_rate %>%
    pivot_longer(x1986:x2020) %>%
    rename(year = name) %>%
    mutate(year = as.integer(gsub("x", "", year)))

  exchange_rate <- exchange_rate %>%
    select(year, everything()) %>%
    arrange(year, country_name, indicator_name)

  unique(exchange_rate$x42)

  exchange_rate <- exchange_rate %>%
    select(-x42)

  exchange_rate <- exchange_rate %>%
    filter(attribute == "Value") %>%
    filter(indicator_name == "Exchange Rates, US Dollar per Domestic Currency, Period Average, Rate") %>%
    select(-attribute, -indicator_name, -country_code, -indicator_code, -base_year)

  dbSendQuery(
    con,
    "CREATE TABLE imf_exchange_rate (
    	year int4 NULL,
    	country_name text NULL,
    	value float8 NULL
    )"
  )

  dbWriteTable(con, "imf_exchange_rate", exchange_rate, overwrite = F, append = T)
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
        left_join(
          tbl(con2, "hs_rev1992_countries") %>%
            select(reporter_iso = country_iso, reporter_code = country_code,
                   reporter = country)
        ) %>%
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

  dbSendQuery(
    con,
    "CREATE TABLE uncomtrade_country_codes (
    	country_iso3 text NULL,
    	country_id int4 NULL,
    	country text NULL
    )"
  )

  dbWriteTable(con, "uncomtrade_country_codes", d_iso_codes, overwrite = F, append = T)

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

  ### Get raw production in local currencies ----

  d_prod <- tbl(con, "undata_production") %>%
    filter(year %in% 1986L:2020) %>%
    select(year, country = country_or_area, sub_item, currency, production_local_currency = value) %>%
    mutate(country = toupper(country)) %>%
    left_join(
      tbl(con, "uncomtrade_country_codes")
    ) %>%
    collect()

  ### Fix ISO-3 codes ----

  d_prod <- d_prod %>%
    mutate(
      country_iso3 = case_when(
        # https://en.wikipedia.org/wiki/ISO_3166-1_alpha-3
        country == "BOSNIA AND HERZEGOVINA" ~ "BIH",
        country == "BRITISH VIRGIN ISLANDS" ~ "VGB",
        country == "CAYMAN ISLANDS" ~ "CYM",
        country == "CHINA, HONG KONG SPECIAL ADMINISTRATIVE REGION" ~ "HKG",
        country == "CURAÇAO" ~ "CUW",
        country == "DOMINICAN REPUBLIC" ~ "DOM",
        country == "FAROE ISLANDS" ~ "FRO",
        country == "FRANCE" ~ "FRA",
        country == "INDIA" ~ "IND",
        country == "IRAN (ISLAMIC REPUBLIC OF)" ~ "IRN",
        country == "ITALY" ~ "IND",
        country == "LIECHTENSTEIN" ~ "LIE",
        country == "NORWAY" ~ "NOR",
        country == "REPUBLIC OF KOREA" ~ "KOR",
        country == "REPUBLIC OF MOLDOVA" ~ "MDA",
        country == "SAN MARINO" ~ "SMR",
        country == "SINT MAARTEN" ~ "SXM",
        country == "SOUTH SUDAN" ~ "SSD",
        country == "TANZANIA - MAINLAND" ~ "TZA",
        country == "UNITED STATES" ~ "USA",
        country == "ZANZIBAR" ~ "EAZ",
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
    select(year, country_iso3, industry_id, production_local_currency)

  d_prod %>%
    filter(is.na(industry_id))

  ### Local currency in million ----

  d_prod <- d_prod %>%
    arrange(country_iso3, year)

  d_prod <- d_prod %>%
    rename(exporter_iso3 = country_iso3) %>%
    mutate(
      importer_iso3 = exporter_iso3,
      production_local_currency = production_local_currency / 1000000
    ) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, production_local_currency)

  ### Convert to USD ----

  d_prod <- d_prod %>%
    inner_join(
      tbl(con, "imf_exchange_rate") %>%
        select(year, country_name, value) %>%
        mutate(
          country_name = toupper(country_name),
          country_name = str_replace(country_name, ", ISLAMIC REP. OF|, REP.*|, UNITED .*|, KINGDOM .*|, THE|, UNION .*|, ARAB .*", ""),
          country_name = case_when(
            country_name == "BOLIVIA" ~ "BOLIVIA (PLURINATIONAL STATE OF)",
            country_name == "BOSNIA AND HERZEGOVINA" ~ "BOSNIA HERZEGOVINA",
            country_name == "CHINA, P.R.: MAINLAND" ~ "CHINA",
            country_name == "CHINA, P.R.: HONG KONG" ~ "CHINA, HONG KONG SAR",
            country_name == "CHINA, P.R.: MACAO" ~ "CHINA, MACAO SAR",
            country_name == "CONGO, DEM. REP. OF THE" ~ "DEM. REP. OF THE CONGO",
            country_name == "CZECH REP." ~ "CZECHIA",
            country_name == "ERITREA STATE OF" ~ "ERITREA",
            country_name == "ETHIOPIA FEDERAL DEM. REP. OF" ~ "ETHIOPIA",
            country_name == "FAROE ISLANDS" ~ "FAEROE ISDS",
            country_name == "SLOVAK REP." ~ "SLOVAKIA",
            country_name == "UNITED STATES" ~ "USA",
            TRUE ~ country_name
          )
        ) %>%
        inner_join(
          tbl(con, "uncomtrade_country_codes") %>%
            select(country, country_iso3),
          by = c("country_name" = "country")
        ) %>%
        collect(),
      by = c("year", "exporter_iso3" = "country_iso3")
    )

  # d_prod %>%
  #   filter(is.na(value)) %>%
  #   distinct(exporter_iso3, country_name)

  d_prod <- d_prod %>%
    select(-country_name) %>%
    drop_na(value)

  d_prod <- d_prod %>%
    mutate(production_usd = production_local_currency * value) %>%
    select(-value)

  ### Join with trade ---

  d_prod <- d_prod %>%
    full_join(
      d_trade %>%
        # remove domestic trade before substracting from production
        filter(exporter_iso3 != importer_iso3) %>%
        group_by(year, exporter_iso3, industry_id) %>%
        summarise(total_exports = sum(trade, na.rm = T))
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

  d_trade %>%
    filter(exporter_iso3 == importer_iso3)

  rm(d_prod)

  dbSendQuery(
    con,
    "CREATE TABLE public.uncomtrade_trade_tidy (
    	year int4 NULL,
    	exporter_iso3 text NULL,
    	importer_iso3 text NULL,
    	industry_id int4 NULL,
    	trade float8 NULL,
    	trade_flag_code int4 NULL
    )"
  )

  dbWriteTable(con, "uncomtrade_trade_tidy", d_trade, overwrite = F, append = T)
}

dbDisconnect(con)
