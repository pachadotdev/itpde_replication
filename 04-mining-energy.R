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

# Download UNIDO production data ----

# FROM THE ARTICLE: To take full advantage of MINSTAT, and to ensure maximum
# coverage in ITPD-E-R02, we use the ISIC rev. 3 and ISIC rev. 4 versions of
# MINSTAT. Availability of production data determined the last year of coverage
# for ‘Mining & Energy’ as 2019.

# ISIC 3
# the file was downloaded from the browser and points to
# https://stat.unido.org/database/MINSTAT%202022,%20ISIC%20Revision%203
# see image inp/unido_mining_and_energy_isic3_production.png
# the file is inp/export20221213213948_42867e0a-8404-428a-aaed-43b661c72bf0.xlsx

# ISIC 4
# the file was downloaded from the browser and points to
# https://stat.unido.org/database/MINSTAT%202022,%20ISIC%20Revision%204
# see image inp/unido_mining_and_energy_isic4_production.png
# the file is inp/export20221213213948_42867e0a-8404-428a-aaed-43b661c72bf0.xlsx

# Read codes ----

# FROM THE ARTICLE: A list of the ITPD-E mining and energy industries, together
# with the concordance that we created between ISIC rev. 3, ISIC rev. 4, and
# ITPD-E-R02 appear in Table 6.

article_table6 <- read_csv("out/article_table6.csv")

# I only have HS92 to ISIC3 table => I use ISIC3

# look at inp/isic3.txt
isic3_mining <- as.character(c(
  # 29 - Mining and agglomeration of hard coal
  1010,
  # 30 - Mining and agglomeration of lignite
  1020,
  # 32 - Mining of iron ores
  1310,
  # 33 - Other mining and quarrying
  1410,1421,1422,1429
))

# recycle from the previous codes
con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"mining_energy_isic3_to_hs92" %in% dbListTables(con)) {
  mining_codes <- tbl(con, "fishing_forestry_isic3_to_hs92") %>%
    filter(isic3 %in% isic3_mining) %>%
    collect() %>%
    select(-isic3_industry_code) %>%
    mutate(
      industry_id = case_when(
        isic3 == "1010" ~ 29,
        isic3 == "1020" ~ 30,
        isic3 == "1310" ~ 32,
        TRUE ~ 33
      )
    ) %>%
    distinct(hs92, .keep_all = T)

  dbWriteTable(con, "mining_energy_isic3_to_hs92", mining_codes, overwrite = T)
}

dbDisconnect(con, shutdown = T)

# Import raw trade data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"mining_energy_comtrade_trade_raw" %in% dbListTables(con)) {
  map(
    1988:2020,
    function(y) {
      message(y)

      con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

      d_imp <- open_dataset(
        sources = paste0(dir_comtrade_data, "parquet/6/import"),
        partitioning = schema(
          year = int32(), reporter_iso = string())
      ) %>%
        filter(year == y) %>%
        filter(!(partner_iso %in% c("all","wld"))) %>%
        select(year, reporter_iso3 = reporter_iso,
               partner_iso3 = partner_iso,
               commodity_code,
               trade_value_usd_imp = trade_value_usd,
               trade_value_tonnes_imp = netweight_kg) %>%
        collect() %>%
        inner_join(
          tbl(con, "mining_energy_isic3_to_hs92") %>%
            select(commodity_code = hs92, industry_id) %>%
            collect()
        ) %>%
        mutate(trade_value_tonnes_imp = trade_value_tonnes_imp * 1000) %>%
        group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d_exp <- open_dataset(
        sources = paste0(dir_comtrade_data, "parquet/6/import"),
        partitioning = schema(
          year = int32(), reporter_iso = string())
      ) %>%
        filter(year == y) %>%
        filter(!(partner_iso %in% c("all","wld"))) %>%
        select(year, reporter_iso3 = reporter_iso,
               partner_iso3 = partner_iso,
               commodity_code,
               trade_value_usd_exp = trade_value_usd,
               trade_value_tonnes_exp = netweight_kg) %>%
        collect() %>%
        inner_join(
          tbl(con, "mining_energy_isic3_to_hs92") %>%
            select(commodity_code = hs92, industry_id) %>%
            collect()
        ) %>%
        mutate(trade_value_tonnes_exp = trade_value_tonnes_exp * 1000) %>%
        group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d_imp <- d_imp %>%
        full_join(d_exp) %>%
        rename(
          export_value_usd = trade_value_usd_exp,
          export_quantity_tonnes = trade_value_tonnes_exp,

          import_value_usd = trade_value_usd_imp,
          import_quantity_tonnes = trade_value_tonnes_imp
        )

      rm(d_exp)

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

      dbWriteTable(con, "mining_energy_comtrade_trade_raw", d_imp,
                   append = T)

      dbDisconnect(con, shutdown = T)
    }
  )
}

# Import raw production data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"mining_energy_unido_production_raw" %in% dbListTables(con)) {
  production_isic3 <- read_excel("inp/export20221213213948_42867e0a-8404-428a-aaed-43b661c72bf0.xlsx",
                                 sheet = "Data") %>%
    clean_names() %>%
    filter(table_description_2 == "Output", unit == "$") %>%
    inner_join(
      tbl(con, "mining_energy_isic3_to_hs92") %>%
        select(isic3, industry_id) %>%
        distinct() %>%
        collect() %>%
        mutate(isic3 = substr(isic3, 1, 3)) %>%
        distinct(),
      by = c("isic" = "isic3")
    )

  # I need to convert those 1.23E4 to 1.23*10^4 and store a consistent value column

  production_isic3 <- production_isic3 %>%
    mutate(value = ifelse(value == "...", NA, value))

  production_isic3 <- production_isic3 %>%
    mutate(
      ten_factor = case_when(
        grepl("E", value) ~ TRUE,
        TRUE ~ FALSE
      )
    )

  production_isic3 <- production_isic3 %>%
    mutate(
      power = case_when(
        ten_factor == TRUE ~ gsub(".*E", "", value),
        TRUE ~  NA_character_
      ),
      value2 = case_when(
        ten_factor == TRUE ~ as.numeric(gsub("E.*", "", value)) * 10^ten_factor,
        TRUE ~  as.numeric(value)
      )
    )

  production_isic3 <- production_isic3 %>%
    group_by(year, country_description, industry_id) %>%
    summarise(value2 = sum(value2, na.rm = T))

  dbWriteTable(con, "mining_energy_unido_production_raw", production_isic3, append = T)

  dbDisconnect(con, shutdown = T)
}

# Tidy data (combine trade and production in same final table) ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"mining_energy_trade_tidy" %in% dbListTables(con)) {
  ## Tidy production -----

  ### Country names as ISO-3 ----

  d_prod <- tbl(con, "mining_energy_unido_production_raw") %>%
    mutate(country_description = toupper(country_description)) %>%
    rename(country = country_description) %>%
    left_join(
      tbl(con, "fishing_forestry_country_iso3_codes") %>%
        select(importer_iso3 = country_iso3, country)
    ) %>%
    collect()

  d_prod <- d_prod %>%
    mutate(country = iconv(country, to = "ASCII//TRANSLIT", sub = ""))

  d_prod <- d_prod %>%
    mutate(
      importer_iso3 = case_when(
        country == "CHINA, TAIWAN PROVINCE" ~ "TWN",
        country == "REPUBLIC OF KOREA" ~ "KOR",
        country == "TURKIYE" ~ "TUR",
        country == "NETHERLANDS ANTILLES" ~ "ANT",
        country == "UNITED STATES OF AMERICA" ~ "USA",
        country == "IRAN (ISLAMIC REPUBLIC OF)" ~ "IRN",
        country == "REPUBLIC OF MOLDOVA" ~ "MDA",
        country == "SYRIAN ARAB REPUBLIC" ~ "SYR",
        country == "COOK ISLANDS" ~ "COK",
        country == "CURACAO" ~ "CUW",
        country == "MARSHALL ISLANDS" ~ "MHL",
        TRUE ~ importer_iso3
      )
    )

  d_prod %>%
    filter(is.na(importer_iso3))

  d_prod <- d_prod %>%
    select(year, importer_iso3, industry_id, production_int_usd = value2)

  ### Compute internal flow ----

  d_prod <- d_prod %>%
    mutate(
      year = as.integer(year),
      exporter_iso3 = importer_iso3
    ) %>%
    full_join(
      tbl(con, "mining_energy_comtrade_trade_raw") %>%
        select(year, exporter_iso3 = partner_iso3, industry_id, import_value_usd) %>%
        group_by(year, exporter_iso3, industry_id) %>%
        summarise(total_exports = sum(import_value_usd, na.rm = T)) %>%
        collect()
    ) %>%
    mutate(trade = production_int_usd - total_exports) %>%
    filter(trade >= 0) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, trade)

  ### Add flags ----

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
