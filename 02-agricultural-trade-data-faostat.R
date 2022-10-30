library(archive)
library(readr)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)
library(uncomtrademisc)
library(duckdb)
library(arrow)

# Download trade data ----

url_trade_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip"
zip_trade_data <- "inp/zip/faostat_trade_matrix.zip"

try(dir.create("inp/zip", recursive = T))

if (!file.exists(zip_trade_data)) {
  download.file(url_trade_data, zip_trade_data)
}

if (length(list.files("inp/csv/faostat_trade_matrix")) == 0) {
  archive_extract(zip_trade_data, dir = "inp/csv/faostat_trade_matrix")
}

# Download rice data (UN COMTRADE) ----

dir_comtrade_data <- "sitc-rev2/"
if (!dir.exists(dir_comtrade_data)) {
  data_downloading(arrow = T, token = 1, dataset = 7)
}

# Download production data ----

url_production_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Data.zip"
zip_production_data <- "inp/zip/faostat_production_matrix.zip"

try(dir.create("inp/zip", recursive = T))

if (!file.exists(zip_production_data)) {
  download.file(url_production_data, zip_production_data)
}

if (length(list.files("inp/csv/faostat_production_matrix")) == 0) {
  archive_extract(zip_production_data, dir = "inp/csv/faostat_production_matrix")
}

# Download FCL (FAOSTAT) to ITPD-E codes ----

url_fcl_id <- "https://www.usitc.gov/data/gravity/itpde_concordances/itpd_e_r02_ag_fcl.csv"
csv_fcl_id <- "inp/csv/fcl_to_itpde.csv"

if (!file.exists(csv_fcl_id)) {
  download.file(url_fcl_id, csv_fcl_id)
}

# Download correspondence between FAO country codes and ISO-3 country codes ----

# the file was downloaded from the browser and points to
# blob:https://www.fao.org/57203009-07f0-4867-9eaf-81a56b990566
# see image for https://www.fao.org/faostat/en/#data/QV
# (inp/img/faostats_country_correspondence.png)
# the file is inp/csv/faostat_country_correspondence.csv

# Import raw trade data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"agriculture_fao_trade_raw" %in% dbListTables(con)) {
  fao_trade <- read_csv("inp/csv/faostat_trade_matrix/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv") %>%
    clean_names()

  fao_element_code <- fao_trade %>%
    select(element_code, element) %>%
    distinct()

  fao_trade <- fao_trade %>%
    select(-c(element, element_code_fao))

  fao_trade <- fao_trade %>%
    select(-c(item, item_code_cpc))

  fao_trade <- fao_trade %>%
    select(-c(reporter_country_code_m49, reporter_countries,
              partner_country_code_m49, partner_countries)) %>%
    mutate(
      reporter_country_code = as.integer(reporter_country_code),
      partner_country_code = as.integer(partner_country_code)
    )

  fao_unit_code <- fao_trade %>%
    select(unit) %>%
    distinct() %>%
    mutate(unit_code = row_number())

  fao_trade <- fao_trade %>%
    left_join(fao_unit_code) %>%
    select(reporter_country_code:element_code, unit_code, y1986:y2020)

  dbWriteTable(con, "agriculture_fao_trade_raw", fao_trade)
  dbWriteTable(con, "agriculture_fao_element_code_raw", fao_element_code)
  dbWriteTable(con, "agriculture_fao_unit_code_raw", fao_unit_code)

  rm(fao_trade)
  gc()
}

dbDisconnect(con, shutdown = T)

# Import raw production data ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

if (!"agriculture_fao_production_raw" %in% dbListTables(con)) {
  fao_production <- read_csv("inp/csv/faostat_production_matrix/Value_of_Production_E_All_Data_NOFLAG.csv") %>%
    clean_names()

  fao_production <- fao_production %>%
    select(area_code, item_code, unit, y1986:y2020)

  dbWriteTable(con, "agriculture_fao_production_raw", fao_production)

  rm(fao_production)
  gc()
}

dbDisconnect(con, shutdown = T)

# Read codes ----

con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

fao_reporters <- tbl(con, "agriculture_fao_trade_raw") %>%
  distinct(reporter_country_code) %>%
  arrange() %>%
  pull()

fao_country_correspondence <- read_csv("inp/csv/faostat_country_correspondence.csv") %>%
  clean_names() %>%
  select(country_code, iso3_code)

# HERE I USE THE SITC 2 CODE EQUIVALENT TO HS 100610 BECAUSE HS92 STARTS IN 1988

rice_paddy <- product_correlation %>%
  select(sitc2, hs92) %>%
  filter(hs92 == "100610") %>%
  select(sitc2) %>%
  pull()

fcl_to_itpde <- read_csv("inp/csv/fcl_to_itpde.csv") %>%
  clean_names() %>%
  select(industry_id = itpd_id, item_code = fcl_item_code) %>%
  mutate_if(is.double, as.integer)

if (!"agriculture_fao_trade_tidy" %in% dbListTables(con)) {
  # Tidy trade data ----

  message("==== TRADE ====")

  fao_trade <- map(
    1986:2020,
    function(y) {
      message(y)
      con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

      ## Convert wide to long and tidy units ----

      d <- tbl(con, "agriculture_fao_trade_raw") %>%
        select(reporter_country_code, partner_country_code, item_code,
               element_code, unit_code, z = !!sym(paste0("y", y))) %>%
        collect() %>%

        pivot_longer(z, names_to = "year", values_to = "value") %>%
        mutate(year = y) %>%
        drop_na(value) %>%

        mutate(
          value = case_when(
            unit_code == 1L ~ value,
            unit_code == 2L ~ value * 1000,
            unit_code == 3L ~ value,
            unit_code == 4L ~ value * 1000,
            unit_code == 5L ~ value
          )
        ) %>%

        left_join(
          tbl(con, "agriculture_fao_element_code_raw") %>%
            collect()
        ) %>%
        select(-element_code) %>%
        left_join(
          tbl(con, "agriculture_fao_unit_code_raw") %>%
            collect()
        ) %>%
        mutate(element = paste(element, unit)) %>%
        select(-unit, -unit_code) %>%
        pivot_wider(names_from = "element", values_from = "value") %>%
        clean_names() %>%
        rename(
          import_value_usd = import_value_1000_us,
          export_value_usd = export_value_1000_us
        )

      d <- d %>%
        mutate(
          import_quantity_head = case_when(
            is.na(import_quantity_head) ~ import_quantity_1000_head,
            !is.na(import_quantity_head) ~ import_quantity_head
          ),
          export_quantity_head = case_when(
            is.na(export_quantity_head) ~ export_quantity_1000_head,
            !is.na(export_quantity_head) ~ export_quantity_head
          )
        ) %>%
        select(-export_quantity_1000_head, -import_quantity_1000_head) %>%
        select(reporter_country_code, partner_country_code, item_code, year,
               starts_with("export"), starts_with("import"))

      ## Convert country codes ----

      d <- d %>%
        left_join(
          fao_country_correspondence %>% rename(reporter_iso3 = iso3_code),
          by = c("reporter_country_code" = "country_code")
        ) %>%
        left_join(
          fao_country_correspondence %>% rename(partner_iso3 = iso3_code),
          by = c("partner_country_code" = "country_code")
        ) %>%
        select(reporter_iso3, partner_iso3, everything()) %>%
        ungroup() %>%
        select(-c(reporter_country_code, partner_country_code))

      ## Add rice (paddy) ----

      # FAOSTAT does not include international trade data for FCL item 27 “rice
      # (paddy)”. Instead, we use data from the UN Commodity Trade Statistics Database
      # (COMTRADE), HS sector 100,610 “Cereals; rice in the husk (paddy or rough)”.

      d_imp <- open_dataset(
        sources = paste0(dir_comtrade_data, "parquet/5/import"),
        partitioning = schema(
          year = int32(), reporter_iso = string())
      ) %>%
        filter(year == y) %>%
        filter(!(partner_iso %in% c("all","wld"))) %>%
        filter(commodity_code == rice_paddy) %>%
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

      d_exp <- open_dataset(
        sources = paste0(dir_comtrade_data, "/parquet/5/export"),
        partitioning = schema(
          year = int32(), reporter_iso = string())
      ) %>%
        filter(year == y) %>%
        filter(!(partner_iso %in% c("all","wld"))) %>%
        filter(commodity_code == rice_paddy) %>%
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

      d_imp <- d_imp %>%
        full_join(d_exp) %>%
        select(-commodity_code) %>%
        mutate(item_code = 27) %>%
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
          d %>%
            select(reporter_iso3, partner_iso3) %>%
            distinct()
        ) %>%
        group_by(year, reporter_iso3, partner_iso3, item_code) %>%
        summarise_if(is.numeric, sum, na.rm = T)

      d <- d %>%
        filter(item_code != 27) %>%
        bind_rows(
          d %>%
            filter(item_code == 27) %>%
            bind_rows(d_imp) %>%
            group_by(year, reporter_iso3, partner_iso3, item_code) %>%
            summarise_if(is.numeric, sum, na.rm = T) %>%
            ungroup()
        )

      ## Convert FCL to ITPD-E, filter and aggregate ----

      d <- d %>%
        inner_join(fcl_to_itpde, by = "item_code") %>%
        group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
        summarise(
          import_value_usd = sum(import_value_usd, na.rm = T),
          export_value_usd = sum(export_value_usd, na.rm = T)
        )

      ## Add mirrored flows and flags ----

      d2 <- d %>%
        ungroup() %>%
        select(-export_value_usd)

      d <- d %>%
        ungroup() %>%
        select(-import_value_usd)

      d <- d %>%
        full_join(d2, by = c("year", "industry_id",
                             "reporter_iso3" = "partner_iso3",
                             "partner_iso3" = "reporter_iso3")) %>%
        mutate_if(is.double, function(x) ifelse(is.na(x), 0, x)) %>%
        mutate(
          trade = case_when(
            import_value_usd == 0 ~ export_value_usd,
            TRUE ~ import_value_usd
          ),
          flag_mirror = case_when(
            trade == export_value_usd ~ 0L,
            TRUE ~ 1L
          ),
          flag_zero = case_when(
            trade > 0 ~ "p",
            trade == 0 ~ "r" # I still need to add "flag = u" later
          ),
          flag_flow = "i"
        ) %>%
        rename(
          exporter_iso3 = reporter_iso3,
          importer_iso3 = partner_iso3
        )

      rm(d2)

      if (!"export_quantity_no" %in% colnames(d)) {
        d$export_quantity_no <- 0
      }

      if (!"import_quantity_no" %in% colnames(d)) {
        d$import_quantity_no <- 0
      }

      d <- d %>%
        rename(
          export_quantity_no_unit = export_quantity_no,
          import_quantity_no_unit = import_quantity_no
        )

      d <- d[colnames(d) %in% c("year", "exporter_iso3", "importer_iso3",
                                "industry_id", "trade", "flag_mirror",
                                "flag_zero", "flag_flow")]

      dbWriteTable(con, "agriculture_fao_trade_tidy", d, append = T, overwrite = F)
      dbDisconnect(con, shutdown = T)
      gc()

      return(TRUE)
    }
  )

  # Tidy production data ----

  message("==== PRODUCTION ====")

  fao_production <- map(
    1986:2020,
    function(y) {
      message(y)
      con <- dbConnect(duckdb(), dbdir = "out/itpde_replication.duckdb", read_only = FALSE)

      ## Convert wide to long and country codes ----

      d <- tbl(con, "agriculture_fao_production_raw") %>%
        select(area_code, item_code, unit, z = !!sym(paste0("y",y))) %>%
        pivot_longer(z, names_to = "year", values_to = "value") %>%
        mutate(year = y) %>%
        collect() %>%

        drop_na(value) %>%
        group_by(year, area_code, item_code, unit) %>%
        summarise(value = sum(value, na.rm = T)) %>%
        ungroup() %>%

        pivot_wider(names_from = "unit", values_from = "value") %>%
        clean_names() %>%

        left_join(
          fao_country_correspondence %>%
            select(area_code = country_code, producer_iso3 = iso3_code)
        ) %>%
        select(year, producer_iso3, everything()) %>%
        select(-area_code)

      ## Tidy units ----

      d <- d %>%
        mutate(
          production_int_usd = 1000 * x1000_int,
          production_slc = 1000 * x1000_slc,
          production_usd = 1000 * x1000_us
        ) %>%
        select(-starts_with("x"))

      ## Convert FCL to ITPD-E, filter and aggregate ----

      # Construct domestic trade. Domestic trade is calculated as the difference between the
      # (gross) value of total production and total exports. Total exports are constructed as
      # the sum of bilateral trade for each exporting country. If we obtain a negative domestic
      # trade value, we do not include this observation in the ITPD-E-R02.

      d <- d %>%
        inner_join(fcl_to_itpde, by = "item_code") %>%
        select(-item_code) %>%
        group_by(year, producer_iso3, industry_id) %>%
        summarise_if(is.double, sum, na.rm = T) %>%
        ungroup()

      d <- d %>%
        rename(exporter_iso3 = producer_iso3) %>%
        mutate(importer_iso3 = exporter_iso3) %>%
        select(year, exporter_iso3, importer_iso3, industry_id, production_int_usd) %>%
        full_join(
          tbl(con, "agriculture_fao_trade_tidy") %>%
            filter(year == y) %>%
            group_by(year, exporter_iso3, industry_id) %>%
            summarise(total_exports = sum(trade, na.rm = T)) %>%
            collect()
        ) %>%
        mutate(trade = production_int_usd - total_exports) %>%
        filter(trade >= 0) %>%
        select(year, exporter_iso3, importer_iso3, industry_id, trade)

      ## Add flags ----

      d <- d %>%
        mutate(
          flag_mirror = 0L,
          flag_zero = case_when(
            trade > 0 ~ "p",
            trade == 0 ~ "r"
          ),
          flag_flow = "d"
        )

      dbWriteTable(con, "agriculture_fao_trade_tidy", d, append = T, overwrite = F)
      dbDisconnect(con, shutdown = T)
      gc()

      return(TRUE)
    }
  )
}
