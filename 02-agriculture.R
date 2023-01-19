library(archive)
library(readr)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)
library(RPostgres)
library(readxl)
library(stringr)

# Download trade data ----

url_trade_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip"
zip_trade_data <- "inp/faostat_trade_matrix.zip"

try(dir.create("inp/", recursive = T))

if (!file.exists(zip_trade_data)) {
  download.file(url_trade_data, zip_trade_data)
}

if (length(list.files("inp/faostat_trade_matrix")) == 0) {
  archive_extract(zip_trade_data, dir = "inp/faostat_trade_matrix")
}

# Download production data ----

url_production_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Data.zip"
zip_production_data <- "inp/faostat_production_matrix.zip"

try(dir.create("inp/", recursive = T))

if (!file.exists(zip_production_data)) {
  download.file(url_production_data, zip_production_data)
}

if (length(list.files("inp/faostat_production_matrix")) == 0) {
  archive_extract(zip_production_data, dir = "inp/faostat_production_matrix")
}

# Download FCL (FAOSTAT) to ITPD-E codes ----

url_fcl_id <- "https://www.usitc.gov/data/gravity/itpde_concordances/itpd_e_r02_ag_fcl.csv"
csv_fcl_id <- "inp/fcl_to_itpde.csv"

if (!file.exists(csv_fcl_id)) {
  download.file(url_fcl_id, csv_fcl_id)
}

# Download correspondence between FAO country codes and ISO-3 country codes ----

# the file was downloaded from the browser and points to
# blob:https://www.fao.org/57203009-07f0-4867-9eaf-81a56b990566
# see image for https://www.fao.org/faostat/en/#data/QV
# (inp/faostats_country_correspondence.png)
# the file is inp/faostat_country_correspondence.csv

# Import raw trade data ----

con <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "itpde_replication",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD")
)

if (!"fao_trade_matrix" %in% dbListTables(con)) {
  fao_trade <- read_csv("inp/faostat_trade_matrix/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv") %>%
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

  gc()

  fao_trade <- fao_trade %>%
    group_by(reporter_country_code) %>%
    nest()

  fao_trade <- fao_trade %>%
    arrange(reporter_country_code)

  fao_trade <- map_df(
    fao_trade$reporter_country_code,
    function(r) {
      print(r)
      fao_trade %>%
        filter(
          reporter_country_code == r
        ) %>%
        unnest(data) %>%
        pivot_longer(y1986:y2020, names_to = "year", values_to = "value") %>%
        mutate(year = as.integer(gsub("y", "", year))) %>%
        filter(!is.na(value))
    }
  )

  gc()

  dbWriteTable(con, "fao_trade_matrix_element_code", fao_element_code, overwrite = T)
  dbWriteTable(con, "fao_trade_matrix_unit_code", fao_unit_code, overwrite = T)

  fao_trade <- fao_trade %>%
    ungroup() %>%
    mutate(p = floor(row_number() / 2500000) + 1) %>%
    group_by(p) %>%
    nest() %>%
    ungroup() %>%
    select(data) %>%
    pull()

  gc()

  map(
    seq_along(fao_trade),
    function(x) {
      message(sprintf("Writing fragment %s of %s", x, length(fao_trade)))
      dbWriteTable(con, "fao_trade_matrix", fao_trade[[x]], append = TRUE, overwrite = FALSE)
    }
  )

  rm(fao_trade, fao_element_code, fao_unit_code)
  gc()
}

# Import raw production data ----

if (!"fao_production_matrix" %in% dbListTables(con)) {
  fao_production <- read_csv("inp/faostat_production_matrix/Value_of_Production_E_All_Data_NOFLAG.csv") %>%
    clean_names()

  fao_production <- fao_production %>%
    select(area_code, item_code, element, unit, y1986:y2020) %>%
    mutate(unit = str_replace_all(element, "Gross Production Value \\(|\\)", ""))

  fao_production <- fao_production %>%
    pivot_longer(y1986:y2020, names_to = "year", values_to = "value") %>%
    mutate(year = as.integer(gsub("y", "", year))) %>%
    filter(!is.na(value))

  fao_unit_code <- fao_production %>%
    select(unit) %>%
    distinct() %>%
    mutate(unit_code = row_number())

  fao_production <- fao_production %>%
    left_join(fao_unit_code) %>%
    select(area_code:item_code, unit_code, year, value)

  dbWriteTable(con, "fao_production_matrix", fao_production, overwrite = T)
  dbWriteTable(con, "fao_production_matrix_unit_code", fao_unit_code, overwrite = T)

  rm(fao_production, fao_unit_code)
  gc()
}

# Read codes ----

fao_reporters <- tbl(con, "fao_trade_matrix") %>%
  distinct(reporter_country_code) %>%
  arrange() %>%
  pull()

if (!"fao_country_correspondence" %in% dbListTables(con)) {
  fao_country_correspondence <- read_csv("inp/faostat_country_correspondence.csv") %>%
    clean_names()

  dbWriteTable(con, "fao_country_correspondence", fao_country_correspondence, overwrite = T)

  rm(fao_country_correspondence)
}

if (!"fao_fcl_to_itpde" %in% dbListTables(con)) {
  fcl_to_itpde <- read_csv(csv_fcl_id) %>%
    clean_names() %>%
    select(industry_id = itpd_id, item_code = fcl_item_code) %>%
    mutate_if(is.double, as.integer)

  dbWriteTable(con, "fao_fcl_to_itpde", fcl_to_itpde)
}

if (!"fao_trade_matrix_tidy" %in% dbListTables(con)) {
  # Tidy trade data ----

  message("==== TRADE ====")

  fao_trade <- map(
    1986:2020,
    function(y, add_comtrade = FALSE) {
      message(y)

      ## Convert wide to long and tidy units ----

      d <- tbl(con, "fao_trade_matrix") %>%
        filter(year == y) %>%
        collect() %>%
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
          tbl(con, "fao_trade_matrix_element_code") %>%
            collect()
        ) %>%
        select(-element_code) %>%

        left_join(
          tbl(con, "fao_trade_matrix_unit_code") %>%
            collect() %>%
            mutate(unit = gsub("1000 ", "", unit)) # remove the 1000 bc we re-scaled before
        ) %>%
        mutate(element = paste(element, unit)) %>%
        select(-unit, -unit_code) %>%

        pivot_wider(names_from = "element", values_from = "value") %>%
        clean_names() %>%

        rename(
          import_value_usd = import_value_us,
          export_value_usd = export_value_us
        )

      d <- d %>%
        select(reporter_country_code, partner_country_code, item_code, year,
               starts_with("export"), starts_with("import"))

      ## Convert country codes ----

      fao_country_correspondence <- tbl(con, "fao_country_correspondence") %>%
        select(country_code, iso3_code) %>%
        collect()

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

      ## Convert FCL to ITPD-E, filter and aggregate ----

      d <- d %>%
        inner_join(
          tbl(con, "fao_fcl_to_itpde") %>%
            collect(),
          by = "item_code"
        ) %>%
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
          trade_flag_code = case_when(
            is.na(trade) ~ 1L, # 1 means trade = NA
            import_value_usd == 0 ~ 2L, # 2 means trade = exports
            import_value_usd > 0 ~ 3L # 3 means trade = imports
          ),
          trade = case_when(
            is.na(trade) ~ 0,
            TRUE ~ trade
          )
        ) %>%
        rename(
          exporter_iso3 = reporter_iso3,
          importer_iso3 = partner_iso3
        )

      rm(d2)

      # if (!"export_quantity_no" %in% colnames(d)) {
      #   d$export_quantity_no <- 0
      # }
      #
      # if (!"import_quantity_no" %in% colnames(d)) {
      #   d$import_quantity_no <- 0
      # }
      #
      # d <- d %>%
      #   rename(
      #     export_quantity_no_unit = export_quantity_no,
      #     import_quantity_no_unit = import_quantity_no
      #   )
      #
      # d <- d[colnames(d) %in% c("year", "exporter_iso3", "importer_iso3",
      #                           "industry_id", "trade", "flag_mirror",
      #                           "flag_zero", "flag_flow")]

      dbWriteTable(con, "fao_trade_matrix_tidy", d, append = T, overwrite = F)
      rm(d)
      gc()

      return(TRUE)
    }
  )

  # Tidy production data (goes to same table as trade) ----

  message("==== PRODUCTION ====")

  fao_production <- tbl(con, "fao_production_matrix") %>%
    select(area_code, item_code, unit_code, year, value) %>%
    filter(unit_code == 4) %>%
    mutate(value = value * 1000) %>%
    collect() %>%
    drop_na(value) %>%

    left_join(
      tbl(con, "fao_production_matrix_unit_code") %>%
        collect() %>%
        mutate(unit = gsub("thousand ", "", unit)) # remove the 1000 bc we re-scaled before
    ) %>%
    select(-unit_code) %>%
    pivot_wider(names_from = "unit", values_from = "value") %>%
    clean_names() %>%

    left_join(
      tbl(con, "fao_country_correspondence") %>%
        select(area_code = country_code, producer_iso3 = iso3_code) %>%
        collect()
    ) %>%

    # here we select values in standard local currency
    select(year, producer_iso3, item_code, production_usd = current_us)

  ## Convert FCL to ITPD-E, filter and aggregate ----

  # Construct domestic trade. Domestic trade is calculated as the difference between the
  # (gross) value of total production and total exports. Total exports are constructed as
  # the sum of bilateral trade for each exporting country. If we obtain a negative domestic
  # trade value, we do not include this observation in the ITPD-E-R02.

  fao_production <- fao_production %>%
    inner_join(
      tbl(con, "fao_fcl_to_itpde") %>%
        collect(), by = "item_code"
    ) %>%
    select(-item_code) %>%
    group_by(year, producer_iso3, industry_id) %>%
    summarise_if(is.double, sum, na.rm = T) %>%
    ungroup()

  fao_production <- fao_production %>%
    rename(exporter_iso3 = producer_iso3) %>%
    mutate(importer_iso3 = exporter_iso3) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, production_usd) %>%
    inner_join(
      tbl(con, "fao_trade_matrix_tidy") %>%
        # remove domestic trade before substracting from production
        filter(exporter_iso3 != importer_iso3) %>%
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

  # add table with flags
  fao_trade_tidy_flag_code <- tibble(
    trade_flag_code = 1L:7L,
    trade_flag = c(
      "trade = NA",
      "trade = exports",
      "trade = imports",
      "production = NA",
      "exports = NA and production = NA",
      "exports = NA and production != NA",
      "production - trade < 0")
  )

  dbSendQuery(con, "delete from fao_trade_matrix_tidy where exporter_iso3 = importer_iso3")

  dbWriteTable(con, "fao_trade_matrix_tidy", fao_production, append = T, overwrite = F)
  dbWriteTable(con, "fao_trade_matrix_tidy_flag_code", fao_trade_tidy_flag_code, append = F, overwrite = T)
  gc()
}

dbDisconnect(con)
