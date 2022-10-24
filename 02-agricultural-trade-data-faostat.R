library(archive)
library(readr)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)

# 1: Download trade data ----

url_trade_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip"
zip_trade_data <- "inp/zip/faostat_trade_matrix.zip"

try(dir.create("inp/zip", recursive = T))

if (!file.exists(zip_trade_data)) {
  download.file(url_trade_data, zip_trade_data)
}

if (length(list.files("inp/csv/faostat_trade_matrix")) == 0) {
  archive_extract(zip_trade_data, dir = "inp/csv/faostat_trade_matrix")
}

# 2: Download FCL (FAOSTAT) to ITPD-E codes ----

url_fcl_id <- "https://www.usitc.gov/data/gravity/itpde_concordances/itpd_e_r02_ag_fcl.csv"
csv_fcl_id <- "inp/csv/fcl_to_itpde.csv"

if (!file.exists(csv_fcl_id)) {
  download.file(url_fcl_id, csv_fcl_id)
}

# 2: Download correspondence between FAO country codes and ISO-3 country codes ----

# the file was downloaded from the browser and points to
# blob:https://www.fao.org/57203009-07f0-4867-9eaf-81a56b990566
# see image for https://www.fao.org/faostat/en/#data/QV
# (inp/img/faostats_country_correspondence.png)
# the file is inp/csv/faostat_country_correspondence.csv

# 3: Download correspondence between FAO product codes and HS product codes ----

# the file was downloaded from the browser and points to
# blob:https://www.fao.org/57203009-07f0-4867-9eaf-81a56b990566
# see image for https://www.fao.org/faostat/en/#data/QV
# (inp/img/faostats_country_correspondence.png)
# the file is inp/csv/faostat_country_correspondence.csv

# 4: Download HS 92 to ISIC 3 correspondence table ----

url_fcl_hs <- "http://wits.worldbank.org/data/public/concordance/Concordance_H0_to_I3.zip"
zip_fcl_hs <- "inp/zip/hs92_to_isic3.zip"

if (!file.exists(zip_fcl_hs)) {
  download.file(url_fcl_hs, zip_fcl_hs)
}

if (length(list.files("inp/csv/hs92_to_isic3")) == 0) {
  archive_extract(zip_fcl_hs, dir = "inp/csv/hs92_to_isic3")
}

# 5: Tidy data ----

## 5.1 tidy codes ----

fao_data <- read_csv("inp/csv/faostat_trade_matrix/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv") %>%
  clean_names()

fao_element_code <- fao_data %>%
  select(element_code, element) %>%
  distinct()

fao_data <- fao_data %>%
  select(-c(element, element_code_fao))

fao_item_code <- fao_data %>%
  select(item_code, item_code_cpc, item) %>%
  mutate(item_code_cpc = gsub("^\'", "", item_code_cpc)) %>%
  distinct()

fao_data <- fao_data %>%
  select(-c(item, item_code_cpc))

fao_data <- fao_data %>%
  select(-c(reporter_country_code_m49, reporter_countries,
            partner_country_code_m49, partner_countries)) %>%
  mutate(
    reporter_country_code = as.integer(reporter_country_code),
    partner_country_code = as.integer(partner_country_code)
  )

fao_unit_code <- fao_data %>%
  select(unit) %>%
  distinct() %>%
  mutate(unit_code = row_number())

fao_data <- fao_data %>%
  left_join(fao_unit_code) %>%
  select(reporter_country_code:element_code, unit_code, y1986:y2020)

## 5.2 convert wide to long ----

fao_data <- fao_data %>%
  group_by(reporter_country_code) %>%
  nest()

fao_data <- map_df(
  fao_data %>% pull(reporter_country_code),
  function(c) {
    message(c)
    fao_data %>%
      filter(reporter_country_code == c) %>%
      unnest(data) %>%
      pivot_longer(y1986:y2020, names_to = "year", values_to = "value") %>%
      mutate(year = as.integer(gsub("y", "", year))) %>%
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

      left_join(fao_element_code) %>%
      select(-element_code) %>%
      left_join(fao_unit_code) %>%
      mutate(element = paste(element, unit)) %>%
      select(-unit, -unit_code) %>%
      pivot_wider(names_from = "element", values_from = "value") %>%
      clean_names() %>%
      rename(
        import_value_usd = import_value_1000_us,
        export_value_usd = export_value_1000_us
      )
  }
)

fao_data <- fao_data %>%
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
         starts_with("export"), starts_with("import")) %>%
  rename(
    export_quantity_no_unit = export_quantity_no,
    import_quantity_no_unit = import_quantity_no
  )

## 5.3 convert country codes ----

fao_country_correspondence <- read_csv("inp/csv/faostat_country_correspondence.csv") %>%
  clean_names() %>%
  select(country_code, iso3_code)

fao_data <- fao_data %>%
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

# 6: Convert FCL to ITPD-E, filter and aggregate ----

fcl_to_itpde <- read_csv("inp/csv/fcl_to_itpde.csv") %>%
  clean_names() %>%
  select(industry_id = itpd_id, item_code = fcl_item_code) %>%
  mutate_if(is.double, as.integer)

fao_data <- fao_data %>%
  inner_join(fcl_to_itpde, by = "item_code") %>%
  group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
  summarise(import_value_usd = sum(import_value_usd, na.rm = T))

fao_data %>%
  filter(reporter_iso3 == "CHL", partner_iso3 == "BRA", year == 2010L,
         industry_id %in% unique(fcl_to_itpde$industry_id)) %>%
  mutate(import_value_usd = import_value_usd / 1000000) %>%
  arrange(industry_id)

library(usitcgravity)

con <- usitcgravity_connect()

industries <- unique(fcl_to_itpde$industry_id)

tbl(con, "trade") %>%
  filter(importer_iso3 == "CHL", exporter_iso3 == "BRA", year == 2010L,
         industry_id %in% industries) %>%
  arrange(industry_id) %>%
  select(exporter_iso3, importer_iso3, industry_id, trade) %>%
  collect()

# I need to check these values

# X: save ----

try(dir.create("out/rds", recursive = T))

saveRDS(fao_data, "out/rds/fao_data.rds")
saveRDS(fao_item_code, "out/rds/fao_item_code.rds")
