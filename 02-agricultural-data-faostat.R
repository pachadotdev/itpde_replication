library(archive)
library(readr)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)

url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip"
zip <- "inp/zip/faostat_trade_matrix.zip"

try(dir.create("inp/zip", recursive = T))

if (!file.exists(zip)) {
  download.file(url, zip)
}

if (length(list.files("inp/csv/faostat_trade_matrix")) == 0) {
  archive_extract(zip, dir = "inp/csv/faostat_trade_matrix")
}

fao_data <- read_csv("inp/csv/faostat_trade_matrix/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv") %>%
  clean_names() %>%
  filter(element == "Import Value") %>%
  select(reporter_country_code, partner_country_code, item_code_cpc, y1986:y2020)

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
      pivot_longer(y1986:y2020, names_to = "year", values_to = "imports") %>%
      mutate(year = as.integer(gsub("y", "", year))) %>%
      drop_na(imports)
  }
)

fao_data <- fao_data %>%
  mutate(
    item_code_cpc = gsub("^\'", "", item_code_cpc),
    imports = 1000 * imports
  )

try(dir.create("out/rds", recursive = T))

saveRDS(fao_data, "out/rds/fao_data.rds")
