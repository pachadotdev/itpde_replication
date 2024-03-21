# Tidy FAO data ----

## Attributes ----

fao_reporters <- tbl(con, "fao_trade_country_code") %>%
  pull(country_code)

if (!"fao_country_correspondence" %in% dbListTables(con)) {
  fao_country_correspondence <- read_csv("inp/faostat_country_correspondence.csv") %>%
    clean_names()

  dbWriteTable(con, "fao_country_correspondence", fao_country_correspondence, overwrite = T, append = F)

  rm(fao_country_correspondence)
}

if (!"usitc_fao_to_itpde" %in% dbListTables(con)) {
  fcl_to_itpde <- read_csv(csv_fcl_id) %>%
    clean_names() %>%
    select(industry_id = itpd_id, item_code = fcl_item_code) %>%
    mutate_if(is.double, as.integer)

  dbWriteTable(con, "usitc_fao_to_itpde", fcl_to_itpde, overwrite = T, append = F)

  rm(fcl_to_itpde)
}
