# Tidy FAO data ----

## Attributes ----

fao_reporters <- tbl(con, "fao_trade_country_code") %>%
  pull(country_code)

if (!"fao_country_correspondence" %in% dbListTables(con)) {
  fao_country_correspondence <- read_csv("inp/faostat_country_correspondence.csv") %>%
    clean_names()

  saveRDS(fao_country_correspondence, "out/fao_country_correspondence.rds")

  dbWriteTable(con, "fao_country_correspondence", fao_country_correspondence, overwrite = T)

  rm(fao_country_correspondence)
}

if (!"usitc_fao_to_itpde" %in% dbListTables(con)) {
  usitc_fao_to_itpde <- read_csv(csv_fcl_id) %>%
    clean_names() %>%
    select(industry_id = itpd_id, item_code = fcl_item_code) %>%
    mutate_if(is.double, as.integer)

  saveRDS(usitc_fao_to_itpde, "out/usitc_fao_to_itpde.rds")

  dbWriteTable(con, "usitc_fao_to_itpde", usitc_fao_to_itpde, overwrite = T)

  rm(fcl_to_itpde)
}
