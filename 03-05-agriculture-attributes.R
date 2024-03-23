# Tidy FAO data ----

## Attributes ----

fao_reporters <- tbl(con, "fao_trade_country_code") %>%
  pull(country_code)

if (!"fao_country_correspondence" %in% dbListTables(con)) {
  fao_country_correspondence <- read_csv("inp/faostat_country_correspondence.csv") %>%
    clean_names() %>%
    mutate(country = iconv(country, to = "UTF-8", sub = ""))

  # fao_country_correspondence %>%
  #   filter(country_code %in% c(107, 182, 223))

  fao_country_correspondence <- fao_country_correspondence %>%
    mutate(
      start_year = as.integer(case_when(
        is.na(start_year) ~ 1986L,
        TRUE ~ start_year
      )),
      end_year = as.integer(case_when(
        is.na(end_year) ~ 2020L,
        TRUE ~ end_year
      ))
    ) %>%
    pivot_longer(
      cols = c(start_year, end_year),
      names_to = "year_type",
      values_to = "year"
    ) %>%
    select(-year_type)

  fao_country_correspondence <- map_df(
    unique(fao_country_correspondence$country_code),
    function(code) {
      d <- fao_country_correspondence %>%
        filter(country_code == code)

      d2 <- tibble(year = d$year[1]:d$year[2])

      d2 %>%
        left_join(d, by = "year") %>%
        fill(country_code, country, iso2_code, iso3_code, m49_code)
    }
  )

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
