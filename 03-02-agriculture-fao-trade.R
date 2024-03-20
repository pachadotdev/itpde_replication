# Import raw trade data ----

## FAO Trade data ----

if (!"fao_trade" %in% dbListTables(con)) {
  ### read ---
  fao_trade <- read_csv("inp/faostat_trade_matrix_normalized/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv") %>%
    clean_names()

  ### tidy countries ----

  fao_reporter_country_code <- fao_trade %>%
    select(reporter_country_code, reporter_countries) %>%
    distinct() %>%
    mutate(reporter_country_code = as.integer(reporter_country_code))

  fao_reporter_country_code_m49 <- fao_trade %>%
    select(reporter_country_code_m49, reporter_countries) %>%
    distinct() %>%
    mutate(reporter_country_code_m49 = as.character(gsub("\'", "", reporter_country_code_m49)))

  fao_trade <- fao_trade %>%
    select(-c(reporter_country_code_m49, reporter_countries)) %>%
    mutate(reporter_country_code = as.integer(reporter_country_code))

  fao_partner_country_code <- fao_trade %>%
    select(partner_country_code, partner_countries) %>%
    distinct() %>%
    mutate(partner_country_code = as.integer(partner_country_code))

  fao_partner_country_code_m49 <- fao_trade %>%
    select(partner_country_code_m49, partner_countries) %>%
    distinct() %>%
    mutate(partner_country_code_m49 = as.character(gsub("\'", "", partner_country_code_m49)))

  fao_trade <- fao_trade %>%
    select(-c(partner_country_code_m49, partner_countries)) %>%
    mutate(partner_country_code = as.integer(partner_country_code))

  fao_country_code <- fao_reporter_country_code %>%
    rename(country_code = reporter_country_code, country = reporter_countries) %>%
    bind_rows(
      fao_partner_country_code %>%
        rename(country_code = partner_country_code, country = partner_countries)
    ) %>%
    distinct() %>%
    arrange() %>%
    mutate(country = iconv(country, to = "ASCII//TRANSLIT"))

  fao_country_code_m49 <- fao_reporter_country_code_m49 %>%
    rename(country_code_m49 = reporter_country_code_m49, country = reporter_countries) %>%
    bind_rows(
      fao_partner_country_code_m49 %>%
        rename(country_code_m49 = partner_country_code_m49, country = partner_countries)
    ) %>%
    distinct() %>%
    arrange() %>%
    mutate(country = iconv(country, to = "ASCII//TRANSLIT"))

  ### tidy items ----

  fao_item_code <- fao_trade %>%
    select(item_code, item) %>%
    distinct() %>%
    mutate(
      item_code = as.integer(item_code),
      item = iconv(item, to = "ASCII//TRANSLIT")
    )

  fao_item_code_cpc <- fao_trade %>%
    select(item_code_cpc, item) %>%
    distinct() %>%
    mutate(
      item_code_cpc = as.character(gsub("\'", "", item_code_cpc)),
      item = iconv(item, to = "ASCII//TRANSLIT")
    )

  fao_trade <- fao_trade %>%
    select(-c(item, item_code_cpc)) %>%
    mutate(item_code = as.integer(item_code))

  ### tidy elements ----

  fao_element_code <- fao_trade %>%
    select(element_code, element) %>%
    distinct() %>%
    mutate(element_code = as.integer(element_code))

  fao_trade <- fao_trade %>%
    select(-element) %>%
    mutate(element_code = as.integer(element_code))

  ### tidy year ----

  fao_trade <- fao_trade %>%
    select(year = year_code, everything()) %>%
    mutate(year = as.integer(year))

  ### tidy units ----

  fao_unit_code <- fao_trade %>%
    select(element_code, unit) %>%
    distinct()

  fao_trade <- fao_trade %>%
    select(-unit)

  fao_trade <- fao_trade %>%
    group_by(year, element_code) %>%
    nest()

  fao_trade <- fao_trade %>%
    arrange(year, element_code)

  ### tidy flags ----

  fao_flag_code <- read_csv("inp/faostat_trade_matrix_normalized/Trade_DetailedTradeMatrix_E_Flags.csv") %>%
    clean_names() %>%
    select(flag_id = flag, flag = description)

  fao_trade <- fao_trade %>%
    rename(flag_id = flag)

  ### write ----

  map2(
    pull(fao_trade, year),
    pull(fao_trade, element_code),
    function(y, e) {
      message(sprintf("Processing year %s and element %s", y, e))

      d <- fao_trade %>%
        filter(year == y, element_code == e) %>%
        unnest(data)

      dbWriteTable(con, "fao_trade", d, append = T, overwrite = F)
    }
  )

  dbWriteTable(con, "fao_trade_country_code", fao_country_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_trade_country_code_m49", fao_country_code_m49, overwrite = T, append = F)
  dbWriteTable(con, "fao_trade_item_code", fao_item_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_trade_item_code_cpc", fao_item_code_cpc, overwrite = T, append = F)
  dbWriteTable(con, "fao_trade_element_code", fao_element_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_trade_unit_code", fao_unit_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_trade_flag_code", fao_flag_code, overwrite = T, append = F)

  rm(fao_trade, fao_country_code, fao_country_code_m49, fao_item_code, fao_item_code_cpc, fao_element_code, fao_unit_code, fao_flag_code)
  gc()
}
