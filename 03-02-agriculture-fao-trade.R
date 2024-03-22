# Import raw trade data ----

## FAO Trade data ----

if (!"fao_trade" %in% dbListTables(con)) {
  ### read ---
  out <- "inp/faostat_trade_matrix_normalized/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).rds"

  if (!file.exists(out)) {
    fao_trade <- read_csv("inp/faostat_trade_matrix_normalized/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).csv") %>%
      clean_names()

    fao_trade <- fao_trade %>%
      mutate_if(is.character, as.factor)

    saveRDS(fao_trade, out)
  } else {
    fao_trade <- readRDS(out)
  }

  ### tidy countries ----

  fao_reporter_country_code <- fao_trade %>%
    select(reporter_country_code, reporter_countries) %>%
    distinct() %>%
    mutate(
      reporter_country_code = as.integer(reporter_country_code),
      reporter_countries = iconv(as.character(reporter_countries), to = "ASCII//TRANSLIT")
    )

  fao_reporter_country_code_m49 <- fao_trade %>%
    select(reporter_country_code_m49, reporter_countries) %>%
    distinct() %>%
    mutate(
      reporter_country_code_m49 = gsub("\'", "", as.character(reporter_country_code_m49)),
      reporter_countries = iconv(as.character(reporter_countries), to = "ASCII//TRANSLIT")
    )

  fao_trade <- fao_trade %>%
    select(-c(reporter_country_code_m49, reporter_countries)) %>%
    mutate(reporter_country_code = as.integer(reporter_country_code))

  fao_partner_country_code <- fao_trade %>%
    select(partner_country_code, partner_countries) %>%
    distinct() %>%
    mutate(
      partner_country_code = as.integer(partner_country_code),
      partner_countries = iconv(as.character(partner_countries), to = "ASCII//TRANSLIT")
    )

  fao_partner_country_code_m49 <- fao_trade %>%
    select(partner_country_code_m49, partner_countries) %>%
    distinct() %>%
    mutate(
      partner_country_code_m49 = as.character(gsub("\'", "", partner_country_code_m49)),
      partner_countries = iconv(as.character(partner_countries), to = "ASCII//TRANSLIT")
    )

  fao_trade <- fao_trade %>%
    select(-c(partner_country_code_m49, partner_countries)) %>%
    mutate(partner_country_code = as.integer(partner_country_code))

  fao_trade_country_code <- fao_reporter_country_code %>%
    rename(country_code = reporter_country_code, country = reporter_countries) %>%
    bind_rows(
      fao_partner_country_code %>%
        rename(country_code = partner_country_code, country = partner_countries)
    ) %>%
    distinct() %>%
    arrange()

  saveRDS(fao_trade_country_code, "out/fao_trade_country_code.rds")

  fao_trade_country_code_m49 <- fao_reporter_country_code_m49 %>%
    rename(country_code_m49 = reporter_country_code_m49, country = reporter_countries) %>%
    bind_rows(
      fao_partner_country_code_m49 %>%
        rename(country_code_m49 = partner_country_code_m49, country = partner_countries)
    ) %>%
    distinct() %>%
    arrange()

  saveRDS(fao_trade_country_code_m49, "out/fao_trade_country_code_m49.rds")

  ### tidy items ----

  fao_trade_item_code <- fao_trade %>%
    select(item_code, item) %>%
    distinct() %>%
    mutate(
      item_code = as.integer(item_code),
      item = iconv(item, to = "ASCII//TRANSLIT")
    )

  saveRDS(fao_trade_item_code, "out/fao_trade_item_code.rds")

  fao_trade_item_code_cpc <- fao_trade %>%
    select(item_code_cpc, item) %>%
    distinct() %>%
    mutate(
      item_code_cpc = gsub("\'", "", as.character(item_code_cpc)),
      item = iconv(item, to = "ASCII//TRANSLIT")
    )

  saveRDS(fao_trade_item_code_cpc, "out/fao_trade_item_code_cpc.rds")

  fao_trade <- fao_trade %>%
    select(-c(item, item_code_cpc)) %>%
    mutate(item_code = as.integer(item_code))

  ### tidy elements ----

  fao_trade_element_code <- fao_trade %>%
    select(element_code, element) %>%
    distinct() %>%
    mutate(
      element_code = as.integer(element_code),
      element = iconv(element, to = "ASCII//TRANSLIT")
    )

  saveRDS(fao_trade_element_code, "out/fao_trade_element_code.rds")

  fao_trade <- fao_trade %>%
    select(-element) %>%
    mutate(element_code = as.integer(element_code))

  ### tidy year ----

  fao_trade <- fao_trade %>%
    mutate(
      year = as.integer(year),
      year_code = as.integer(year_code)
    ) %>%
    select(year, year_code, everything())

  # all.equal(fao_trade$year, fao_trade$year_code)
  fao_trade <- fao_trade %>%
    select(-year_code)

  ### tidy units ----

  fao_trade_unit_code <- fao_trade %>%
    select(element_code, unit) %>%
    distinct() %>%
    mutate(unit = as.character(unit))

  fao_trade <- fao_trade %>%
    select(-unit)

  ### tidy flags ----

  fao_trade_flag_code <- read_csv("inp/faostat_trade_matrix_normalized/Trade_DetailedTradeMatrix_E_Flags.csv") %>%
    clean_names() %>%
    select(flag_id = flag, flag = description)

  saveRDS(fao_trade_flag_code, "out/fao_trade_flag_code.rds")

  fao_trade <- fao_trade %>%
    rename(flag_id = flag)

  ### write ----

  fao_trade <- fao_trade %>%
    arrange(year, element_code)

  saveRDS(fao_trade, "out/fao_trade.rds")

  fao_trade <- fao_trade %>%
    group_by(year) %>%
    nest()

  # drop fao_trade if exists
  if ("fao_trade" %in% dbListTables(con)) {
    dbRemoveTable(con, "fao_trade")
  }

  map(
    pull(fao_trade, year),
    function(y) {
      message(sprintf("Processing year %s", y))

      d <- fao_trade %>%
        filter(year == y) %>%
        unnest(data)

      dbWriteTable(con, "fao_trade", d, append = T)
    }
  )

  dbWriteTable(con, "fao_trade_country_code", fao_trade_country_code, overwrite = T)
  dbWriteTable(con, "fao_trade_country_code_m49", fao_trade_country_code_m49, overwrite = T)
  dbWriteTable(con, "fao_trade_item_code", fao_trade_item_code, overwrite = T)
  dbWriteTable(con, "fao_trade_item_code_cpc", fao_trade_item_code_cpc, overwrite = T)
  dbWriteTable(con, "fao_trade_element_code", fao_trade_element_code, overwrite = T)
  dbWriteTable(con, "fao_trade_unit_code", fao_trade_unit_code, overwrite = T)
  dbWriteTable(con, "fao_trade_flag_code", fao_trade_flag_code, overwrite = T)

  rm(fao_trade, fao_trade_country_code, fao_trade_country_code_m49, fao_trade_item_code, fao_trade_item_code_cpc, fao_trade_element_code, fao_trade_unit_code, fao_trade_flag_code)
  gc()
}
