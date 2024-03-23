## Production ----

if ("fao_trade_domestic_tidy" %in% dbListTables(con)) {
  dbRemoveTable(con, "fao_trade_domestic_tidy")
}

if (!"fao_trade_domestic_tidy" %in% dbListTables(con)) {
  message("==== FAO PRODUCTION ====")

  ### read ----

  # tbl(con, "fao_production_unit_code")

  fao_production <- tbl(con, "fao_production") %>%
    select(area_code, item_code, unit_code, year, value) %>%
    filter(year >= 1986L, unit_code == 3) %>%
    mutate(value = value * 1000) %>%
    group_by(year, area_code, item_code) %>%
    summarise(value = sum(value, na.rm = T)) %>%
    inner_join(
      tbl(con, "fao_country_correspondence") %>%
        select(area_code = country_code, producer_iso3 = iso3_code)
    ) %>%
    select(year, producer_iso3, item_code, production_value_usd = value) %>%
    collect()

  # fao_production %>%
  #   group_by(year, producer_iso3, item_code) %>%
  #   summarise(n = n()) %>%
  #   filter(n > 1)

  ### convert FCL to ITPD-E, filter and aggregate ----

  # Construct domestic trade. Domestic trade is calculated as the difference
  #  between the (gross) value of total production and total exports.
  # Total exports are constructed as the sum of bilateral trade for each
  #  exporting country.
  # If we obtain a negative domestic trade value, we do not include this
  #  observation in the ITPD-E-R02.

  fao_production <- fao_production %>%
    inner_join(
      tbl(con, "usitc_fao_to_itpde") %>%
        collect(),
      by = "item_code"
    ) %>%
    select(-item_code) %>%
    group_by(year, producer_iso3, industry_id) %>%
    summarise(production_value_usd = sum(production_value_usd, na.rm = T)) %>%
    ungroup()

  # fao_production %>%
  #   group_by(year, producer_iso3, industry_id) %>%
  #   summarise(n = n()) %>%
  #   filter(n > 1)

  ### remove exports from production ----

  fao_production <- fao_production %>%
    left_join(
      tbl(con, "fao_trade_tidy") %>%
        group_by(year, producer_iso3 = exporter_iso3, industry_id) %>%
        summarise(export_value_usd = -1.0 * sum(trade, na.rm = T)) %>%
        collect()
    ) %>%
    rowwise() %>%
    mutate(trade = sum(production_value_usd, export_value_usd, na.rm = T)) %>%
    ungroup()

  # the article says that if trade < 0, we should remove the observation

  fao_production <- fao_production %>%
    filter(trade >= 0)

  fao_production <- fao_production %>%
    rename(exporter_iso3 = producer_iso3) %>%
    mutate(importer_iso3 = exporter_iso3) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, production_value_usd, export_value_usd, trade)

  fao_production <- fao_production %>%
    mutate(
      production_value_usd = case_when(
        is.na(production_value_usd) ~ 0,
        TRUE ~ production_value_usd
      ),
      trade_flag_code = case_when(
        is.na(export_value_usd) ~ 5L, # 5 means exports = NA and production = NA
        !is.na(export_value_usd) ~ 6L # 6 means exports = NA and production != NA"
      ),
      export_value_usd = case_when(
        is.na(export_value_usd) ~ 0,
        TRUE ~ export_value_usd
      ),
      trade = case_when(
        is.na(trade) | trade < 0 ~ 0,
        TRUE ~ trade
      )
    )

  dbWriteTable(con, "fao_trade_domestic_tidy", fao_production, overwrite = T, append = F)
}
