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

## Trade ----

if (!"fao_trade_tidy" %in% dbListTables(con)) {
  message("==== FAO TRADE ====")

  fao_country_correspondence <- tbl(con, "fao_country_correspondence") %>%
    select(country_code, iso3_code) %>%
    collect()

  map(
    1986:2020,
    function(y, add_comtrade = FALSE) {
      message(y)

      ## Convert long to wide and tidy units ----

      d <- tbl(con, "fao_trade") %>%
        filter(year == y, element_code %in% c(5622L, 5922L)) %>%
        mutate(value = value * 1000) %>% # see tbl fao_trade_unit_code
        left_join(
          tbl(con, "fao_trade_element_code") %>%
            mutate(element = tolower(str_replace_all(element, " ", "_")))
        ) %>%
        select(-c(element_code, flag)) %>%
        collect() %>%
        pivot_wider(names_from = "element", values_from = "value")

      ## Convert country codes ----

      d <- d %>%
        left_join(
          fao_country_correspondence %>%
            rename(reporter_iso3 = iso3_code),
          by = c("reporter_country_code" = "country_code")
        ) %>%
        left_join(
          fao_country_correspondence %>%
            rename(partner_iso3 = iso3_code),
          by = c("partner_country_code" = "country_code")
        ) %>%
        select(-c(reporter_country_code, partner_country_code))

      ## Convert FCL to ITPD-E, filter and aggregate ----

      d <- d %>%
        inner_join(
          tbl(con, "usitc_fao_to_itpde") %>%
            collect(),
          by = "item_code"
        ) %>%
        group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
        summarise(
          import_value_usd = sum(import_value, na.rm = T),
          export_value_usd = sum(export_value, na.rm = T)
        ) %>%
        ungroup()

      ## Add rice paddy from UN COMTRADE ----

      # industry id "2" is item code "27"

      # use SITC 2 before 1988
      cmty_cd <- ifelse(y < 1988, "04211", "100610")

      d_aux_imp <- tbl(con, "uncomtrade_imports") %>%
        filter(
          year == y,
          commodity_code == cmty_cd
        ) %>%
        select(year, reporter_code,
          reporter_iso3 = reporter_iso,
          partner_code, partner_iso3 = partner_iso,
          import_value_usd = trade_value_usd
        ) %>%
        mutate(industry_id = 2L) %>%
        mutate(
          reporter_iso3 = toupper(case_when(
            reporter_code == 490L ~ "TWN",
            reporter_iso3 %in% c("DRC", "ZAR") ~ "COD",
            reporter_iso3 == "ROM" ~ "ROU",
            TRUE ~ reporter_iso3
          )),
          partner_iso3 = toupper(case_when(
            partner_code == 490L ~ "TWN",
            partner_iso3 %in% c("DRC", "ZAR") ~ "COD",
            partner_iso3 == "ROM" ~ "ROU",
            TRUE ~ partner_iso3
          ))
        ) %>%
        select(-reporter_code, -partner_code) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            distinct() %>%
            rename(reporter_iso3 = country_iso3)
        ) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            distinct() %>%
            rename(partner_iso3 = country_iso3)
        ) %>%
        collect()

      d_aux_exp <- tbl(con, "uncomtrade_exports") %>%
        filter(
          year == y,
          commodity_code == cmty_cd
        ) %>%
        select(year, reporter_code,
          reporter_iso3 = reporter_iso,
          partner_code, partner_iso3 = partner_iso,
          export_value_usd = trade_value_usd
        ) %>%
        mutate(industry_id = 2L) %>%
        mutate(
          reporter_iso3 = toupper(case_when(
            reporter_code == 490L ~ "TWN",
            reporter_iso3 %in% c("DRC", "ZAR") ~ "COD",
            reporter_iso3 == "ROM" ~ "ROU",
            TRUE ~ reporter_iso3
          )),
          partner_iso3 = toupper(case_when(
            partner_code == 490L ~ "TWN",
            partner_iso3 %in% c("DRC", "ZAR") ~ "COD",
            partner_iso3 == "ROM" ~ "ROU",
            TRUE ~ partner_iso3
          ))
        ) %>%
        select(-reporter_code, -partner_code) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            distinct() %>%
            rename(reporter_iso3 = country_iso3)
        ) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            distinct() %>%
            rename(partner_iso3 = country_iso3)
        ) %>%
        collect()

      d_aux <- d_aux_imp %>%
        full_join(
          d_aux_exp,
          by = c("year",
            "reporter_iso3",
            "partner_iso3",
            "industry_id"
          )
        )

      rm(d_aux_imp, d_aux_exp)

      d <- d %>%
        bind_rows(d_aux) %>%
        group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
        summarise_if(is.numeric, sum, na.rm = T)

      rm(d_aux)

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
          "partner_iso3" = "reporter_iso3"
        )) %>%
        mutate(
          trade = case_when(
            is.na(import_value_usd) | import_value_usd == 0 ~ export_value_usd,
            TRUE ~ import_value_usd
          ),
          trade_flag_code = case_when(
            is.na(trade) ~ 1L, # 1 means trade = NA
            import_value_usd == 0 ~ 2L, # 2 means trade = exports
            import_value_usd > 0 ~ 3L # 3 means trade = imports
          ),
          trade = case_when(
            is.na(trade) | trade < 0 ~ 0,
            TRUE ~ trade
          )
        ) %>%
        rename(
          exporter_iso3 = reporter_iso3,
          importer_iso3 = partner_iso3
        )

      rm(d2)

      dbWriteTable(con, "fao_trade_tidy", d, append = T, overwrite = F)
      gc()

      return(TRUE)
    }
  )

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
      "production - trade < 0"
    )
  )

  dbWriteTable(con, "fao_trade_tidy_flag_code", fao_trade_tidy_flag_code, overwrite = T)
}

## Production ----

if (!"fao_trade_domestic_tidy" %in% dbListTables(con)) {
  message("==== FAO PRODUCTION ====")

  ### read ----

  fao_production <- tbl(con, "fao_production") %>%
    select(area_code, item_code, unit_code, year, value) %>%
    filter(unit_code == 3) %>%
    mutate(value = value * 1000) %>%
    inner_join(
      tbl(con, "fao_country_correspondence") %>%
        select(area_code = country_code, producer_iso3 = iso3_code)
      # filter(str_length(producer_iso3) == 3L) %>%
      # filter(!str_detect(producer_iso3, "[0-9]"))
    ) %>%
    select(year, producer_iso3, item_code, production_value_usd = value) %>%
    collect()

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

  ### subset 1986+ ----

  fao_production <- fao_production %>%
    filter(year >= 1986L)

  ### remove exports from production ----

  fao_production <- fao_production %>%
    left_join(
      tbl(con, "fao_trade_tidy") %>%
        group_by(year, producer_iso3 = exporter_iso3, industry_id) %>%
        summarise(export_value_usd = sum(trade, na.rm = T)) %>%
        collect(),
      by = c("year", "producer_iso3", "industry_id")
    ) %>%
    rowwise() %>%
    mutate(
      trade = sum(production_value_usd, -1.0 * export_value_usd, na.rm = T)
    ) %>%
    ungroup()

  fao_production <- fao_production %>%
    rename(exporter_iso3 = producer_iso3) %>%
    mutate(importer_iso3 = exporter_iso3) %>%
    select(year, exporter_iso3, importer_iso3, industry_id, production_value_usd, export_value_usd, trade)

  fao_production <- fao_production %>%
    mutate(
      trade_flag_code = case_when(
        is.na(production_value_usd) ~ 4L # 4 means production = NA
      ),
      production_value_usd = case_when(
        is.na(production_value_usd) ~ 0,
        TRUE ~ production_value_usd
      ),
      trade_flag_code = case_when(
        trade_flag_code == 4L & is.na(export_value_usd) ~ 5L, # 5 means exports = NA and production = NA
        trade_flag_code == 4L & !is.na(export_value_usd) ~ 6L, # 6 means exports = NA and production != NA"
        TRUE ~ trade_flag_code
      ),
      export_value_usd = case_when(
        is.na(export_value_usd) ~ 0,
        TRUE ~ export_value_usd
      ),
      trade_flag_code = case_when(
        trade < 0 ~ 7L, # 7 means production - trade < 0
        TRUE ~ trade_flag_code
      ),
      trade = case_when(
        is.na(trade) | trade < 0 ~ 0,
        TRUE ~ trade
      )
    )

  dbWriteTable(con, "fao_trade_domestic_tidy", fao_production, overwrite = T, append = F)
}

# Verification ----

tbl(con, "fao_trade_domestic_tidy") %>%
  filter(year == 2000L) %>%
  select(year, exporter_iso3, importer_iso3, industry_id, trade.y1 = trade) %>%
  filter(industry_id == 7L, exporter_iso3 == "CHN") %>%
  summarise(trade = sum(trade.y1, na.rm = T) / 1000000)

tbl(con, "fao_trade_tidy") %>%
  filter(year == 2000L) %>%
  select(year, exporter_iso3, importer_iso3, industry_id, trade.y1 = trade) %>%
  filter(industry_id == 7L, exporter_iso3 == "CHN") %>%
  summarise(trade = sum(trade.y1, na.rm = T) / 1000000)

tbl(con, "usitc_trade") %>%
  filter(year == 2000L) %>%
  group_by(year, exporter_iso3, importer_iso3, industry_id) %>%
  summarise(trade = sum(trade, na.rm = T)) %>%
  ungroup() %>%
  collect() %>%
  full_join(
    tbl(con, "fao_trade_domestic_tidy") %>%
      filter(year == 2000L) %>%
      group_by(year, exporter_iso3, importer_iso3, industry_id) %>%
      summarise(trade = sum(trade, na.rm = T) / 1000000) %>%
      ungroup() %>%
      collect() %>%
      bind_rows(
        tbl(con, "fao_trade_tidy") %>%
          filter(year == 2000L) %>%
          group_by(year, exporter_iso3, importer_iso3, industry_id) %>%
          summarise(trade = sum(trade, na.rm = T) / 1000000) %>%
          ungroup() %>%
          collect()
      ),
    by = c("year", "exporter_iso3", "importer_iso3", "industry_id")
  ) %>%
  filter(trade.x != trade.y) %>%
  arrange(desc(abs(trade.x - trade.y)))
