## Trade ----

if ("fao_trade_tidy" %in% dbListTables(con)) {
  dbRemoveTable(con, "fao_trade_tidy")
}

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

      # tbl(con, "fao_trade_unit_code")

      d <- tbl(con, "fao_trade") %>%
        filter(year == y, element_code %in% c(5622L, 5922L)) %>%
        mutate(value = value * 1000) %>% # see tbl fao_trade_unit_code
        left_join(
          tbl(con, "fao_trade_element_code") %>%
            mutate(element = tolower(str_replace_all(element, " ", "_")))
        ) %>%
        select(-c(element_code, flag_id)) %>%
        collect() %>%
        pivot_wider(names_from = "element", values_from = "value")

      ## Convert country codes ----

      # fao_country_correspondence %>%
      #   rename(reporter_iso3 = iso3_code) %>%
      #   group_by(country_code) %>%
      #   summarise(n = n()) %>%
      #   filter(n > 1)

      d <- d %>%
        inner_join(
          fao_country_correspondence %>%
            rename(reporter_iso3 = iso3_code),
          by = c("reporter_country_code" = "country_code")
        ) %>%
        inner_join(
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

      d_aux <- tbl(con, "uncomtrade_imports") %>%
        filter(year == y, commodity_code == cmty_cd) %>%
        select(year, reporter_code,
          reporter_iso3 = reporter_iso,
          partner_code, partner_iso3 = partner_iso,
          import_value_usd = trade_value_usd
        ) %>%
        full_join(
          tbl(con, "uncomtrade_exports") %>%
            filter(year == y, commodity_code == cmty_cd) %>%
            select(year, reporter_code,
              reporter_iso3 = reporter_iso,
              partner_code, partner_iso3 = partner_iso,
              export_value_usd = trade_value_usd
            )
        ) %>%
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
        group_by(year, reporter_iso3, partner_iso3) %>%
        summarise(
          import_value_usd = sum(import_value_usd, na.rm = T),
          export_value_usd = sum(export_value_usd, na.rm = T)
        ) %>%
        ungroup() %>%
        mutate(industry_id = 2L) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            filter(year == y) %>%
            rename(
              reporter_iso3 = country_iso3,
              reporter_dynamic_code = country_dynamic_code,
              reporter_name = country_name
            )
        ) %>%
        inner_join(
          tbl(con, "usitc_country_codes") %>%
            distinct() %>%
            rename(
              partner_iso3 = country_iso3,
              partner_dynamic_code = country_dynamic_code,
              partner_name = country_name
            )
        ) %>%
        collect()

      # d_aux %>%
      #   group_by(reporter_iso3, partner_iso3) %>%
      #   summarise(n = n()) %>%
      #   filter(n > 1)

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

      dbWriteTable(con, "fao_trade_tidy", d, append = T)
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
