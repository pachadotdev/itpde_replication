## Trade ----

message("==== FAO TRADE ====")

fao_country_correspondence <- tbl(con, "fao_country_correspondence") %>%
  select(year, country_code, m49_code, iso3_code) %>%
  collect() %>%
  filter(!str_detect(iso3_code, "\\d")) %>%
  inner_join(
    tbl(con, "usitc_country_codes") %>%
      filter(country_iso3 == country_dynamic_code) %>%
      select(year, country_iso3, country_dynamic_code) %>%
      distinct() %>%
      collect(),
    by = c("year", "iso3_code" = "country_iso3")
  ) %>%
  inner_join(
    tbl(con, "usitc_country_codes") %>%
      filter(country_iso3 == country_dynamic_code) %>%
      select(year, country_iso3, country_dynamic_code) %>%
      distinct() %>%
      collect(),
    by = c("year", "country_dynamic_code")
  )

# fao_country_correspondence %>%
#   filter(year == 1998, iso3_code == "COD")

# fao_country_correspondence %>%
#   filter(year == 1989, iso3_code == "MMR")

usitc_changing_names <- tbl(con, "usitc_country_codes") %>%
  filter(country_iso3 != country_dynamic_code) %>%
  select(year, country_iso3, country_dynamic_code, country_name) %>%
  distinct() %>%
  collect() %>%
  arrange(country_name)

fao_country_correspondence_2 <- tbl(con, "fao_country_correspondence") %>%
  select(year, country, country_code, m49_code, iso3_code) %>%
  collect() %>%
  filter(country %in% c(unique(usitc_changing_names$country_name), "Myanmar", "Democratic Republic of the Congo", "Viet Nam")) %>%
  arrange(country) %>%
  left_join(
    usitc_changing_names,
    by = c("year", "country" = "country_name")
  )

fao_country_correspondence_2 <- fao_country_correspondence_2 %>%
  mutate(
    country_iso3 = case_when(
      country == "Myanmar" ~ "MMR",
      country == "Democratic Republic of the Congo" ~ "COD",
      country == "Viet Nam" ~ "VNM",
      TRUE ~ country_iso3
    ),
    country_dynamic_code = case_when(
      country == "Myanmar" & year >= 1989L ~ "MMR",
      country == "Myanmar" & year < 1989L ~ "BUR",
      country == "Democratic Republic of the Congo" & year >= 1998L ~ "COD",
      country == "Democratic Republic of the Congo" & year < 1998L ~ "ZAR",
      country == "Viet Nam" ~ "VNM.X",
      TRUE ~ country_dynamic_code
    )
  )

fao_country_correspondence <- fao_country_correspondence %>%
  bind_rows(fao_country_correspondence_2)

fao_country_correspondence <- fao_country_correspondence %>%
  select(-country) %>%
  distinct()

# fao_country_correspondence_2 %>%
#   group_by(year, country_code, country_iso3) %>%
#   summarise(n = n()) %>%
#   filter(n > 1)

fao_country_correspondence <- fao_country_correspondence %>%
  mutate(
    country_iso3 = case_when(
      iso3_code == "ROU" ~ "ROU",
      iso3_code == "SAU" ~ "SAU",
      iso3_code == "ZAF" ~ "ZAF",
      iso3_code == "YEM" ~ "YEM",
      iso3_code == "SRB" ~ "SRB",
      iso3_code == "PAN" ~ "PAN",
      iso3_code == "KIR" ~ "KIR",
      iso3_code == "SDN" ~ "SDN",
      iso3_code == "PAK" ~ "PAK",
      iso3_code == "MYS" ~ "MYS",
      TRUE ~ country_iso3
    ),
    country_dynamic_code = case_when(
      iso3_code == "ROU" & year < 2002L ~ "ROM",
      iso3_code == "ROU" & year >= 2002L ~ "ROU",
      iso3_code == "SAU" & year < 1993L ~ "SAU",
      iso3_code == "SAU" & year >= 1993L ~ "SAU.X",
      iso3_code == "ZAF" & year < 1990 ~ "ZAF",
      iso3_code == "ZAF" & year >= 1990 ~ "ZAF.X",
      iso3_code == "YEM" & year < 1990L & year >= 1987L ~ "YEM",
      iso3_code == "YEM" & year >= 1990L ~ "YEM.X",
      iso3_code == "SRB" & year < 2008L ~ "SRB",
      iso3_code == "SRB" & year >= 2008L ~ "SRB.X",
      iso3_code == "PAN" & year == 2020L ~ "PAN.X",
      iso3_code == "KIR" & year == 2020L ~ "KIR.X",
      iso3_code == "SDN" & year == 2020L ~ "SDN.X",
      iso3_code == "PAK" & year == 2020L ~ "PAK.X",
      iso3_code == "MYS" & year == 2020L ~ "MYS.Y",
      TRUE ~ country_dynamic_code
    )
  ) %>%
  distinct()

# fao_country_correspondence %>%
#   filter(is.na(country_iso3))

# dbRemoveTable(con, "fao_trade_tidy")

if (!"fao_trade_tidy" %in% dbListTables(con)) {
  map(
    1986:2020,
    function(y, add_comtrade = FALSE) {
      message(y)

      ## Convert long to wide and tidy units ----

      # tbl(con, "fao_trade_unit_code")

      d <- tbl(con, "fao_trade") %>%
        filter(year == y, element_code %in% c(5622L, 5922L)) %>%
        filter(reporter_country_code != partner_country_code) %>%
        mutate(value = value * 1000) %>% # see tbl fao_trade_unit_code
        left_join(
          tbl(con, "fao_trade_element_code") %>%
            mutate(element = tolower(str_replace_all(element, " ", "_")))
        ) %>%
        select(-c(element_code, flag_id)) %>%
        collect() %>%
        pivot_wider(names_from = "element", values_from = "value")

      ## Convert FCL to ITPD-E, filter and aggregate ----

      d <- d %>%
        inner_join(
          tbl(con, "usitc_fao_to_itpde") %>%
            collect(),
          by = "item_code"
        ) %>%
        group_by(year, reporter_country_code, partner_country_code, industry_id) %>%
        summarise(
          import_value_usd = sum(import_value, na.rm = T),
          export_value_usd = sum(export_value, na.rm = T)
        ) %>%
        ungroup()

      # unique(d$reporter_country_code)
      # unique(d$partner_country_code)
      # unique(d$industry_id)

      ## Convert country codes ----

      d_country <- fao_country_correspondence %>%
        filter(year == y) %>%
        select(
          year,
          reporter_country_code = country_code,
          reporter_iso3 = country_iso3,
          reporter_dynamic_code = country_dynamic_code
        )

      d <- d %>%
        inner_join(d_country) %>%
        inner_join(
          d_country %>%
            rename(
              partner_country_code = reporter_country_code,
              partner_iso3 = reporter_iso3,
              partner_dynamic_code = reporter_dynamic_code
            )
        ) %>%
        select(-c(reporter_country_code, partner_country_code))

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
        collect() %>%
        inner_join(
          d_country %>%
            select(reporter_iso3, reporter_dynamic_code)
        ) %>%
        inner_join(
          d_country %>%
            select(partner_iso3 = reporter_iso3, partner_dynamic_code = reporter_dynamic_code)
        )

      # d_aux %>%
      #   group_by(reporter_iso3, partner_iso3) %>%
      #   summarise(n = n()) %>%
      #   filter(n > 1)

      d <- d %>%
        bind_rows(d_aux) %>%
        group_by(year, reporter_iso3, partner_iso3, reporter_dynamic_code, partner_dynamic_code, industry_id) %>%
        summarise_if(is.numeric, sum, na.rm = T)

      rm(d_aux)

      ## Add mirrored flows and flags ----

      d2 <- d %>%
        ungroup() %>%
        select(-export_value_usd) %>%
        drop_na(import_value_usd)

      d <- d %>%
        ungroup() %>%
        select(-import_value_usd) %>%
        drop_na(export_value_usd)

      d <- d %>%
        full_join(d2, by = c(
          "year",
          "industry_id",
          "reporter_iso3" = "partner_iso3",
          "partner_iso3" = "reporter_iso3",
          "reporter_dynamic_code" = "partner_dynamic_code",
          "partner_dynamic_code" = "reporter_dynamic_code"
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
          importer_iso3 = partner_iso3,
          exporter_dyanmic_code = reporter_dynamic_code,
          importer_dynamic_code = partner_dynamic_code
        )

      # d %>%
      #   filter(exporter_iso3 == "CHN", industry_id == 7L) %>%
      #   summarise(
      #     trade = sum(trade, na.rm = T)
      #   )

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
