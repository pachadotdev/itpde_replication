# Verification ----

tbl(con, "fao_trade_domestic_tidy") %>%
  filter(year == 2000L) %>%
  select(year, exporter_iso3, importer_iso3, industry_id, trade = trade) %>%
  filter(industry_id == 13L, exporter_iso3 == "CHN") %>%
  summarise(trade = sum(trade, na.rm = T) / 1000000)

tbl(con, "fao_trade_tidy") %>%
  filter(year == 2000L) %>%
  select(year, exporter_iso3, importer_iso3, industry_id, trade) %>%
  filter(industry_id == 13L, exporter_iso3 == "CHN") %>%
  summarise(trade = sum(trade, na.rm = T) / 1000000)

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
  rename(trade.original = trade.x, trade.pacha = trade.y) %>%
  filter(trade.original != trade.pacha) %>%
  arrange(desc(abs(trade.original - trade.pacha))) %>%
  mutate(pct_diff = 100 * (trade.original - trade.pacha) / trade.original) %>%
  select(-year)

dbDisconnect(con)
