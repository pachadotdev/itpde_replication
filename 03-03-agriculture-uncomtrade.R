## UN COMTRADE data ----

if (!all(c("uncomtrade_imports", "uncomtrade_exports") %in% dbListTables(con))) {
  con2 <- dbConnect(
    Postgres(),
    host = "localhost",
    dbname = "uncomtrade",
    user = Sys.getenv("LOCAL_SQL_USR"),
    password = Sys.getenv("LOCAL_SQL_PWD")
  )

  ### UNCOMTRADE imports ----

  dimp <- map_df(
    1986:2020,
    function(y) {
      message(y)

      if (y < 1988) {
        d <- tbl(con2, "sitc_rev2_tf_import_al_5") %>%
          filter(year == y) %>%
          filter(!(partner_iso %in% c("all", "wld"))) %>%
          filter(commodity_code == "04211") %>%
          collect()
      } else {
        d <- tbl(con2, "hs_rev1992_tf_import_al_6") %>%
          filter(year == y) %>%
          filter(!(partner_iso %in% c("all", "wld"))) %>%
          filter(commodity_code == "100610") %>%
          collect()
      }

      d
    }
  )

  saveRDS(dimp, "out/uncomtrade_imports.rds")
  dbWriteTable(con, "uncomtrade_imports", dimp, overwrite = T)

  ### UNCOMTRADE exports ----

  dexp <- map_df(
    1986:2020,
    function(y) {
      message(y)

      if (y < 1988) {
        d <- tbl(con2, "sitc_rev2_tf_export_al_5") %>%
          filter(year == y) %>%
          filter(!(partner_iso %in% c("all", "wld"))) %>%
          filter(commodity_code == "04211") %>%
          collect()
      } else {
        d <- tbl(con2, "hs_rev1992_tf_export_al_6") %>%
          filter(year == y) %>%
          filter(!(partner_iso %in% c("all", "wld"))) %>%
          filter(commodity_code == "100610") %>%
          collect()
      }

      d
    }
  )

  saveRDS(dexp, "out/uncomtrade_exports.rds")
  dbWriteTable(con, "uncomtrade_exports", dexp, overwrite = T)

  dbDisconnect(con2)
  rm(con2)
}
