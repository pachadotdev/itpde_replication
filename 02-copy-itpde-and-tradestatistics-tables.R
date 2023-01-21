library(dplyr)
library(purrr)
library(usitcgravity)
library(duckdb)
library(RPostgres)

con <- usitcgravity_connect()

con2 <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "itpde_replication",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD")
)

# this table is to avoid writing special country codes that we won't need
country_names <- tbl(con, "country_names") %>%
  select(country_iso3) %>%
  collect()

if (!"usitc_country_codes" %in% dbListTables(con2)) {
  dbWriteTable(con2, "usitc_country_codes", country_names, overwrite = T)
}

# this table is to compare replication values later
if (!"usitc_trade" %in% dbListTables(con2)) {
  map(
    1986:2020,
    function(y) {
      message(y)

      trade <- tbl(con, "trade") %>%
        filter(year == y) %>%
        collect()

      dbWriteTable(con2, "usitc_trade", trade, overwrite = F, append = T)
    }
  )
}

dbDisconnect(con, shutdown = T)
dbDisconnect(con2)
