# Packages ----

library(archive)
library(readr)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)
library(stringr)
library(RPostgres)

# SQL connection ----

con <- dbConnect(
  Postgres(),
  host = "localhost",
  dbname = "itpde_replication",
  user = Sys.getenv("LOCAL_SQL_USR"),
  password = Sys.getenv("LOCAL_SQL_PWD")
)

# Tasks ----

source("03-01-agriculture-download.R")
source("03-02-agriculture-fao-trade.R")
source("03-03-agriculture-uncomtrade.R")
source("03-04-agriculture-fao-production.R")
source("03-05-agriculture-attributes.R")
source("03-06-agriculture-trade.R")
source("03-07-agriculture-production.R")
