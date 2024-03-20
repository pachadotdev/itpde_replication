# Download trade data ----

url_trade_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data_(Normalized).zip"
zip_trade_data <- "inp/faostat_trade_matrix_normalized.zip"

try(dir.create("inp/", recursive = T))

if (!file.exists(zip_trade_data)) {
  download.file(url_trade_data, zip_trade_data)
}

if (length(list.files("inp/faostat_trade_matrix_normalized")) == 0) {
  archive_extract(zip_trade_data, dir = "inp/faostat_trade_matrix_normalized")
}

# Download production data ----

url_production_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Value_of_Production_E_All_Data_(Normalized).zip"
zip_production_data <- "inp/faostat_production_matrix_normalized.zip"

try(dir.create("inp/", recursive = T))

if (!file.exists(zip_production_data)) {
  download.file(url_production_data, zip_production_data)
}

if (length(list.files("inp/faostat_production_matrix_normalized")) == 0) {
  archive_extract(zip_production_data, dir = "inp/faostat_production_matrix_normalized")
}

# Download FCL (FAOSTAT) to ITPD-E codes ----

url_fcl_id <- "https://www.usitc.gov/data/gravity/itpde_concordances/itpd_e_r02_ag_fcl.csv"
csv_fcl_id <- "inp/fcl_to_itpde.csv"

if (!file.exists(csv_fcl_id)) {
  download.file(url_fcl_id, csv_fcl_id)
}

# Download correspondence between FAO country codes and ISO-3 country codes ----

# the file was downloaded from the browser and points to
# blob:https://www.fao.org/57203009-07f0-4867-9eaf-81a56b990566

# see image for https://www.fao.org/faostat/en/#data/QV
# (inp/faostats_country_correspondence.png)

# the file is inp/faostat_country_correspondence.csv
