# Import FAO production data ----

if (!"fao_production" %in% dbListTables(con)) {
  ## read ----

  fao_production <- read_csv("inp/faostat_production_matrix_normalized/Value_of_Production_E_All_Data_(Normalized).csv") %>%
    clean_names()

  fao_production <- fao_production %>%
    mutate(year = as.integer(year)) %>%
    select(-year_code) %>%
    select(year, everything())

  ## tidy units ----

  fao_unit_code <- fao_production %>%
    select(unit) %>%
    distinct() %>%
    mutate(unit_code = row_number())

  fao_production <- fao_production %>%
    left_join(fao_unit_code) %>%
    select(-unit)

  ## tidy areas ----

  fao_area_code <- fao_production %>%
    select(area_code, area) %>%
    distinct() %>%
    mutate(
      area_code = as.integer(area_code),
      area = iconv(area, to = "ASCII//TRANSLIT")
    )

  fao_area_code_m49 <- fao_production %>%
    select(area_code_m49, area) %>%
    distinct() %>%
    mutate(area_code_m49 = as.character(gsub("\'", "", area_code_m49)))

  fao_production <- fao_production %>%
    select(-c(area_code_m49, area)) %>%
    mutate(area_code = as.integer(area_code))

  ## tidy items ----

  fao_item_code <- fao_production %>%
    select(item_code, item) %>%
    distinct() %>%
    mutate(
      item_code = as.integer(item_code),
      item = iconv(item, to = "ASCII//TRANSLIT")
    )

  fao_item_code_cpc <- fao_production %>%
    select(item_code_cpc, item) %>%
    distinct() %>%
    mutate(
      item_code_cpc = as.character(gsub("\'", "", item_code_cpc)),
      item = iconv(item, to = "ASCII//TRANSLIT")
    )

  fao_production <- fao_production %>%
    select(-c(item_code_cpc, item)) %>%
    mutate(item_code = as.integer(item_code))

  ## tidy elements ----

  fao_element_code <- fao_production %>%
    select(element_code, element) %>%
    distinct() %>%
    mutate(element_code = as.integer(element_code))

  fao_production <- fao_production %>%
    select(-element) %>%
    mutate(element_code = as.integer(element_code))

  ## write ----

  dbWriteTable(con, "fao_production", fao_production, overwrite = T, append = F)
  dbWriteTable(con, "fao_production_unit_code", fao_unit_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_production_area_code", fao_area_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_production_item_code", fao_item_code, overwrite = T, append = F)
  dbWriteTable(con, "fao_production_element_code", fao_element_code, overwrite = T, append = F)

  rm(fao_production, fao_unit_code, fao_area_code, fao_area_code_m49, fao_item_code, fao_item_code_cpc, fao_element_code)
  gc()
}
