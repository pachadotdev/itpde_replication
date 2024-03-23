# Import FAO production data ----

if (!"fao_production" %in% dbListTables(con)) {
  ## read ----

  out <- "inp/faostat_production_matrix_normalized/Value_of_Production_E_All_Data_(Normalized).rds"

  if (!file.exists(out)) {
    fao_production <- read_csv("inp/faostat_production_matrix_normalized/Value_of_Production_E_All_Data_(Normalized).csv") %>%
      clean_names()

    fao_production <- fao_production %>%
      mutate_if(is.character, as.factor)

    saveRDS(fao_production, out)
  } else {
    fao_production <- readRDS(out)
  }

  fao_production <- fao_production %>%
    mutate(year = as.integer(year)) %>%
    select(-year_code) %>%
    select(year, everything())

  ## tidy units ----

  fao_production_unit_code <- fao_production %>%
    select(unit) %>%
    distinct() %>%
    mutate(unit_code = row_number())

  saveRDS(fao_production_unit_code, "out/fao_production_unit_code.rds")

  fao_production <- fao_production %>%
    left_join(fao_production_unit_code) %>%
    select(-unit)

  ## tidy areas ----

  fao_production_area_code <- fao_production %>%
    select(area_code, area) %>%
    distinct() %>%
    mutate(
      area_code = as.integer(area_code),
      area = iconv(as.character(area), to = "UTF-8", sub = "")
    )

  # fao_production_area_code %>%
  #   filter(area_code == 107)

  saveRDS(fao_production_area_code, "out/fao_production_area_code.rds")

  fao_production_area_code_m49 <- fao_production %>%
    select(area_code_m49, area) %>%
    distinct() %>%
    mutate(
      area_code_m49 = gsub("\'", "", as.character(area_code_m49)),
      area = iconv(as.character(area), to = "UTF-8", sub = "")
    )

  saveRDS(fao_production_area_code_m49, "out/fao_production_area_code_m49.rds")

  fao_production <- fao_production %>%
    select(-c(area_code_m49, area)) %>%
    mutate(area_code = as.integer(area_code))

  ## tidy items ----

  fao_production_item_code <- fao_production %>%
    select(item_code, item) %>%
    distinct() %>%
    mutate(
      item_code = as.integer(item_code),
      item = iconv(item, to = "UTF-8", sub = "")
    )

  saveRDS(fao_production_item_code, "out/fao_production_item_code.rds")

  fao_production_item_code_cpc <- fao_production %>%
    select(item_code_cpc, item) %>%
    distinct() %>%
    mutate(
      item_code_cpc = as.character(gsub("\'", "", item_code_cpc)),
      item = iconv(item, to = "UTF-8", sub = "")
    )

  saveRDS(fao_production_item_code_cpc, "out/fao_production_item_code_cpc.rds")

  fao_production <- fao_production %>%
    select(-c(item_code_cpc, item)) %>%
    mutate(item_code = as.integer(item_code))

  ## tidy elements ----

  fao_production_element_code <- fao_production %>%
    select(element_code, element) %>%
    distinct() %>%
    mutate(
      element_code = as.integer(element_code),
      element = iconv(element, to = "UTF-8", sub = "")
    )

  saveRDS(fao_production_element_code, "out/fao_production_element_code.rds")

  fao_production <- fao_production %>%
    select(-element) %>%
    mutate(element_code = as.integer(element_code))

  saveRDS(fao_production, "out/fao_production.rds")

  ## write ----

  dbWriteTable(con, "fao_production", fao_production, overwrite = T)
  dbWriteTable(con, "fao_production_unit_code", fao_production_unit_code, overwrite = T)
  dbWriteTable(con, "fao_production_area_code", fao_production_area_code, overwrite = T)
  dbWriteTable(con, "fao_production_item_code", fao_production_item_code, overwrite = T)
  dbWriteTable(con, "fao_production_element_code", fao_production_element_code, overwrite = T)

  rm(fao_production, fao_production_unit_code, fao_production_area_code, fao_production_area_code_m49, fao_production_item_code, fao_production_item_code_cpc, fao_production_element_code)
  gc()
}
