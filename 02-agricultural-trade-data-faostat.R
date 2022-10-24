library(archive)
library(readr)
library(janitor)
library(dplyr)
library(tidyr)
library(purrr)
library(arrow)

# 1: Download trade data ----

url_trade_data <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Trade_DetailedTradeMatrix_E_All_Data.zip"
zip_trade_data <- "inp/zip/faostat_trade_matrix.zip"

try(dir.create("inp/zip", recursive = T))

if (!file.exists(zip_trade_data)) {
  download.file(url_trade_data, zip_trade_data)
}

if (length(list.files("inp/csv/faostat_trade_matrix")) == 0) {
  archive_extract(zip_trade_data, dir = "inp/csv/faostat_trade_matrix")
}

# 2: Download FCL (FAOSTAT) to ITPD-E codes ----

url_fcl_id <- "https://www.usitc.gov/data/gravity/itpde_concordances/itpd_e_r02_ag_fcl.csv"
csv_fcl_id <- "inp/csv/fcl_to_itpde.csv"

if (!file.exists(csv_fcl_id)) {
  download.file(url_fcl_id, csv_fcl_id)
}

# 2: Download correspondence between FAO country codes and ISO-3 country codes ----

# the file was downloaded from the browser and points to
# blob:https://www.fao.org/57203009-07f0-4867-9eaf-81a56b990566
# see image for https://www.fao.org/faostat/en/#data/QV
# (inp/img/faostats_country_correspondence.png)
# the file is inp/csv/faostat_country_correspondence.csv

# 3: Download HS 92 to ISIC 3 correspondence table ----

url_fcl_hs <- "http://wits.worldbank.org/data/public/concordance/Concordance_H3_to_I3.zip"

zip_fcl_hs <- "inp/zip/hs07_to_isic3.zip"

if (!file.exists(zip_fcl_hs)) {
  download.file(url_fcl_hs, zip_fcl_hs)
}

if (length(list.files("inp/csv/hs07_to_isic3")) == 0) {
  archive_extract(zip_fcl_hs, dir = "inp/csv/hs07_to_isic3")
}

# 4: Tidy data ----

## 4.1 tidy codes ----

fao_data <- read_csv("inp/csv/faostat_trade_matrix/Trade_DetailedTradeMatrix_E_All_Data_NOFLAG.csv") %>%
  clean_names()

fao_element_code <- fao_data %>%
  select(element_code, element) %>%
  distinct()

fao_data <- fao_data %>%
  select(-c(element, element_code_fao))

fao_item_code <- fao_data %>%
  select(item_code, item_code_cpc, item) %>%
  mutate(item_code_cpc = gsub("^\'", "", item_code_cpc)) %>%
  distinct()

fao_data <- fao_data %>%
  select(-c(item, item_code_cpc))

fao_data <- fao_data %>%
  select(-c(reporter_country_code_m49, reporter_countries,
            partner_country_code_m49, partner_countries)) %>%
  mutate(
    reporter_country_code = as.integer(reporter_country_code),
    partner_country_code = as.integer(partner_country_code)
  )

fao_unit_code <- fao_data %>%
  select(unit) %>%
  distinct() %>%
  mutate(unit_code = row_number())

fao_data <- fao_data %>%
  left_join(fao_unit_code) %>%
  select(reporter_country_code:element_code, unit_code, y1986:y2020)

## 4.2 convert wide to long ----

fao_data <- fao_data %>%
  group_by(reporter_country_code) %>%
  nest()

fao_data <- map_df(
  fao_data %>% pull(reporter_country_code),
  function(c) {
    message(c)
    fao_data %>%
      filter(reporter_country_code == c) %>%
      unnest(data) %>%
      pivot_longer(y1986:y2020, names_to = "year", values_to = "value") %>%
      mutate(year = as.integer(gsub("y", "", year))) %>%
      drop_na(value) %>%

      mutate(
        value = case_when(
          unit_code == 1L ~ value,
          unit_code == 2L ~ value * 1000,
          unit_code == 3L ~ value,
          unit_code == 4L ~ value * 1000,
          unit_code == 5L ~ value
        )
      ) %>%

      left_join(fao_element_code) %>%
      select(-element_code) %>%
      left_join(fao_unit_code) %>%
      mutate(element = paste(element, unit)) %>%
      select(-unit, -unit_code) %>%
      pivot_wider(names_from = "element", values_from = "value") %>%
      clean_names() %>%
      rename(
        import_value_usd = import_value_1000_us,
        export_value_usd = export_value_1000_us
      )
  }
)

fao_data <- fao_data %>%
  mutate(
    import_quantity_head = case_when(
      is.na(import_quantity_head) ~ import_quantity_1000_head,
      !is.na(import_quantity_head) ~ import_quantity_head
    ),
    export_quantity_head = case_when(
      is.na(export_quantity_head) ~ export_quantity_1000_head,
      !is.na(export_quantity_head) ~ export_quantity_head
    )
  ) %>%
  select(-export_quantity_1000_head, -import_quantity_1000_head) %>%
  select(reporter_country_code, partner_country_code, item_code, year,
         starts_with("export"), starts_with("import")) %>%
  rename(
    export_quantity_no_unit = export_quantity_no,
    import_quantity_no_unit = import_quantity_no
  )

## 4.3 convert country codes ----

fao_country_correspondence <- read_csv("inp/csv/faostat_country_correspondence.csv") %>%
  clean_names() %>%
  select(country_code, iso3_code)

fao_data <- fao_data %>%
  left_join(
    fao_country_correspondence %>% rename(reporter_iso3 = iso3_code),
    by = c("reporter_country_code" = "country_code")
  ) %>%
  left_join(
    fao_country_correspondence %>% rename(partner_iso3 = iso3_code),
    by = c("partner_country_code" = "country_code")
  ) %>%
  select(reporter_iso3, partner_iso3, everything()) %>%
  ungroup() %>%
  select(-c(reporter_country_code, partner_country_code))

# 5: Identify manufacturing data in FAOSTAT ----

# Speciﬁcally, we classify all industries between 1500 and 1601 of ISIC rev. 3 as manufacturing indus-
# tries. Using the FCL to HS and HS to ISIC rev. 3 correspondence tables, we identify the FCL items that are part of the manufacturing
# data. 12 Note that these FCL items typically do not have matching production data in the FAO’s database. Some FCL items could not
# be uniquely matched to broad sectors. In these cases, we allocated FCL items according the number of constituent HS lines. 13 We
# also dropped industries we could not match to any ISIC or HS code 14 and industries with FCL item codes above 1296, which are
# aggregates and industries such as fertilizers, pesticides, and machinery, belonging to one of the other broad sectors. Table 4 includes
# the correspondence between ITPD-E agricultural industries and FCL items.

# here the 50 is totally arbitrary, it's used to create NAs and then filter
# which is better than ommiting HS codes when moving to long format

faostat_product_correspondence <- read_csv("inp/csv/faostat_product_correspondence.csv") %>%
  clean_names() %>%
  select(item_code, hs07 = hs07_code) %>%
  separate(hs07, paste0("hs07_", 1:50)) %>%
  pivot_longer(hs07_1:hs07_50) %>%
  drop_na() %>%
  rename(hs07 = value) %>%
  select(-name)

hs07_to_isic <- read_csv("inp/csv/hs07_to_isic3/JobID-48_Concordance_H3_to_I3.CSV") %>%
  clean_names() %>%
  select(hs07 = hs_2007_product_code, isic3 = isic_revision_3_product_code,
         isic_revision_3_product_description)

faostat_product_correspondence <- faostat_product_correspondence %>%
  left_join(hs07_to_isic)

# footnote 12 in the article

fcl_manufacturing <- tibble(
  item_code = c(16, 18, 19, 20, 21, 22, 23, 24, 26, 28, 29, 31, 32, 34, 36, 37, 38, 39, 41, 45, 46, 48, 49, 50, 51, 57, 58, 60, 61, 64, 66, 72, 76, 80, 82, 84,
                86, 90, 95, 98, 104, 109, 110, 111, 113, 114, 115, 117, 118, 119, 121, 126, 127, 129, 150, 154, 155, 158, 159, 160, 162, 163, 164, 165, 166, 167, 168, 172, 173,
                175, 212, 235, 237, 238, 239, 240, 241, 244, 245, 246, 247, 252, 253, 257, 258, 259, 261, 262, 264, 266, 268, 269, 271, 272, 273, 274, 276, 278, 281, 282, 290,
                291, 293, 294, 295, 297, 298, 306, 307, 313, 314, 331, 332, 334, 335, 337, 338, 340, 341, 343, 390, 391, 392, 447, 448, 450, 451, 466, 469, 471, 472, 473, 474,
                475, 476, 491, 492, 496, 498, 499, 509, 510, 513, 514, 517, 518, 519, 538, 539, 562, 563, 564, 565, 575, 576, 580, 583, 584, 622, 623, 624, 625, 626, 631, 632,
                633, 634, 657, 658, 659, 660, 662, 664, 665, 666, 672, 737, 753, 768, 770, 773, 774, 828, 829, 831, 840, 841, 842, 843, 845, 849, 850, 851, 852, 853, 854, 855,
                867, 869, 870, 871, 872, 873, 874, 875, 877, 878, 882, 883, 885, 886, 887, 888, 889, 890, 891, 892, 893, 894, 895, 896, 897, 898, 899, 900, 901, 903, 904, 905,
                907, 908, 909, 910, 916, 917, 919, 920, 921, 922, 927, 928, 929, 930, 947, 949, 951, 952, 953, 954, 955, 957, 958, 959, 977, 979, 982, 983, 984, 985, 988, 994,
                995, 996, 997, 998, 999, 1008, 1010, 1017, 1019, 1020, 1021, 1022, 1023, 1035, 1037, 1038, 1039, 1040, 1041, 1042, 1043, 1058, 1059, 1060, 1061, 1063, 1064,
                1065, 1066, 1069, 1073, 1074, 1075, 1080, 1081, 1089, 1097, 1098, 1102, 1103, 1104, 1105, 1108, 1109, 1111, 1112, 1127, 1128, 1129, 1130, 1141, 1151, 1158,
                1160, 1163, 1164, 1166, 1167, 1168, 1172, 1173, 1174, 1175, 1186, 1187, 1221, 1222, 1223, 1225, 1241, 1242, 1243, 1273, 1274, 1275, 1276, 1277, 1296)
)

# 6: Convert FCL to ITPD-E, filter and aggregate ----

fcl_to_itpde <- read_csv("inp/csv/fcl_to_itpde.csv") %>%
  clean_names() %>%
  select(industry_id = itpd_id, item_code = fcl_item_code) %>%
  mutate_if(is.double, as.integer)

fao_data <- fao_data %>%
  inner_join(fcl_to_itpde, by = "item_code") %>%
  group_by(year, reporter_iso3, partner_iso3, industry_id) %>%
  summarise(
    import_value_usd = sum(import_value_usd, na.rm = T),
    export_value_usd = sum(export_value_usd, na.rm = T)
  )

fao_data_2 <- fao_data %>%
  ungroup() %>%
  select(-export_value_usd)

fao_data <- fao_data %>%
  ungroup() %>%
  select(-import_value_usd)

fao_data <- fao_data %>%
  full_join(fao_data_2, by = c("year", "industry_id",
                               "reporter_iso3" = "partner_iso3",
                               "partner_iso3" = "reporter_iso3")) %>%
  mutate_if(is.double, function(x) ifelse(is.na(x), 0, x)) %>%
  mutate(
    trade = case_when(
      import_value_usd == 0 ~ export_value_usd,
      TRUE ~ import_value_usd
    ),
    flag_mirror = case_when(
      trade == export_value_usd ~ 0L,
      TRUE ~ 1L
    ),
    flag_zero = case_when(
      trade > 0 ~ "p",
      trade == 0 ~ "r" # I still need to add "flag = u" later
    )
  ) %>%
  rename(
    exporter_iso3 = reporter_iso3,
    importer_iso3 = partner_iso3
  )

# 7: Add zeroes ----

# here I add missing industries for year-exporter-importer combinations with
# total trade > 0

# TODO: the original data has less added zeroes (i.e. industry_id 9 for BRA-CHL 2010 is not in the
# original data)

# original: 5,988,747 rows with added zeroes
# here: 10,845,545

fao_data_0 <- fao_data %>%
  group_by(year, exporter_iso3, importer_iso3) %>%
  summarise(trade = sum(trade, na.rm = T)) %>%
  filter(trade > 0) %>%
  ungroup() %>%
  select(-trade)

fao_data_0 <- expand_grid(
  fao_data_0,
  industry_id = unique(fcl_to_itpde$industry_id)
) %>%
  anti_join(fao_data) %>%
  mutate(
    trade = 0,
    flag_mirror = 0L,
    flag_zero = "u"
  )

fao_data <- fao_data %>%
  bind_rows(fao_data_0) %>%
  select(-export_value_usd, -import_value_usd)

fao_data <- fao_data %>%
  arrange(year, exporter_iso3, importer_iso3, industry_id)

# TODO: this comparison should match

# fao_data %>%
#   filter(exporter_iso3 == "BRA", importer_iso3 == "CHL", year == 2010L,
#          industry_id %in% unique(fcl_to_itpde$industry_id)) %>%
#   mutate(trade = trade / 1000000) %>%
#   arrange(industry_id) %>%
#   print(n = 20) %>%
#   knitr::kable()
#
# library(usitcgravity)
#
# con <- usitcgravity_connect()
#
# industries <- unique(fcl_to_itpde$industry_id)
#
# tbl(con, "trade") %>%
#   filter(importer_iso3 == "CHL", exporter_iso3 == "BRA", year == 2010L,
#          industry_id %in% industries) %>%
#   arrange(industry_id) %>%
#   select(exporter_iso3, importer_iso3, industry_id, trade, flag_mirror, flag_zero) %>%
#   collect() %>%
#   print(n = 20)

# 8: Save ----

try(dir.create("out/parquet/", recursive = T))

write_parquet(fao_data, "out/parquet/fao_data.parquet")
write_parquet(fao_item_code, "out/parquet/fao_item_code.parquet")
write_parquet(fcl_manufacturing, "out/parquet/fcl_manufacturing.parquet")
