library(tabulizer)
library(dplyr)
library(janitor)
library(stringr)
library(forcats)
library(tidyr)
library(readr)

tables <- extract_tables(
  "inp/pdf/Borchert et al. - 2021 - The International Trade and Production Database fo.pdf",
  output = "data.frame")

tidy_table <- function(i) {
  tables[[i]] %>%
    as_tibble() %>%
    clean_names() %>%
    mutate_if(is.numeric, as.character) %>%
    mutate_if(is.character, str_trim)
}

# table 1 ----

table1 <- tidy_table(1) %>%
  bind_rows(tidy_table(2)) %>%
  bind_rows(tidy_table(3)) %>%
  rename(no_observations = x_observations, no_zeroes = x_zeroes) %>%
  mutate(
    id = as.integer(id),
    mean_exports = as.numeric(mean_exports),
    max_exports = as.integer(gsub(",", "", max_exports)),
    no_observations = as.integer(gsub(",", "", no_observations)),
    no_zeroes = as.integer(gsub(",", "", no_zeroes))
  ) %>%
  distinct(id, .keep_all = T)

# table 2 ----

table2 <- tidy_table(4) %>%
  bind_rows(tidy_table(5)) %>%
  bind_rows(tidy_table(6)) %>%
  bind_rows(tidy_table(7)) %>%
  rename(no_observations = x_observations, no_zeroes = x_zeroes) %>%
  mutate(
    country_name = str_trim(country_name),
    mean_exports = as.numeric(mean_exports),
    max_exports = as.integer(gsub(",", "", max_exports)),
    no_observations = as.integer(gsub(",", "", no_observations)),
    no_zeroes = as.integer(gsub(",", "", no_zeroes))
  ) %>%
  distinct(iso3, .keep_all = T)

# table 4 ----

table4 <- tidy_table(8) %>%
  bind_rows(tidy_table(9)) %>%
  bind_rows(tidy_table(10)) %>%
  bind_rows(tidy_table(11)) %>%
  mutate(
    itpd_e_code = as.integer(itpd_e_code),
    fcl_item_code = as.integer(fcl_item_code)
  ) %>%
  distinct(fcl_item_code, .keep_all = T)

# table 5 ----

table5 <- tidy_table(12) %>%
  mutate(
    itpd_e_code = as.integer(itpd_e_code),
    isic3 = as.integer(isic3),
    isic4 = as.integer(isic4)
  )

# table 6 ----

table6 <- tidy_table(13) %>%
  bind_rows(tidy_table(14)) %>%
  mutate(
    itpd_e_code = as.integer(itpd_e_code)
  ) %>%
  separate(
    isic3, c("isic31", "isic32"), sep = "\\+"
  ) %>%
  pivot_longer(c("isic31","isic32"), values_to = "isic3") %>%
  drop_na(isic3) %>%
  select(-name) %>%
  separate(
    isic4, c("isic41", "isic42"), sep = "\\+"
  ) %>%
  pivot_longer(c("isic41","isic42"), values_to = "isic4") %>%
  drop_na(isic4) %>%
  select(-name) %>%
  mutate(
    isic3 = as.integer(str_trim(isic3)),
    isic4 = as.integer(str_trim(isic4))
  )

# table 7 ----

table7 <- tidy_table(15) %>%
  mutate(
    itpd_e_code = as.integer(itpd_e_code)
  ) %>%
  mutate_if(is.character, function(x) gsub("–", NA, x)) %>%
  separate(ebops_2002, c("ebops_20021", "ebops_20022", "ebops_20023"), sep = ",") %>%
  pivot_longer(c("ebops_20021", "ebops_20022", "ebops_20023"), values_to = "ebops_2002") %>%
  select(-name) %>%
  separate(ebops_2010, c("ebops_20101", "ebops_20102"), sep = "\\+") %>%
  pivot_longer(c("ebops_20101", "ebops_20102"), values_to = "ebops_2010") %>%
  select(-name) %>%
  separate(isic_rev_4, c("isic_rev_41", "isic_rev_42"), sep = "\\+") %>%
  pivot_longer(c("isic_rev_41", "isic_rev_42"), values_to = "isic_rev_4") %>%
  select(-name) %>%
  mutate_if(is.character, str_trim) %>%
  filter(!(is.na(ebops_2002) & is.na(ebops_2010) & is.na(isic_rev_4))) %>%
  distinct() %>%
  group_by(itpd_e_description) %>%
  fill(ebops_2002) %>%
  fill(ebops_2010) %>%
  fill(isic_rev_4) %>%
  distinct()

# table 8 ----

table8 <- tidy_table(16) %>%
  rename(
    un_tsd_only = un_tsd,
    un_tsd_updated = un_tsd_1
  ) %>%
  mutate(year = as.integer(year)) %>%
  filter(!is.na(year)) %>%
  mutate_if(is.character, function(x) as.integer(gsub(",", "", x)))

# save ----

try(dir.create("out/csv/article", recursive = T))

write_csv(table1, "out/csv/article/table1.csv")
write_csv(table2, "out/csv/article/table2.csv")
write_csv(table4, "out/csv/article/table4.csv")
write_csv(table5, "out/csv/article/table5.csv")
write_csv(table6, "out/csv/article/table6.csv")
write_csv(table7, "out/csv/article/table7.csv")
write_csv(table8, "out/csv/article/table8.csv")
