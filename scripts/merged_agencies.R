#! /usr/bin/Rscript --vanilla --default-packages=utils

args <- commandArgs(trailingOnly = TRUE)

hild_dir <- args[1]
hmis_dir <- args[2]

if(!require(colorout)){
  !requireNamespace("devtools", quietly = TRUE)
  install.packages("devtools")
  devtools::install_github("jalvesaq/colorout")
  require(colorout)
}

if(!require(igraph)){
  install.packages("igraph")
  require(igraph)
}

if(!require(magrittr)){
  install.packages("magrittr")
  require(magrittr)
}

if(!require(IRanges)){
  !requireNamespace("BiocManager", quietly = TRUE)
  install.packages("BiocManager")
  BiocManager::install("IRanges")
  require(IRanges)
}

if(!require(lubridate)){
  !requireNamespace("devtools", quietly = TRUE)
  install.packages("devtools")
  devtools::install_github("tidyverse/lubridate")
  require(lubridate)
}

if(!require(data.table)){
  install.packages("data.table")
  require(data.table)
}

if(!require(tidyverse)){
  !requireNamespace("devtools", quietly = TRUE)
  install.packages("devtools")
  devtools::install_github("tidyverse/tidyverse")
  require(tidyverse)
}

links <- fread(paste0(hild_dir, "PHA_HMIS_linked.csv"))

hmis <-
  fread(paste0(hmis_dir, "puget_preprocessed.csv")) %>%
  mutate(pid0 = paste("HMIS0_",PersonalID,sep=""),
         pid1 = paste0("HMIS1_",
                       stringr::str_pad(seq(1,nrow(.)),6,pad='0')))

pha <-
  fread(paste0(hild_dir, "pha_longitudinal.csv")) %>%
  # pid comes from PHA, then pid0 is an id for our purposes
  mutate(pid0 = paste("PHA0_", pid, sep = ""),
         pid1 = paste0("PHA1_",
                       stringr::str_pad(seq(1,nrow(.)),6,pad='0')))

# ==========================================================================
# Join PHA and HMIS Data
# ==========================================================================

#
# HMIS
# --------------------------------------------------------------------------
hmis_c <-
  hmis %>%
  mutate(agency = "HMIS",
         EntryDate = lubridate::ymd(EntryDate),
         ExitDate = lubridate::ymd(ExitDate),
         DOB = lubridate::ymd(DOB),
         RelationshipToHoH = factor(RelationshipToHoH),
         hh_id = paste("HMIS_",HouseholdID, sep = ""),
         gender = if_else(Gender == 0, "Fem",
                  if_else(Gender == 1, "Mal",
				  if_else(Gender == 2, "MTF",
				  if_else(Gender == 3, "FTM",
				  if_else(Gender == 4, "GNC",
					NA_character_)))))) %>%
  select(pid0,
         hh_id,
         relcode = RelationshipToHoH,
         lname = LastName,
         fname = FirstName,
         mname = MiddleName,
         gender,
         dob = DOB,
         r_aian = AmIndAKNative,
         r_asian = Asian,
         r_black = BlackAfAmerican,
         r_nhpi = NativeHIOtherPacific,
         r_white = White,
         r_ethnicity = Ethnicity,
         entry = EntryDate,
         exit = ExitDate,
         agency,
         hmis_proj_type = ProjectType,
         hmis_proj_name = ProjectName)

#
# PHA
# --------------------------------------------------------------------------
pha_c <- pha %>%
  mutate(startdate = lubridate::ymd(startdate),
         enddate = lubridate::ymd(enddate),
         dob = lubridate::ymd(dob),
         relcode = factor(relcode),
         r_aian = if_else(r_aian_new == 1 |
                            race_new == "AIAN only", 1, 0),
         r_asian = if_else(r_asian_new == 1 |
                             race_new == "Asian only", 1, 0),
         r_black = if_else(r_black_new == 1 |
                             race_new == "Black only", 1, 0),
         r_nhpi = if_else(r_nhpi_new == 1 |
                            race_new == "NHPI only", 1, 0),
         r_white = if_else(r_white_new == 1 |
                             race_new == "White only", 1, 0),
         r_multi = if_else(race_new == "Multiple race", 1, 0),
         r_ethnicity = r_hisp_new,
         race_cat = race_new,
         hh_id = paste(agency_new, hh_id_new, sep = "_"),
         gender = if_else(gender2 == "Female", "Fem",
                          if_else(gender2 == "Male", "Mal",
                                  NA_character_))) %>%
  select(pid0,
         hh_id,
         relcode,
         lname = lname_new,
         fname = fname_new,
         mname = mname_new,
         gender,
         dob,
         r_aian,
         r_asian,
         r_black,
         r_nhpi,
         r_white,
         r_multi,
         r_ethnicity,
         # race_cat,
         disability = disability2,
         entry = startdate,
         exit = enddate,
         agency = agency_new,
         ha_maj_prog = major_prog,
         ha_prog_type = prog_type,
         ha_sub_type = subsidy_type,
         ha_vouch_type = vouch_type_final,
         ha_op_type = operator_type,
         ha_portfolio = portfolio_final)
#
# Agency Data Merge w/ linkages
# --------------------------------------------------------------------------
merged_agencies <-
  bind_rows(hmis_c,pha_c) %>% ungroup() %>%
  left_join(., links %>%
              select(pid0, pid1, pid2, linkage_PID),
            by = "pid0") %>%
  mutate(ha_prog_cat = ifelse(agency == "KCHA" | agency == "SHA",
                              paste0(agency, ",",
                                     ha_sub_type, ",",
                                     ha_vouch_type, ",",
                                     ha_op_type, ",",
                                     ha_portfolio), NA),
         hmis_prog_cat = ifelse(agency == "HMIS",
                                paste0(agency, ",",
                                       hmis_proj_type, ",",
                                       hmis_proj_name, ","), NA))

write_csv(merged_agencies, paste0(hild_dir,"merged_agencies.csv"))
