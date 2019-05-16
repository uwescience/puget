# Age
# --------------------------------------------------------------------------
# look at all DOB's and use the most commonly used one weighting towards PHA dob
# Prioritize PHA dob first and find most common dob
pha_real_dob <-
  merged_agencies %>%
  filter(agency %in% c("SHA", "KCHA")) %>%
  mutate(h_dob = ymd(dob)) %>%
  group_by(linkage_PID) %>%
  count(linkage_PID, h_dob) %>%
  filter(n == max(n)) %>% #### problem child ####
  ungroup() %>%
  mutate(agency = "PHA")

# Subset HMIS dob not in PHA
hmis_real_dob <-
  merged_agencies %>%
  filter(!linkage_PID %in% pha_real_dob$linkage_PID) %>%
  mutate(h_dob = ymd(dob)) %>%
  group_by(linkage_PID) %>%
  count(linkage_PID, h_dob) %>%
  filter(n == max(n)) %>%
  ungroup() %>%
  mutate(agency = "HMIS")

# Combine PHA and HMIS dob
real_dob <-
  bind_rows(pha_real_dob, hmis_real_dob) %>%
  select(-agency) %>%
  ungroup()


### WIP - adjust dob within a short period or call them NA

#
# Build final database
# --------------------------------------------------------------------------

hmisproj <- c("Day Shelter", # short hmis services get 90 day exits
               "Emergency Shelter",
               "Homelessness Prevention",
               "Other Programs",
               "Services Only",
               "Street Outreach")

# Join to df
hild_df <-
  left_join(merged_agencies, real_dob) %>%
  select(-n) %>%
  filter(!is.na(linkage_PID), entry >= "2004-01-01") %>% # remove refused names
  group_by(hh_id, entry) %>%
  mutate(h_hh_ct = n()) %>%
  ungroup() %>%
  mutate_at(vars(dob, entry, exit), list(ymd)) %>%
  ## Age
  mutate(h_age = time_length(difftime(entry,h_dob),"year")) %>%
  mutate(h_age = if_else(h_age < 0 |
                           h_age > 120,
                         NA_real_,
                         h_age),
         h_dob = if_else(h_age < 0 |
                           h_age > 120,
                         NA_Date_,
                         h_dob),
         h_relcode = if_else(relcode == "" |
                               relcode == "99" |
                               relcode == "D" |
                               relcode == "M" |
                               relcode == "N",
                             NA_character_,
                             relcode)) %>%
  mutate(h_child = if_else(h_age < 18 &
                             h_relcode == "Y", "yes",
                           if_else(h_age < 18 &
                                     h_relcode == "F", "yes",
                                   if_else(h_age < 18 &
                                             h_relcode == "2", "yes",
                                           if_else(is.na(h_age) &
                                                     h_relcode == "Y",
                                                   "relcode",
                                                   if_else(h_age >= 18 &
                                                             h_relcode == "Y",
                                                           "relcode", "no"))))),
         h_age = if_else(h_child != "no" &
                           h_age > 25, # can pick a better cutoff than 25
                         NA_real_,
                         h_age),
         h_elder = if_else(h_child == "no" &
                             h_age >= 62,
                           "yes",
                           if_else(h_child == "no" &
                                     is.na(h_age),
                                   NA_character_,
                                   "no"))) %>%
  group_by(hh_id, entry) %>%
  mutate(h_hh_kids = sum(h_child != "no"),
         h_hh_adults = sum(h_child == "no"),
         h_hh_type1 = if_else(any(h_elder == "yes"),
                              "Elderly household",
                              NA_character_)) %>%
  mutate(h_hh_type1 = if_else(is.na(h_hh_type1) &
                                any(disability == "Disabled"),
                              "Non-elderly disabled household",
                              h_hh_type1)) %>%
  mutate(h_hh_type1 = if_else(is.na(h_hh_type1),
                              "Non-elderly non-disabled household",
                              h_hh_type1),
         h_hh_type2 = if_else(h_hh_kids > 0 &
                                h_hh_adults > 1,
                              "multi-adult household with children",
                              if_else(h_hh_kids > 0 &
                                        h_hh_adults == 1,
                                      "single parent household with children",
                                      if_else(h_hh_kids == 0 &
                                                h_hh_adults > 0,
                                              "household without children",
                                              NA_character_)))) %>%
  ungroup() %>%
  group_by(linkage_PID) %>%
  arrange(entry) %>%
  mutate(h_exit = as_date(
                          if_else(agency == "HMIS" &
                                  is.na(exit) &
                                  !hmis_proj_type %in% hmisproj &
                                  !is.na(lead(exit)),
                                  lead(entry),
                          if_else(agency == "HMIS" &
                                  is.na(exit) &
                                  !hmis_proj_type %in% hmisproj &
                                  is.na(lead(exit)),
                                  ymd("2017-12-31"),
                          if_else(agency == "HMIS" &
                                 hmis_proj_type %in% hmisproj &
                                 is.na(exit) &
                                 !is.na(lead(exit)) &
                                 difftime(lead(entry), entry) >= 90,
                                 entry + 90,
                          if_else(agency == "HMIS" &
                                 hmis_proj_type %in% hmisproj &
                                 is.na(exit) &
                                 !is.na(lead(exit)) &
                                 difftime(lead(entry), entry) < 90,
                                 lead(entry),
                          if_else(agency == "HMIS" &
                                 hmis_proj_type %in% hmisproj &
                                 is.na(exit) &
                                 is.na(lead(exit)),
                                 entry + 90,
                                 exit))))))) %>%
  mutate(h_servdays = time_length(difftime(h_exit,entry), "day"),
         h_entry = if_else(h_servdays < 0, exit, entry)) %>%
  group_by(linkage_PID, agency) %>%
  mutate(h_agency_time = sum(width(IRanges::reduce(IRanges(as.numeric(h_entry),
                                                  as.numeric(h_exit) - 1))))) %>%
  group_by(linkage_PID) %>%
  mutate(h_tot_time = sum(width(IRanges::reduce(IRanges(as.numeric(h_entry),
                                               as.numeric(h_exit) - 1))))) %>%
  mutate(h_servdays = if_else(h_servdays < 0, NA_real_, h_servdays),
         h_exit = if_else(is.na(h_servdays), NA_Date_, h_exit),
         h_entry = if_else(is.na(h_servdays), NA_Date_, entry),
         h_servdays = time_length(difftime(h_exit,h_entry), "day") + 1,
         h_time_qual = if_else(is.na(h_servdays), 1, 0)) %>%
  ungroup() %>%
  arrange(linkage_PID, h_entry, h_exit) %>%
  group_by(linkage_PID) %>%
  distinct() %>% # there are several cases with dupe dates
  mutate(h_agency_trans = if_else(is.na(lead(agency)),
                          agency,
                          paste(agency, lead(agency), sep = " to ")),
         h_t2next_prog = time_length(difftime(lead(h_entry),h_exit), "day"),
         h_overlap = int_overlaps(interval(lead(h_entry),
                                           lead(h_exit)),
                                  interval(h_entry,
                                           h_exit)),
         h_within = interval(lead(h_entry),
                             lead(h_exit))
                    %within%
                    interval(h_entry,
                             h_exit)) %>%
  ungroup()

# # ==========================================================================
# # Write csv
# # ==========================================================================

  usr <- "t77"
  write_csv(hild_df, paste0("/home/",usr,"/data/HILD/hild_df.csv"))
  # write_csv(hild_df, paste0("/home/",usr,"/data/HILD/hild_df_testbed.csv"))
