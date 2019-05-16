#
# Create Clusters
# --------------------------------------------------------------------------

edges <-
  left_join(merged_agencies %>%
              select(hh_id, from = linkage_PID) %>%
              na.omit(),
            merged_agencies %>%
              select(hh_id, to = linkage_PID) %>%
              na.omit()) %>%
  arrange(from) %>%
  select(-hh_id) %>%
  distinct() %>%
  data.frame()

clu <-
  clusters(graph_from_data_frame(edges))

clusters <-
  with(clu,
       data.frame(
         linkage_PID = names(membership),
         h_cluster = membership,
         h_cluster_ct = csize[membership]
       )
  ) %>%
  arrange(h_cluster) %>%
  mutate(linkage_PID = as.integer(as.character(linkage_PID)))

merged_agencies <-
  left_join(merged_agencies, clusters)
