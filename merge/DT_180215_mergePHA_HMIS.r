# ==========================================================================
# PHA and HMIS merge
# Tim Thomas - t77@uw.edu
# ==========================================================================

	rm(list=ls()) #reset
	options(max.print = 10000, tibble.print_max = 50, scipen = 999, width = 140)
	gc()

# ==========================================================================
# Library
# ==========================================================================

	library(colorout)
	library(RecordLinkage)
	library(fastLink)
	library(lubridate)
	# library(data.table)
	library(tidyverse)

# ==========================================================================
# Data pull
# ==========================================================================

	hmis <- data.table::fread("data/HMIS/puget_preprocessed.csv")

	load("data/Housing/OrganizedData/pha_longitudinal.Rdata")

	pha <- pha_longitudinal

# ==========================================================================
# Data prep
# ==========================================================================

### Create table ID in PHA data ###
	# pha$tID <- paste("pha_",str_pad(seq(1, nrow(pha)),8,pad='0'), sep = "")

### Subset & clean ###

# subset PHA
	pha.rl <- pha %>%
		select(pid = pid,
				hh_id = hhold_id_new,
				ssn = ssn_id_m6,
				lname = lname_new_m6,
				fname = fname_new_m6,
				mname = mname_new_m6,
				suf = lnamesuf_new_m6,
				dob = hh_dob_m6,
				gen = gender2) %>%
		distinct() %>%
		mutate(gen = ifelse(gen =="Female", 1,
					ifelse(gen =="Male", 0, NA)),
				ssn = as.numeric(ssn),
				dob_y = year(dob),
				dob_m = month(dob),
				dob_d = day(dob),
				dob = as.character(dob),
				tID = paste("pha_",str_pad(seq(1, nrow(.)),8,pad='0'), sep = "")) %>%
		data.frame()
# Change "" to NA
	pha.rl[pha.rl==""] <- NA
# Convert DOB to Date
	pha.rl <- pha.rl %>% mutate(dob = ymd(dob))

# Subset HMIS
	hmis.rl <- hmis %>%
		select(pid = PersonalID,
				hh_id = HouseholdID,
				ssn = SSN,
				lname = LastName,
				fname = FirstName,
				mname = MiddleName,
				suf = NameSuffix,
				dob = DOB,
				gen = Gender) %>%
		distinct() %>%
		mutate(dob = as.Date(dob),
				dob_y = year(dob),
				dob_m = month(dob),
				dob_d = day(dob),
				tID = paste("hmis_",str_pad(seq(1, nrow(.)),8,pad='0'), sep = "")) %>%
		mutate(dob = as.character(dob))
# Change "" to NA
	hmis.rl[hmis.rl==""] <- NA
# Convert DOB to Date
	hmis.rl <- hmis.rl %>% mutate(dob = ymd(dob))

### Combine ###
	df <- bind_rows(pha.rl,hmis.rl)	%>% filter(!is.na(lname) & !is.na(fname))
	df %>% arrange(fname) %>% head(100)

### Remove names that are odd


# ==========================================================================
# Raw check
# ==========================================================================
### filter ssn's that are in both hmis and pha
	hmis.ssn <- hmis.rl %>% filter(!is.na(ssn),ssn %in% pha.rl$ssn)
	pha.ssn <- pha.rl %>% filter(!is.na(ssn),ssn %in% hmis.rl$ssn)

### dim ###
	dim(hmis.ssn) # 38,422
	dim(pha.ssn)  # 17,827

# ==========================================================================
# Compare and Deduplicate - RecordLinkage Style
# ==========================================================================
system.time(
	RL1 <- compare.dedup(df,
			blockfld = c("ssn", "fname", "dob_y"),
	  		strcmp = c("dob_d","dob_m","mname", "lname","gen", "suf"),
	  		phonetic = c("lname", "fname", "mname"), phonfun = soundex,
	  		exclude = c("pid", "hh_id", "tID", "dob"))
	)

	RL1.wt <- epiWeights(RL1)
	classify1 <- epiClassify(RL1.wt, threshold.upper = .7)
	summary(classify1)
	system.time(
		pairs1 <- getPairs(classify1, single.rows = FALSE)
	)

 	head(pairs1, 100)
 	pairs1 %>% filter(Weight >= .99)

 	####
 	library(RecordLinkage)

	# Attempt 4 with ID creation
	df_names <- df
	# rm(names)
	gc()

	# df_names <- df_names %>% mutate(ID = 1:nrow(df_names))


	rpairs <- compare.dedup(df_names,
	                        phonetic = 1:2,
	                        blockfld = c(2,4),
	                        exclude = c("NID","ID", "H3H", "HHE_ID"))

	p=epiWeights(rpairs)
	classify <- epiClassify(p,0.5)
	summary(classify)
	matches <- getPairs(classify, show = "links", single.rows = TRUE)
	matches <- matches %>%
	  filter(H3C.1 != "0") #missing values become 0s, which the matching fx ends up finding matches on.
	                       #this removes the matches where inds w/ missing first names were matched to each other
	matches %>% arrange(ID.1) %>% View()
	#this code writes an "ID" column that is the same for similar names
	matches <- matches %>% arrange(ID.1) %>% filter(!duplicated(ID.2))
	df_names$ID_prior <- df_names$ID

	#look at N of matches < 1.00 prob
	length(matches$Weight[matches$Weight < 1])

	# merge matching information with the original data
	df_names <- left_join(df_names, matches %>% select(ID.1,ID.2), by=c("ID"="ID.2"))

	# replace matches in ID with the thing they match with from ID.1
	df_names$ID <- ifelse(is.na(df_names$ID.1), df_names$ID, df_names$ID.1)

# ==========================================================================
# fastLink style - LONG RUNS
# ==========================================================================

	# system.time(
	# 	mo1 <- fastLink(
	# 		dfA = pha.rl, dfB = hmis.rl,
	# 		varnames = c("ssn","fname", "mname", "lname", "suf", "dob_y"),
	# 		stringdist.match = c("ssn", "fname", "mname","lname","suf"),
	# 		partial.match = c("fname", "lname")
	# 		)
	# )

	# system.time(
	# mo2 <- fastLink(
	# 	dfA = pha.rl, dfB = pha.rl,
	# 	varnames = c("ssn","fname", "mname", "lname", "suf", "dob_y"),
	# 	stringdist.match = c("ssn", "fname", "mname","lname","suf"),
	# 	partial.match = c("fname", "lname")
	# 	)
	# )

	# system.time(
	# 	mo1 <- fastLink(
	# 		dfA = pha.rl, dfB = hmis.rl,
	# 		varnames = c("ssn","fname", "mname", "lname", "suf", "dob_y"),
	# 		stringdist.match = c("ssn", "fname", "mname","lname","suf"),
	# 		partial.match = c(fnam"lname")
	# 		)
	# )

# Save file
	# save(mo,file = "data/Housing/temp/matches_temp.RData")	summary(mo)
	load("data/Housing/temp/matches_temp.RData")

# Get matches
	# pha.rl.match <- pha.rl[mo$inds.a,]
	# hmis.rl.match <- hmis.rl[mo$inds.b,]
	m <- getMatches(pha.rl,hmis.rl,matches.out)


# ==========================================================================
# TESTBED
# ==========================================================================
### Measure ###
 	data(samplematch)
 	FL1 <- fastLink(
	  	dfA = dfA, dfB = dfB,
	  	varnames = c("firstname", "middlename", "lastname", "housenum", "streetname", "city", "birthyear"),
	  	stringdist.match = c("firstname", "middlename", "lastname", "streetname", "city"),
	  	partial.match = c("firstname", "lastname", "streetname"),

	)
	FL1$matches

 	RL1 <-


	dfA.match <- dfA[matches.out$matches$inds.a,]
	dfB.match <- dfB[matches.out$matches$inds.b,]

	mo1$matches

### individual steps ###
	g_firstname <- gammaCKpar(dfA$firstname, dfB$firstname)
	g_middlename <- gammaCK2par(dfA$middlename, dfB$middlename)
	g_lastname <- gammaCKpar(dfA$lastname, dfB$lastname)
	g_housenum <- gammaKpar(dfA$housenum, dfB$housenum)
	g_streetname <- gammaCKpar(dfA$streetname, dfB$streetname)
	g_city <- gammaCK2par(dfA$city, dfB$city)
	g_birthyear <- gammaKpar(dfA$birthyear, dfB$birthyear)

	gammalist <- list(g_firstname, g_middlename, g_lastname, g_housenum, g_streetname, g_city, g_birthyear)
	tc <- tableCounts(gammalist, nobs.a = nrow(dfA), nobs.b = nrow(dfB))