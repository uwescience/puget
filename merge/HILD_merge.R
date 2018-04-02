[# ==========================================================================
# PHA and HMIS merge
# Tim Thomas - t77@uw.edu
# ==========================================================================

	rm(list=ls()) #reset
	options(max.print = 10000,
		    tibble.print_max = 100,
		    scipen = 999,
		    width = 90,
		    error = traceback)
	gc()

# ==========================================================================
# Library
# ==========================================================================

	library(colorout)
	library(RecordLinkage)
	library(lubridate)
	library(tidyverse)

# ==========================================================================
# Data pull
# ==========================================================================

	hmis <- data.table::fread("data/HMIS/puget_preprocessed.csv") %>%
			mutate(pid0 = paste("HMIS0_",PersonalID,sep=""))

	load("data/Housing/OrganizedData/pha_longitudinal.Rdata")

	pha <- pha_longitudinal %>%
		   mutate(pid0 = paste("PHA0_",pid, sep = ""))

# ==========================================================================
# Data prep
# ==========================================================================

### subset bad birthdays Create bad dob - frequent january 1 dob
		# data.frame(table(pha$dob)) %>% arrange(desc(Freq)) %>% head(80)
		# data.frame(table(hmis$DOB)) %>% arrange(desc(Freq)) %>% head(80)
		bad_dob <- c("2016-01-01","2015-01-01","2014-01-01","2013-01-01","2012-01-01","2011-01-01","2010-01-01","2009-01-01","2008-01-01","2007-01-01","2006-01-01","2005-01-01","2004-01-01","2003-01-01","2002-01-01","2001-01-01","2000-01-01","1999-01-01","1998-01-01","1997-01-01","1996-01-01","1995-01-01","1994-01-01","1993-01-01","1992-01-01","1991-01-01","1990-01-01","1989-01-01","1988-01-01","1987-01-01","1986-01-01","1985-01-01","1984-01-01","1983-01-01","1982-01-01","1981-01-01","1980-01-01","1979-01-01","1978-01-01","1977-01-01","1976-01-01","1975-01-01","1974-01-01","1973-01-01","1972-01-01","1971-01-01","1970-01-01","1969-01-01","1968-01-01","1967-01-01","1966-01-01","1965-01-01","1964-01-01","1963-01-01","1962-01-01","1961-01-01","1960-01-01","1959-01-01","1958-01-01","1957-01-01","1956-01-01","1955-01-01","1954-01-01","1953-01-01","1952-01-01","1951-01-01","1950-01-01","1949-01-01","1948-01-01","1947-01-01","1946-01-01","1945-01-01","1944-01-01","1943-01-01","1942-01-01","1941-01-01","1940-01-01","1939-01-01","1938-01-01","1935-01-01","1934-01-01","1933-01-01","1932-01-01","1930-01-01","1901-01-01","1900-01-01")

# subset PHA
	pha.rl <- pha %>%
			  select(pid0 = pid0,
					  # hh_id = hhold_id_new,
					  ssn = ssn_id_m6,
					  lname = lname_new_m6,
					  fname = fname_new_m6,
					  mname = mname_new_m6,
					  suf = lnamesuf_new_m6,
					  dob = dob,
					  gen = gender2) %>%
			  distinct() %>%
			  mutate(gen = ifelse(gen =="Female", 1,
						   ifelse(gen =="Male", 0, NA)),
					  ssn = as.numeric(ssn),
					  ssn_dq = ifelse(is.na(ssn), 3,
							  ifelse(nchar(ssn)<9,2,1)),
					  dob_y = year(dob),
					  dob_m = month(dob),
					  dob_d = day(dob),
					  dob = as.character(dob),
					  dob_dq=ifelse(dob %in% bad_dob,
								    2,
								    1),
					  pid1 = paste("pha1_",
								   str_pad(seq(1,
								 			   nrow(.)),
								 		   6,
								 		   pad='0'),
								   sep = "")) %>%
			  mutate_at(vars(lname:suf),
					    funs(toupper)) %>%
			  data.frame()
# Change "" to NA
	pha.rl[pha.rl==""] <- NA
# Convert DOB to Date
	pha.rl <- pha.rl %>% mutate(dob = ymd(dob))

# Subset HMIS
	hmis.rl <- hmis %>%
		select(pid0 = pid0,
				# hh_id = HouseholdID,
				ssn = SSN,
				ssn_dq = SSNDataQuality,
				lname = LastName,
				fname = FirstName,
				mname = MiddleName,
				suf = NameSuffix,
				dob = DOB,
				gen = Gender) %>%
		distinct() %>%
		mutate(dob = ymd(dob),
				dob_y = year(dob),
				dob_m = month(dob),
				dob_d = day(dob),
				pid1 = paste("hmis1_",str_pad(seq(1, nrow(.)),6,pad='0'), sep = "")) %>%
		mutate(dob = as.character(dob),
				ssn_dq=ifelse(ssn_dq>2,3,ssn_dq),
				dob_dq=ifelse(dob %in% bad_dob, 2,1)) %>%
		mutate_at(vars(lname:suf), funs(toupper)) %>%
		data.frame()

# Change "" to NA
	hmis.rl[hmis.rl==""] <- NA
# Mutate dob to dateo
	hmis.rl <- hmis.rl %>%
		mutate(dob=ymd(dob))

# combine
	df <- bind_rows(pha.rl,hmis.rl) %>%
		# Change ssn dq if ssn is the same digit
		mutate(ssn_dq=ifelse(ssn == as.numeric(paste(rep(0,9),collapse="")) |
							 ssn == as.numeric(paste(rep(1,9),collapse="")) |
							 ssn == as.numeric(paste(rep(2,9),collapse="")) |
							 ssn == as.numeric(paste(rep(3,9),collapse="")) |
							 ssn == as.numeric(paste(rep(4,9),collapse="")) |
							 ssn == as.numeric(paste(rep(5,9),collapse="")) |
							 ssn == as.numeric(paste(rep(6,9),collapse="")) |
							 ssn == as.numeric(paste(rep(7,9),collapse="")) |
							 ssn == as.numeric(paste(rep(8,9),collapse="")) |
							 ssn == as.numeric(paste(rep(9,9),collapse="")),
							 3,
							 ssn_dq)
				) %>%
		mutate(pid2 = paste("pid2_", str_pad(seq(1, nrow(.)),6,pad='0'),sep = ""),
				ssn1 = ifelse(ssn_dq==1 & nchar(ssn)==9, ssn, NA),
				dob1 = ifelse(dob_dq==1, as.character(dob), NA)
				)  %>%
		mutate(dob1 = ymd(dob1),
				dob1_y = year(dob1),
				dob1_m = month(dob1),
				dob1_d = day(dob1)) %>%
			distinct()

### Subset out refuesed names
	df_sub <- df %>% filter(!grepl("REFUSED",lname),
							!grepl("REFUSED",fname),
							!grepl("ANONYMOUS",lname),
							!grepl("ANONYMOUS",fname))
	####
	# Codebook: PID's and SSN's
	# pid0 = personal ID from pha and hmis - Alastair's linkage and HMIS ID's
	# pid1 = generated pid by Tim within pha and hmis - unique id for each row
	# pid2 = generated pid by Tim after df merge - unique id for each row
	# ssn_dq =
	#	 1 = 9-digit ssn or HMIS dq == 1
	#	 2 = less than 9 digits or HMIS dq == 2
	#    3 = NA, all same digit, or HMIS dq == 3
	# ssn  = original ssn
	# ssn1 = ssn quality == 1 and 9 digits
	# dob1 = dob that is not in the list of very frequent 1/1 dates
	####

# ==========================================================================
# Function Creation
# ==========================================================================

	rlink <- function(df,
					  block,
					  string,
					  phonetic,
					  phonfun = soundex,
					  ex = ex,
					  threshold = 0.1,
					  name_wt = paste0(block,"_wt")){

		ex <- names(df)[!names(df) %in% string]
		# name <- paste0(block, '_wt')
		RL <- df %>%
				compare.dedup(.,
				  blockfld = block,
				  strcmp = string[!string %in% block],
				  phonetic = phonetic,
				  phonfun = phonfun,
				  exclude = ex) %>%
				epiWeights(.) %>%
				epiClassify(.,threshold) %>%
				getPairs(., show = "links", single.rows = TRUE) %>%
		### Reduce duplicated id2
			# Consider automating this below
				arrange(id1) %>%
				filter(!duplicated(id2)) %>%
				select(ssn.1:ssn_dq.1,
						pid0.1,
						pid0.2,
						pid1.1,
						pid1.2,
						pid2.1,
						pid2.2,
						Weight) %>%
				# rename_(.dots = setNames("Weight", paste0(block, "_wt")))
				rename_(.dots = setNames("Weight", name_wt))

		return(RL)
	}

gc()

# ==========================================================================
# Record Linkages
# ==========================================================================

### Fields
	s <- c( "ssn1","dob1_d","dob1_m","dob_y","fname","mname","lname","suf")
	p <- c("lname", "fname", "mname")

### ssn1 RL
	system.time(
		rl_ssn1 <- rlink(df = df_sub,
			block = "ssn1",
			string = s,
			phonetic = p
			))
	gc()

### dob1_ym RL
	system.time(
		rl_dob1_ym <- rlink(df = df_sub,
			block = c("dob_y","dob1_m"),
			string = s,
			phonetic = p,
			name_wt = "ym_wt"
			))
	gc()

### flname RL
	system.time(
		rl_flname <- rlink(df = df_sub,
			block = c("fname","lname"),
			string = s,
			phonetic = p,
			threshold = .1,
			name_wt = "flname_wt"
			))
	gc()

# ==========================================================================
# Create link df
# ==========================================================================

### Join links
	link <- rl_ssn1 %>%
			select(ssn.1:ssn_dq.1,
				   pid0.1,
				   pid0.2,
				   pid1.1,
				   pid1.2,
				   pid2.1,
				   pid2.2,
				   ends_with("_wt"))

	link <- full_join(link,
					  rl_dob1_ym %>%
			select(pid2.1,
				   pid2.2,
				   ends_with("_wt")))

	link <- full_join(link,
					  rl_flname %>%
			select(pid2.1,
				   pid2.2,
				   ends_with("_wt")))

### Change NA to 0
	link <- link %>%
			mutate_at(vars(ends_with("wt")),
					 	   funs(ifelse(is.na(.)==T,
					  	   0.0,
					  	   .))
					  )
glimpse(link)
# ==========================================================================
# Create product weights
# ==========================================================================

### Weighting function
	wtp <- function(p,weights){
			1-prod((1-p)^weights)^(1/sum(weights))
		}

### Different weighting scenerios
	w1 <- link %>%
		mutate(wtp111 = apply( # apply function across rows
							select(., ends_with("wt")), # select columns to apply to
							1, # w/in each row (2 would do it w/in each col)
							wtp, # function to apply
							weights=c(1,1,1)), # weights argument
				wtp1.8.8 = apply( # apply function across rows
							select(., ends_with("wt")), # select columns to apply to
							1, # w/in each row (2 would do it w/in each col)
							wtp, # function to apply
							weights=c(1,.8,.8)), # weights argument
				wtp1.5.5 = apply( # apply function across rows
							select(., ends_with("wt")), # select columns to apply to
							1, # w/in each row (2 would do it w/in each col)
							wtp, # function to apply
							weights=c(1,.5,.5)) # weights argument
						)

	head(w1) # weights all at 1
	hist(w1$wtp111, breaks = 40)
	hist(w1$wtp1.8.8, breaks = 40)
	hist(w1$wtp1.5.5, breaks = 40)

# ==========================================================================
# Save file
# ==========================================================================
	write.csv(w1, "data/Housing/links/WeightedLinks_20180327.csv")

# ==========================================================================
# ==========================================================================
# ==========================================================================
# ===================  Testbed - DO NOT RUN BELOW  =========================
# ==========================================================================
# ==========================================================================
# ==========================================================================

# 	df_sub2 <- df_sub[(is.na(df_sub$ssn1)) &
# 					  (is.na(df_sub$dob1_d)) &
# 					  (is.na(df_sub$dob1_m)) &
# 					  (is.na(df_sub$dob_y)) &
# 					  (is.na(df_sub$fname)) &
# 					  (is.na(df_sub$mname)) &
# 					  (is.na(df_sub$lname)) &
# 					  (is.na(df_sub$suf)),
# 					 ]

# 	df_sub2 <- df_sub %>% filter_at(vars(ssn:dob_d), all.vars(is.na))

# 	ssn_test <- df_sub %>%
# 				compare.dedup(.,
# 				  blockfld = "ssn1",
# 				  strcmp = s[!s %in% "ssn1"],
# 				  phonetic = p,
# 				  phonfun = soundex,
# 				  exclude = names(df_sub)[!names(df_sub) %in% s]) %>%
# 				epiWeights(.) %>%
# 				epiClassify(.,.1) %>%
# 				getPairs(., show = "links", single.rows = TRUE) %>%
# 		### Reduce duplicated id2
# 				arrange(id1) %>%
# 				filter(!duplicated(id2)) %>%
# 				select(ssn.1:ssn_dq.1,
# 						pid0.1,
# 						pid0.2,
# 						pid1.1,
# 						pid1.2,
# 						pid2.1,
# 						pid2.2,
# 						ssn1_wt = Weight)

# 	dob_test <- df_sub %>%
# 				compare.dedup(.,
# 				  blockfld = c("dob_y", "dob1_m"),
# 				  strcmp = s[!s %in% c("dob_y", "dob1_m")],
# 				  phonetic = p,
# 				  phonfun = soundex,
# 				  exclude = names(df_sub)[!names(df_sub) %in% s]) %>%
# 				epiWeights(.) %>%
# 				epiClassify(.,.1) %>%
# 				getPairs(., show = "links", single.rows = TRUE) %>%
# 		### Reduce duplicated id2
# 				arrange(id1) %>%
# 				filter(!duplicated(id2)) %>%
# 				select(ssn.1:ssn_dq.1,
# 						pid0.1,
# 						pid0.2,
# 						pid1.1,
# 						pid1.2,
# 						pid2.1,
# 						pid2.2,
# 						dob_wt = Weight)

# 	name_test <- df_sub %>%
# 				compare.dedup(.,
# 				  blockfld = c("fname","lname"),
# 				  strcmp = s[!s %in% c("fname","lname")],
# 				  phonetic = p,
# 				  phonfun = soundex,
# 				  exclude = names(df_sub)[!names(df_sub) %in% s]) %>%
# 				epiWeights(.) %>%
# 				epiClassify(.,.1) %>%
# 				getPairs(., show = "links", single.rows = TRUE) %>%
# 		### Reduce duplicated id2
# 				arrange(id1) %>%
# 				filter(!duplicated(id2)) %>%
# 				select(ssn.1:ssn_dq.1,
# 						pid0.1,
# 						pid0.2,
# 						pid1.1,
# 						pid1.2,
# 						pid2.1,
# 						pid2.2,
# 						name_wt = Weight)


# 	link_test <- ssn_test %>%
# 			select(ssn.1:ssn_dq.1,
# 				   pid0.1,
# 				   pid0.2,
# 				   pid1.1,
# 				   pid1.2,
# 				   pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt"))

# 	link_test <- full_join(link_test,
# 					  dob_test %>%
# 			select(pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt")))

# 	link_test <- full_join(link_test,
# 					  name_test %>%
# 			select(pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt")))

# ### Change NA to 0
# 	link_test <- link_test %>%
# 			mutate_at(vars(ends_with("wt")),
# 					 	   funs(ifelse(is.na(.)==T,
# 					  	   0.0,
# 					  	   .))
# 					  )

# glimpse(link_test)

# 		wtp(p = c(0.5999086, 0.0000000,  0.5999086), weights = 1)
# 		(1-1)^0.5999086

# # ==========================================================================
# # Count
# # ==========================================================================
# 	w1 %>%
# 		filter(wtp > .7) %>%
# 		# filter(grepl("PHA",pid0.1) & grepl("HMIS",pid0.2)) %>%
# 		glimpse()

# 	w1 %>%
# 		filter(wtp > .7) %>%
# 		filter(grepl("HMIS",pid0.1) & grepl("PHA",pid0.2)) %>%
# 		glimpse()


# # ==========================================================================
# # Loop testbed
# # ==========================================================================

# ### Fields
# 	test_df <- head(df_sub,100)
# 	# s <- c( "ssn1","dob1_d","dob1_m","dob_y","fname","mname","lname","suf")
# 	s <- c( "ssn1","fname","mname","lname")
# 	# ex <- names(df_sub)[!names(df_sub) %in% s]
# 	p <- c("lname", "fname", "mname")

# ### SSN ###

# rl <- lapply(test_df, function(rlink) c(df = test_df,block = s[i], string = s, phonetic = p))
# ### SSN ### - kinda works
# 	for(i in s){
# 	rl <- as.list(NULL)
# 	rl[[i]] <- list(rlink(df = test_df,
# 			block = i,
# 			string = s,
# 			phonetic = p
# 			))

#    }

# rl
# rm(rl)
# 	glimpse(ssn_rl)


# # cols <- c("Sepal.Length", "Petal.Length")
# to_app <- "_wt"
# cols <- rename_(rl, setNames(Weight, paste0("name", to_app))



# # ==========================================================================
# # DO NOT RUN BELOW
# # ==========================================================================


# ### SSN ###
# 	dob_d <- rlink(df = df_sub,
# 		`	blockfld = "dob_d1",
# 			strcmp = c( "ssn1",
# 						#"dob1_d",
# 	  					"dob1_m",
# 	  					"dob_y",
# 	  					"ssn1",
# 	  					"fname",
# 	  					"mname",
# 	  					"lname",
# 	  					"suf"),
# 			phonetic = c("lname",
# 						 "fname",
# 						 "mname"),
# 			phonfun = soundex,
# 			exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq","ssn","dob_dq","dob", "dob_d", "dob_m", "dob1_y"),
# 			threshold = .1
# 			)
# 	glimpse(ssn)


# mtcars %>%
#   split(.$cyl) %>% # from base R
#   map(~ lm(mpg ~ wt, data = .)) %>%
#   map(summary) %>%
#   map_dbl("r.squared")

# # ==========================================================================
# # RL1 - SSN
# # Block on quality ssn's using df w/o "Refused" names
# # Logic: Match on quality SSN's and the DOB year, however, strcmp on month
# # 		and days not in the "bad" "01-01" DOB onject above.
# # ==========================================================================

# 	RL1 <- compare.dedup(df_sub,
# 			blockfld = c("ssn1"),
# 	  		strcmp = c( "dob1_d",
# 	  					"dob1_m",
# 	  					"dob_y",
# 	  					# "ssn",
# 	  					"fname",
# 	  					"mname",
# 	  					"lname",
# 	  					"suf"),
# 	  		phonetic = c("lname", "fname", "mname"),
# 	  		phonfun = soundex,
# 	  		exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq","ssn","dob_dq","dob", "dob_d", "dob_m", "dob1_y"))

# # epiWeigts
# 	w1 <- epiWeights(RL1)

# # Match threshold choice
# 	matches1 <- epiClassify(w1,.1)
# 	matches1 <- getPairs(matches1, show = "links", single.rows = TRUE)

# 	# Checks
# 		# glimpse(matches1)
# 		# head(matches1)
# 		# hist(matches1$Weight, breaks = 40)

# ### Reduce duplicated id2
# 	matches1 <- matches1 %>% arrange(id1) %>% filter(!duplicated(id2))

# ### Create link file
# 	link <- matches1 %>% select(ssn.1:ssn_dq.1,pid0.1,pid1.1,pid2.1,pid2.2, ssn_wt=Weight)
# 	head(link)
# 	dim(link)

# # ==========================================================================
# # RL2 - lname, dob1_y, dob1_m, dob1_d
# # Logic: lnames, original DOB year, and good DOB month and day
# # ==========================================================================

# 	RL2 <- compare.dedup(df_sub,
# 			blockfld = c("lname", "dob_y", "dob1_m", "dob1_d"),
# 	  		strcmp = c( # "dob_d",
# 	  					# "dob_m",
# 	  					# "dob_y",
# 	  					"ssn",
# 	  					"fname",
# 	  					"mname",
# 	  					# "lname",
# 	  					"suf"),
# 	  		phonetic = c("lname", "fname", "mname"),
# 	  		phonfun = soundex,
# 	  		exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq","dob_dq","dob", "dob_d", "dob_m", "dob1_y","ssn1"))

# # epiWeigts
# 	w2 <- epiWeights(RL2)

# # Match threshold choice
# 	matches2 <- epiClassify(w2,.1)
# 	matches2 <- getPairs(matches2, show = "links", single.rows = TRUE)

# ### Reduce duplicated id2
# 	matches2 <- matches2 %>% arrange(id1) %>% filter(!duplicated(id2))


# ### Merge link file
# 	link <- full_join(link, matches2 %>% select(pid2.1,pid2.2, lname_dobwt=Weight))
# 	head(link)

# # ==========================================================================
# # RL3 - fname, dob_y, dob_m
# # ==========================================================================

# 	RL3 <- compare.dedup(df_sub,
# 			blockfld = c("fname", "dob_y", "dob1_m"),
# 	  		strcmp = c( "dob1_d",
# 	  					# "dob1_m",
# 	  					# "dob_y",
# 	  					"ssn",
# 	  					# "fname",
# 	  					"mname",
# 	  					"lname",
# 	  					"suf"),
# 	  		phonetic = c("lname", "fname", "mname"),
# 	  		phonfun = soundex,
# 	  		exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq", "dob_dq","dob1", "dob_d", "dob_m", "dob1_y","ssn1"))

# # epiWeigts
# 	w3 <- epiWeights(RL3)

# # Match threshold choice
# 	matches3 <- epiClassify(w3,.1)
# 	matches3 <- getPairs(matches3, show = "links", single.rows = TRUE)

# ### Reduce duplicated id3
# 	matches3 <- matches3 %>% arrange(id1) %>% filter(!duplicated(id2))

# ### Merge link file
# 	link <- full_join(link, matches3 %>% select(pid2.1,pid2.2, fname_dobwt=Weight))
# 	head(link)
# 	dim(link)


# # ==========================================================================
# # End code
# # ==========================================================================