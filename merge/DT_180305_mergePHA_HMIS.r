# ==========================================================================
# PHA and HMIS merge
# Tim Thomas - t77@uw.edu
# Install Microsoft open R: https://docs.microsoft.com/en-us/machine-learning-server/r-client/install-on-linux
# ==========================================================================

	rm(list=ls()) #reset
	options(max.print = 10000, tibble.print_max = 50, scipen = 999, width = 90)
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

	pha <- pha_longitudinal %>% mutate(pid0 = paste("PHA0_",pid, sep = ""))

# ==========================================================================
# Data prep
# ==========================================================================

### Subset & clean ###

# Create bad dob - frequent january 1 dob
		data.frame(table(pha$dob)) %>% arrange(desc(Freq)) %>% head(80)
		data.frame(table(hmis$DOB)) %>% arrange(desc(Freq)) %>% head(80)
		bad_dob <- c("1980-01-01", "1970-01-01", "1982-01-01", "1990-01-01", "1985-01-01", "1975-01-01", "1981-01-01", "1978-01-01", "1983-01-01", "1986-01-01", "1960-01-01", "1979-01-01", "1972-01-01", "1988-01-01", "1989-01-01", "1984-01-01", "2009-01-01", "2010-01-01", "1968-01-01", "1965-01-01", "1974-01-01", "1987-01-01", "1969-01-01", "1977-01-01", "1971-01-01", "1967-01-01", "2011-01-01", "1976-01-01", "2007-01-01", "1961-01-01", "1963-01-01", "1973-01-01", "2008-01-01", "1964-01-01", "1991-01-01", "1966-01-01", "1962-01-01", "2006-01-01", "2004-01-01", "2012-01-01", "2005-01-01", "1992-01-01", "2000-01-01", "1959-01-01", "1958-01-01", "2003-01-01", "2001-01-01", "1994-01-01", "1993-01-01", "2002-01-01", "1995-01-01", "1999-01-01", "1996-01-01", "1957-01-01", "1955-01-01", "2013-01-01", "1998-01-01", "1956-01-01", "1997-01-01", "1954-01-01", "2014-01-01", "1950-01-01", "1953-01-01", "1952-01-01", "2015-01-01", "1951-01-01", "1949-01-01", "1948-01-01", "1947-01-01", "1945-01-01", "2016-01-01", "1946-01-01", "1900-01-01", "1943-01-01", "1901-01-01", "1944-01-01", "1940-01-01", "1942-01-01", "1941-01-01", "1935-01-01", "1939-01-01", "1938-01-01", "1932-01-01", "1934-01-01", "1933-01-01", "1930-01-01")

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
				dob_dq=ifelse(dob %in% bad_dob, 2,1),
				pid1 = paste("pha1_",str_pad(seq(1, nrow(.)),6,pad='0'), sep = "")) %>%
		mutate_at(vars(lname:suf), funs(toupper)) %>%
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
# Mutate dob to date
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
							 ssn == as.numeric(paste(rep(9,9),collapse="")), 3, ssn_dq)
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

	# Checks
		# dim(df)
		# glimpse(df)
		# head(df, 100)
		# df %>% filter(ssn==444444444)
		# df %>% filter(nchar(ssn)>8, ssn_dq==3)

### Subset out refuesed names
	df_sub <- df %>% filter(!grepl("REFUSED",lname),
							!grepl("REFUSED",fname),
							!grepl("ANONYMOUS",lname),
							!grepl("ANONYMOUS",fname))
	# dim(df_sub)

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
# RL1 - SSN
# Block on quality ssn's using df w/o "Refused" names
# Logic: Match on quality SSN's and the DOB year, however, strcmp on month
# 		and days not in the "bad" "01-01" DOB onject above.
# ==========================================================================

	RL1 <- compare.dedup(df_sub,
			blockfld = c("ssn1"),
	  		strcmp = c( "dob1_d",
	  					"dob1_m",
	  					"dob_y",
	  					# "ssn",
	  					"fname",
	  					"mname",
	  					"lname",
	  					"suf"),
	  		phonetic = c("lname", "fname", "mname"),
	  		phonfun = soundex,
	  		exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq","ssn","dob_dq","dob", "dob_d", "dob_m", "dob1_y"))

# epiWeigts
	w1 <- epiWeights(RL1)

# Match threshold choice
	matches1 <- epiClassify(w1,.1)
	matches1 <- getPairs(matches1, show = "links", single.rows = TRUE)

	# Checks
		# glimpse(matches1)
		# head(matches1)
		# hist(matches1$Weight, breaks = 40)

### Reduce duplicated id2
	matches1 <- matches1 %>% arrange(id1) %>% filter(!duplicated(id2))

### Create link file
	link <- matches1 %>% select(ssn.1:ssn_dq.1,pid0.1,pid1.1,pid2.1,pid2.2, ssn_wt=Weight)
	head(link)
	dim(link)

# ==========================================================================
# RL2 - lname, dob1_y, dob1_m, dob1_d
# Logic: lnames, original DOB year, and good DOB month and day
# ==========================================================================

	RL2 <- compare.dedup(df_sub,
			blockfld = c("lname", "dob_y", "dob1_m", "dob1_d"),
	  		strcmp = c( # "dob_d",
	  					# "dob_m",
	  					# "dob_y",
	  					"ssn",
	  					"fname",
	  					"mname",
	  					# "lname",
	  					"suf"),
	  		phonetic = c("lname", "fname", "mname"),
	  		phonfun = soundex,
	  		exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq","dob_dq","dob", "dob_d", "dob_m", "dob1_y","ssn1"))

# epiWeigts
	w2 <- epiWeights(RL2)

# Match threshold choice
	matches2 <- epiClassify(w2,.1)
	matches2 <- getPairs(matches2, show = "links", single.rows = TRUE)

### Reduce duplicated id2
	matches2 <- matches2 %>% arrange(id1) %>% filter(!duplicated(id2))


### Merge link file
	link <- full_join(link, matches2 %>% select(pid2.1,pid2.2, lname_dobwt=Weight))
	head(link)

# ==========================================================================
# RL3 - fname, dob_y, dob_m
# ==========================================================================

	RL3 <- compare.dedup(df_sub,
			blockfld = c("fname", "dob_y", "dob1_m"),
	  		strcmp = c( "dob1_d",
	  					# "dob1_m",
	  					# "dob_y",
	  					"ssn",
	  					# "fname",
	  					"mname",
	  					"lname",
	  					"suf"),
	  		phonetic = c("lname", "fname", "mname"),
	  		phonfun = soundex,
	  		exclude = c("pid0", "pid1", "pid2", "dob", "ssn_dq", "dob_dq","dob1", "dob_d", "dob_m", "dob1_y","ssn1"))

# epiWeigts
	w3 <- epiWeights(RL3)

# Match threshold choice
	matches3 <- epiClassify(w3,.1)
	matches3 <- getPairs(matches3, show = "links", single.rows = TRUE)

### Reduce duplicated id3
	matches3 <- matches3 %>% arrange(id1) %>% filter(!duplicated(id2))

### Merge link file
	link <- full_join(link, matches3 %>% select(pid2.1,pid2.2, fname_dobwt=Weight))
	head(link)
	dim(link)

# ==========================================================================
# Create product weights
# ==========================================================================

### change NA to 0
	link <- link %>%
		mutate_at(vars(ends_with("wt")), funs(ifelse(is.na(.)==T, 0.0, .)))

### function
	wtp <- function(p,weights){
			1-prod((1-p)^weights)^(1/sum(weights))
		}

### Different weighting scenerios
	w1 <- link %>%
		mutate(wtp = apply( # apply function across rows
							select(., ends_with("wt")), # select columns to apply to
							1, # w/in each row (2 would do it w/in each col)
							wtp, # function to apply
							weights=1) # weights argument
						)
	w2 <- link %>% mutate(wtp = apply(select(., ends_with("wt")),1,wtp, weights=c(1,.8,.8)))
	w3 <- link %>% mutate(wtp = apply(select(., ends_with("wt")),1,wtp, weights=c(1,.5,.5)))

	head(w1) # weights all at 1
	head(w2) # weights at 1, .5, & .5
	head(w3) # weights at 1, .8, & .8
	hist(w1$wt, breaks = 40)
	hist(w2$wt, breaks = 40)
	hist(w3$wt, breaks = 40)

# ==========================================================================
# End code
# ==========================================================================