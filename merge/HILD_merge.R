# ==========================================================================
# PHA and HMIS merge
# Tim Thomas - t77@uw.edu
# ==========================================================================

	rm(list=ls()) #reset
	# options(max.print = 10000,
	# 	    tibble.print_max = 100,
	# 	    scipen = 999,
	# 	    width = 78, # adjust for terminal screen size
	# 	    error = traceback)
	# gc()

# ==========================================================================
# Library
# ==========================================================================
if(!require(devtools)){
	    install.packages("devtools")
	    require(devtools)
	}

if(!require(gdata)){
	    install.packages("gdata")
	    require(gdata)
	}

if(!require(data.table)){
	install.packages("data.table")
	require(data.table)
}

if(!require(tidyverse)){
	devtools::install_github("tidyverse/tidyverse")
	require(tidyverse)
}

if(!require(lubridate)){
	    devtools::install_github("tidyverse/lubridate")
	    require(lubridate)
	}
# ==========================================================================
# Data pull
# ==========================================================================

	path <- "/home/ubuntu/data"

	hmis <- fread(paste0(path,"/HMIS/2016/puget_preprocessed.csv")) %>%
			mutate(pid0 = paste("HMIS0_",PersonalID,sep=""))

	pha <- fread(paste0(path,"/HILD/pha_longitudinal.csv")) %>%
			mutate(pid0 = paste("PHA0_",pid, sep = ""))
	

# ==========================================================================
# Data prep
# ==========================================================================

### subset bad birthdays Create bad dob - frequent january 1 dob

	pha_bad_dob <- data.frame(table(ymd(pha$dob))) %>%
				   rename(dob = Var1) %>%
				   mutate(dob = ymd(dob)) %>%
				   arrange(desc(Freq)) %>%
				   filter(year(dob) > 2018 |
						  year(dob) < 1900 |
						  Freq >= 64) %>% # about the freq of bad jan 1 dob's
				   arrange(desc(dob)) %>%
				   pull(1)

	hmis_bad_dob <- data.frame(table(ymd(hmis$DOB))) %>%
					rename(dob = Var1) %>%
					mutate(dob = ymd(dob)) %>%
					arrange(desc(Freq)) %>%
					filter(year(dob) > 2018 |
						   year(dob) < 1900 |
						   Freq >= 81) %>% # about the freq of bad jan 1 dob's
					arrange(desc(dob)) %>%
					pull(1)

# subset PHA
	pha.rl <- pha %>%
			  select(pid0 = pid0,
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
					 dob_dq=ifelse(dob %in% pha_bad_dob,2,1),
					 pid1 = paste("pha1_",
								  str_pad(seq(1,nrow(.)),6,pad='0'),
								  sep = "")) %>%
			  mutate_at(vars(lname:suf),
					    funs(toupper)) %>%
			  data.frame()
# Change "" to NA
	pha.rl[pha.rl==""] <- NA

# # Convert DOB to Date
# 	pha.rl <- pha.rl %>%
# 			  mutate(dob = ymd(dob))

# Subset HMIS
	hmis.rl <- hmis %>%
			   select(pid0 = pid0,
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
					  pid1 = paste("hmis1_",
					   			   str_pad(seq(1, nrow(.)),6,pad='0'),
					   			   sep = "")
					  ) %>%
			   mutate(dob = as.character(dob),
					  ssn_dq=ifelse(ssn_dq>2,3,ssn_dq),
					  dob_dq=ifelse(dob %in% hmis_bad_dob, 2,1)) %>%
			   mutate_at(vars(lname:suf), funs(toupper)) %>%
			   data.frame()

# Change "" to NA
	hmis.rl[hmis.rl==""] <- NA

# Mutate dob to date
	# hmis.rl <- hmis.rl %>%
	# 		   mutate(dob=ymd(dob))

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
		mutate(pid2 = paste("pid2_",
							str_pad(seq(1, nrow(.)),6,pad='0'),
							sep = ""),
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

	#=======================================================================
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
	#=======================================================================

# ==========================================================================
# Linkage Function Creation
# ==========================================================================

	# rlink <- function(df,
	# 				  block,
	# 				  string,
	# 				  phonetic,
	# 				  phonfun = soundex,
	# 				  ex = ex,
	# 				  threshold = 0.1,
	# 				  name_wt = paste0(block,"_wt")){

	# 	ex <- names(df)[!names(df) %in% string]
	# 	RL <- df %>%
	# 			compare.dedup(.,
	# 			  			  blockfld = block,
	# 			  			  strcmp = string[!string %in% block],
	# 			  			  phonetic = phonetic,
	# 			  			  phonfun = phonfun,
	# 			  			  exclude = ex) %>%
	# 			epiWeights(.) %>%
	# 			epiClassify(.,threshold) %>%
	# 			getPairs(., show = "links", single.rows = TRUE) %>%
	# 			arrange(id1) %>%
	# 			filter(!duplicated(id2)) %>% # reduce duplicated id's
	# 			select(ssn.1:ssn_dq.1,
	# 				   pid0.1,
	# 				   pid0.2,
	# 				   pid1.1,
	# 				   pid1.2,
	# 				   pid2.1,
	# 				   pid2.2,
	# 				   Weight) %>%
	# 			rename_(.dots = setNames("Weight", name_wt))

	# 	return(RL)
	# }

	# gc()

# ==========================================================================
# Record Linkages
# ==========================================================================

# ### Fields
# 	s <- c( "ssn1","dob1_d","dob1_m","dob_y","fname","mname","lname","suf")
# 	p <- c("lname", "fname", "mname")

# ### ssn1 RL
# 	system.time(
# 		rl_ssn1 <- rlink(df = df_sub,
# 			block = "ssn1",
# 			string = s,
# 			phonetic = p
# 			))
# 	gc()

# ### dob1_ym RL
# 	system.time(
# 		rl_dob1_ym <- rlink(df = df_sub,
# 			block = c("dob_y","dob1_m"),
# 			string = s,
# 			phonetic = p,
# 			name_wt = "ym_wt"
# 			))
# 	gc()

# ### fname RL
# 	system.time(
# 		rl_fname <- rlink(df = df_sub,
# 			block = c("fname"),
# 			string = s,
# 			phonetic = p,
# 			threshold = .1,
# 			name_wt = "fname_wt"
# 			))
# 	gc()

# ### lname RL
# 	system.time(
# 		rl_lname <- rlink(df = df_sub,
# 			block = c("lname"),
# 			string = s,
# 			phonetic = p,
# 			threshold = .1,
# 			name_wt = "lname_wt"
# 			))
# 	gc()

# ==========================================================================
# Create link df
# ==========================================================================

### Join links
# 	link <- rl_ssn1 %>%
# 			select(ssn.1:ssn_dq.1,
# 				   pid0.1,
# 				   pid0.2,
# 				   pid1.1,
# 				   pid1.2,
# 				   pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt"))

# 	link <- full_join(link,
# 					  rl_dob1_ym %>%
# 			select(pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt")))

# 	link <- full_join(link,
# 					  rl_fname %>%
# 			select(pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt")))

# 	link <- full_join(link,
# 					  rl_lname %>%
# 			select(pid2.1,
# 				   pid2.2,
# 				   ends_with("_wt")))

# ### Change NA to 0
# 	link <- link %>%
# 			mutate_at(vars(ends_with("wt")),
# 					 	   funs(ifelse(is.na(.)==T,
# 					  	   0.0,
# 					  	   .))
# 					  )
# 	glimpse(link)

# ==========================================================================
# Create product weights
# ==========================================================================

### Weighting function
	# wtp <- function(p,w){
	# 		# 1-prod((1-p)^w)^(1/sum(w))
	# 		1-(prod((1-p)^w))^(1/(sum(w)))
	# 	}

### Different weighting scenerios
	# w1 <- link %>%
	# 	mutate(wtp1111 = apply( # apply function across rows
	# 					select(., ends_with("wt")), # select columns to apply to
	# 					1, # w/in each row (2 would do it w/in each col)
	# 					wtp, # function to apply
	# 					w=c(1,1,1,1)), # w argument
	# 		   wtp1.8.8.8 = apply(
	# 					select(., ends_with("wt")),
	# 					1,
	# 					wtp,
	# 					w=c(1,.8,.8,.8)),
	# 		   wtp1.5.5.5 = apply(
	# 					select(., ends_with("wt")),
	# 					1,
	# 					wtp,
	# 					w=c(1,.5,.5,.5))
	# 					)

### View distributions

	# head(w1) # weights all at 1
	# hist(w1$wtp111, breaks = 40)
	# hist(w1$wtp1.8.8, breaks = 40)
	# hist(w1$wtp1.5.5, breaks = 40)

# ==========================================================================
# Save file
# ==========================================================================

	# write.csv(w1, paste0(path,"/Housing/links/WeightedLinks.csv"))
	write.csv(df_sub, paste0(path,"HILD/PreLinkData_test.csv"))

# ==========================================================================
# End code
# ==========================================================================