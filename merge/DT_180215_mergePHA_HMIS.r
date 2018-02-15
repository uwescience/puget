# ==========================================================================
# PHA and HMIS merge
# Tim Thomas - t77@uw.edu
# ==========================================================================

	rm(list=ls()) #reset
	options(max.print = 350, tibble.print_max = 50, scipen = 999, width = 100)
	gc()

# ==========================================================================
# Library
# ==========================================================================

	library(colorout)
	library(RecordLinkage)
	library(data.table)
	library(tidyverse)

# ==========================================================================
# Data pull
# ==========================================================================

	hmis <- fread("data/HMIS/puget_preprocessed.csv")
	load("data/Housing/OrganizedData/pha_longitudinal.Rdata")