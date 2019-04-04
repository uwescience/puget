#! /usr/bin/Rscript --vanilla --default-package=utils

# Function to load libraries without halting subprocess call in python
# Need to specify specific repository non cran libraries 
# Needs a list with c("package-name", "cran/dev/other", "if dev/other repo name")

install_required_packages <- function() {
    for(package in required_packages) {
        is_installed <- require(package[[1]], character.only=T, quietly=T)
        if(!is_installed & package[[2]] == "dev") {
            devtools::install_github(package[[3]])
            require(package[[1]], character.only=T, quietly=T)
            is_installed <- require(package[[1]], character.only=T, quietly=T)
            if(!is_installed) {
                stop(paste("ERROR: can not install,", package[[1]], "install manually"))
            }
        }
        if(!is_installed & package[[2]] == "cran") {
            install.packages(package[[1]], repos = "http://cran.us.r-project.org")
            require(package[[1]], character.only=T, quietly=T)
            is_installed <- require(package[[1]], character.only=T, quietly=T)
            if(!is_installed) {
                stop(paste("ERROR: can not install,", package[[1]], "install manually"))
            }
        }
        if(!is_installed & package[[2]] == "other") {
            install.packages(package[[3]], repos = "http://cran.us.r-project.org")
            BiocManager::install(package[[1]])
            require(package[[1]], character.only=T, quietly=T)
            is_installed <- require(package[[1]], character.only=T, quietly=T)
            if(!is_installed) {
                stop(paste("ERROR: can not install,", package[[1]], "install manually"))
            }
        } else {
            require(package[[1]], character.only=T, quietly=T)
                }
    }   
}

# hild list of r packages being used to process data

required_packages <- list(
   c("devtools", "cran"),
   c("tidyverse","dev", "tidyverse/tidyverse"),
   c("lubridate", "dev", "tidyverse/lubridate"),
   c("colorout", "dev", "jalvesaq/colorout"),
   c("BiocManager", "cran"),
   c("IRanges", "other", "BiocManager"),
   c("data.table", "cran"),
   c("magrittr", "cran"),
   c("igraph", "cran")
)

install_required_packages()

