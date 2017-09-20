#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts calculates performance of elastic score vs hopper score
# and visualise it
#
# date: Sep 2017
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")
library("jsonlite")

setwd('~/R/address-index')
source('helper_compare_matches.R')
source('helper_plot_match_changes.R')

### helper functions
parse_score_json <- function(df){
  # this function expands json from one column to several new ones
  # but it is ridiculously slow !!!HELP!!!
  Sys.time()
  dada <- lapply (df$bespokeScore, function(x) gsub("'", '"', x) %>%
                    fromJSON( simplifyVector=F) %>%
                    as.data.frame(stringsAsFactors=F))
  Sys.time()
  haha <- do.call( 'rbind', dada)
  Sys.time()
  cbind(df, haha)
}

wrap_dataset_compare_bespoke <- function(input, viz=F, sep_in_set=F){
  # create a cross-table of performance of one dataset at two different scorings
  # the data is read from the network drive (JSON parsed by python!) 
  # :input: data.frame[data_name, prev_date, curr_date]  
  data_name <- input['data_name']
  curr_date <- input['curr_date'] 
  curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_confidence_score_results.csv')
  PREV <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
  CURR <- PREV %>% mutate(score = structural_score*1000 + object_score) %>%
    mutate(UPRN_beta = ifelse(score>0, UPRN_beta, NA ))
  tab <- compare_performance(PREV, CURR, viz=viz, sep_in_set=sep_in_set)
} 

read_pivot <- function(data_name, curr_date){
  curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_pivot_ivy.csv')
  pivot <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
  rownames(pivot) <- pivot$results
  pivot$results <- NULL
  colnames(pivot) <- as.vector(sapply(colnames(pivot), substring, first=2))
  as.matrix(pivot)  
  # some more tweaks needed to the format....
}


########## example calculation for one dataset (using R to parse json)
data_name <-  'EdgeCases' 
curr_date <- 'August_24_dev_hopper_score' #curr_date <- 'September_15_dev_baseline'

curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_beta_DedupExist.csv')
PREV <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)

#calculate the bespoke score from the json
CURR <- parse_score_json(PREV)  #this is ridiculously slow !!!HELP!!!
CURR <- CURR %>% mutate(score = structuralScore*1000 + objectScore) %>%
  mutate(UPRN_beta = ifelse(score>0, UPRN_beta, NA ))

out_list <- compare_performance(PREV, CURR, sep_in_set=T,viz=F)

########### running on all datasets (reading files with JSON parsed in python)
datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC',   'PatientRecords','WelshGov')
curr_date <- curr_date <- 'September_15_dev_baseline'

#create a scoring_summary for all dataset in a list
if (F) {# calculate tables in r 
scoring_summary <- apply(data.frame(data_name=datasets, curr_date=curr_date, stringsAsFactors=F), 
                      1, wrap_dataset_compare_bespoke, viz=F, sep_in_set=T)
names(scoring_summary) <- datasets
save(scoring_summary, file = paste0('cross_tables/compare_scoring_',curr_date,'.RData'))
cross_tables <- lapply(scoring_summary, function(x) x$cross_table)
} else if (F) { # read in python pivot tables
  cross_tables <- lapply(datasets, read_pivot, curr_date = curr_date)
  names(cross_tables) <- datasets
}else{ # read in saved Rdata
  load( file = paste0('cross_tables/compare_scoring_',curr_date,'.RData'))
  cross_tables <- lapply(scoring_summary, function(x) x$cross_table)
}
  
# save heatmap of the cross tables
pdf(file= paste0('pictures/confidence_scoring_',curr_date,'_heat.pdf'), width = 15, height = 10)
g<-percentage_heatmap (cross_tables, datasets=names(cross_tables), curr_date, confidenceScore=T) +
  xlab("Confidence score") +
  ylab("ElasticSearch score") +
  ggtitle(paste0("Matching performance based on different scores - ", curr_date) )
print(g)
dev.off()
