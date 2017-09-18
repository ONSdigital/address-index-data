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
  # the data is read from the network drive (created by python) 
  # :input: data.frame[data_name, prev_date, curr_date]  
  data_name <- input['data_name']
  curr_date <- input['curr_date'] 
  curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_beta_DedupExist.csv')
  PREV <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
  CURR <- parse_score_json(PREV)
  CURR <- CURR %>% mutate(score = structuralScore*1000 + objectScore) %>%
    mutate(UPRN_beta = ifelse(score>0, UPRN_beta, NA ))
  tab <- compare_performance(PREV, CURR, viz=viz, sep_in_set=sep_in_set)
} 


########## example calculation for one dataset
data_name <-  'EdgeCases' 
curr_date <- 'August_24_dev_hopper_score' # 
curr_date <- 'September_15_dev_baseline'

curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_beta_DedupExist.csv')
PREV <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)

#calculate the bespoke score from the json
CURR <- parse_score_json(PREV)  #this is ridiculously slow !!!HELP!!!
CURR <- CURR %>% mutate(score = structuralScore*1000 + objectScore) %>%
  mutate(UPRN_beta = ifelse(score>0, UPRN_beta, NA ))

out_list <- compare_performance(PREV, CURR, sep_in_set=T,viz=F)

########### running on all datasets
datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC',   'PatientRecords','WelshGov')
curr_date <- curr_date <- 'September_15_dev_baseline'

#create a scoring_summary for all dataset in a list
scoring_summary <- apply(data.frame(data_name=datasets, curr_date=curr_date, stringsAsFactors=F), 
                      1, wrap_dataset_compare_bespoke, viz=F, sep_in_set=F)
names(scoring_summary) <- datasets
save(scoring_summary, file = paste0('cross_tables/compare_scoring_',curr_date,'.RData'))

# save heatmap of the cross tables
pdf(file= paste0('pictures/compare_scoring_',curr_date,'_heat.pdf'), width = 15, height = 10)
g<-percentage_heatmap (cross_tables, datasets=names(cross_tables), curr_date)
print(g)
dev.off()
