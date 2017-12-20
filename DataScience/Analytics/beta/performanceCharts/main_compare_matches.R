##############################################
# ONS Address Index - Compare Baseline Performance
#
# 
# Main script to produce the matching performance figures
# requires helper functions stored in 'compare_matches.R' and 'plot_match_changes.R'
#
#
# date: May 2017
# author: ivyONS
######################################################################


library("dplyr")
#library("stringr")
library("ggplot2")
#library("googleVis")

setwd('~/R/address-index')
source('helper_compare_matches.R')
source('helper_plot_match_changes.R')

datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC',   'PatientRecords','WelshGov')

prev_date <- 'July_19_branch_bigrams_sep.2_fallbackboost.075_baseline'
curr_date <- 'September_07_branch_skipEnd_buldingNum_param_townlocality'

#create a cross-tables for all dataset in a list
cross_tables <- apply(data.frame(data_name=datasets, prev_date=prev_date, curr_date=curr_date, stringsAsFactors=F), 
                      1, wrap_dataset2cross_table, viz=F, sep_in_set=F)
names(cross_tables) <- datasets
save(cross_tables, file = paste0('cross_tables/',curr_date,'.RData'))

# save barchart of the performance change
pdf(file= paste0('pictures/',curr_date,'_bar.pdf'), width = 8, height = 10)
g<-percentage_barchart (cross_tables, datasets=names(cross_tables), curr_date)
print(g)
dev.off()

# save heatmap of the cross tables
pdf(file= paste0('pictures/',curr_date,'_heat.pdf'), width = 15, height = 10)
g<-percentage_heatmap (cross_tables, datasets=names(cross_tables), curr_date)
print(g)
dev.off()

# save the performance barchart of the latest beta results
pdf(file= paste0('pictures/',curr_date,'_perf.pdf'), width = 8, height = 10)
g<-performance_barchart (cross_tables, datasets=names(cross_tables), curr_date)
print(g)
dev.off()