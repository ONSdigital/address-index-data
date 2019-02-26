#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts calculates performance and save it in small text file 
# (so that it doesn't have to be recalculated each time)
# requires 'helper_compare_matches.R'
#
# date: May 2017
# author: ivyONS
######################################################################


library("dplyr")

setwd('~/R/address-index')
source('helper_compare_matches.R')
#source('helper_plot_match_changes.R')

datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords','WelshGov')


########### create summary for all response files  newer than given date
date_threshold <- as.Date('January_01', "%b_%d")
overwrite <- F

for (data_name in datasets){
  dir_names <- list.dirs(path = paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/'), full.names = F, recursive = F)
  print(paste0('Processing dataset: ', data_name, '. It has ', length(dir_names), ' directories.' ))  
  for (dd in dir_names){
    #parse date
    ss <- strsplit(dd,split='_', fixed=TRUE)[[1]]
    ddate <- as.Date(paste(ss[1:2], collapse='_')  , "%b_%d")
    dexpl <- if (length(ss)>2) paste(ss[3:length(ss)], collapse='_') else ''   
    #test whether the the destination exists and the date is newer than requested
    target_name <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', dd, '/' , data_name ,'_summary.csv' ) 
    if ((ddate>date_threshold) & (overwrite | !file.exists(target_name))){
      # find the file we need to read in
      file_name <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', dd, '/' , data_name ,'_beta_DedupExist.csv')
      if (!file.exists(file_name)) {
        file_names <- list.files(path = paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', dd, '/'), full.names = F, recursive = F)
        ind <- which(sapply(file_names,function(x) grepl('_beta_Existing.csv', x, fixed=T)))[1]
        file_name <- if (length(ind)>0) paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', dd, '/' ,file_names[ind]) else ''
    }
     if (file.exists(file_name)){
        # calculate summary and save 
          print(paste('Processing', dd))
          PREV <- read.table(file_name, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
          prev_summary <- PREV %>% 
            group_by(ID_original, UPRN_comparison, ADDRESS) %>% 
            summarise(results = eval_match(UPRN_comparison, UPRN_beta, score, sep_in_set=F)) %>% 
            ungroup() %>%
            deduplicate_ID_original(verbose=F) %>%
            group_by(match_type = results) %>%  summarise (count= n()) %>% 
            ungroup() %>% mutate(dataset = data_name, date = ddate, explanatory = dexpl)
          write.csv(prev_summary, file=target_name, row.names=F)
    }}
  }
}

