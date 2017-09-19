##############################################
# ONS Address Index - Compare Baseline Performance
#
# 
# A simple script to compare matching performance on one dataset (using the Beta matching service)
#
# 
# date: May 2017
# author: ivyONS
######################################################################

library("dplyr")
library("ggplot2")
library("googleVis")

##################################
#helper funtions
LABELS <- c( '1_top_match', '2_in_set', '3_not_found', '4_wrong_match', "5_new_uprn", "6_both_missing" )
LABELS_sep <- c( '1_top_match',  '2_in_set_equal', '2_in_set_lower', '3_not_found', '4_wrong_match', "5_new_uprn", "6_both_missing" )

eval_match <- function(comp, beta, score = length(beta):1, sep_in_set=F){
#   This function evaluates the match.
#   :param comp: true UPRN of the object (vector of length 1-5)
#   :type comp: vector[character]
#   :param beta: new UPRN as returned by the beta matching service
#   :type beta: vector[character]
#   :param score: optional, the matching score relating to the beta UPRNs 
#   :type score: vector[numeric]
# 
#   :return: string describing the quality of the match (agreement bnetween comp and beta)
#   :rtype: character
#   
  ind <- order(-score)
  comp <- comp[ind]
  beta <- beta[ind]
  score <- score[ind]
  topscore <- (length(score)==1) | (score[1]>score[2]) 
  
  if (is.na(comp[1])) 
    if (sum(!is.na(beta))==0) res <- LABELS[6] # both missing uprns
    else res <- LABELS[5] # new uprn found
  else if (sum(!is.na(beta))==0) res <- LABELS[3] # nothing found 
    else if ((beta[1]==comp[1]) & (topscore)) res <- LABELS[1] # top match
    else if (comp[1] %in% beta) res <- LABELS[2] # in the set
    else res <- LABELS[4] # wrong match
  
  if (sep_in_set & (res==LABELS[2])) {
    if ((score[comp==beta])[1]==max(score)) res <- LABELS_sep[2] # in set equal to the maximum 
      else res <- LABELS_sep[3] # in set lower score than max 
    }

  res
}

deduplicate_ID_original <- function(df, verbose=T){
  #   This function removes duplicates from the dataset based on the input address. 
  #   It prints a message when two records have the same address but different uprn. 
  #   :param df: input dataframe 
  #   :type df: data.frame[column.names = c('ID_original','UPRN_comparison', 'ADDRESS')]
  # 
  #   :return: output dataframe without duplicated rows 
  #   :rtype: data.frame
  #   
  unique_ID_per_address <- df %>% group_by(ADDRESS) %>% 
      summarise(ID_min = ID_original[order(score, UPRN_beta, results, UPRN_comparison)[1]],  
                #if there are diff UPRN's keep the non missing (lower) but also keep better match!
                ID_count= n_distinct(ID_original), 
                UPRN_count = n_distinct(UPRN_comparison)) %>% 
      ungroup()
  bad_addresses <- df %>% filter(ADDRESS %in% unique_ID_per_address$ADDRESS[unique_ID_per_address$UPRN_count>1]) %>%
    group_by(ID_original,UPRN_comparison, ADDRESS) %>% summarise(matchedFormattedAddress=matchedFormattedAddress[1]) %>%
     ungroup() %>% arrange(ADDRESS, ID_original)
  if (nrow(bad_addresses)>0 & verbose) {
    print('Deduplication conflicts: ')
    print(bad_addresses)
    }
  res <- df %>% filter(ID_original %in% unique_ID_per_address$ID_min)
}

compare_performance <- function(prev=PREV,curr=CURR, viz=F, sep_in_set=F, onlytable=F){
#   
#   This function compare two different matching results on the same dataset. It prints a cross-tabulation of the achieved classification and plots a sankey diagram.
#   It returns a list containing the cross-table, joined data, got_worse, got_better.  
# 
#   :param prev: previous matching result as returned by the beta matching service
#   :type prev: data.frame
#   :param curr: current (new) matching result as returned by the beta matching service
#   :type curr: data.frame
#   :param viz: true when the sankey visualisation should be produced
#   :type viz: boolean
# 
#   :return: list(cross-table, joined_data, got_worse, got_better) 
#           cross_table: cross-tabulation of the achieved matching classification
#           joined_data: joined summary of the original input files and the relevant classifications
#           got_worse: detailed list of cases for which the matching got worse
#           got_better: detailed list of cases for which the matching improved
#   :rtype: list(matrix, data.frame, data.frame, data.frame)
#  
  
  prev <- deduplicate_ID_original(prev) 
  curr <- deduplicate_ID_original(curr) 
  prev_summary <- prev %>% group_by(ID_original, UPRN_comparison, ADDRESS) %>% 
                      summarise(match_prev = eval_match(UPRN_comparison, UPRN_beta, score, sep_in_set=sep_in_set)) %>% ungroup()
  curr_summary <- curr %>% group_by(ID_original, UPRN_comparison, ADDRESS) %>% 
                      summarise(match_curr = eval_match(UPRN_comparison, UPRN_beta, score, sep_in_set=sep_in_set)) %>% ungroup()
  joined_data <- left_join (prev_summary, curr_summary, by= c( "ADDRESS")) %>% ungroup()
  cross_table <- table (joined_data$match_prev, joined_data$match_curr, dnn=c('previous' ,'current')) 
  
  if (onlytable){
  got_worse <- NA
  got_better <- NA
  }else{
  joined_full <- full_join(prev %>% group_by(ID_original) %>% mutate (index = paste0(ID_original,  index - min(index))) %>% ungroup(),                                   
                           curr %>% group_by(ID_original) %>% mutate (index = paste0(ID_original,  index - min(index))) %>% ungroup(),
                           by = c('index', 'ID_original', 'UPRN_comparison', 'ADDRESS', 'UPRN_prototype', 'id_response', 'inputAddress'))
  joined_full <- left_join(left_join(joined_full, prev_summary, by=c("ID_original", "UPRN_comparison", "ADDRESS")), 
                           curr_summary, by=c("ID_original", "UPRN_comparison", "ADDRESS"))
  got_worse <- joined_full %>% filter(as.numeric(substr(match_prev,1,1))<as.numeric(substr(match_curr,1,1))) %>%
                                      ungroup() %>% arrange(desc(match_curr), match_prev, index)   #reordering by severity of the problem
  got_better <- joined_full %>% filter(as.numeric(substr(match_prev,1,1))>as.numeric(substr(match_curr,1,1))) %>%
                                      ungroup() %>% arrange(desc(match_prev), match_curr, index)
  }
  
  if (viz){
    
    sankey_dat <- data.frame(cross_table) %>% 
      mutate(From=paste0("prev_",previous), To=paste0("curr_",current), Weight = Freq) %>% 
      select(From, To, Weight)
    sk1 <- gvisSankey(sankey_dat, from="From", to="To", weight="Weight",
                    options=list(sankey="{ iterations: 0 }")) 
    plot(sk1)
  }

  cross_table <- cross_table %>% addmargins()
  print(cross_table)
  
  list(cross_table=cross_table, joined_data=joined_data, got_worse =got_worse, got_better=got_better )
}

#################################
# run example:

if(F){
# set working directory and path names to the csv files as created by Sami's script 'run_baseline.py' 
setwd('~/R/address-index')

data_name <-  'EdgeCases' 
prev_date <-  'July_19_branch_bigrams_sep.2_fallbackboost.075_baseline'
curr_date <- 'September_07_branch_skipEnd_buldingNum_param_townlocality'

# load data
prev_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', prev_date, '/' , data_name ,'_beta_DedupExist.csv')
curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_beta_DedupExist.csv')


# run comparison and save bad cases
PREV <- read.table(prev_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
CURR <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
out_list <- compare_performance(PREV, CURR, viz=F)
write.table(out_list$got_worse, file=paste0('got_worse/',data_name,'_', curr_date ,'.csv'), sep=',', row.names=F)
rm(PREV, CURR)
}

###################################################################################
# > sessionInfo()
# 
# R version 3.3.0 (2016-05-03)
# Platform: x86_64-w64-mingw32/x64 (64-bit)
# Running under: Windows 7 x64 (build 7601) Service Pack 1
# 
# locale:
#   [1] LC_COLLATE=English_United Kingdom.1252  LC_CTYPE=English_United Kingdom.1252    LC_MONETARY=English_United Kingdom.1252 LC_NUMERIC=C                           
#   [5] LC_TIME=English_United Kingdom.1252    
# 
# attached base packages:
#   [1] stats     graphics  grDevices utils     datasets  methods   base     
# 
# other attached packages:
#   [1] googleVis_0.5.10 ggplot2_2.1.0    dplyr_0.4.3     
# 
# loaded via a namespace (and not attached):
#   [1] Rcpp_0.12.5      assertthat_0.1   grid_3.3.0       R6_2.1.2         plyr_1.8.3       gtable_0.2.0     
#   [7] DBI_0.4-1        magrittr_1.5     scales_0.4.0     stringi_1.0-1    lazyeval_0.1.10  RJSONIO_1.3-0    
#  [12] tools_3.3.0      munsell_0.4.3    parallel_3.3.0   colorspace_1.2-6
