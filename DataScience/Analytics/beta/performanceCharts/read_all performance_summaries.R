#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts reads performance (saved in small text file on the network drive) 
# and visualise the performance
#
# date: Jul 2017
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")
setwd('~/R/address-index')


########## function to read in the summary files
# if no 'dirs' provided then it reads all directories, where the summary.csv is found
read_summaries <- function (datasets = c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords','WelshGov'), dirs= NULL){
  summary_list <- list()  
  for (data_name in datasets){
    dir_names <- list.dirs(path = paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/'), full.names = F, recursive = F)
    if (!is.null(dirs)) dir_names <- intersect(dir_names, dirs)
    print(paste0('Reading in summaries for dataset: ', data_name, '. It has ', length(dir_names), ' directories.' ))  
    for (dd in dir_names){
      file_name <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', dd, '/' , data_name ,'_summary.csv')
      if (file.exists(file_name)){
        summary_list[[length(summary_list)+1]] <- read.csv(file_name, header=T, stringsAsFactors=F)
      }
    }
  }
  summary_list
}


######## helper function for ploting
create_dummy <- function(plot_df, ratio = NULL, columns = c('label',  '4_wrong_match', '1_top_match'), scale=F, band = 1.066){
  # make sure tha ratio in each facet is the same (although it might be shifted)
  
  df <- data.frame(label = plot_df[[ columns[1]]], x = plot_df[[columns[2]]], y = plot_df[[ columns[3]]])
  
  df_s <- df %>% group_by(label) %>% summarise (min_x = min(x), max_x= max(x), min_y= min(y), max_y = max(y)) %>%
    mutate(centre_x = (min_x+max_x)/2, halfrange_x = (max_x-min_x)/2,  centre_y = (min_y+max_y)/2, 
           halfrange_y = (max_y-min_y)/2, ratio_yx = halfrange_y/halfrange_x)  
  if (is.null(ratio)) ratio <- mean(df_s$ratio_yx)
  df_s <- df_s %>% group_by(label) %>% mutate(ratio_x = max(1, ratio_yx/ratio), ratio_y = max(1, ratio/ratio_yx) ) %>%
    ungroup() %>%
    mutate(halfrange_x = halfrange_x*ratio_x, halfrange_y = halfrange_y*ratio_y)
  if (scale) df_s <- df_s %>% mutate(halfrange_x= max(halfrange_x), halfrange_y= max(halfrange_y))
  df <- rbind(df_s %>% mutate(x =  centre_x - halfrange_x*band, y =  centre_y - halfrange_y*band),
              df_s %>% mutate(x =  centre_x + halfrange_x*band, y =  centre_y + halfrange_y*band))  %>%
    select(label, x, y)
  res <- plot_df[1:nrow(df),]
  res[[columns[1]]] <- df$label
  res[[columns[2]]] <- df$x
  res[[columns[3]]] <- df$y
  res
}


### example of baseline plots, do not run
if (F) {
  datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC', 'PatientRecords','WelshGov')
  sami_baseline <- c('March_28_baseline', 'March_24_baseline', 'March_22_baseline', 'March_16_baseline', 'March_15_baseline')
  baseline_dirs <-c('May_24_branch_locality_40_baseline', 
                    'May_31_dev_e48_baseline', 
                    'July_19_branch_bigrams_sep.2_fallbackboost.075_baseline',
                    'September_07_branch_skipEnd_buldingNum_param_townlocality',
                    'September_15_dev-baseline')
  
  summaries <- do.call('rbind', read_summaries(datasets = datasets, dirs = c(sami_baseline,baseline_dirs)  )) 

  summaries <- summaries %>% mutate( date = as.Date(date, "%Y-%m-%d"), 
                                     text = paste(ifelse(explanatory == 'dev_e48_baseline', 'e48',
                                                   ifelse(explanatory == 'branch_locality_40_baseline', 'e39',format(date, '%h%y')))))
  
  type_code <- function(x)  if (grepl('townlocality',x)) 'latest' else
    if (grepl('baseline',x)) 'baseline' else  'experimental'
  
  summaries$type <- sapply(summaries$explanatory, type_code)
  
  # add calculation of performance in percentage (within group with/without UPRN)
  all_perf <- summaries %>%
    group_by(as.numeric(substr(match_type, 1,1))<5, dataset, explanatory, date) %>%
    mutate(perc = count/sum(count)*100, size = sum(count)) %>% ungroup() %>%
    select(dataset, type, text, match_type,  size, count, explanatory, date) %>%
    spread(match_type, count)
  
  # transform for plotting and drop datasets wuthout ground truth UPRNs
  df_spread <- all_perf %>%  filter (!is.na(`2_in_set`)) %>% 
    arrange(type, date, explanatory) %>%
    mutate(label= paste0(dataset, ' (', size, ')'))
  
  # add dummy values to garantee that x & y axes have same units (separately for each dataset)
  dummy_var <- create_dummy(df_spread, ratio =1, scale=F)
  
  g<-ggplot(df_spread, aes(x= `4_wrong_match`, y= `1_top_match`)) +
    geom_path()+
    geom_text(aes(label=text, colour= type),hjust=-0.3, size=3)+
    geom_point(aes(color=type))+
    facet_wrap(~label, scales="free") +  geom_blank(data=dummy_var) + 
    theme_bw() +
    theme(legend.position = c(0.8, 0.1)) 
  
  print(g)
  
  factor_levels <- unique(df_spread$text)   
  df_gather <- df_spread %>% gather('match_category', 'count', 7:10) %>%
    mutate(count = ifelse(is.na(count),0, count ),
           x_date = as.numeric(factor(text, levels=factor_levels)))
      
  g<-ggplot(df_gather, aes(x= x_date, y= count, color = match_category)) +
    geom_path()+
    geom_point()+
    facet_wrap(match_category~label, scales="free_y")  + 
    theme_bw() + guides(color='none') +
    scale_x_continuous(labels= factor_levels) + 
    scale_color_manual(values = c('#4daf4a', '#377eb8', '#984ea3', '#e41a1c'))
  
  print(g)
}