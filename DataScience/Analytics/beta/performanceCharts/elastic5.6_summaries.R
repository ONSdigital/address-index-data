#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts calculates performance of elastic score when upgraded to v5.6
# and visualise it
#
# date: Feb 2018
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")
library("jsonlite")

setwd('~/R/address-index')
source('helper_compare_matches.R')
source('helper_plot_match_changes.R')
source('read_all performance_summaries.R')

### baseline plots
  datasets <- c('EdgeCases',  'WelshGov3', 'CQC', 'PatientRecords','WelshGov')
  new_dirs <- c('February_20_test_check', 'February_23_test_check2')
  baseline_dirs <-c('January_16_test_e54_baseline',
                    'November_28_branch_county_CO_baseline', 
                    'July_19_branch_bigrams_sep.2_fallbackboost.075_baseline',
                    'May_31_dev_e48_baseline')

  summaries <- do.call('rbind', read_summaries(datasets = datasets, dirs = c(new_dirs,baseline_dirs)  )) 
  
  summaries <- summaries %>% mutate( date = as.Date(date, "%Y-%m-%d"), 
                                     text = paste(ifelse(explanatory == 'dev_e48_baseline', 'May_e48',
                                                  ifelse(explanatory == 'branch_county_CO_baseline', 'Nov_counties', 
                                                  ifelse(explanatory == 'branch_bigrams_sep.2_fallbackboost.075_baseline', 'Jul_bigrams',
                                                  ifelse(explanatory == 'test_e54_baseline', 'Jan_e54',       
                                                                       'Feb_test'))))))
  
  type_code <- function(x)  if (grepl('check',x)) 'experimental' else 'baseline'

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
  
  g<-ggplot(df_spread, aes(x= `4_wrong_match`, y= `1_top_match`, color=type)) +
    geom_path(size=.1)+
    geom_text(aes(label=text, colour= type),hjust=-0.3, size=3)+
    geom_point(aes(color=type), size=2)+
    facet_wrap(~label, scales="free") +  geom_blank(data=dummy_var) + 
    theme_bw() +
    theme(legend.position = c(0.8, 0.1)) 
  
  print(g)

  
  pdf(file= paste0('pictures/Jan_16_es5_lpi_performance.pdf'), width = 14, height = 10)
  print(g)
  dev.off()
  
  
############################    


  flow_sum <- function(tab){
    print(tab)
    tab <- tab[1:(nrow(tab)-1), 1:(ncol(tab)-1)]
    print(c(sum(tab*upper.tri(tab)),
            sum(tab*upper.tri(tab))/sum(tab)*100,
            sum(tab*lower.tri(tab)),
            sum(tab*lower.tri(tab))/sum(tab)*100))
    
  }
##################################
datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC')#,   'PatientRecords')
prev_date <- 'November_28_branch_county_CO_baseline'
curr_date <- 'December_13_dev_es5_classic'#'December_11_dev_es5_boost' 

df <- data.frame(data_name=datasets ,prev_date=prev_date, curr_date=curr_date , stringsAsFactors=F)

cross_tables <- as.list(datasets)
names(cross_tables) <- datasets

for (i in 1:5)  cross_tables[[i]]<- wrap_dataset2cross_table (df[i,], viz=F, sep_in_set=F)

save(cross_tables, file = paste0('cross_tables/',curr_date,'.RData'))

#####################################################

source('helper_compare_matches.R')
source('helper_plot_match_changes.R')

datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC',   'PatientRecords','WelshGov')

prev_date <- 'November_28_branch_county_CO_baseline'
curr_date <-  'January_11_test_lpi_classic'

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
  