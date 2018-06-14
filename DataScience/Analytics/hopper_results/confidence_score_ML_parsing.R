#####################################################################
# ONS Address Index - ML for confidence score 
# 
# This scripts analyses possible confidence score from python ML
#
# date: June 2018
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases','CQC',   'PatientRecords','WelshGov')#, 'LifeEvents',  'WelshGov2',  'WelshGov3')
scores <- c('e_sigmoid', 'rfc_predicted', 'logreg_predicted', 'simple_confidence', 'e_score', 'h_power')
curr_date <- 'May_15_test_threshold0'

#####################################################################
# load data from each dataset and concatenate together 
#####################################################################
load_data <- function(dataset, curr_date){
  print(dataset)
  filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_slim_predicted.csv")
  my_data <- read.csv(filename, stringsAsFactors=F)
  my_data <- my_data %>% 
    mutate(dataset = dataset,
           ID_original = as.character(ID_original))
  my_data
}
data_list <- lapply(datasets, load_data, curr_date = curr_date)

my_data <- do.call('bind_rows', data_list) %>% 
  mutate(simple_confidence = (99*pmax(e_sigmoid, h_power)+e_sigmoid)/100)

# how does distribution of each score_type look? what if we drop the lowest 10%?
for (score_name in scores) {
  hist(my_data[[score_name]])
  print(c(score_name, quantile(my_data[[score_name]], 0.1)) )
}

# use our matching performance metric to evaluate each score type
source('helper_compare_matches.R')
my_sum <- list()
for (score_name in scores){
  df <- my_data
  df$score = my_data[[score_name]]
  df<- df %>% filter(score > quantile(my_data[[score_name]], 0.1)) 
  for (data_name in datasets){
    my_sum[[paste(score_name, data_name)]] <- df %>%
       filter(dataset == data_name)  %>%
       group_by(ID_original, UPRN_comparison, ADDRESS) %>%
       summarise(results = eval_match(UPRN_comparison, UPRN_beta, score, sep_in_set=F)) %>%
       ungroup()  %>%
       deduplicate_ID_original(verbose=F) %>%
       group_by( match_type = results) %>%
       summarise (count= n(), score_name = score_name, dataset = data_name) %>%
       ungroup()
    print(my_sum[[paste(score_name, data_name)]])
}}

# it took quit long, so save it in case some more figures are needed
perform_df <- do.call('bind_rows', my_sum)
save(perform_df, file = 'cross_tables/ML_scores_performance.RData')

# a bit of data wrangling so that it plots pretty
perform_df <- perform_df %>% group_by(score_name, dataset) %>%
  mutate(percent = count/sum(count)) %>%
  group_by(match_type, dataset) %>%
  mutate(diff_from_elastic = count - max(0,count[score_name=='e_sigmoid']))%>%
  ungroup()%>%
  mutate(score_type = as.numeric(factor(score_name, levels = scores)),
         data_name = as.numeric(factor(dataset, levels = datasets)))
 
# plot the number of top matches - it vaties accross datasets (because of their size)
# thus look at the difference from default elastic instead
g<-ggplot(perform_df %>% filter(match_type=='1_top_match', score_name %in% scores[1:4]), aes(x=data_name, y=diff_from_elastic, color=score_name))+
  geom_point(size=3) +
  geom_line() +
  scale_x_discrete(limits=1:4, labels = datasets[1:4])+
  ylab( 'difference in number of top matches from default elastic score') +
  ggtitle('Comparison of performance of proposed confidence scores')+
  theme_bw()
print(g)

png(file= paste0('pictures/',curr_date,'_ML_scores2.png'), width = 600, height = 400)
print(g)
dev.off()

# plot the number of top matches vs wrong matches
source('read_all performance_summaries.R')
# transform for plotting and drop datasets wuthout ground truth UPRNs
df_spread <- perform_df %>%
  filter(score_name %in% scores[1:4]) %>%
  select(match_type, count, dataset, score_name) %>%
  spread(match_type, count) %>%
  mutate(label = dataset)

# add dummy values to garantee that x & y axes have same units (separately for each dataset)
dummy_var <- create_dummy(df_spread, ratio =1, scale=F)

g<-ggplot(df_spread, aes(x= `4_wrong_match`, y= `1_top_match`, color=score_name)) +
  geom_text(aes(label=score_name, colour= score_name),hjust=-0.3, size=3)+
  geom_point(size=2)+
  facet_wrap(~label, scales="free") +  geom_blank(data=dummy_var) + 
  theme_bw() 
  #theme(legend.position = c(0.8, 0.1)) 
print(g)

# from both figures we see that the simple confidence is best for most datasets (except CQC which has a lot of errors in reference)