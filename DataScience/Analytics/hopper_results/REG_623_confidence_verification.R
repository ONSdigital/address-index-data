#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts calculates matching performance of new simple confidence score
# versus plain elastic score and visualise it
#
# date: May 2018
# author: ivyONS
######################################################################

library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases')#,'CQC',   'PatientRecords','WelshGov')#, 'LifeEvents',  'WelshGov2',  'WelshGov3')
curr_date <-  'May_02_test_simpleconfidence'#'February_20_test_check'

dataset='EdgeCases'
#helper function
load_data <- function(dataset, curr_date, top = 'confidence_score'){
  filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_features_all5.csv")# "_debug_codes.csv")
  my_data <- read.csv(filename, stringsAsFactors=F)
 # my_data <- my_data %>% group_by(ADDRESS) %>% 
  #  mutate(top_scoring = (e_score == max(e_score)) ,
   #        dataset = dataset) %>%            
    #filter(top_scoring) %>% 
    ungroup ()     
  print(paste(Sys.time(),' Loaded data:', dataset) )
  my_data$dataset <- dataset
  my_data
}

data_list <- lapply(datasets, load_data, curr_date = curr_date, top= 'e_score')
my_data <- do.call('bind.rows', data_list)


####################################################################
## create one number for hopper score
####################################################################
# the challenge is that NAs ('-1') in unitScore stand for different values based on input
# also the value 1 doesn't guarantee a true match -> schould be more like .95
# so i split it in two cases: A. at least one candidate has positive unitScore, B. the opposite

my_data <- my_data %>% group_by(dataset, ID_original) %>%
  mutate(h_case1 = ifelse (max(unitScore, na.rm=T)>0, 'A', 'B'),
         h_case2 = ifelse (SubBuildingName + SaoStartNumber + OrganisationName > 0, 'A', 'B')) %>%
  ungroup()

# print counts of each case
h_summary <- my_data %>% group_by(dataset, ID_original) %>% 
  summarise(h_case1 = h_case1[1], 
            h_case2 = h_case2[1]) %>%
  ungroup() 

h_summary %>% group_by(dataset) %>% count(h_case1, h_case2)

# suggested formula to create one hopper score number
h_score_formula <- function(structuralScore, unitScore, h_case ='A'){
  unitScoreNA <- ifelse(unitScore==-1, .2, unitScore)
  alpha <- ifelse(h_case=='A', .8, .9)
  res <- structuralScore*(alpha+(.99-alpha)*unitScoreNA)
  res
} 

####################################################################
# use the max of transfordmed h_score & e_ratio to create simple confidence score
####################################################################
e_sigmoid <- function(x) 1/(1+exp(15*(.99-x)))
h_power <- function(h) h^6

my_data <- my_data %>% mutate(
  h_score = h_score_formula(structuralScore, unitScore, h_case2),
  e_sigm = e_sigmoid(e_score_ratio),
  simple_confidence = pmax(e_sigm, h_power(h_score)))

all_my_data <- my_data 

some_join <- my_data %>% select('ID_original', 'ADDRESS', UPRN_beta, results, e_score, h_score, simple_confidence) %>%
  full_join(all_my_data %>% select('ID_original', 'ADDRESS', UPRN_beta, score, results), by = c('ID_original', 'ADDRESS', 'UPRN_beta'))

ggplot(some_join, aes(x=score, y= simple_confidence, color =results.y))+
       geom_point()

haha <-some_join %>% filter(abs(score - simple_confidence)>.1)



# check the transformations in the score vs accu plots
# import::here(score_group, .from = "confidence_score_ROC.R")
label_score <-  c( 'e_score_ratio', 'h_score', 'simple_confidence')
bin_size <- 150

df2 <- list()
for (data_name in datasets){
  data <- my_data %>% filter(dataset == data_name) 
  bins <- ceiling(min(sqrt(nrow(data)),nrow(data)/bin_size))
  df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data, bins=bins)[[1]]))
  df <- df %>% group_by (var) %>% 
    mutate(quant = quant * 100 / length(unique(quant)),
           dataset = data_name)
  df2[[data_name]] <- df %>% ungroup()
}

df <- do.call('rbind', df2)
df$var <- factor(df$var, levels = label_score)


# add smooth trend - parameters will be optimized below...
df <- df %>% mutate(smooth = ifelse(var=='e_score_ratio', 1/(1+exp(15*(.99-avg_score))), 
                                    ifelse(var=='h_score', (avg_score)^6 , NA)) )

g <- ggplot(df, aes(x=avg_score, y=avg_accu, col = quant)) +
  geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
  geom_line(aes(x=avg_score, y=smooth), col = 'red') +
  facet_grid(dataset~var, scales = "free")+ theme_bw() + 
  ggtitle('Accuracy vs *score per quantile bin for top 5 candidates from each dataset')
print(g)

pdf(file= paste0('pictures/2018Mar_accu_vs_score.pdf'), width = 16, height = 10)
print(g)
dev.off()

# ROC
# import::here(get_points_ROC, give_me_ROC, .from = "confidence_score_ROC.R")
haha0 <- give_me_ROC(my_data, label_score)
score_list0 <- do.call('rbind',haha0)

g<- ggplot(data=score_list0, aes(x=FPR, y=TPR, color = score_type)) +
  geom_line() +  
  facet_wrap(~dataset) + 
  ggtitle('ROC for 5 candidates from each dataset') +
  theme_bw() #+  xlim(0,.1) +ylim(0.9,1)
g

######################################################################
# analysis of the results when using simple_score
######################################################################
# how many cases get screwed up because hopper score is larger for second candidate?

subs <- my_data %>% group_by(dataset, ID_original) %>%
  mutate(ind = which.max(simple_confidence),
         keep = (ind>1), 
         change= if (matches[ind]=='True') 'good' else 
           if (results=='1_top_unique')  'bad' else 'nevermind') %>%
  subset(keep==T)

subs %>% group_by(dataset, ID_original) %>% 
  summarize(change = change[1]) %>% count(change)

subs %>% group_by(dataset, ID_original) %>% 
  summarize(change = change[1]) %>% 
  ungroup() %>%
  count(change)

############################################################################
#
#########################################################################
setwd('~/R/address-index')
source('helper_compare_matches.R')
source('helper_plot_match_changes.R')
source('read_all performance_summaries.R')
## baseline plots
datasets <- c('EdgeCases',  'WelshGov3', 'CQC', 'PatientRecords','WelshGov')
new_dirs <- c('May_03_branch_simpleconfidence2',
              'May_15_test_threshold0',
              'June_04_test_unitdebug',
              'June_05_test_unitdebug')
baseline_dirs <-c('February_23_test_check2',
                  #'January_16_test_e54_baseline',
                  'November_28_branch_county_CO_baseline', 
                  'July_19_branch_bigrams_sep.2_fallbackboost.075_baseline',
                  'May_31_dev_e48_baseline')

summaries <- do.call('rbind', read_summaries(datasets = datasets, dirs = c(new_dirs,baseline_dirs)  )) 

summaries <- summaries %>% mutate( date = as.Date(date, "%Y-%m-%d"), 
                                   text = paste(ifelse(explanatory == 'dev_e48_baseline', 'May_e48',
                                                ifelse(explanatory == 'branch_county_CO_baseline', 'Nov_counties', 
                                                ifelse(explanatory == 'branch_bigrams_sep.2_fallbackboost.075_baseline', 'Jul_bigrams',
                                                ifelse(explanatory == 'test_e54_baseline', 'Jan_e54',       
                                                ifelse(explanatory == 'test_check2', 'Feb_test',
                                                ifelse(explanatory == 'branch_simpleconfidence2',   'May_confidence',
                                                ifelse(explanatory == 'test_threshold0',   'May_confidence0',
                                                       'Jun_confidence' )))))))))

type_code <- function(x)  if (x > as.Date('2018-05-01', "%Y-%m-%d")) 'simple_confidence' else 'baseline'

summaries$type <- sapply(summaries$date, type_code)

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


pdf(file= paste0('pictures/June_confidence_performance.pdf'), width = 14, height = 10)
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


#####################################################

source('helper_compare_matches.R')
source('helper_plot_match_changes.R')

datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC',   'PatientRecords','WelshGov')

prev_date <- 'February_23_test_check2'
prev_date <-  'May_03_branch_simpleconfidence2'
curr_date <- 'June_04_test_unitdebug'

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


