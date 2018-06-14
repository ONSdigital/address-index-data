#####################################################################
# ONS Address Index - ROC performance visualization
# 
# This scripts creates and evaluates simple confidence score 
# from e_score_ratio and h_score
#
# date: Mar 2018
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases','CQC',   'PatientRecords','WelshGov')#, 'LifeEvents',  'WelshGov2',  'WelshGov3')
curr_date <- 'February_20_test_check'

load("~/R/address-index/cross_tables/thresholds_confidence_February_20_test_check.RData")
all_my_data <- my_data

####################################################################
## create one number for hopper score
####################################################################
# the challenge is that NAs ('-1') in unitScore stand for different values based on input
# also the value 1 doesn't guarantee a true match -> schould be more like .95
# so i split it in two cases: A. at least one candidate has positive unitScore, B. the opposite

my_data <- all_my_data %>% group_by(dataset, ID_original) %>%
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

######################################################################
# grid_search for parameters
######################################################################

df <- NULL
for (aA in c(.8)){
  for (aB in c(.9)){
    for (m1 in c( .3, .5)){
      for (hp in c(6, 7)){
        for (es in c(15, 18)){
          h_score_formula <- function(structuralScore, unitScore, h_case ='A'){
            unitScoreNA <- ifelse(unitScore==-1, m1, unitScore)
            alpha <- ifelse(h_case=='A', aA, aB)
            res <- structuralScore*(alpha+(.98-alpha)*unitScoreNA)
          } 
          e_sigmoid <- function(x) 1/(1+exp(es*(.99-x)))
          h_power <- function(h) h^hp
          
          my_data <- my_data %>% mutate(
            h_score = h_score_formula(structuralScore, unitScore, h_case2),
            e_sigm = e_sigmoid(e_score_ratio),
            simple_confidence = pmax(e_sigm, h_power(h_score)))
          
          subs <- my_data %>% group_by(dataset, ID_original) %>%
            mutate(ind = which.max(simple_confidence),
                   keep = (ind>1)) %>% 
            filter(keep==T) %>%
            summarise( change= if (matches[ind]=='True') 'good' else 
              if (results=='1_top_unique')  'bad' else 'nevermind') %>%
            ungroup() %>%
            count(change) %>%
            mutate(aA=aA, aB=aB, m1=m1, hp=hp, es=es)
          print(subs)
          df <- bind_rows(df, subs) 
}}}}}

grid_search_df3 <- df
save(grid_search_df3, file= "cross_tables/grid_search3_simple_confidence.RData")
# grid_search1 and grid_search2 covered:
#   aA in c(.7 .75, .8, .85)
#   aB in c (.8, .85, .9, .95)
#   m1 in c(.1, .2, .3)
#   hp in c(3, 4, 5, 6)
#   es in c(10, 12, 15, 20) 

summary_df <- df %>% group_by(aA, aB, m1, hp, es) %>%
  summarise (worse = n[change=='bad'],
    better = n[change=='good'],
    diff= better-worse) %>%
  ungroup() %>%
  arrange(-diff, worse)

head(summary_df )
# optimal parameters copied to the transformation code above

######################################################################
# karen's nice picture 
# about dropped cases when threshold set on given score
######################################################################

## Count number of true matches dropped 

kept_dropped_by_result = NULL
for (one_dataset in unique(all_my_data$dataset)){
  print(Sys.time())
  print(one_dataset)
  for (one_result in unique(all_my_data$results)){
    cat(one_result)
    my_data <- all_my_data %>% filter(results == one_result, dataset == one_dataset)   
    total_cases <- nrow(my_data %>% group_by(dataset, ID_original) %>% summarise(count = 1))
    total_correct_cases <- sum(my_data$matches=="True")
    kept_dropped <- NULL
    for(i in seq(0,1,0.01)){
      my_ratio <- i
      # Total number of cases kept at each e_score_ratio
      cases_kept <- my_data %>%
        filter(simple_confidence >= my_ratio) %>%
        summarise(count = n(),
                  count_matches = sum(matches=='True'))
      correct_cases_remaining <- cases_kept$count_matches[1]
      cases_kept <- cases_kept$count[1]
      correct_cases_dropped <- total_correct_cases - correct_cases_remaining
      # How many possible matches are there for each address (1-5) at each e_score_ratio
      address_choices <- my_data %>%
        group_by(dataset, ID_original) %>%
        summarise(num_choices = sum(simple_confidence >= my_ratio)) %>% 
        group_by(num_choices) %>%
        summarise(value=n())  %>%
        full_join(data.frame(num_choices=0:5), by= 'num_choices') %>%
        mutate( value = ifelse(is.na(value), 0, value),
          my_ratio=my_ratio, results = one_result, dataset = one_dataset,
          cases_kept=cases_kept, correct_cases_dropped = correct_cases_dropped) 
      # Combine these metrics into one dataset
      kept_dropped = bind_rows (kept_dropped, address_choices)
    }
    kept_dropped_by_result = rbind(kept_dropped_by_result, kept_dropped)
  }
}

kept_dropped_simple_confidence <- kept_dropped_by_result
save(kept_dropped_simple_confidence, file= "cross_tables/kept_dropped_simple_confidence.RData")

plot_df <- kept_dropped_simple_confidence %>% filter(results != '3_not_found') %>%
  mutate(results = ifelse (results %in% c('2_in_set_lower', '2_in_set_equal'), '2_in_set', results)) %>%
  group_by(dataset, results, my_ratio, num_choices) %>% summarise(value=sum(value)) %>%
  ungroup() %>% arrange(-num_choices)

plot_df2 <- plot_df %>% 
  group_by(my_ratio, num_choices, dataset) %>% summarise(value=sum(value)) %>%
  mutate(results = 'All') %>%
  bind_rows(plot_df) 

plot_df <- plot_df2 %>% 
  group_by(my_ratio, num_choices, results) %>% summarise(value=sum(value)) %>%
  mutate(dataset = 'All') %>%
  bind_rows(plot_df2) %>%
  mutate(dataset= factor(dataset, levels= c(datasets, 'All')))

## Plot the results
ggplot(plot_df) + 
  #geom_rect(data =  data.frame(dataset= 'All', results= 'All'), aes(xmin = -0.05, xmax = 1.05, ymin = -0.05, ymax = 1.05), fill = NA, col = 'darkred') +
  geom_area(stat = "identity", position = "fill", aes(x=my_ratio, y=value, fill=num_choices, color= num_choices, group=-num_choices))+
  #geom_bar(position = "fill", stat='identity') +
  labs(x="Simple confidence threshold", y="Percentage",
       title="Number of candidates per input using thresholding on simple confidence score") +
  facet_grid(dataset~results) +
  theme_bw() 


pdf(file= paste0('pictures/2018Mar_threshold_simple_confidence.pdf'), width = 16, height = 10)
print(g)
dev.off()



##the other plot
plot_df <- kept_dropped_simple_confidence %>% group_by(my_ratio, dataset, results) %>% 
       summarise(correct_cases_dropped=correct_cases_dropped[1], cases_kept = cases_kept[1]) %>%
       gather('type', 'value', 4:5) %>%
       filter(results != '3_not_found') %>%
       mutate(results = ifelse (results %in% c('2_in_set_lower', '2_in_set_equal'), '2_in_set', results)) %>%
       group_by(dataset, results, my_ratio, type) %>% 
       summarise(value=sum(value))

plot_df2 <- plot_df %>% 
  group_by(my_ratio, type, dataset) %>% summarise(value=sum(value)) %>%
  mutate(results = 'All') %>%
  bind_rows(plot_df) 

plot_df <- plot_df2 %>% 
  group_by(my_ratio, type, results) %>% summarise(value=sum(value)) %>%
  mutate(dataset = 'All') %>%
  bind_rows(plot_df2) %>%
  mutate(dataset= factor(dataset, levels= c(datasets, 'All'))) %>%
  group_by(dataset, results, type) %>%
  mutate(percentage = value/max(value) ) %>%
  ungroup()

## Plot the results
ggplot(plot_df, aes(x= my_ratio, y=percentage, color=type)) + 
  geom_line()+
  labs(x="Simple confidence score threshold", y="Percentage",
       title="Percentage of dropped candidates per input using thresholding on simple confidence score") +
  facet_grid(dataset~results, scale='free') +
  theme_bw() 

pdf(file= paste0('pictures/2018Mar_dropped_simple_confidence.pdf'), width = 16, height = 10)
#print(g)
dev.off()
