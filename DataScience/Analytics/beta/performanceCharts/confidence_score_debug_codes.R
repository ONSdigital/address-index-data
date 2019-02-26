#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts analyses confidence score & debug categories
# and visualise it
#
# date: Sep 2017
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases',  'LifeEvents',  'WelshGov2',  'WelshGov3', 'CQC',   'PatientRecords','WelshGov')

#helper function
load_data <- function(dataset, curr_date, top = 'confidence_score'){
    filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_confidence_score_results.csv")
    my_data <- read.csv(filename, stringsAsFactors=F)
    my_data <- my_data %>% group_by(ADDRESS) %>% 
      mutate(top_scoring = c(T, F, F, F, F, F, F)[rank(- if (top=='confidence_score') confidence_score else  score , ties.method =  "first")],
             dataset = dataset) %>%            
      filter(top_scoring) %>% ungroup ()     
    print(paste(Sys.time(),' Loaded data:', dataset) )
    my_data
   }

## Read in data
if(F){ # run only first time, then read from saved file
curr_date <- 'September_15_dev_baseline'
data_list <- lapply(datasets, load_data, curr_date = curr_date, top= 'elastic_score')
my_data <- do.call('rbind', data_list)
save(my_data, file = paste0('cross_tables/debug_confidence_',curr_date,'.RData'))
}else load("H:/My Documents/R/address-index/cross_tables/debug_confidence_September_15_dev_baseline.RData")

## plot structural score vs accuracy (by debug codes)

my_data <- my_data %>% mutate(match = ifelse(is.na(UPRN_comparison), NA, as.numeric(as.factor(matches))-1)) %>%
  group_by(dataset) %>%  
  mutate(size_ds = n())  


## plot one picture for all datasets
plot_all <- my_data %>%
  group_by(building_score_debug, locality_score_debug) %>% 
  summarize(group_accuracy = mean(match, na.rm=T), 
         group_size = n()/230578,
         build_n_all = n()/230578,
         multi = n_distinct(structural_score),
         structural_max = max(structural_score),
         difference = as.character(ifelse(is.na(group_accuracy),0,  round(0.999*(group_accuracy - structural_max)))),
         label = ifelse(difference=='0', '', paste0(locality_score_debug,':',building_score_debug)),
         dataset = 'ALL')  %>%  
  ungroup() 

ggplot(plot_all %>% filter(build_n_all>=10/230578)  , aes(x=structural_max, y=group_accuracy, size=group_size, color= difference))+
  geom_point() +
  geom_text(aes(label=label, hjust=-0.5), size=3) + 
  theme_bw()  #+ guides(color=FALSE) +


## panel per dataset
plot_ds <- my_data %>%
  group_by(building_score_debug, locality_score_debug) %>% 
  mutate( group_accuracy = mean(match, na.rm=T), 
          build_n_all = n()/230578 ,
          structural_max = max(structural_score),
          difference = as.character(ifelse(is.na(group_accuracy),0,  round(0.999*(group_accuracy - structural_max)))),
          label = ifelse(difference=='0', '', paste0(locality_score_debug,':',building_score_debug))) %>%
  group_by(dataset, building_score_debug, locality_score_debug, 
           build_n_all, difference, label) %>% 
  summarize(group_accuracy = mean(match, na.rm=T), 
            group_size = n()/size_ds[1],
            multi = n_distinct(structural_score),
            structural_max = max(structural_score)) %>%
  ungroup() %>%  
  filter(dataset %in% datasets[c(1, 4:7)] ) 


g<- ggplot(rbind(plot_all, plot_ds) %>%
         filter(group_size>1/5000)  , aes(x=structural_max, y=group_accuracy, size=group_size, color= difference))+
  geom_point() +
  geom_text(aes(label=label, hjust=-0.5), size=3) + 
  theme_bw() +
  facet_wrap(~dataset)  
print(g)



pdf(file= paste0('pictures/confidence_debug_',curr_date,'.pdf'), width = 15, height = 10)
print(g)
dev.off()


## extract(estimate) the value of ambiguity penalty, that has been applied 
ambig <- my_data %>%
  group_by(building_score_debug, locality_score_debug) %>% 
  summarize( structural_max = sum(max(structural_score) == structural_score),
             n= n(),
             prop=structural_max/n) %>%
  ungroup()



## Plot structural_score by hopper_results
qplot(seq_along(my_data$structural_score), my_data$structural_score, colour=my_data$hopper_results) +
  labs(x="", y="Structural score", title="Structural score by result of match (Edge cases)") + 
  guides(colour=guide_legend(title="Hopper score results"))

## Produce confidence intervals in table
building_debug <- group.CI(structural_score~building_score_debug, my_data, ci = 0.95)
building_debug$cases <- table(my_data$building_score_debug)

