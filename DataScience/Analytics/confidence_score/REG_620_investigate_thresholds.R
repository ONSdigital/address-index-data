#####################################################################
# ONS Address Index - thresholds for confidence score
# 
# This script analyses different thresholds for the confidence score
#
# date: Mar 2018
# author: Karen, using lots of Iva's code
######################################################################


library("plyr")
library("dplyr")
library("tidyr")
library("ggplot2")
library("reshape2")

setwd('~/R/address-index')
datasets <- c('EdgeCases', 'CQC', 'PatientRecords', 'WelshGov')

#helper function
load_data <- function(dataset, curr_date, top = 'confidence_score'){
  filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_features_all5.csv")
  my_data <- read.csv(filename, stringsAsFactors=F)
  my_data <- my_data %>% group_by(ADDRESS) %>% 
    mutate(top_scoring = (e_score == max(e_score)) ,
           dataset = dataset) %>%            
    #filter(top_scoring) %>% 
    ungroup ()     
  print(paste(Sys.time(),' Loaded data:', dataset) )
  my_data
}

## Read in data
if(T){ # run only first time, then read from saved file
  curr_date <- 'February_20_test_check'
  data_list <- lapply(datasets, load_data, curr_date = curr_date, top= 'e_score')
  my_data <- do.call('rbind.fill', data_list)
  save(my_data, file = paste0('cross_tables/thresholds_confidence_',curr_date,'.RData'))
}else load("H:/My Documents/R/address-index/cross_tables/thresholds_confidence_February_20_test_check.RData")

## Remove this to save memory
rm(data_list)



## Count number of true matches dropped by Elastic / Hopper score ratio
total_correct_cases <- nrow(subset(my_data, matches=="True"))
kept_dropped <- NULL
for(i in seq(0,2,0.01)){
  my_ratio <- i
  # Total number of cases kept at each e_score_ratio
  cases_kept <- my_data %>%
    filter(e_score_ratio >= my_ratio) %>%
    summarise(count = n())
  cases_kept <- cases_kept[1,1]
  # Correct cases we are dropping at each e_score_ratio
  correct_cases_remaining <- my_data %>%
    filter(e_score_ratio >= my_ratio, matches=="True") %>%
    summarise(count = n())
  correct_cases_remaining <- correct_cases_remaining[1,1]
  correct_cases_dropped <- total_correct_cases - correct_cases_remaining
  # How many possible matches are there for each address (1-5) at each e_score_ratio
  address_choices <- my_data %>%
    filter(e_score_ratio >= my_ratio) %>%
    group_by(dataset, ID_original) %>%
    summarise(number = n())
  address_choices <- as.data.frame(table(address_choices$number))
  address_choice_1 <- address_choices[1,2]
  address_choice_2 <- address_choices[2,2]
  address_choice_3 <- address_choices[3,2]
  address_choice_4 <- address_choices[4,2]
  address_choice_5 <- address_choices[5,2]
  # Combine these metrics into one dataset
  kept_dropped = rbind(kept_dropped, data.frame(my_ratio, cases_kept, correct_cases_dropped, address_choice_1,
                                                address_choice_2, address_choice_3, address_choice_4,
                                                address_choice_5))
}

rm(address_choices)


## All cases kept vs correct cases dropped
kept_dropped$pc_cases_kept <- kept_dropped$cases_kept / nrow(my_data)*100
kept_dropped$pc_correct_cases_dropped <- kept_dropped$correct_cases_dropped / total_correct_cases*100

## Melt data for the right ggplot2 format
kept_dropped_melted <- subset(kept_dropped, select=c("my_ratio", "pc_cases_kept", "pc_correct_cases_dropped"))
kept_dropped_melted <- melt(kept_dropped_melted, id="my_ratio")

## Plot the results
ggplot(data=kept_dropped_melted, aes(x=my_ratio, y=value, fill=variable, colour=variable)) + geom_line() +
  labs(x="Confidence score ratio", y="Percentage",
       title="Percentage of correct cases dropped and kept vs confidence score ratio") +
  scale_colour_discrete(name="Percentages", labels=c("% all cases kept","% correct cases dropped"))

## Output summary
write.table(kept_dropped, file="Z:/B2011/Big Data Karen/Address Index/Hopper_score/REG-620/Hopper_summary_detail.csv",
            sep=",", col.names=TRUE, row.names=FALSE, append=FALSE)

## How about e_score_ratio = 0.8? Drop 62% of addresses from being displayed but only stops 0.2% of addresses
## from being displayed. But depends on appetite to drop correct cases


## Look at choice of possible matches per address at each e_score_ratio
## Again, melt data for correct ggplot2 format
kept_dropped_address_choices <- subset(kept_dropped, select=c("my_ratio", "address_choice_1", "address_choice_2",
                                                              "address_choice_3", "address_choice_4",
                                                              "address_choice_5"))
kept_dropped_address_choices <- melt(kept_dropped_address_choices, id="my_ratio")
kept_dropped_address_choices[is.na(kept_dropped_address_choices)] <- 0

## Plot the results
ggplot(kept_dropped_address_choices, aes(x=my_ratio, y=value, fill=variable)) + 
  geom_bar(position = "fill", stat='identity') +
  labs(x="Confidence score ratio", y="Percentage",
       title="Number of choices available to user per address") +
  scale_fill_discrete(name="Choices", labels=c("1","2", "3", "4", "5"))


