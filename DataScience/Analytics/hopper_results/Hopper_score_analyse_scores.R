################################################
##                                            ##
## Analyse Hopper scores                      ##
##                                            ##
## Karen Gask                                 ##
## 25 August 2017                             ##      
##                                            ##  
################################################

## Use libraries
library(dplyr)
library(ggplot2)

## Read in data
cqc <- read.csv("T:/Beta_Results/CQC/August_24_dev_hopper_score/CQC_hopper_score_results_no_duplicates.csv")
edge <- read.csv("T:/Beta_Results/EdgeCases/August_24_dev_hopper_score/EdgeCases_hopper_score_results_no_duplicates.csv")
pr <- read.csv("T:/Beta_Results/PatientRecords/August_24_dev_hopper_score/PatientRecords_hopper_score_results_no_duplicates.csv")
wg <- read.csv("T:/Beta_Results/WelshGov/August_24_dev_hopper_score/WelshGov_hopper_score_results_no_duplicates.csv")
wg3 <- read.csv("T:/Beta_Results/WelshGov3/August_24_dev_hopper_score/WelshGov3_hopper_score_results_no_duplicates.csv")

## Append all data
all_data <- rbind(cqc, edge, pr, wg, wg3)

## Establish structural scores grouped by hopper results
summary_counts <- all_data %>% group_by(hopper_results) %>% count(round(structural_score,2))
colnames(summary_counts) [2] <- "structural_score"

## Top matches overwhelm the graphic. Remove them to see the rest of the picture
summary_counts <- subset(summary_counts, hopper_results %in% c('2_in_set_equal','2_in_set_lower'))

## Plot structural score counts grouped by hopper results
summary_counts$structural_score_str <- as.character(summary_counts$structural_score)
ggplot(data=summary_counts, aes(x=structural_score_str, y=n)) + geom_bar(stat="identity") +
  labs(x="Structural score", y="Count", title="Structural scores by correct match flag") +
  facet_grid(hopper_results ~ .)

