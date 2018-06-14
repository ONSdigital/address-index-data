################################################
##                                            ##
## Visualise Hopper score results             ##
##                                            ##
## Karen Gask                                 ##
## 19 July 2017                               ##      
##                                            ##  
################################################

## Use libraries
library(ggplot2) # for plotting
library(Rmisc) # for calculating confidence intervals
library(dplyr) # for manipulating and summarising data
library(reshape2) # for changing long thin data to fat wide

## Read in data
my_data <- read.csv("T:/Beta_Results/EdgeCases/July_14_branch_hopper_score/EdgeCases_hopper_score_results_no_duplicates.csv")

## Sort dataset by structural_score
my_data <- my_data[order(-my_data$structural_score),]

## Plot structural_score by hopper_results
qplot(seq_along(my_data$structural_score), my_data$structural_score, colour=my_data$hopper_results) +
  labs(x="", y="Structural score", title="Structural score by result of match (Edge cases)") + 
  guides(colour=guide_legend(title="Hopper score results"))

## Produce confidence intervals in table
building_debug <- group.CI(structural_score~building_score_debug, my_data, ci = 0.95)
building_debug$cases <- table(my_data$building_score_debug)

# Plot confidence intervals
ggplot(building_debug, aes(x=building_score_debug, y=structural_score.mean)) + geom_point() +
  geom_errorbar(aes(ymin = structural_score.lower, ymax = structural_score.upper), colour = "blue") +
  geom_text(aes(label=building_debug$building_score_debug, hjust=-0.5), size=3) + 
  labs(x="Building score debug code", y="Structural score", title="Structural score by debug code (Edge cases)") 

## Percentage correct by structural score, include scores with 10+ cases
percent_right <- my_data %>% group_by(hopper_results) %>% count(structural_score)
percent_right <- dcast(percent_right, structural_score ~ hopper_results, value.var="n")
percent_right$total <- rowSums(percent_right[,2:6], na.rm=TRUE)
percent_right <- subset(percent_right, total >= 10)
percent_right$percent <- percent_right$`1_top_unique` / percent_right$total *100

## Plot percentage correct
percent_right$structural_score_str <- as.character(percent_right$structural_score)
ggplot(data=percent_right, aes(x=structural_score_str, y=percent)) + geom_bar(stat="identity") +
  labs(x="Structural score", y="Percentage", title="Percentage of correct unique top matches by structural score (Edge cases)")




