#####################################################################
# ONS Address Index - compare accuracy and confidence score
# 
# This script analyses confidence score and accuracy by
# debug categories
#
# date: Feb 2018
# author: Karen, using lots of Iva's code
######################################################################


"Iva's code
"

library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases', 'CQC', 'PatientRecords', 'WelshGov')

#helper function
load_data <- function(dataset, curr_date, top = 'confidence_score'){
  filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_confidence_score_results.csv")
  my_data <- read.csv(filename, stringsAsFactors=F)
  my_data <- my_data %>% group_by(ADDRESS) %>% 
    mutate(top_scoring = (score == max(score)) ,
           dataset = dataset) %>%            
    #filter(top_scoring) %>% 
    ungroup ()     
  print(paste(Sys.time(),' Loaded data:', dataset) )
  my_data
}

## Read in data
if(F){ # run only first time, then read from saved file
  curr_date <- 'January_16_test_e54_baseline'
  data_list <- lapply(datasets, load_data, curr_date = curr_date, top= 'elastic_score')
  my_data <- do.call('rbind', data_list)
  save(my_data, file = paste0('cross_tables/debug_confidence_',curr_date,'.RData'))
}else load("H:/My Documents/R/address-index/cross_tables/debug_confidence_January_16_test_e54_baseline.RData")

### features

my_data2 <- my_data %>%  filter(top_scoring, !is.na(UPRN_comparison)) %>%
  #mutate(match = ifelse(is.na(UPRN_comparison), NA, as.numeric(as.factor(matches))-1)) %>%
  #group_by(dataset) %>%  mutate(size_ds = n())  %>% 
  ungroup()

my_data2$combined_debug_code <- as.factor(paste0(as.character(my_data2$locality_score_debug), ':',
                                                as.character(my_data2$building_score_debug)))

N_data2 <- nrow(my_data2)

plot_all <- my_data2 %>%
  group_by(building_score_debug, locality_score_debug) %>% 
  summarize(group_accuracy = mean(matches == 'True'), 
            group_size = n()/N_data2,
            multi = n_distinct(structural_score),
            structural_max = max(structural_score),
            structural_mean = mean(structural_score),
            structural_min = min(structural_score),
            difference = as.character(ifelse(is.na(group_accuracy),0,  round(0.999*(group_accuracy - structural_max)))),
            label = ifelse(difference=='0', '', paste0(locality_score_debug,':',building_score_debug)),
            dataset = 'ALL')  %>%  
  ungroup() 

ggplot(plot_all %>% filter(group_size>=10/N_data2), 
       aes(x=structural_max, y=group_accuracy, size=group_size, color= difference))+
  geom_point() +
  geom_text(aes(label=label, hjust=-0.5), size=3) + 
  labs(x="Maximum structural score", y="Group accuracy", title="Maximum structural score by group accuracy") + 
  theme_bw()


#################################################################################

# REG-384



# Only select debug codes which contain >= 10 addresses for input into linear regression
lm_input <- subset(plot_all, group_size>=(10/N_data2))

# Log(group_accuracy) = log(locality_score_debug) + log(building_score_debug)
# As if you have a = b*c, then log(a) = log(b)+log(c)
lm_input$log_group_accuracy <- log(lm_input$group_accuracy)
# Remove -Inf from log_group_accuracy otherwise regression doesn't work
lm_input <- subset(lm_input, group_accuracy>0)

# Set building_score_debug and locality_score_debug to character
lm_input$building_score_debug <- as.character(lm_input$building_score_debug)
lm_input$locality_score_debug <- as.character(lm_input$locality_score_debug)

# Run linear regression with intercept = zero
lm_summary <- lm( log_group_accuracy ~ -1+locality_score_debug+building_score_debug, data=lm_input)
# Get summary on goodness-of-fit
summary(lm_summary)
# Adjusted R-squared:  0.7877

# Get solved locality_score_debug and building_score_debug probabilities
coeffs <- as.data.frame(lm_summary$coefficients)
coeffs$name <- rownames(coeffs)
coeffs$name <- gsub("locality_score_debug", "", coeffs$name, fixed=TRUE)
coeffs$name <- gsub("building_score_debug", "", coeffs$name, fixed=TRUE)
colnames(coeffs) <- c("log_debug_scores", "debug_codes")
coeffs$debug_scores <- exp(coeffs$log_debug_scores)
coeffs$log_debug_scores <- NULL

# Split datasets into locality and building score debug codes
locality_debug_scores <- subset(coeffs, nchar(coeffs$debug_codes)==4)
colnames(locality_debug_scores) <- c("locality_score_debug", "estimated_locality_score")
building_debug_scores <- subset(coeffs, nchar(coeffs$debug_codes)==2)
colnames(building_debug_scores) <- c("building_score_debug", "estimated_building_score")

# Now merge estimated values for locality_score_debug and building_score_debug
estimated_scores <- merge(locality_debug_scores, lm_input, by="locality_score_debug", all=TRUE)
estimated_scores <- merge(building_debug_scores, estimated_scores, by="building_score_debug", all=TRUE)

# Now compare estimated group accuracy with group_accuracy
estimated_scores$estimated_group_accuracy <- estimated_scores$estimated_building_score * estimated_scores$estimated_locality_score

# For graph
estimated_scores$difference = as.character(ifelse(is.na(estimated_scores$group_accuracy),0,
                                                  round(0.999*(estimated_scores$group_accuracy - estimated_scores$estimated_group_accuracy))))
estimated_scores$label = ifelse(estimated_scores$difference=='0', '', paste0(estimated_scores$locality_score_debug,':',estimated_scores$building_score_debug))

# Graph for comparison
ggplot(estimated_scores, 
       aes(x=estimated_group_accuracy, y=group_accuracy, size=group_size, color= difference))+
  geom_point() +
  geom_text(aes(label=label, hjust=-0.5), size=3) + 
  labs(x="Estimated group accuracy", y="Group accuracy", title="Estimated group accuracy") + 
  theme_bw()


# Doesn't account for number of addresses in each debug code eg. 9111:91 is quite large
# Doesn't estimate for building_score_debug=11




# REG-553 (spread of accuracy within each debug code)

# Examine spread of structural score for REG-553 (spread of accuracy within each debug code)
struct_score_diffs <- plot_all %>%
  filter(structural_max != structural_min) %>%
  mutate(group_size_proper = group_size*N_data2,
         all_labels = paste0(as.character(locality_score_debug), ':',
                             as.character(building_score_debug))) %>%
  filter(group_size_proper>=10)

# Keep only the categories in struct_score_diffs['all_labels']
my_vector <- as.vector(struct_score_diffs['all_labels'])
my_vector <- dput(my_vector)

# Subset my_data2 with categories in struct_score_diffs['all_labels']
struct_score_diffs_all_data <- subset(my_data2, combined_debug_code %in% c("1114:11", "1119:11", "1119:21", 
                                                                           "9115:71", "9119:71", "9114:91",
                                                                           "9115:91", "9116:91", "9119:91"))
struct_score_diffs_all_data$combined_debug_code <- factor(struct_score_diffs_all_data$combined_debug_code)

# Produce box plots on spread of these cases
ggplot(data=struct_score_diffs_all_data, 
       aes(x=combined_debug_code, y=structural_score)) + 
  geom_boxplot() + labs(x="Debug code", y="Structural score", title="Spread of structural score by debug code") 

# Get summary table
spread_summary <- as.data.frame.matrix(table(struct_score_diffs_all_data$structural_score, struct_score_diffs_all_data$combined_debug_code))

# Hmm, that explains why there isn't much spread in the box plots!
write.table(spread_summary, 
            file="Z:/B2011/Big Data Karen/Address Index/Hopper_score/REG-384/REG553_spread_summary.csv", sep=",", col.names=TRUE, row.names=TRUE, append=FALSE)





