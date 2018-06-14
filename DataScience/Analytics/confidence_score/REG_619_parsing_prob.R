#####################################################################
# ONS Address Index - performance visualization
# 
# This scripts looks at parsing probabilities
# versus other scores and visualise it
#
# date: May 2018
# author: ivyONS
######################################################################

library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases','CQC',   'PatientRecords','WelshGov')#, 'LifeEvents',  'WelshGov2',  'WelshGov3')
curr_date <-  'May_15_test_threshold0'#'February_20_test_check'

dataset='EdgeCases'
#helper function
load_data <- function(dataset, curr_date, top = ''){
  filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_features_5_parse.csv")# "_debug_codes.csv")
  my_data <- read.csv(filename, stringsAsFactors=F)

  if (top %in% names(my_data)){
     my_data$score <- my_data[[top]]
     my_data <- my_data %>% group_by(ID_original) %>% 
      mutate(top_scoring = ( score == max(score)) ,
            dataset = dataset) %>%            
      arrange(ID_original, desc(score)) %>% 
       ungroup () 
  }
  
  print(paste(Sys.time(),' Loaded data:', dataset) )
  my_data <- my_data %>% 
    mutate(dataset = dataset,
           ID_original = as.character(ID_original))
  my_data
}

data_list <- lapply(datasets, load_data, curr_date = curr_date, top = 'simple_confidence')
my_data <- do.call('bind_rows', data_list)

################################################
# compare Richard's implementation of confidence score with our python results
################################################
ggplot(my_data %>% filter (abs(simple_confidence-confidenceScore)>.05)%>% mutate(matches2 = paste('Matches = ', matches)), 
       aes(x= simple_confidence, y= confidenceScore, color = matches)) +
  geom_jitter(height=.02, width =0.02, size=.1) +
  #facet_grid(.~matches2) +
  geom_abline(slope=1) + 
  xlim(c(0,1.1)) + ylim(c(0,1.1))+
  xlab('simple_confidence (python prototype)') + ylab('confidenceScore (scala implementation)') +
  theme_bw()

## investigate discrepancies manually:
ff<- my_data %>% filter(simple_confidence>.8, confidenceScore<.6)
# ACTION: if only one candidate is returned, we need to adjust the e_score_ratio to ~ 1.2

ff<-my_data %>% filter(simple_confidence<.4, confidenceScore>.5) 
# ACTION: revise why the h_alpha is .9 instead of .8


################################################
# check each score vs accu plots
# import::here(score_group, .from = "confidence_score_ROC.R")
label_score <-  c( 'e_sigmoid', 'h_power', 'simple_confidence', 'parsing_score')#, 'e_parse', 'h_parse', 'conf_parse' )
bin_size <- 150
data <-  my_data %>% filter(top_scoring) %>% 
         mutate(e_parse =  e_sigmoid*parsing_score,
                h_parse = h_power*parsing_score,
                conf_parse = simple_confidence*parsing_score)
  bins <- ceiling(min(sqrt(nrow(data)),nrow(data)/bin_size))
  df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data, bins=bins)[[1]]))
  df <- df %>% group_by (var) %>% 
    mutate(quant = quant * 100 / length(unique(quant)))
  

df$var <- factor(df$var, levels = label_score)

g <- ggplot(df, aes(x=avg_score, y=avg_accu, col = quant)) +
  geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
  facet_grid(~var, scales = "free")+ theme_bw() + xlim(0,1)+
  ggtitle('Accuracy vs *score per quantile bin for only the top scoring candidates from each dataset') #+
  #geom_abline(slope=1, linetype=6) 
print(g)

jpeg(file= paste0('pictures/2018May_accu_vs_parsing_top1.jpg'), width = 1000, height = 500)
print(g)
dev.off()

#### does the parsing score differ for different results category?
categ <- my_data %>% group_by(dataset, ID_original) %>%
  summarise (results1 = results[1], 
             parsing_score = parsing_score[1]) %>%
  mutate(results = ifelse(results1 %in% c('2_in_set_equal', '2_in_set_lower'), '2_in_set', results1)) 

g<- ggplot(categ, aes(x=results, y=parsing_score, color = results)) +
  geom_boxplot() +
  facet_wrap(~dataset)+
  theme_bw()

jpeg(file= paste0('pictures/2018May_results_vs_parsing.jpg'), width = 1000, height = 500)
print(g)
dev.off()

## in the box plot we see highest parsing score for top matches and lowest for not found, is it significant? YES
cat_aov <-  aov(data = categ, parsing_score~results)
summary(cat_aov)
plot(cat_aov)
kruskal.test(data = categ, parsing_score~results)
x <- (categ %>% filter(results == '1_top_unique' )) [['parsing_score']]
y <- (categ %>% filter(results %in% c('2_in_set', '4_wrong_match' ))) [['parsing_score']]
wilcox.test(x,y)

########################################################################
# can we visualise relationship hopper-parsing-elastic?

ggplot(my_data %>% mutate(matches2 = paste('Matches = ', matches)) %>% filter(top_scoring), 
       aes(x= e_sigmoid, y= h_power, color = parsing_score)) +
  geom_jitter(height=.04, width =0.04, size=.01) +
  facet_grid(.~matches2) +
  theme_bw()

jpeg(file= paste0('pictures/2018May_parsing_prob_vs_scores2.jpg'), width = 1500, height = 900)
#print(g)
dev.off()

#### the same, but stuck the dots in bins 
# create a double mesh of bins in h_power and e_sigmoid
df <- my_data %>% filter(top_scoring, !is.na(UPRN_beta)) %>%
  mutate(results = ifelse(results %in% c('2_in_set_equal', '2_in_set_lower'), '2_in_set', results)) 

nn=50
for (var in c('h_power', 'e_sigmoid')){
  grr <- df[[var]]
  quantiles <- (0:nn)/nn#unique(quantile(grr, probs=((0:bins)/bins)))
  df <- df %>% mutate(quant = as.numeric(cut(grr, quantiles )),
                     quant = ifelse(is.na(quant),1,quant)) 
  df[[paste0(var, '_quant')]] = df$quant
}
df_plot <- df %>% group_by(h_power_quant, e_sigmoid_quant) %>%
  summarise(h_power = h_power_quant[1]/nn, #mean(h_power),
            e_sigmoid =e_sigmoid_quant[1]/nn,
            parsing_score = mean(parsing_score),
            size = n())

ggplot(df_plot, aes(x= e_sigmoid, y= h_power, color = parsing_score)) +
  geom_point(aes(size=log(size))) +
  theme_bw()


### just look at e_sigmoid - h_power  vs parsing_score
ggplot(df, aes(x = e_sigmoid - h_power, y= parsing_score)) +
  geom_jitter(aes(color=results) ,size=.01)+ 
  geom_smooth(se=F)+
  ggtitle('Parsing probabilities vs difference in score for the topscoring candidates')+
  theme_bw()

jpeg(file= paste0('pictures/2018May_parsing_prob_vs_score_diff.jpg'), width = 1500, height = 900)
#print(g)
dev.off()

### we see that the parsing probabilities are lowes when e_sigmoid-h_power is high, 
### is it significant?

x <- (df %>% filter((e_sigmoid - h_power)>.9 )) [['parsing_score']]
y <- (df %>% filter((e_sigmoid - h_power) <= .9)) [['parsing_score']]
wilcox.test(x,y)

cor.test(df$e_sigmoid - df$h_power, df$parsing_score, method='spearman', exact=F)

#### quantiles on parsing score 

bin_size <- 1000
bins <- ceiling(min(sqrt(nrow(df)),nrow(df)/bin_size))
grr <- df$parsing_score
quantiles <- unique(quantile(grr, probs=((0:bins)/bins)))
df <- df %>% mutate(quant = as.numeric(cut(grr, quantiles )),
                    quant = ifelse(is.na(quant),1,quant)) 
df_quant <- df %>% group_by(quant) %>%
  summarise(parsing_score = mean(parsing_score),
         e_sigmoid = mean(e_sigmoid),
         h_power = mean(h_power) +.3,
         accu = mean(results == '1_top_unique'),
         size =n()) %>%
  ungroup()
df_plot <- df_quant %>% gather('score_type', 'value', 3:5)

ggplot(df_plot, aes(x= parsing_score, y= value, colour = score_type))+
  geom_line() + #geom_point(aes(size= size))+
  theme_bw()

df2 <- df %>% mutate(accuracy = as.numeric(results == '1_top_unique'),
                     e_h_diff = e_sigmoid-h_power ) %>%
  select(parsing_score, h_power, e_sigmoid, e_h_diff, accuracy) %>%
  gather('score_type', 'score_value', 2:5)

ggplot(df2, aes(x= parsing_score, y= score_value, colour = score_type))+
  geom_smooth(se=F) + #geom_point(aes(size= size))+
  ggtitle('Relationship of several scores vs parsing probability for top-scoring candidates')+
  theme_bw()

jpeg(file= paste0('pictures/2018May_scores_vs_parsing.jpg'), width = 800, height = 700)
#print(g)
dev.off()
