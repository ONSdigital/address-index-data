#####################################################################
# ONS Address Index - ROC performance visualization
# 
# This scripts analyses possible confidence score using ROC
#
# date: Feb 2018
# author: ivyONS
######################################################################


library("dplyr")
library("tidyr")
library("ggplot2")

setwd('~/R/address-index')
datasets <- c('EdgeCases','CQC',   'PatientRecords','WelshGov')#, 'LifeEvents',  'WelshGov2',  'WelshGov3')
curr_date <- 'February_20_test_check'
label_score <- c( 'e_score_ratio', 'h_score_diff', 'e_score',  'h_score')# 'h_score_ratio','e_score_diff'
#break_points <- list(c(-1,0,1), c(.8,1,1.2), c(-1,0,1), c(.8,1,1.2), NULL, NULL) 
break_points <- list(0, 1, 0, 1, 4, 950) 
names(break_points) <- label_score

get_points_ROC <- function(Score, Class) {
  Pp = sum(Class)
  Nn = length(Class) - sum(Class)
  df <- data.frame(score = Score, cl = Class) %>% arrange(-score) %>%
    mutate(ind = 1: n(), TP = cumsum(cl), TPR= TP/Pp, FPR = (ind - TP)/Nn)
  res <- df %>% group_by(score) %>% 
    summarize (TPR= TPR[which.max(ind)], FPR = FPR[which.max(ind)]) %>%
    ungroup() %>%
    bind_rows(data.frame (score=c(NA, NA), TPR = c(0, 1), FPR = c(0,1))) %>%  
    arrange(TPR, FPR)
  res
} 

add_labels <- function(df, break_pt=break_points){
  res <-  data.frame()
  for (dataname in unique(df$dataset)){
  for (sc_lab in unique(df$score_type)){ 
    if (!is.null(break_pt[[sc_lab]])){
      for (i in break_pt[[sc_lab]]){
        df1 <- df %>% filter (score_type == sc_lab, dataset == dataname) %>% arrange (abs(score - i))
        res <- rbind(res, data.frame(score_type = sc_lab, dataset = dataname,  score = df1$score[1], TPR = df1$TPR[1], FPR = df1$FPR[1], lab = i))
      }
    }
  }
  }
  res
}


#helper function
give_me_ROC <- function(data,  label_sc = label_score, break_pt = break_points, datasets = unique(data$dataset)){
  data$class <- data$matches=='True'
  score_list <- lapply(datasets, function(d) {
    print(d)
    my_data  <- data %>% filter(dataset == d)  
    var_list <- lapply(label_sc, function(x){ 
       tdf <- get_points_ROC(my_data[[x]], my_data$class)
       tdf$score_type = x
       tdf$dataset = d
       tdf
    })
    do.call('rbind', var_list)
  })
  score_list
}


score_group <- function( var, data, bins=10){
  df<- data
  df$grr <- data[[var]]
  quantiles <- unique(quantile(df$grr, probs=((0:bins)/bins)))
  df <- df %>% mutate(quant = as.numeric(cut(grr, quantiles )),
                      quant= ifelse(is.na(quant),1,quant)) 
  data[[paste0(var, '_quant')]] = df$quant
  df <- df %>%  group_by(quant) %>% summarize (size = n(),
                                avg_accu = mean(matches=='True'),
                                var_accu = var(matches=='True'),
                                avg_score = mean(grr),
                                var = var)
  list(df, data)
  }

give_me_ACCU <- function(data, label_score = label_score, n=100){
  df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data, n=n)[[1]]))
  g <- ggplot(df, aes(x=avg_score, y=avg_accu, col = quant)) +
    geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
    facet_wrap(~var, scales = "free")+ theme_bw() + ggtitle('Accuracy vs *score per quantile bin for all candidates from EdgeCases' )
  print(g)
  df
}

#################################################################


filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_features.csv")
data <- read.csv(filename, stringsAsFactors=F)
#data$e_score <- glm(matches=='True'~ e_score_ratio, family = binomial(link = "logit"), data = data)$fitted.values
df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data, bins=10)[[1]]))
df2 <- df %>%mutate(includes = '2 top two candidates' )
data <- data %>% filter(e_score_diff>0) 
df1<- df %>%mutate(includes = '1 only top scoring' )
filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_features5.csv")
data <- read.csv(filename, stringsAsFactors=F)
#data$e_score <- glm(matches=='True'~ e_score_ratio, family = binomial(link = "logit"), data = data)$fitted.values
df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data, n=n)[[1]]))
df5<- df %>%mutate(includes = '5 all available candidates' )

df <- rbind(df1, df2, df5)
df$quant = df$quant*2
df$avg_score[df$var %in% c('h_score', 'h_score_diff')] =   df$avg_score[df$var %in% c('h_score', 'h_score_diff')]/1000


g <- ggplot(df, aes(x=avg_score, y=avg_accu, col = quant)) +
    geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
    facet_grid(includes~var, scales = "free")+ theme_bw() + ggtitle('Accuracy vs *score per quantile bin for *candidates from EdgeCases' )
print(g)
  
  
#pdf(file= paste0('pictures/2018Feb_EdgeCases_ScoreAccuracy.pdf'), width = 10, height = 6)
#print(g)
#dev.off()


#################################################################


df1 <- list()
df2 <- list()
data <- list()
bins <- 50
for (dataset in datasets){
filename <- paste0("//tdata8/AddressIndex/Beta_Results/",dataset,"/", curr_date,"/", dataset, "_ML_features.csv")
  data[[dataset]] <- read.csv(filename, stringsAsFactors=F)
  #data$e_score <- glm(matches=='True'~ e_score_ratio, family = binomial(link = "logit"), data = data)$fitted.values
  df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data[[dataset]], bins=bins)[[1]]))
  df2[[dataset]] <- df %>%mutate(includes = '2 top two candidates', dataset = dataset )
  data1 <- data[[dataset]] %>% filter(e_score_diff>0) 
  df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data1, bins=bins)[[1]]))
  df1[[dataset]] <- df %>% mutate(includes = '1 only top scoring', dataset =  dataset )
}

data2 <- do.call('rbind', lapply(names(data), function(x) data[[x]] %>% select(label_score, matches, ADDRESS) %>% mutate(dataset =x)))

df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data2, bins=bins)[[1]]))
df2[['All']] <- df %>% mutate(includes = '2 top two candidates', dataset = 'All' )
data1 <- data2 %>% filter(e_score_ratio>1) 
df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data1, bins=bins)[[1]]))
df1[['All']] <- df %>% mutate(includes = '1 only top scoring', dataset =  'All')
df <- do.call('rbind', c(df1,df2))

df$quant = df$quant*100/bins
df$avg_score[df$var %in% c('h_score', 'h_score_diff')] =   df$avg_score[df$var %in% c('h_score', 'h_score_diff')]/1000

for (dataname in c(datasets, 'All')){
g <- ggplot(df %>% filter(dataset == dataname), aes(x=avg_score, y=avg_accu, col = quant)) +
  geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
  facet_grid(includes~var, scales = "free")+ theme_bw() + ggtitle(paste('Accuracy vs *score per quantile bin for *candidates from', dataname))
print(g)
}

g <- ggplot(df %>% filter(includes == '2 top two candidates'), aes(x=avg_score, y=avg_accu, col = quant)) +
  geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
  facet_grid(dataset~var, scales = "free")+ theme_bw() + ggtitle('Accuracy vs *score per quantile bin for top two candidates from each dataset')
print(g)

pdf(file= paste0('pictures/2018Feb_All4_ScoreAccuracy.pdf'), width = 10, height = 6)
print(g)
dev.off()

############################################
haha <- give_me_ROC(data2, label_score)
score_list <- do.call('rbind',haha)
points_list <- add_labels(score_list, break_points)


g<- ggplot(data=score_list, aes(x=FPR, y=TPR, color = score_type)) +
  geom_line() +  
  facet_wrap(~dataset) + 
  ggtitle('ROC for top two candidates from each dataset') +
  theme_bw() 
  #geom_point(data=points_list) +
  #geom_text(data=points_list, aes(label=lab, hjust=-0.5), size=3)
g

#########################################
# read big data frame
filename <- paste0("//tdata8/AddressIndex/Beta_Results/Edgecases/", curr_date,"/All_data_ML_features_predictions.csv")
data0 <- read.csv(filename, stringsAsFactors=F)
label_score <-  c( 'e_score', 'e_score_ratio', 'h_score', 'h_score_diff', 'clf_predicted', 'logreg_predicted')

haha0 <- give_me_ROC(data0, label_score)
score_list0 <- do.call('rbind',haha0)

g<- ggplot(data=score_list0, aes(x=FPR, y=TPR, color = score_type)) +
  geom_line() +  
  facet_wrap(~dataset) + 
  ggtitle('ROC for top two candidates from each dataset') +
  theme_bw() #+  xlim(0,.1) +ylim(0.9,1)
g

pdf(file= paste0('pictures/2018Feb_All4_ROC.pdf'), width = 16, height = 10)
print(g)
dev.off()

df2 <- list()
bin_size <- 100
for (data_name in datasets){
  data <- data0 %>% filter(dataset == data_name) 
  bins <- ceiling(min(sqrt(nrow(data)),nrow(data)/bin_size))
  #data$e_score <- glm(matches=='True'~ e_score_ratio, family = binomial(link = "logit"), data = data)$fitted.values
  df <- do.call('rbind', lapply(label_score, function(x) score_group(x,  data=data, bins=bins)[[1]]))
  df$quant = df$quant*100/bins
  df2[[data_name]] <- df %>%mutate(includes = '2 top two candidates', dataset = data_name )
}

df <- do.call('rbind', df2)
df$var <- factor(df$var, levels = label_score)

df$avg_score[df$var %in% c('h_score', 'h_score_diff')] =   df$avg_score[df$var %in% c('h_score', 'h_score_diff')]/1000

g <- ggplot(df %>% filter(includes == '2 top two candidates'), aes(x=avg_score, y=avg_accu, col = quant)) +
  geom_line() + geom_point(aes(size=size)) + ylim(0,1)+
  facet_grid(dataset~var, scales = "free")+ theme_bw() + ggtitle('Accuracy vs *score per quantile bin for top two candidates from each dataset')
print(g)

pdf(file= paste0('pictures/2018Feb_All4_Accu_score.pdf'), width = 16, height = 10)
print(g)
dev.off()
