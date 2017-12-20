#####################################################################
# ONS Address Index - performance visualization
# 
# This file contains helper function for visualization of performance changes beween two timepoints
# 
# date: May 2017
# author: ivyONS
######################################################################


library(ggplot2)
library(scales)
library(dplyr)
library(tidyr)
LABELS <- c( '1_top_match', '2_in_set', '3_not_found', '4_wrong_match', "5_new_uprn", "6_both_missing")
LABELS_sep <- c( '1_top_match', '2_in_set_equal', '2_in_set_lower', '3_not_found', '4_wrong_match', "5_new_uprn", "6_both_missing")

wrap_dataset2cross_table <- function(input, viz=F, sep_in_set=F){
  # create a cross-table of performance of one dataset at two different times 
  # the data is read from the network drive (created by python) 
  # :input: data.frame[data_name, prev_date, curr_date]  
  data_name <- input['data_name']
  prev_date <- input['prev_date'] 
  curr_date <- input['curr_date'] 
  prev_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', prev_date, '/' , data_name ,'_beta_DedupExist.csv')
  curr_file <- paste0('//tdata8/AddressIndex/Beta_Results/',data_name, '/', curr_date, '/' , data_name ,'_beta_DedupExist.csv')
  print(prev_file)
  PREV <- read.table(prev_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
  CURR <- read.table(curr_file, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
  tab <- compare_performance(PREV, CURR, viz=viz, sep_in_set=sep_in_set)$cross_table
} 

percentage_df <- function(tab, name = 'dataset', total = F, labels=LABELS){
  # tranform the cross table into a df for barchart ploting 
  diff <- rep(0,length(labels)+total)
  names(diff) <- c(if (total) '0_total_success', labels )
  for (category in names(diff)){
    diff[category] =  (ifelse(category %in% colnames(tab),tab['Sum',category], 0) - 
                         ifelse(category %in% rownames(tab), tab[category, 'Sum'], 0 ))# absolute percentage change
  }
  diff_per <- diff / tab['Sum', 'Sum']*100 
  #diff <- (tab['Sum',] - tab[,'Sum'])/ tab['Sum', 'Sum']*100 # absolute percentage change
  #rel_diff <- (tab[, 'Sum'] - tab['Sum',])/ tab['Sum', ]*100  #relative percentage change  
  plot_df <- data.frame(dataset=name, 
                        category = names(diff), 
                        value=diff, 
                        percentage = diff_per, stringsAsFactors=F)
  positive_cases <- c('0_total_success', '1_top_match', '2_in_set_equal', '2_in_set_lower', '2_in_set', "5_new_uprn")
  if (total) plot_df[1, 'value'] <- sum(plot_df[names(diff) %in% positive_cases, 'value'])  # top_mach + in_set + new_uprn
  plot_df <- plot_df %>% mutate(colour = factor(1+pmin(0,(2*(names(diff) %in% positive_cases)-1)*sign(value))))  # assign 1 to positive change and -1 to negative (based on category)
  plot_df$category <- factor(plot_df$category, levels= c(labels[length(labels):1], if (total)'0_total_success' )) 
  plot_df
}

percentage_barchart <- function(cross_tables, datasets=names(cross_tables), add_title = '', labels=NULL){
  if (is.null(labels)) 
    labels <- sort(setdiff(do.call(c, lapply(cross_tables, function(x) union(rownames(x), colnames(x)))), 'Sum'))
  plot_df <- lapply(1:length(datasets), function(i) percentage_df(cross_tables[[i]], name=datasets[i], labels=labels))
  df <-  do.call(rbind, plot_df) %>% 
    mutate(label = paste0(ifelse(value<0, '- ', '+ '), formatC(abs(percentage),digits=2,format="f"),'% (',abs( value), ')'))
  xlims <- max(2.5, abs(df$percentage))
  g <- ggplot(df, aes(x=category, fill=factor(colour, levels=1:0), y=percentage)) +
    geom_bar(stat='identity')+
    scale_fill_brewer(palette="Dark2")+
    geom_text(aes(label = label , y=-xlims-.8#sign(value)*0.1 
    ), size = 3, hjust = 0) +
    coord_flip() +  theme_bw() + guides(fill=FALSE) +
    xlab(NULL) + ylab(NULL) + ylim(c(-xlims-1,xlims))+
    facet_wrap(~dataset, nrow=4)+
    ggtitle(paste('Change in matching performance',add_title))
}

melted_df <- function(tab, name = 'dataset', labels=LABELS){
  melted <- as.data.frame(tab) %>% filter(current %in% labels, previous %in% labels) %>%
            mutate(previous = factor(previous, levels= labels[length(labels):1]),
                   current = factor(current, levels=labels)) %>%
            complete(previous, current, fill=list(Freq=0)) %>%
            mutate(percentage = Freq/sum(Freq)*100,
                   change = sign(length(labels)+1-as.numeric(previous)-as.numeric(current)),
                   colour = percentage*change,
                   dataset = name)
}

S_power_trans <- function() trans_new("S_power",transform = function(x){sign(x)*(abs(x)^.5)},inverse = function(x){sign(x)*(abs(x)^2)})

percentage_heatmap <- function(cross_tables, datasets=names(cross_tables), add_title = '', labels=NULL, confidenceScore =F){
  if (is.null(labels)) 
      labels <- sort(setdiff(do.call(c, lapply(cross_tables, function(x) union(rownames(x), colnames(x)))), 'Sum'))
  
  plot_df <- lapply(1:length(datasets), function(i) melted_df(cross_tables[[i]], name=datasets[i], labels=labels))

  df <-  do.call(rbind, plot_df) %>% 
         mutate(curr_miss = current %in% LABELS[5:6],
                prev_miss = previous %in% LABELS[5:6],
                # there can't be any movement between categories with known UPRN <-> unknown true UPRN
                label=ifelse(((curr_miss + prev_miss) == 1) & Freq==0 , '', 
                             paste0(formatC(abs(percentage),digits=2,format="f"),'% (',abs(Freq), ')')))
  if (confidenceScore)
    df<- df %>% # remove places which can't have movement, e.g. from not found anywhere else
       mutate(impossible = ((previous == LABELS[3]) & (current %in% c(LABELS[c(1:2,4)], LABELS_sep[2:3]) ))|
                           ((previous == LABELS[6]) & (current == LABELS[5]))|
                           ((previous == LABELS[4]) & !(current %in% LABELS[3:6])),
              label = ifelse (impossible & Freq == 0, '--', label))             
  
  xlims <- max(abs(df$colour)) # max(2.5, abs(df$colour))
  
  g <- ggplot(df, aes(current, previous, fill = colour))+
    geom_tile(color = "white")+
    scale_fill_gradient2(low = "#d95f02", high = "#1b9e77", mid = "white", 
                         midpoint = 0,  space = "Lab", limit = c(-xlims,xlims),
                         name = "Percentage change", trans = "S_power") +
    theme_bw()+ facet_wrap(~dataset, nrow=4)+
    geom_text(aes(label = label), color = "black", size = 3) +
    ggtitle(paste('Change in matching performance', add_title)) +
    theme(legend.position = c(0.7, 0.1),  
          legend.direction = "horizontal")+
    guides(fill = guide_colorbar(title.position = "top"))
}

performance_barchart <- function(cross_tables, datasets=names(cross_tables), add_title = '', labels=NULL){
  cross_tables <- lapply(cross_tables, function(tab) {tab[1:(nrow(tab)-1),'Sum']=0
                                                        tab} )
  if (is.null(labels)) 
    labels <- sort(setdiff(do.call(c, lapply(cross_tables, function(x) union(rownames(x), colnames(x)))), 'Sum'))
  
  plot_df <- lapply(1:length(datasets), function(i) percentage_df(cross_tables[[i]], name=datasets[i], labels=labels))
  df <-  do.call(rbind, plot_df) %>% 
    group_by(dataset) %>%
    mutate(percentage=value/sum(value)*100,
      label = paste0(formatC(abs(percentage),digits=2,format="f"),'% (',abs( value), ')'))
  g <- ggplot(df, aes(x=category, fill=factor(colour, levels=1:0), y=percentage)) +
    geom_bar(stat='identity')+
    scale_fill_brewer(palette="Dark2")+
    geom_text(aes(label = label , y= - 60), size = 3, hjust = 0) +
    coord_flip() +  theme_bw() + guides(fill=FALSE) +
    scale_y_continuous(breaks= 12.5*(0:8),labels=sapply(0:8, function(x) if (x%%2 == 0) paste0(12.5*x, '%') else ''), limits=c(-65, 100), name=NULL)+
    xlab(NULL) +
    facet_wrap(~dataset, ncol=2)+
    ggtitle(paste('Matching performance',add_title))
}
