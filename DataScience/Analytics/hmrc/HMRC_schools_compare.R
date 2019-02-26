#####################################################################
# ONS Address Index - compare performance on HMRC schools datased
# 
# This is a simple script to compare reference UPRNs attached to input address strings (schools) 
# we compare candidates provided by hmrc with candidates obtained from ons addressindex
#
# date: Nov 2017
# author: ivyONS
######################################################################

library("dplyr")
library("tidyr")
library("ggplot2")

######################################################################
##path specs
setwd('~/R/address-index')
ons_filename <- '//tdata8/AddressIndex/Beta_Results/HMRC/EduBase_Schools_July_2017_01_response.csv'
hmrc_filename <- '//tdata8/AddressIndex/Beta_Results/HMRC/UPRN_SCHOOLS.txt'
outpur_filename <- 'got_worse/EduBase_Schools_UPRN_disagree.csv'


######################################################################
##helper functions def

#this function implements the logic of how we will evaluate the compared matches: 
eval_match <- function(uprn_ons, uprn_hmrc, stat_ons, stat_hmrc){
  # input: matching results for one particular address string (unique id) 
  # output: string with possible values:
  #   both_missing
  #   hmrc_only (ONS missing)
  #   ons_only
  #   disagreement:
  #   partial_agreement .. to be split in categories based on matched status
  if (is.na(uprn_ons[1])){
    if (is.na(uprn_hmrc)[1]) res = 'both_missing'
    else res = 'hmrc_only'
  }else if (is.na(uprn_hmrc)[1]) res = 'ons_only' 
        else if (length(intersect(uprn_ons,uprn_hmrc))==0) res = 'disagreement'
             else {
               matched = intersect(uprn_ons,uprn_hmrc) 
               matched_ons = lapply(matched, function(x) stat_ons[which(x==uprn_ons)])
               matched_hmrc = lapply(matched, function(x) stat_hmrc[which(x==uprn_hmrc)])
               res_list = sapply(1:length(matched), function(i) paste(matched_ons[[i]], matched_hmrc[[i]], sep='-', collapse = ', '))
               res = paste(res_list, collapse = ', ')
               }
 res
} 

# this function splits all possible 'partial agreement' into few groups
simplify_eval <- function(long_eval){
  longeval <- strsplit(long_eval, ', ')[[1]]  
  if (long_eval %in% c('both_missing', 'hmrc_only', 'ons_only', 'disagreement')) res = long_eval
  else if ('Top-Single UPRN UPRN' %in% longeval)
              res = 'Top - single agreement' 
       else if ('Top-Multiple UPRN UPRN' %in% longeval)
                  res = 'Top - multiple agreement'
            else  if (('Top-Single UPRN Parent_UPRN' %in% longeval) |
                      ('Top-Multiple UPRN Parent_UPRN' %in% longeval)) 
                         res = 'Top - parent agreement'
                  else if (('In set-Single UPRN UPRN' %in% longeval) |
                           ('In set-Single UPRN Parent_UPRN' %in% longeval)) 
                              res = 'In set - single (parent) agreement'
                       else res = 'In set - multiple (parent) agreement'
 res
}

# add the matching info to each hmrc candidate (helper function)
eval_one_candidate <- function(uprn_ons, uprn_hmrc, stat_ons, stat_hmrc){
  if (is.na(uprn_hmrc)) '' else if (! (uprn_hmrc %in% uprn_ons) ) '' else 
    paste(stat_ons[which(uprn_hmrc == uprn_ons)],stat_hmrc, sep='-')  
}

# alternative method of simple evaluation (take the candidate level info from the eval_one_candidate)
simplify_one_eval <- function(uprn_ons, uprn_hmrc, longeval, stat_ons){
  if (is.na(uprn_ons[1])){
    if (is.na(uprn_hmrc)[1]) res = 'both_missing'
    else res = 'hmrc_only'
  }else if (is.na(uprn_hmrc)[1]) {
    if (stat_ons[1] == 'Top' ) res = 'ons_only (top)' 
    else res = 'ons_only (set)'
  }else if (length(intersect(uprn_ons,uprn_hmrc))==0) res = 'disagreement'
  else if ('Top-Single UPRN UPRN' %in% longeval)
    res = 'Top - single agreement' 
  else if ('Top-Multiple UPRN UPRN' %in% longeval)
    res = 'Top - multiple agreement'
  else if (('Top-Single UPRN Parent_UPRN' %in% longeval) |
           ('Top-Multiple UPRN Parent_UPRN' %in% longeval)) 
    res = 'Top - parent agreement'
  else if (('In set-Single UPRN UPRN' %in% longeval) |
           ('In set-Single UPRN Parent_UPRN' %in% longeval)) 
    res = 'In set - single (parent) agreement'
  else res = 'In set - multiple (parent) agreement'
  res
}
######################################################################
#DO NOT RUN

# read in matching results for the HMRC schools dataset
ONS <- read.table(ons_filename, header=T, sep=',',  quote = "\"", stringsAsFactors=F)
HMRC <-  read.table(hmrc_filename, header=T, sep='|',  quote = "\"", stringsAsFactors=F)

#assign a unique 'subid' to each potential match candidate (or parent) uprn in both datasets
ONS <- ONS %>% group_by(id) %>%     
  arrange(desc(score), uprn) %>%
  mutate(subid = id*1000 + 1:n(),         #add row ids for table join
         UPRN_ons = uprn,                 #a bit of wrangling for easier comparison    
         STATUS_ons = if (is.na(uprn[1])) 'Unmatched' else 
           if ((n()==1)|(score[1]>score[2])) c('Top', rep('In set', 4))[1:n()] else
             rep('In set', 5)[1:n()]) %>% 
  ungroup()  %>% 
  select(subid, id, inputAddress, UPRN_ons, STATUS_ons, matchedFormattedAddress, score)
HMRC <- HMRC %>% gather(UPRN_level, UPRN_hmrc, 3:4) %>%       #a bit of wrangling again (include parent uprns in the list of candidates)
  filter(!is.na(UPRN_hmrc) | Match_Status=='Unmatched') %>%      #remove emty parent uprns
  group_by(Unique_Record_ID, Match_Status, UPRN_level, UPRN_hmrc) %>% 
  summarise(STATUS_hmrc = paste(Match_Status, UPRN_level)[1]) %>% # remove duplicate uprns for each record id
  group_by(Unique_Record_ID) %>%   
  mutate(subid = Unique_Record_ID*1000 + 1:n()) %>%      #add row ids for table join
  ungroup()

# join the tables just by the new 'subid' (include all candidates from both datasets)
joined <- full_join(ONS, HMRC) %>% arrange(subid) %>% mutate(id=subid%/%1000) 

#apply the above defined evaluation functions:
matched_groups <- joined %>% 
  group_by(id) %>% 
  summarise(compare_status = eval_match(UPRN_ons, UPRN_hmrc, STATUS_ons, STATUS_hmrc)) %>%
  group_by(id) %>%  
  mutate(compare_simple = simplify_eval(compare_status) ) %>% 
  ungroup()
matched_groups$compare_simple <- factor(matched_groups$compare_simple,       #just to get nice ordering on the categories names
                                        levels = c('Top - single agreement', 'Top - multiple agreement', 'Top - parent agreement', 'In set - single (parent) agreement', 
                                                   'In set - multiple (parent) agreement', 'disagreement', 'ons_only', 'hmrc_only', 'both_missing'))
print(t(t(table(matched_groups$compare_simple))))
#expected output:
####################################
#Top - single agreement               18903
#Top - multiple agreement              1339
#Top - parent agreement                 241
#In set - single (parent) agreement     871
#In set - multiple (parent) agreement   150
#disagreement                           199
#ons_only                              2566
#hmrc_only                               13
#both_missing                            20
######################################

# apply the second evaluation method:
joined <- joined %>% 
  group_by(id) %>% 
  mutate(compare_hmrc_candidate = unlist(lapply(1:n(), function(i) eval_one_candidate(UPRN_ons, UPRN_hmrc[i], STATUS_ons, STATUS_hmrc[i]))),
         compare_simple = simplify_one_eval(UPRN_ons, UPRN_hmrc, compare_hmrc_candidate, STATUS_ons)) %>%
  ungroup()
joined$compare_simple <- factor(joined$compare_simple,      #just to get nice ordering on the categories names again
                                levels = c('Top - single agreement', 'Top - multiple agreement', 'Top - parent agreement', 'In set - single (parent) agreement', 
                                           'In set - multiple (parent) agreement', 'disagreement', 'ons_only (top)', 'ons_only (set)', 'hmrc_only', 'both_missing'))
matched_groups2 <- joined %>% 
  group_by(id) %>% 
  summarise(compare_simple=compare_simple[1]) 
print(t(t(table(matched_groups2$compare_simple))))    # looks the same as above, hurray 
#expected output:
#######################################
#Top - single agreement               18903
#Top - multiple agreement              1339
#Top - parent agreement                 241
#In set - single (parent) agreement     871
#In set - multiple (parent) agreement   150
#disagreement                           199
#ons_only (top)                        2424
#ons_only (set)                         142
#hmrc_only                               13
#both_missing                            20
#######################################


# export some subsets for clerical resolutions ...
clerical <- joined %>% filter(compare_simple != 'Top - single agreement') %>% arrange(desc(compare_simple), id, -score, UPRN_ons)
hmrc_only <- joined %>% filter(compare_simple = 'hmrc_only')  # yap, long bussiness names are causing troubles to our fallback query
disagree  <- joined %>% filter(compare_simple = 'disagreement')

write.csv(disagree, file= output_filename, row.names = F)