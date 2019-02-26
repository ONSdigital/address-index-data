#####prepare data from ledr

library("dplyr")
library("tidyr")
library("ggplot2")
setwd('~/R')

####################################################################
####prepare the files from ledr for API
####################################################################
folder_name <- "//tdata8/AddressIndex/Beta_Results/extra/LEDR2/"

filenames <- Sys.glob(paste0(folder_name, "*.txt"))

ledr_list <- lapply(filenames, read.table, header =T, quote='"',
                    sep="|", stringsAsFactors=F, colClasses = "character")
for (i in 1:4){
  ledr_list[[i]] <- ledr_list[[i]] %>% 
    mutate(ADDRESS = gsub(',', '',paste(FLAT_NUMBER, BUILDING_NAME, BUILDING_OR_HOUSE, 
                                   LINE_1, LINE_2, LINE_3, LINE_4, TOWN, POSTCODE)),
           ledr_compare = ifelse(i%%2==0, 'same', 'different'))
}

ledr <- do.call('bind_rows', ledr_list)

ledr_min <- ledr %>% select (ADDRESS, MATCHED_POSTCODE, MANUAL_POSTCODE, ledr_compare)%>%
  mutate(ID = 1:nrow(ledr))
  
write.csv(ledr_min[,c(5, 1)], file=paste0(folder_name, "ledr_minimal.csv"), row.names = F, quote=F)

####################################################################
#### run python 'run_baseline.py' or upload through web bulk
####################################################################
#### process the response
####################################################################
#ledr_res <- read.csv(paste0(folder_name, "ledr_response_web.csv"), header= T, stringsAsFactors = F)
ledr_res <- read.csv(paste0(folder_name, "LEDR_debug_codes.csv"), header= T, stringsAsFactors = F)
ledr_res <- ledr_res %>% select(id, inputAddress, matchedAddress = matchedFormattedAddress,
                                documentScore = score, buildingScore = structuralScore, unitScore )
ledr_res <- ledr_res %>% mutate(AI_postcode = gsub(".*, ([^,]*)$", "\\1", matchedAddress))

### add confidence score
h_score_formula <- function(structuralScore, unitScore, h_case ='A'){
  unitScoreNA <- ifelse(unitScore==-1, .2, unitScore)
  alpha <- ifelse(h_case=='A', .8, .9)
  res <- structuralScore*(alpha+(.99-alpha)*unitScoreNA)
  res
} 
e_sigmoid <- function(x) 1/(1+exp(15*(.99-x)))
h_power <- function(h) h^6

ledr_res <- ledr_res  %>%
  group_by(id) %>% mutate(e_ratio = documentScore/(mean(head(documentScore,2)) )) %>%
  ungroup() %>% 
  mutate(h_score = h_score_formula(buildingScore, unitScore),
         simple_confidence = pmax(e_sigmoid(e_ratio), h_power(h_score))) 

ledr_ai <- ledr_res %>% group_by(id, inputAddress) %>%
  arrange(id, -simple_confidence ) %>%
  mutate(AI_alternative = ifelse(length(unique(AI_postcode))<2, NA, unique(AI_postcode)[2]),
         AI_alternative_confidence = ifelse(is.na(AI_alternative), NA, 
                                            simple_confidence[AI_postcode == AI_alternative])
            )%>%
  summarise(AI_postcode = AI_postcode[1],
            AI_confidence = simple_confidence[1],
            AI_alternative = AI_alternative[1],
            AI_alternative_confidence = AI_alternative_confidence[1])          
            
####################################################################
### join back to matchcode and compare
####################################################################
ledr_both <- full_join (ledr_ai, ledr_min, by = c("id"="ID", "inputAddress"= "ADDRESS" ))

ledr_both <- ledr_both %>% mutate(matched_ai = ifelse(is.na(MATCHED_POSTCODE), NA, 
                                                      ifelse(is.na(AI_confidence), 'below', ifelse(AI_confidence <= .6, 'below',
                                                             as.character(AI_postcode==MATCHED_POSTCODE)))),
                                  manual_ai = ifelse(is.na(MANUAL_POSTCODE), NA, 
                                                     ifelse(is.na(AI_confidence), 'below', ifelse(AI_confidence <= .6, 'below',
                                                            as.character(AI_postcode==MANUAL_POSTCODE)))))

table(ledr_both$manual_ai, ledr_both$ledr_compare)
table(ledr_both$matched_ai, ledr_both$ledr_compare)

ledr_both <- ledr_both %>% mutate(AI_disagree = is.na((AI_postcode==MATCHED_POSTCODE)|(AI_postcode==MANUAL_POSTCODE)))

ledr_bad <- ledr_both %>% filter(AI_disagree)# %>%
  arrange(is.na(MATCHED_POSTCODE), -AI_confidence)
  
write.csv(ledr_bad, file=paste0(folder_name, "ledr_disagreement.csv"), row.names = F, quote=F)
write.csv(ledr_both, file=paste0(folder_name, "ledr_full_join.csv"), row.names = F, quote=F)


#########################################################################
### check the performance of their alghorithm - matching to input when 2+ candidates >60%


ledr_both <- read.csv(file=paste0(folder_name, "ledr_full_join.csv"))

write.csv(ledr_both %>% filter(AI_alternative_confidence>.6), file=paste0(folder_name, "ledr_ambiguous.csv"), row.names = F, quote=F)

