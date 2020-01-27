######################################################
######################################################
#### ID matching based on names and 2 more character variables
######################################################
######################################################

### Example using ToCA problem:
# OUTPATH= paste0("/Users/",user,"/Box Sync/Box 3EA Team Folder/Data Management/2017-2018 Lebanon/")
# COUNTRY="LEBANON"
# NAME="STUDENT_NAME"
# MASTERLIST=M
# STUDENT_ID="STUDENT_ID"
# VAR1="TEACHER_NAME"
# VAR1ID="TEACHER_ID"
# VAR2="LOCATION_NAME"
# VAR2ID="LOCATION_ID"
# SURVEYDATA=Data
#######################################################


ID.retrieval <- function(COUNTRY,SURVEYDATA,MASTERLIST,NAME,STUDENT_ID,VAR1,VAR1ID,VAR2,VAR2ID,OUTPATH){

library(RecordLinkage)

################################################################################################################
################################################################################################################
#### NAME based matches starting at a 10% margin of error
################################################################################################################
################################################################################################################
SN_MATCHES <- list()
SIDs       <- list()
ITER <- length(SURVEYDATA[,paste0(NAME)])

for(s in 1:ITER){
  print(paste0("Percentage of survey entries matched to masterlist:",s/ITER))
  
  if(grepl("^leb",COUNTRY,ignore.case = T)){ ## Lebanon ID's are split by region
    region  <- ifelse(SURVEYDATA$REGION[s]==1|SURVEYDATA$REGION[s]=="A","A","B")
  }else{
    region <- "S"            ## Niger IDs all start with "S"
  }
  
  LOCA                       <- grepl(paste0(region),MASTERLIST[,STUDENT_ID])
  MASTER                     <- MASTERLIST[LOCA,paste0(NAME)]
  rset                       <- nrow(MASTERLIST[LOCA,])
  Perc                       <- rep(NA,rset)
  NAMECONTAINED              <- rep(FALSE,rset)
  BinA                       <- rep(NA, rset)
  AinB                       <- rep(NA, rset)
  SURVEY                     <- SURVEYDATA[s,paste0(NAME)]
  
  ## Subset the masterlist pool to look for matches only in the Region of that student
  for(m in 1:rset){
    print(paste0("Survey entry being matched:",s,"  Masterlist Progress for that entry:",m/rset))
    BinA[m]                  <- grepl(SURVEY,MASTER[m],ignore.case = T)
    AinB[m]                  <- grepl(MASTER[m],SURVEY,ignore.case = T)
    Perc[m]                  <- ceiling(max(nchar(SURVEY),nchar(MASTER[m]),na.rm = T)*.1)
    NAMECONTAINED[m]         <- BinA[m]==T | AinB[m]==T
  }
  ##If more than 90% of the letters match, match it
  SN_MATCHES[[s]]            <- !((levenshteinDist(gsub("[[:space:]]+","",rep(SURVEY,rset)),
                                                   gsub("[[:space:]]+","",MASTER))) > Perc) | NAMECONTAINED==T
  SIDs[[s]]                  <- MASTERLIST[LOCA,STUDENT_ID][SN_MATCHES[[s]]]
}

SIDs[lapply(SIDs,length)==0]   <- "NULL"
SIDs[lapply(lapply(SIDs,is.na),all)==TRUE] <- "NULL"


saveRDS(SIDs,paste0(OUTPATH,"StudentMatches_10percent.rds"))
SIDs <- readRDS(paste0(OUTPATH,"StudentMatches_10percent.rds"))



################################################################################################################
################################################################################################################
#### TEACHER-NAME based matches or second nesting character variable (could be phone number too)
################################################################################################################
################################################################################################################

TN_MATCHES <- list()
TIDs       <- list()
ITER       <- length(SURVEYDATA[,paste0(NAME)])
               
for(s in 1:length(SURVEYDATA[,VAR1])){
  print(paste0("Unique ",VAR1," case:",s," Progress: ",s/ITER))
  
  if(grepl("^leb",COUNTRY,ignore.case = T)){ ## Lebanon ID's are split by region
    ## Only search for teachers (or VAR1s)that are in the region of that student
    region <- ifelse(SURVEYDATA$REGION[s]==1|SURVEYDATA$REGION[s]=="A","A","B")
  }else{
    region <- "S"            ## Niger IDs all start with "S"
  }
  
  ## Keep VAR1 level unique observations only
  MT                         <- MASTERLIST[!duplicated(MASTERLIST[,VAR1ID]),]
  ## Subset the masterlist pool to look for matches by region and by unique VAR1 or VAR2
  LOCA                       <- grepl(paste0(region),MT[,STUDENT_ID])
  rset                       <- nrow(MT[LOCA,])
  Perc                       <- rep(NA,rset)
  NAMECONTAINED              <- rep(FALSE,rset)
  BinA                       <- rep(NA, rset)
  AinB                       <- rep(NA, rset)
  MASTER                     <- MT[LOCA,VAR1]
  SURVEY                     <- SURVEYDATA[s,VAR1]
  
  for(m in 1:rset){
    #print(paste0("Survey case:",s,"  Masterlist Progress for that case:",m/rset))
    BinA[m]             <- grepl(SURVEY,MASTER[m],ignore.case = T)
    AinB[m]             <- grepl(MASTER[m],SURVEY,ignore.case = T)
    Perc[m]             <- ceiling(max(nchar(SURVEY),nchar(MASTER[m]),na.rm = T)*.1)
    NAMECONTAINED[m]    <- BinA[m]==T | AinB[m]==T
  }
  ##If more than 90% of the letters match, match it
  TN_MATCHES[[s]]                <- !((levenshteinDist(gsub("[[:space:]]+","",rep(SURVEY,rset)), gsub("[[:space:]]+","",MASTER))) > Perc) | NAMECONTAINED==T
  TIDs[[s]]                      <- MT[,VAR1ID][LOCA][TN_MATCHES[[s]]]
}

TIDs[lapply(TIDs,length)==0] <- "NULL"
TIDs[lapply(lapply(TIDs,is.na),all)==TRUE] <- "NULL"
TIDs[unlist(lapply(TIDs,length))>1]

saveRDS(TIDs,paste0(OUTPATH,VAR1,"Matches_10percent.rds"))
TIDs <- readRDS(paste0(OUTPATH,VAR1,"Matches_10percent.rds"))




################################################################################################################
################################################################################################################
#### SITE based matches or VAR2 (3rd nesting level) matches, could be school, teacher 
#### (if not used as 2nd nesting level matching), etc
################################################################################################################
################################################################################################################

LN_MATCHES <- list()
LIDs       <- list()
ITER <- length(SURVEYDATA[,paste0(NAME)])

for(s in 1:length(SURVEYDATA[,VAR2])){
  print(paste0("Unique", VAR2," survey case being matched:",s," Progress: ",s/ITER))
  
  if(grepl("^leb",COUNTRY,ignore.case = T)){ ## Lebanon ID's are split by region
    ## Only search for teachers (or VAR1s)that are in the region of that student
    region <- ifelse(SURVEYDATA$REGION[s]==1|SURVEYDATA$REGION[s]=="A","A","B")
  }else{
    region <- "S"            ## Niger IDs all start with "S"
  }
  
  ## Subset the masterlist pool to look for matches by Region and by unique LOCATION or Location IDs
  ML                         <- MASTERLIST[!duplicated(MASTERLIST[,VAR2ID]),]
  LOCA                       <- grepl(paste0(region),ML[,STUDENT_ID])
  rset                       <- nrow(ML[LOCA,])
  Perc                       <- rep(NA,rset)
  NAMECONTAINED              <- rep(FALSE,rset)
  BinA                       <- rep(NA, rset)
  AinB                       <- rep(NA, rset)
  MASTER1                    <- ML[LOCA,VAR2]
  MASTER2                    <- ML[LOCA,VAR2ID]
  SURVEY                     <- SURVEYDATA[s,VAR2]
  for(m in 1:rset){
    print(paste0(VAR2," survey case: ",s," Masterlist Progress: ",m/rset))
    
    ## They entered the Location ID sometimes, the location name sometimes, and sometimes both
    ## So this code needs to be expanded a bit
    BinA[m]             <- grepl(SURVEY,MASTER1[m],ignore.case = T)|grepl(SURVEY,MASTER2[m],ignore.case = T)
    AinB[m]             <- grepl(MASTER1[m],SURVEY,ignore.case = T)|grepl(MASTER2[m],SURVEY,ignore.case = T)
    Perc[m]             <- ceiling(max(nchar(SURVEY),nchar(MASTER1[m]),na.rm = T)*.1)
    NAMECONTAINED[m]    <- BinA[m]==T | AinB[m]==T
  }
  ##If more than 90% of the letters match, match it
  LN_MATCHES[[s]]                <- NAMECONTAINED==T
  LIDs[[s]]                      <- MASTER2[LN_MATCHES[[s]]]
}

LIDs[lapply(LIDs,length)==0] <- "NULL"
LIDs[lapply(lapply(LIDs,is.na),all)==TRUE] <- "NULL"
LIDs[unlist(lapply(LIDs,length))>1]

saveRDS(LIDs,paste0(OUTPATH,VAR2,"Matches_10percent.rds"))
LIDs <- readRDS(paste0(OUTPATH,VAR2,"Matches_10percent.rds"))


################################################################################################################
################################################################################################################
### Reduce the ID possibilities to students that are in the matched locations
################################################################################################################
################################################################################################################

MS_NNL    <- SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] ## Multiple student matches leaving out null Location IDs
ML_NNL    <- LIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] ## Location matches leaving out null Location IDs
Reduction <- list()

for(l in 1:length(MS_NNL)){
  Reduction[[l]] <- MASTERLIST[(MASTERLIST[,STUDENT_ID] %in% MS_NNL[[l]]) & (MASTERLIST[,VAR2ID] %in% ML_NNL[[l]]),STUDENT_ID]
}

blanksL <- SIDs
blanksL[which(unlist(lapply(blanksL,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] <- Reduction
saveRDS(blanksL,paste0(OUTPATH,"NO_",VAR2,"_MATCH_10percent.rds"))
blanksL <- readRDS(paste0(OUTPATH,"NO_",VAR2,"_MATCH_10percent.rds"))

## If the length of an entry in the reduction object is 0, then it means the Location IDs(VAR2ID) of the found matches don't correspond to the masterlist
## And we want to flag that observation
LOCATION_FLAG     <- lapply(blanksL,length)==0

## Reverse those blank cases to the originally found matches
Reduction[lapply(Reduction,length)==0] <- SIDs[lapply(blanksL,length)==0]

## Then reduce the SIDs multiple matches
SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] <- Reduction


################################################################################################################
################################################################################################################
### Reduce the ID possibilities to students that are registered under the matched teachers (VAR1)
################################################################################################################
################################################################################################################

MS_NNT    <- SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] ## Multiple student matches leaving out null Teacher IDs (VAR1ID)
MT_NNT    <- TIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] ## Teacher (VAR1) matches leaving out null teacher IDs
Reduction <- list()

for(l in 1:length(MS_NNT)){
  Reduction[[l]] <- MASTERLIST[(MASTERLIST[,STUDENT_ID] %in% MS_NNT[[l]]) & (MASTERLIST[,VAR1ID] %in% MT_NNT[[l]]),STUDENT_ID]
}

blanksT          <- SIDs
blanksT[which(unlist(lapply(blanksT,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] <- Reduction
saveRDS(blanksT,paste0(OUTPATH,"NO_",VAR1,"_MATCH_10percent.rds"))
blanksT          <- readRDS(paste0(OUTPATH,"NO_",VAR1,"_MATCH_10percent.rds"))

## If the length of an entry in the reduction object is 0, then it means the VAR1 ID of the found matches don't correspond to the masterlist

## And, first,  we want to flag that observation
TEACHER_FLAG     <- lapply(blanksT,length)==0

## Then, we want to reverse those blank cases to the originally found matches, ignoring the VAR1 criteron
Reduction[lapply(Reduction,length)==0] <- SIDs[lapply(blanksT,length)==0]

## For the rest of the cases that did have a correspondence for VAR1, we then reduce the SIDs multiple matches based on VAR1
SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] <- Reduction

saveRDS(SIDs,paste0(OUTPATH,"StudentMatchesReduced_10percent.rds"))
SIDs      <- readRDS(paste0(OUTPATH,"StudentMatchesReduced_10percent.rds"))
print(paste("Number of unmatched students after 10%:", sum(SIDs=="NULL")))



################################################################################################################
################################################################################################################
#### Second round of STUDENT-NAME based matches but with more relaxed criterion
################################################################################################################
################################################################################################################

SN_MATCHES2 <- list()
SIDs2       <- list()
NULLS <- SURVEYDATA[SIDs=="NULL",]
ITER  <- nrow(NULLS)

for(s in 1:ITER){
  print(paste0("Second-round matching at 20%. Student case:",s/ITER))
  
  if(grepl("^leb",COUNTRY,ignore.case = T)){ ## Lebanon ID's are split by region
    region <- ifelse(SURVEYDATA$REGION[s]==1|SURVEYDATA$REGION[s]=="A","A","B")
  }
  if(grepl("^nig",COUNTRY,ignore.case = T)){
    region <- "S"            ## Niger IDs all start with "S"
  }
  
  LOCA                       <- grepl(paste0(region),MASTERLIST[,STUDENT_ID])
  rset                       <- nrow(MASTERLIST[LOCA,])
  Perc                       <- rep(NA,rset)
  NAMECONTAINED              <- rep(FALSE,rset)
  BinA                       <- rep(NA, rset)
  AinB                       <- rep(NA, rset)
  MASTER                     <- MASTERLIST[LOCA, paste0(NAME)]
  NAMES                      <- NULLS[s,paste0(NAME)]
  
  ## Subset the masterlist pool to look for matches by Region
  for(m in 1:rset){
    BinA[m]                  <- grepl(NAMES,MASTER[m],ignore.case = T)
    AinB[m]                  <- grepl(MASTER[m],NAMES,ignore.case = T)
    Perc[m]                  <- ceiling(max(nchar(NAMES),nchar(MASTER[m]),na.rm = T)*.2)
    NAMECONTAINED[m]         <- BinA[m]==T | AinB[m]==T
  }
  
  ##If more than 80% of the letters match, match it
  SN_MATCHES2[[s]]            <- !((levenshteinDist(gsub("[[:space:]]+","",rep(NAMES,rset)),gsub("[[:space:]]+","",MASTER))) > Perc) | NAMECONTAINED==T
  SIDs2[[s]]                  <- MASTERLIST[LOCA,STUDENT_ID][SN_MATCHES2[[s]]]
}

SIDs2[lapply(SIDs2,length)==0]   <- "NULL"
SIDs[lapply(lapply(SIDs,is.na),all)==TRUE] <- "NULL"
saveRDS(SIDs2,paste0(OUTPATH,"StudentMatches_20percent.rds"))
SIDs2 <- readRDS(paste0(OUTPATH,"StudentMatches_20percent.rds"))


## Substitute the NULL matches in SIDs with the matches found in SIDs2 under the wider criteria
SIDs[SIDs=="NULL"] <- SIDs2



################################################################################################################
################################################################################################################
### Again, reduce the ID possibilities to students that are in the matched locations
################################################################################################################
################################################################################################################

MS_NNL    <- SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] ## Multiple student matches leaving out null Location IDs
ML_NNL    <- LIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] ## Location matches leaving out null Location IDs
Reduction <- list()

## Only look for possible reductions if there are any survey-takers with multiple matches with a retrieved VAR2ID, otherwise no reduction
### is possible using the VAR2ID criterion
if(length(MS_NNL)>0){
  for(l in 1:length(MS_NNL)){
    Reduction[[l]] <- MASTERLIST[(MASTERLIST[,STUDENT_ID] %in% MS_NNL[[l]]) & (MASTERLIST[,VAR2ID] %in% ML_NNL[[l]]),STUDENT_ID]
  }
  
  blanksL2 <- SIDs
  blanksL2[which(unlist(lapply(blanksL2,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] <- Reduction
  
  saveRDS(blanksL2,paste0(OUTPATH,"NO_",VAR2,"_MATCH_20percent.rds"))
  blanksL2         <- readRDS(paste0(OUTPATH,"NO_",VAR2,"_MATCH_20percent.rds"))
  
  ## If the length of an entry in the reduction object is 0, then it means the Location IDs of the found matches don't correspond to the masterlist
  ## And we want to flag that observation
  LOCATION_FLAG[LOCATION_FLAG==F]        <- lapply(blanksL2[LOCATION_FLAG==F],length)==0
  
  ## Reverse those blank cases to the originally found matches
  Reduction[lapply(Reduction,length)==0] <- SIDs[lapply(blanksL2,length)==0]
  
  SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(LIDs,function(x){any(x!="NULL")})))] <- Reduction
}


######################## VAR1ID-based reductions #############################################################
### Reduce the ID possibilities to students that are registered under the matched teachers (VAR1)
MS_NNT    <- SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] ## Multiple student matches leaving out null Teacher IDs
MT_NNT    <- TIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] ## Teacher matches leaving out null teacher IDs
Reduction <- list()

## Only look for possible reductions if there are any survey-takers with multiple matches with a retrieved VAR1ID, otherwise no reduction
### is possible using the VAR1/iD criterion

if(length(MS_NNT)>0){
  for(l in 1:length(MS_NNT)){
    Reduction[[l]] <- MASTERLIST[(MASTERLIST[,STUDENT_ID] %in% MS_NNT[[l]]) & (MASTERLIST[,VAR1ID] %in% MT_NNT[[l]]),STUDENT_ID]
  }
  
  blanksT2 <- SIDs
  blanksT2[which(unlist(lapply(blanksT2,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] <- Reduction
  
  saveRDS(blanksT2,paste0(OUTPATH,"NO_",VAR1,"_MATCH_20percent.rds"))
  blanksT2  <- readRDS(paste0(OUTPATH,"NO_",VAR1,"_MATCH_20percent.rds"))
  
  ## If the length of an entry in the reduction object is 0, then it means the teacher IDs of the found matches don't correspond to the masterlist
  ## And we want to flag that observation
  TEACHER_FLAG[TEACHER_FLAG==F]          <- lapply(blanksT2[TEACHER_FLAG==F],length)==0
  
  ## Reverse those blank cases to the originally found matches
  Reduction[lapply(Reduction,length)==0] <- SIDs[lapply(blanksT2,length)==0]
  
  SIDs[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(TIDs,function(x){any(x!="NULL")})))] <- Reduction
}



print(paste("Cases remaining with multiple matches:",length(SIDs[lapply(SIDs,length)>1])))

saveRDS(SIDs,paste0(OUTPATH,"StudentMatchesReduced_20percent.rds"))
SIDs         <- readRDS(paste0(OUTPATH,"StudentMatchesReduced_20percent.rds"))



################################################################################################################
################################################################################################################
#### Append matches
#### Simple
################################################################################################################
################################################################################################################

SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]     <- unlist(lapply(SIDs,function(x)as.character(paste0(x,collapse = "|"))))

SURVEYDATA[,paste0("Retrieved_",VAR2ID)]         <- unlist(lapply(LIDs,function(x)as.character(paste0(x,collapse = "|"))))

SURVEYDATA[,paste0("Retrieved_",VAR1ID)]         <- unlist(lapply(TIDs,function(x)as.character(paste0(x,collapse = "|"))))



################################################################################################################
################################################################################################################
####### Verify that all the matches found do correspond to a child in that location (or nesting variable VAR2)
####### Otherwise, create a flag
################################################################################################################
################################################################################################################

#### PROBLEM HERE ####
SURVEYDATA$LOCATION_FLAG <- rep(FALSE,nrow(SURVEYDATA)) ## or VAR2 flag
## Check if the location ID(s) retrieved from the survey data for each student, match the location ID in the Masterlist for the student ID that was retrieved based on the student names.
## If it/they doesn't/don't, then flag it

for(s in 1:length(SURVEYDATA$LOCATION_FLAG[SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR2ID)]!="NULL"])){
  print(s)
  SURVEYDATA$LOCATION_FLAG[SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR2ID)]!="NULL"][s] <- 
    any(!grepl(SURVEYDATA[,paste0("Retrieved_",VAR2ID)][SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR2ID)]!="NULL"][s], 
               MASTERLIST[,paste0(VAR2ID)][grepl(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)][SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR2ID)]!="NULL"][s],MASTERLIST[,paste0(STUDENT_ID)])]))
}

## Add the flags from the multiple matches
SURVEYDATA$LOCATION_FLAG[SURVEYDATA$LOCATION_FLAG==FALSE] <- LOCATION_FLAG[SURVEYDATA$LOCATION_FLAG==FALSE]

#######################################################################################
####### Verify that all the matches found do correspond to a child under that VAR1 (teacher or first nesting level)
####### Otherwise, create a flag
#######################################################################################


SURVEYDATA$TEACHER_FLAG <- rep(FALSE,nrow(SURVEYDATA))
## Check if the VAR1IDs retrieved from the survey data for each student, match the VAR1IDs in the Masterlist for the student ID that was retrieved based on the student names.
## If it/they doesn't/don't, then flag it

for(s in 1:length(SURVEYDATA$TEACHER_FLAG[SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR1ID)]!="NULL"])){
  print(s)
  SURVEYDATA$TEACHER_FLAG[SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR1ID)]!="NULL"][s] <- any(!grepl(SURVEYDATA[,paste0("Retrieved_",VAR1ID)][SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR1ID)]!="NULL"][s], MASTERLIST[,VAR1ID][grepl(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)][SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"& SURVEYDATA[,paste0("Retrieved_",VAR1ID)]!="NULL"][s],MASTERLIST[,STUDENT_ID])]))
}

## Add the flags from the multiple matches
SURVEYDATA$TEACHER_FLAG[SURVEYDATA$TEACHER_FLAG==FALSE] <- TEACHER_FLAG[SURVEYDATA$TEACHER_FLAG==FALSE]

names(SURVEYDATA)[names(SURVEYDATA)=="TEACHER_FLAG"]  <- gsub("_NAME|_ID","",paste0(VAR1,"_FLAG"))
names(SURVEYDATA)[names(SURVEYDATA)=="LOCATION_FLAG"] <- gsub("_NAME|_ID","",paste0(VAR2,"_FLAG"))

## Also create explicit flags for unmatched and multiple match cases
SURVEYDATA$NO_MATCH       <- rep(FALSE,nrow(SURVEYDATA))
SURVEYDATA$MULTIPLE_MATCH <- rep(FALSE,nrow(SURVEYDATA))
SURVEYDATA$NO_MATCH[which(unlist(lapply(SIDs,function(x){any(x=="NULL")})))] <- TRUE
SURVEYDATA$MULTIPLE_MATCH[which(unlist(lapply(SIDs,length))>1 & unlist(lapply(SIDs,function(x){any(x!="NULL")})))] <- TRUE

## Create a flag for duplicated matches
#View(SURVEYDATA[duplicated(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)])|duplicated(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)],fromLast = T),])
SURVEYDATA$DUPLICATE_FLAG <- (duplicated(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)])|duplicated(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)],fromLast = T)) & SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]!="NULL"
SURVEYDATA                <- SURVEYDATA[order(SURVEYDATA[,paste0("Retrieved_",STUDENT_ID)]),]


## Bring the informative variables for human verification to the front:
SURVEYDATA <- SURVEYDATA[,c(names(SURVEYDATA)[grepl(paste(STUDENT_ID,VAR1ID,VAR2ID,"FLAG","MATCH",sep = "|"),names(SURVEYDATA))]
                            ,
                            names(SURVEYDATA)[!grepl(paste(STUDENT_ID,VAR1ID,VAR2ID,"FLAG","MATCH",sep = "|"),names(SURVEYDATA))])]

##
write.csv(SURVEYDATA,paste0(OUTPATH,"Retrieved_IDs_and_Problems.csv"))
file.remove(paste0(OUTPATH,c("StudentMatches_10percent.rds",
              paste0(VAR1,"Matches_10percent.rds"),
              paste0(VAR2,"Matches_10percent.rds"),
              paste0("NO_",VAR2,"_MATCH_10percent.rds"),
              paste0("NO_",VAR1,"_MATCH_10percent.rds"),
              "StudentMatchesReduced_10percent.rds",
              "StudentMatches_20percent.rds",
              paste0("NO_",VAR2,"_MATCH_20percent.rds"),
              paste0("NO_",VAR1,"_MATCH_20percent.rds"),
              "StudentMatchesReduced_20percent.rds")))
}


