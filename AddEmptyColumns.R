#################################################################################
############ Data and verification variables for Lebanon Year 1 & 2 ############
# DATA <- list(R=R, ML_Y2=ML_Y2, 
#             EGRAY1B=EGRA_Y1B, EGRAY1M=EGRA_Y1M, EGRAY1E=EGRA_Y1E, 
#             EGMAY1B=EGMA_Y1B, EGMAY1M=EGMA_Y1M, EGMAY1E=EGMA_Y1E,
#             EGRA_Y2B=EGRA_Y2B,  EGRA_Y2E=EGRA_Y2E,
#             ODK_Y1B=ODK_Y1B, ODK_Y1M=ODK_Y1M, ODK_Y1E=ODK_Y1E, 
#             ODK_Y2B=ODK_Y2B, ODK_Y1E=ODK_Y2E,
#             PQ_Y1=PQ_Y1, PQ_Y2B=PQ_Y2B, PQ_Y2E=PQ_Y2E,
#             RACER_bi_Y1B=RACER_bi_Y1B, RACER_wm_Y1B=RACER_wm_Y1B,
#             RACER_bi_Y1M=RACER_bi_Y1M, RACER_wm_Y1M=RACER_wm_Y1M,
#             RACER_bi_Y1E=RACER_bi_Y1E, RACER_wm_Y1E=RACER_wm_Y1E,
#             SDQ_Y1B=SDQ_Y1B, SDQ_Y1M=SDQ_Y1M, SDQ_Y1E=SDQ_Y1E,
#             TOCA_Y2E=TOCA_Y2E)
#varlist <- varlist <-  c("STUDENT_ID", "NAME", "FEMALE", "AGE", "MOTHER_NAME", "FATHER_NAME", "FAM_NAME", "GRADE","Y1C1CID1", "Y2C1CID1", "Y1SCHOOL", "Y2SCHOOL", "Y1SCHOOL_ID", "Y2SCHOOL_ID")
#################################################################################

# Function: 
add.empty <- function(DATA, R=NULL, VARLIST){
  "%not_in%" <- function(x,table){match(x,table,nomatch = 0)==0}
  for(d in 1:length(DATA)){
    add <-  VARLIST[VARLIST %not_in% c(names(DATA[[d]]))]
    if(length(add)>0){
      DATA[[d]] <- cbind(DATA[[d]], setNames(lapply(add, function(x) x=NA), add))
    }
  }
  if(!is.null(R)){
    add <-  VARLIST[VARLIST %not_in% c(names(R))]
    if(length(add)>0){
      R <- cbind(R, setNames(lapply(add, function(x) x=NA), add))
    }
  }
  return(DATA)
}

