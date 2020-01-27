########## STANDARD VERIFICATION FUNCTION  ########
###################################################
###### **Note: ID variables should be standardized
######   (within and across datasets),
######   and cleaned before applying this function**
###################################################
###################################################

########## Exports basic problematic cases into excel files for manual  verification
########## It works with a simple comparison against one reference file (e.g. Masterlist, Randomization file, etc)

########## Exports duplicate IDs
########## Exports unmatched cases
########## Creates flags based on subject names (Students, Teachers, Schools, depending the level of the survey)
########## Creates flags based on a group of variables specified by the user


########## DATA:    Databases that need to be verified, specified as a list
################### e.g. DATA=list(EGRA=EGRAdata,EGMA=EGMAdata,ODK=ODKdata), DATA=list(BASE=BASELINE, MID=MIDLINE, END=ENDLINE), or if only one: DATA=list(TOCA=TOCAdata)
################### For each element of the list you must write a short name to distinguish that dataset in the function and the name of the
################### imported data.

########## ID:      ID variable name used to link the database with the reference files, specified as string
################### e.g. ID="ST_ID"
################### **Note: make sure that this variable is called and formatted in the same way across all databases and reference file**

########## UNIQUEID: Variable that if combined with the ID variable can function as a unique identifier of each ASSESSMENT in the data (not person)
################### **Note: it doesn't matter if the same person took it twice we want one unique identifier per survey entry.
################### (the only cases in which it is OK for the the identifier+ID combination not to be unique is if the system re-sent the 
################### same entry twice, in which case there would be no way to uniquely distinguish two surveys, and
################### those two would share the same "unique" ID) These are the cases classified as "IDENTICAL" and this function will address them, 
################### but they don't often happen.  
################### E.g. UNIQUEID=c("START_TIME","START_TIME","START_TIME"), or UNIQUEID=c("SUBMISSION_TIME","START_TIME","DATE")

########## REFERENCE: Administrtive database, previously wave of data collection, or any other file that can be used as a reference for verification.

########## NAMES:   Group of name variable(s) (multiple only if at the student level), specified in the form of a character vector 
################### e.g. NAMES = c("NAME", "FATHER_NAME","MOTHER_NAME") or "TEACHER_NAME"
###################   E.g. if you want to include "NAME" as a criterion but not "NAMETAG" make sure you either change the name
###################   of one of them or that you subset the datasets to the relevant variables for verification before applying the function**

########## VARS:    Group of variables that wiill be used for verification, specified by the user in the form of a character vector 
################### e.g. VARS = c("AGE","GRADE","SCHOOL_ID","PACKET")
################### **Note: make sure that these variables are called and formatted in the same way
###################   after importing both the assessment data and the reference data.**
################### **Note2: make sure that there are no other variables in the data set that contain these names in theirs
###################   E.g. if you want to include "CLASS" as a criterion but not "CLASSIFICATION" make sure you either change the name
###################   of one of them or that you subset the datasets to the relevant variables for verification before applying the function**
########## NMARGIN: Percentage of letter discrepancy that we want to allow when comparing names without flagging.
################### e.g. NMARGIN = 0.1    (for 10%)

########## Vlowermargins and Vuppermargins: Specify in the form of a matrix how big of a difference you want to allow between the datasets 
################### and the reference file.
################### for the numeric variables without getting flagged
################### e.g. maximum a difference of +-1 year in age and a difference of 2 on the upper bound for grade and of 0 on the lower bound
################### Vlowermargins= c(1,0,NA,NA)
################### Vuppermargins= c(1,2,NA,NA)

########## OUTPATH: Path to the directory where the issues should be saved
################### e.g. OUTPATH = paste0("/Users/",user,"/Box Sync/Box 3EA Team Folder/Data Management/2016-17 3EA Niger Data Cleaning/Merges Pre-Imputation/ACROSS-WAVE MERGES/ISSUES/")

########## FILE:    Name root for the output files including the country letters, the year, the assessment, and the wave information
################### These should depart from the content of each one of the dataframes specified in "DATA" (in the same order, please).
################### e.g. FILE = "LBY2_EGRA_BASELINE" or FILE = "NGY1_TOCA_MIDLINE" or FILE=c("LBY2_EGRA_BASELINE","LBY2_EGMA_BASELINE","LBY2_ODK_BASELINE")

export.issues <- function(DATA, ID,UNIQUEID, REFERENCE, NAMES, VARS, NMARGIN, Vlowermargins,Vuppermargins,OUTPATH,FILE, FNPATH, debug_file = NULL, allow_duplicates = TRUE){
	## If no path was specified
	if(is.null(FNPATH)){
		print("You forgot to specify the PATH to the folder where all the TIES functions are stored. 
		This function has dependencies on other TIES functions, so make sure to fill in the 'FNPATH=' argument. For example: FNPATH=paste0('/Users/',user,'/Box Sync/Box 3EA Team Folder/Data Management/General Functions/')")
		}
	
  if (length(NAMES)==0){
    NAMES <- NA
  }
  if (length(VARS)==0){
    VARS <- NA
  }
  
  if(length(FILE)!=length(DATA)){
  	print("Looks like the number of DATABASES that you are using and the number of output FILE you entered are different.
  	Please check if you missed to specify how you want the issues for each database to be saved as in the FILE argument.")
  	}
  
  "%not_in%"  <- function(x,table){match(x,table,nomatch = 0)==0 }
  
  ### Temporary chunk for workshop --------------------------------------------------------
  ### -------------------------------------------------------------------------------------
  # STIDS <- DATA$ML_Y2[,c("STID_Y1","STID_Y2")]
  # 
  # DATA$EGRA_Y2B$STID_Y1 <- DATA$EGRA_Y2B$STID_Y1.y
  # DATA$EGRA_Y2B$STID_Y1.x <- NULL
  # DATA$EGRA_Y2B$STID_Y1.y <- NULL
  # 
  # DATA$ODK_Y2B$STID_Y1 <- DATA$ODK_Y2B$STID_Y1.y
  # DATA$ODK_Y2B$STID_Y1.x <- NULL
  # DATA$ODK_Y2B$STID_Y1.y <- NULL
  ### -------------------------------------------------------------------------------------
  ### -------------------------------------------------------------------------------------
  
  ##################################################################################
  ## Add empty columns if the variable doesn't already exist in that database
  #source(paste0("/home/",user,"/GLOBAL_TIES/FUNCTIONS/AddEmptyColumns.R"))
  source(paste0(FNPATH,"AddEmptyColumns.R"))

  DATA <- add.empty(DATA=DATA,R=REFERENCE, VARLIST=c(NAMES,VARS))
  DUPS <- list()
  for(d in 1:length(DATA)){
  	
    ## Keep an assessment-tagged unique identifier for the assessment that won't be changed throughout fixes so that we can always track back to the
    ### original state.
    eval(parse(text=paste0("DATA[[d]]$UNIQUEID_",names(DATA)[d], "<- paste0(DATA[[d]]$",ID,",DATA[[d]]$",UNIQUEID[d],")")))
    
    ## Duplicates
    DATA[[d]] <- DATA[[d]][order(DATA[[d]][,ID]),]
    dups <- duplicated(DATA[[d]][,ID])|duplicated(DATA[[d]][,ID],fromLast = T)
    
    # if(sum(dups)>0){
      # DUPS[[d]] <- DATA[[d]][dups,]
      # write.csv(DUPS[[d]],paste0(OUTPATH,FILE[d],"_DUPLICATES.csv"))
    # }
    
    ## Create a variable that indicates whether there is a duplicate of that ID in a given data set to help make the final verification manageable.
    DATA[[d]]$DUPLICATED       <- rep("NO",nrow(DATA[[d]]))
    DATA[[d]]$DUPLICATED[dups] <- paste0("ID DUPLICATED IN ",names(DATA)[d])
    #names(DATA[[d]])[names(DATA[[d]])=="DUPLICATED"] <- paste0("DUPLICATED_",names(DATA)[d])
    
    # ## Unmatched
    # write.csv(DATA[[d]][DATA[[d]][,ID] %not_in% REFERENCE[,ID],],paste0(OUTPATH,FILE[d],"_ONLYINDATA.csv"))
    # write.csv(REFERENCE[REFERENCE[,ID] %not_in% DATA[[d]][,ID] ,],paste0(OUTPATH,FILE[d],"_ONLYINREFERENCE.csv"))
    ## Keep only the variables used for verification
    DATA[[d]] <- DATA[[d]][,grepl(paste0("^",paste0(names(DATA[[d]])[names(DATA[[d]]) %in% c(VARS,NAMES,ID,UNIQUEID[d])],collapse = "$|^"),
                                         "|ASSESSMENT_TIME|DUPLICATED$|",ID,"$"),names(DATA[[d]]),ignore.case=T)]
    ## Add sufixes to the variables that the dataframes have in common and that will be used for verification
    names(DATA[[d]])[ names(DATA[[d]])!=ID]  <- paste0(names(DATA[[d]])[ names(DATA[[d]])!=ID],"_",names(DATA)[d])
  }
  
  ## Also check for duplicates in the reference file
  dups <- duplicated(REFERENCE[,ID])|duplicated(REFERENCE[,ID],fromLast = T)
  ## and create a flag for them
  REFERENCE$DUPLICATED_REFERENCE       <- rep("NO",nrow(REFERENCE))
  REFERENCE$DUPLICATED_REFERENCE[dups] <- "ID DUPLICATED IN REFERENCE"
  ## Keep only the variables used for verification in Reference file
  REFERENCE <- REFERENCE[,grepl(paste0("^",paste0(names(REFERENCE)[names(REFERENCE) %in% c(VARS,NAMES,ID,UNIQUEID[length(DATA)+1])],collapse = "$|^"),
                                       "|ASSESSMENT_TIME|DUPLICATED_REFERENCE|",ID,"$"),names(REFERENCE),ignore.case=T)]
  
  ####################################################################################
  ## Mismatched
  ####################################################################################
  ####################################################################################
  for(d in 1:length(DATA)){
    print("#######################")
    print(paste0(names(DATA)[d]))
    print(names(DATA[[d]]))
    print(length(names(DATA[[d]])))
  }
  
  ##################################################################################
  ## Merge databases
  # MERGE <- Reduce(function(...) merge(...,by=ID,all=T),DATA)
  merge_warn <- FALSE
  
  MERGE <- Reduce(function(x, y) {
      if (requireNamespace("data.table", quietly = TRUE)) {
          x <- data.table::as.data.table(x)
          y <- data.table::as.data.table(y)

          x_dups <- x[duplicated(x[[ID]]) | duplicated(x[[ID]], fromLast = TRUE), .N]
          y_dups <- y[duplicated(y[[ID]]) | duplicated(y[[ID]], fromLast = TRUE), .N]

          if (x_dups == 0L || y_dups == 0L) {
              return(merge(x, y, by = ID, all = TRUE))
          } else {
              print("Duplicates caught. Cartesian product imminent.")
              
              if (x_dups > 0L) {
                  print("lhs duplicates:")
                  print(x[duplicated(x[[ID]]) | duplicated(x[[ID]], fromLast = TRUE)][[ID]])
              }

              if (y_dups > 0L) {
                  print("rhs duplicates:")
                  print(y[duplicated(y[[ID]]) | duplicated(y[[ID]], fromLast = TRUE)][[ID]])
              }

              if (!isTRUE(allow_duplicates)) {
                  stop("Halting to prevent cartesian product.", call. = FALSE)
              }
          }

          x <- as.data.frame(x)
          y <- as.data.frame(y)
      }

      if (merge_warn != TRUE) {
        warning("Using base R `merge`. For better performance, install 'data.table'", call. = FALSE, immediate. = TRUE)
        merge_warn <<- TRUE
      }
          
      merge(x, y, by = ID, all = TRUE)
  }, DATA)
  
  MERGE <- merge(REFERENCE,MERGE,by=ID,all=T)
  
  ## Concatenate duplicated indicators into one variable and delete the others
  
  ## Create a function that collapses strings without including NA values
  paste3 <- function(...,sep=", ") {
    L <- list(...)
    L <- lapply(L,function(x) {x[is.na(x)] <- ""; x})
    ret <-gsub(paste0("(^",sep,"|",sep,"$)"),"",
               gsub(paste0(sep,sep),sep,
                    do.call(paste,c(L,list(sep=sep)))))
    is.na(ret) <- ret==""
    ret
  }
  
  MERGE$DUPLICATED <- apply(MERGE[,grepl("DUPLICATED",names(MERGE))],1,function(x){if(!is.na(x)){paste3(x,collapse=" & ")}})
  MERGE$DUPLICATED <- gsub("NO & | & NO","",MERGE$DUPLICATED)
  MERGE$DUPLICATED <- gsub("^& | &$| &[[:space:]]+$","",MERGE$DUPLICATED)
  MERGE$DUPLICATED <- gsub("&+","&",MERGE$DUPLICATED)
  MERGE$DUPLICATED <- gsub("^ & $|^$|^[[:space:]]+","NO",MERGE$DUPLICATED)
  
  ## Delete the individual indicators
  MERGE <- MERGE[,!(grepl("DUPLICATED_",names(MERGE))&!grepl("REFERENCE",names(MERGE)))]
  ## Bring indicator to the front
  MERGE <- MERGE[,c(paste0(ID),"DUPLICATED",names(MERGE)[!grepl(paste0(ID,"|DUPLICATED"),names(MERGE))])]
  
  ####################################################################################
  ### By variables
  ####################################################################################
  if(!all(is.na(VARS))){
    for(i in 1:length(VARS)){
    	## Print status message
    	print(paste0("Checking for ",VARS[i]," issues"))
    	
    	## Make sure that if the user specified this as a numeric variable, it wasn't turned by accident into another class like logical or character
    	eval(parse(text=paste0("MERGE[,grepl('^",VARS[i],"',names(MERGE))] <- as.data.frame(sapply(MERGE[,grepl('^",VARS[i],"',names(MERGE))], function(c){as.numeric(as.character(c))}))")))
    	
      ## If there are no "lower" & "upper" margins, just look for any discrepancy
      if(is.na(Vlowermargins[i]) & is.na(Vuppermargins[i])){
        eval(parse(text=paste0("MERGE$",VARS[i],"_ISSUE<- !apply(MERGE[,grepl('^",VARS[i],"',names(MERGE))],1,function(x){
                               all(x==x[which(!is.na(x))[1]],na.rm=T)})")))  
    }                        ## Checking if all entries equal the the first non-missing entry if the vector
      ## If the data is not missing in the reference file, then that is the reference value
      ## But this ensures that if the value is missing from the reference file a check is still run
      ## using the second-priority file as reference or the next assessment that has an observed entry.
      
      ## If an upper margin was indicated, then flag if the upper difference is larger
      if(!is.na(Vuppermargins[i])){
        eval(parse(text=paste0("MERGE$",VARS[i],"_ISSUE<- apply(MERGE[,grepl('^",VARS[i],"',names(MERGE))],1,function(x){
                               any(x>x[which(!is.na(x))[1]]+Vuppermargins[i],na.rm = T)})")))  
      }                        ## Checking if all entries equal the the first non-missing entry if the vector
      ## If the data is not missing in the reference file, then that is the reference value
      ## But this ensures that if the value is missing from the reference file a check is still run
      ## using the second-priority file as reference or the next assessment that has an observed entry.
      
      ## If a lower margin was indicated, then flag if the lower difference is larger
      if(!is.na(Vlowermargins[i])){
        ## If the upper margin was indicated and the ISSUE variable was already created, then we just want to fill it in if no discrepancies were found on the upper side
        eval(parse(text=paste0("if(length(MERGE$",VARS[i],"_ISSUE)>0){MERGE$",VARS[i],"_ISSUE[MERGE$",VARS[i],"_ISSUE!=TRUE]<- apply(MERGE[MERGE$",VARS[i],"_ISSUE!=TRUE,grepl('^",VARS[i],"',names(MERGE)) & !grepl('ISSUE',names(MERGE))],1,function(x){any(x<(x[which(!is.na(x))[1]]-Vlowermargins[i]),na.rm = T)})
      }else{
                               MERGE$",VARS[i],"_ISSUE<- apply(MERGE[,grepl('^",VARS[i],"',names(MERGE))],1,function(x){any(x<(x[which(!is.na(x))[1]]-Vlowermargins[i]),na.rm = T)})
      }")))                  ## Otherwise we just want to create the ISSUE variable from scratch based on the lower margin alone
          }
      
      
      
      }
    }else{print("You didn't specify any characteristics in  VARS, so nothing else will be used for verification")}

  ####################################################################################
  ### Now let's find the mismatches by name
  ####################################################################################
  
  ## First make sure that there are no invald regular expressions in the NAMES variables
  allnames <- paste0("^",NAMES,collapse = "|")
  MERGE[,grepl(allnames,names(MERGE))] <- sapply(MERGE[,grepl(allnames,names(MERGE))],
                                                 function(x){gsub("[[:punct:]]","",x)})
  if(!all(is.na(NAMES))){
    ## This process is computationally burdensome (yes, it crashed my computer once), so we need to use some tools
    ## to parallelize the operations.
    
    ##  For this we need to use the doParallel package
    ## This chunk of code will make sure that such packet is installed in your computer
    check.and.install <- function(p){
      needed.packages <- p[!(p %in% installed.packages()[,"Package"])]
      ## Install missing packages
      if(length(needed.packages)){ install.packages(needed.packages)}
      ## Load all packages
      for (i in 1:length(p)){
        eval(parse(text=paste0("library(",p[i],")")))
      }
    }
    p <- c("doParallel", "RecordLinkage")
    check.and.install(p)
    
    ## Next, for the package to work we need a set up that tells your computer how many cores to allocate for this task
    ## psanker: parallel::detectCores() automatically detects this, but to avoid completely eating your machine, we use detectCores() - 1
    numcores <- parallel::detectCores() - 1

    cl <- if (!is.null(debug_file)) {
        makePSOCKcluster(numcores, outfile = debug_file)
    } else {
        makePSOCKcluster(numcores)
    }
    doParallel::registerDoParallel(cl, cores = numcores)

    print(names(MERGE))
    
    ## Now we can start looping over the text variables in NAMES to look for mismatches
      for (i in 1:length(NAMES)){
      	## Print a status message
        print(paste0("Checking for ",NAMES[i]," issues"))
        
        #writeLines(c(""), paste0(OUTPATH,"log.txt"))
        
        source(paste0(FNPATH,"NameMismatchFunction2.R"))
        name.issues <- foreach(n = names(DATA),.combine="cbind")%:%
          #cat(paste0(NAMES[i], " in ", n))
          foreach(o = names(DATA)[names(DATA) %not_in% n], .export = c("Name.Missmatch","MERGE","NMARGIN", "notsim_chr"), .packages="RecordLinkage", .combine="cbind") %dopar% {
            
            #source(paste0(FNPATH,"NameMismatchFunction2.R"))
            #sink(paste0(OUTPATH,"log.txt"), append=TRUE)
            cat("\n", paste0(n," compared to ",o," in ",NAMES[i]), "\n", sep = "")
            #w <- which(names(DATA)[names(DATA) %not_in% n]==o)
            # eval(parse(text=paste0("issue <-  Name.Missmatch(NAME_A='",NAMES[i],"_",n,"',NAME_B='",NAMES[i],"_",o,"',DATA=MERGE,PROPORTION=NMARGIN, NAME_ROOT='", NAMES[i], "')")))

            issue <- notsim_chr(
                col_a = paste0(NAMES[i], "_", n),
                col_b = paste0(NAMES[i], "_", o),
                database = MERGE,
                proportion = NMARGIN,
                varname = NAMES[i]
            )

            # Check against reference column **if** the reference variable exists in MERGE
            if (NAMES[i] %in% names(MERGE)) {
                issue | notsim_chr(
                    col_a = paste0(NAMES[i], "_", n),
                    col_b = NAMES[i],
                    database = MERGE,
                    proportion = NMARGIN,
                    varname = NAMES[i]
                )
            } else {
                issue
            }
          }
          ## Flag any entries that got at least one mismatch between assessments
          MERGE[, paste0(NAMES[i], "_ISSUE")] <- apply(name.issues, 1L, any, na.rm = TRUE)
          # eval(parse(text=paste0("MERGE$",NAMES[i],"_ISSUE<- apply(name.issues,1,function(x){any(x,na.rm=T)})")))
        
        #apply(name.issues2,1,function(x){any(x,na.rm=T)})
    }
     stopCluster(cl) ## This should be done after concluding the parallelization process
     
  }else{    print("You didn't specify any NAMES variables, so names will be ignored for verification")}
  
  ### Remove all the sub-issue variables
  #MERGE <- MERGE[,!(grepl("ISSUE",names(MERGE)) & grepl("[0-9]$",names(MERGE)))]
  MERGE <- MERGE[,order(names(MERGE))]
  MERGE <- MERGE[,c(names(MERGE)[grepl(paste0(VARS,collapse = "|"),names(MERGE))],names(MERGE)[!grepl(paste0(VARS,collapse = "|"),names(MERGE))])]
  MERGE <- MERGE[,c(names(MERGE)[grepl(paste0(NAMES,collapse = "|"),names(MERGE))],names(MERGE)[!grepl(paste0(NAMES,collapse = "|"),names(MERGE))])]
  MERGE <- MERGE[,c(names(MERGE)[grepl(paste0(ID),names(MERGE))],names(MERGE)[!grepl(paste0(ID),names(MERGE))])]
  
  ### Add a few empty columns at the start that will be needed for verification
  MERGE$Problem     <- rep("",nrow(MERGE))
  MERGE$WHAT        <- rep("",nrow(MERGE))
  MERGE$change.from <- rep("",nrow(MERGE))
  MERGE$change.to   <- rep("",nrow(MERGE))
  MERGE$DATABASE    <- rep("",nrow(MERGE))
  MERGE$Note        <- rep("",nrow(MERGE))
  MERGE$X           <- NULL
  
  ### Move the verification columns to the front
  Verif             <- "^Problem$|^WHAT$|^change.to$|^change.from$|^DATABASE$|^Note$"
  MERGE             <- MERGE[,c(names(MERGE)[grepl(Verif,names(MERGE))],names(MERGE)[!grepl(Verif,names(MERGE))])]
  row.names(MERGE)  <- NULL
  
  ### Run the basic cleaning function to get rid of empty columns
  # Basic Cleaning
  source(paste0(FNPATH,"Basic Cleaning.R"))
  MERGE             <- CLEAN(MERGE)
  
  ### Create a column that counts the number of issues
  MERGE$N_ISSUES <- apply(MERGE[, grepl("ISSUE",names(MERGE))],1, function(x) sum(as.logical(x))) # Since CLEAN removes converts all logicals to characters, swap back
  
  ############## <Ugly chunk> can be written better
  ### Purge the DUPLICATED variable from unnecessary &s
  MERGE$DUPLICATED[grepl("(&[[:space:]]+&)|&$",MERGE$DUPLICATED)] <-
    gsub("(&[[:space:]]+&)|&$", "",MERGE$DUPLICATED[grepl("(&[[:space:]]+&)|&$",MERGE$DUPLICATED)])
  MERGE$DUPLICATED[grepl("[[:space:]]+",MERGE$DUPLICATED)]        <- gsub("[[:space:]]+&[[:space:]]+"," & ",MERGE$DUPLICATED[grepl("[[:space:]]+",MERGE$DUPLICATED)]) 
  MERGE$DUPLICATED[grepl("[[:space:]]+$",MERGE$DUPLICATED)]       <- gsub("[[:space:]]+$","",MERGE$DUPLICATED[grepl("[[:space:]]+$",MERGE$DUPLICATED)]) 
  MERGE$DUPLICATED[grepl("O[[:space:]]+I",MERGE$DUPLICATED)]      <- gsub("O[[:space:]]+I","O & I",MERGE$DUPLICATED[grepl("O[[:space:]]+I",MERGE$DUPLICATED)]) 
  MERGE$DUPLICATED[grepl("O&",MERGE$DUPLICATED)]                  <- gsub("O&","O &",MERGE$DUPLICATED[grepl("O&",MERGE$DUPLICATED)]) 
  
  ### Create a dup flag and compare with the original duplicate column
  MERGE$DUPS                                                      <- duplicated(MERGE[,ID])|duplicated(MERGE[,ID],fromLast = T)
  names(MERGE)[names(MERGE)=="DUPLICATED"]                        <- "DUPLICATED IN"
  
  
  
  ### Create the verification columns and move the N_ISSUES to the front
  MERGE$Problem     <- rep("",nrow(MERGE))
  MERGE$WHAT        <- rep("",nrow(MERGE))
  MERGE$change.from <- rep("",nrow(MERGE))
  MERGE$change.to   <- rep("",nrow(MERGE))
  MERGE$DATABASE    <- rep("",nrow(MERGE))
  MERGE$Note        <- rep("",nrow(MERGE))

  ### Reorder the ISSUES variables
  source(paste0(FNPATH, "Reorder.R"))
  
  MERGE <- ReOrder(MERGE,"Note",ID)
  MERGE <- ReOrder(MERGE,"DATABASE",ID)
  MERGE <- ReOrder(MERGE,"change.to",ID)
  MERGE <- ReOrder(MERGE,"change.from",ID)
  MERGE <- ReOrder(MERGE,"WHAT",ID)
  MERGE <- ReOrder(MERGE,"Problem",ID)
  
  MERGE <- ReOrder(MERGE,"DUPS","Note")
  MERGE <- ReOrder(MERGE,"DUPLICATED IN","Note")
  MERGE <- ReOrder(MERGE,"N_ISSUES","Note")
  
  for( n in 1:length(NAMES)){
    ## Position of first column that contains the name of each variable checked 
    first     <- grep(paste0("^",NAMES[n],"_"),names(MERGE))[1]
    previous  <- names(MERGE)[first-1]
    print(paste0("Moving: ",NAMES[n],"_ISSUE, before: ",names(MERGE)[first]))
    ## Move the ISSUE column to the begining of the group
    MERGE   <- ReOrder(MERGE,paste0(NAMES[n],"_ISSUE"),previous)  
  }
  

  ### Temporary chunk for workshop - data security ----------------------------------------
  ### -------------------------------------------------------------------------------------
  # namevars        <- names(MERGE)[grepl("NAME|INITIAL",names(MERGE),ignore.case = T)]
  # if(length(namevars)>0){
  #   MERGE         <- encrypt(MERGE,namevars,public_key_path=file.path(folder,"Code/id_workshop.pub"))
  # }
  # MERGE <- merge(MERGE, STIDS,by="STID_Y1",all.x=TRUE,all.y=FALSE)
  # MERGE$STID_Y1[grepl("^AR|^BR",MERGE$STID_Y1)] <- NA
  # MERGE <- ReOrder(MERGE,"STID_Y2","STID_Y1")
  ### -------------------------------------------------------------------------------------
  ### -------------------------------------------------------------------------------------
  
  ### Export file for mismatches verification
  write.csv(MERGE,paste0(OUTPATH,"ALL_MISMATCHES.csv"),row.names = F)
 
  }
