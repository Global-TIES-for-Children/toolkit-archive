
## CORRECT: Multiple Verifications Format, filled in with all the necessary changes, and according to the protocol.
## WHAT: Names of the different variables that may need fixing for that assessment
## DATA: Identifier sufix of the database (exported by the Standard Verification Function, based on the names specified for the databases)
## ID:   Same ID as specified when running the standard Verification Function
## Write a function that applies the fixes to the corresponding wave

Multiple.fix  <- function(CORRECT,DATA,WHAT,ID,UNIQUEIDS,OUTPATH,FUN_DIR, CLEANED = FALSE){

    ## Check and install any required packages
    source(paste0(FUN_DIR, "packages_check.R"))
    REQUIREMENTS <- c("crayon","stringr")
    check.and.install(p = REQUIREMENTS)

    ##########################################################
    ## First, make sure that Student IDs are in the same order:
    ##########################################################
    CORRECT      <- CORRECT[order(CORRECT[, ID]), ]

    ## Get rid of spaces in the what & database columns
    CORRECT$WHAT     <- gsub("[[:space:]]+", "", CORRECT$WHAT)
    CORRECT$DATABASE <- gsub("[[:space:]]+", "", CORRECT$DATABASE)

    if (is.null(OUTPATH)) {
        message("`OUTPATH` is NULL. Not exporting any data.")
    }

    ##########################################################
    ## Next, loop over the different databases that may need correction
    ##########################################################
    for (data in 1:length(DATA)){
        DB <- DATA[[data]]

        if (!exists("CLEAN")) {
            # psanker: Swapped print to stop so that the function doesn't unnecessarily crash
            stop("YOU MUST SOURCE THE 'Basic Cleaning' FUNCTION BEFORE RUNNING THIS FUNCTION", call. = FALSE)
        }

        # psanker: Added this because of a strange bug with CLEAN that would crash on using apply(data, 1, f)
        if (!isTRUE(CLEANED)) {
            DB <- CLEAN(DB)
        }

        DATABASE <- names(DATA)[data]
        UNIQUEID <- UNIQUEIDS[data] 

        ## Order the IDs inside each individual dataset:
        DB           <- DB[order(DB[, ID]), ]

        ## Subset the corrections to those concerning each one DATABASE
        FIXE         <- CORRECT[grepl(paste0(DATABASE), CORRECT$DATABASE), ]

        ## If there are any ID fixes that should be done only in one assessment from a wave, then take care of those first
        ##migrate.assessment <- FIXE[FIXE$`Change.needed.only.in.one.assessment.(Enter.1/0)` & !is.na(FIXE$`Change.needed.only.in.one.assessment.(Enter.1/0)`)==1,]   

        ## Subset to the remaining corrections
        ##FIXE         <- FIXE[FIXE$`Change.needed.only.in.one.assessment.(Enter.1/0)`==0|is.na(FIXE$`Change.needed.only.in.one.assessment.(Enter.1/0)`),]

        ## Delete empty observations
        empty        <- as.vector(apply(FIXE, 1, function(x) sum(is.na(x)))) == length(FIXE)
        FIXE         <- FIXE[!empty, ]

        ####################################################################################################################
        ## Extract the variable that needs changing on that particular database (each database may need a change in different variables)
        ####################################################################################################################
        ## If fixes are required in more than one wave/DATABASE:
        multiple     <- unlist(lapply(str_split(FIXE[,"DATABASE"],"\\|"),length))
        ## DATABASE position
        DATABASE_pos <- unlist(lapply(str_split(FIXE[,"DATABASE"],"\\|"),function(x){grep(paste0("^",DATABASE),x)}))
        ## If there are multiple waves/DATABASEs then we want to keep only the variables that need change in that wave
        if (sum(multiple > 1, na.rm = TRUE) > 0) {
            for (i in which(multiple > 1)) {
                print(paste0(i, ", ", DATABASE, ": position", DATABASE_pos[i], " BEFORE:", FIXE[i, "WHAT"], ", NOW: ", str_split(FIXE[,"WHAT"],"\\|")[[i]][DATABASE_pos[i]]))
                ## If the change is disappearing, print a warning:
                if (is.na(str_split(FIXE[, "WHAT"], "\\|")[[i]][DATABASE_pos[i]])){
                    cat(crayon::magenta(paste0("WARNING!! WARNING!! 👾👾👾👾👾👾👾👾👾👾👾👾👾👾👾👾👾👾
                                               The change is not being applied consistently for the observation with ", UNIQUEID,"_",DATABASE," = ",FIXE[i,paste0(UNIQUEID,"_",DATABASE)]," 
                                               Check for that observation in the FIXES file to see what went wrong when filling in the format.")))
                                               cat("\n")
                }
                ## Keep in the 'what' column only the variables that need change in the dataset of the current iteration
                FIXE[i, "WHAT"]             <- str_split(FIXE[, "WHAT"], "\\|")[[i]][DATABASE_pos[i]]
                ## Keep in the 'change.to' column only the changes corresponding in the dataset of the current iteration
                FIXE[i, "change.to"]        <- str_split(FIXE[, "change.to"], "\\|")[[i]][DATABASE_pos[i]]
            }
        }

        if (!exists("%not_in%")) {
            `%not_in%` <- function(x, y) !x %in% y
        }  

        ## If "whole observation" is marked, then delete
        DB   <- DB[DB[, UNIQUEID] %not_in% FIXE[, paste0(UNIQUEID, "_", DATABASE)][grepl("WHOLE", FIXE$WHAT)], ]
        FIXE <- FIXE[!grepl("WHOLE", FIXE$WHAT), ]

        ## Then go over each variable that may need changes
        for (what in WHAT) {

            ## Then we only need the position for the variable change (in case change is needed in multiple vars)
            ## find the position of the corrections for this particular variable "what"
            position <- unlist(lapply(str_split(FIXE[grepl(what,FIXE$WHAT),"WHAT"],","),function(x){grep(paste0("^",what,"$"),x)}))

            fixIDs   <- FIXE[grepl(paste0("(^|,)", what, "(,|$)"), FIXE$WHAT), paste0(UNIQUEID, "_", DATABASE)]

            if (length(fixIDs) > 0) {
                print(paste0(DATABASE,": ",what, " Fixfile IDs:"))

                print(paste(fixIDs))

                print(paste0(DATABASE,": ",what, " Data IDs: "))

                print(paste(DB[DB[,UNIQUEID] %in%  fixIDs,UNIQUEID]))

                if(length(fixIDs)!=length(DB[DB[,UNIQUEID] %in%  fixIDs,UNIQUEID])) {
                    cat(crayon::red("PROBLEM!! 😨 😨 😨 😨 😨 😨  😨 😨 😨 😨 😨
                                    The number of cases that are set to be fixed in the format is different from the number of cases in the database. 
                                    Check the consistency between your databases and the unique IDs in your verification format."))
                }
            }

            ## Extract the changes' entries for only the observations that need a change in "what"
            allfix   <- FIXE[grepl(paste0("(^|,)", what, "(,|$)"), FIXE$WHAT), "change.to"]

            ## psanker: If no fixes needed, move on to prevent read.table from dying by trying to read
            ## read.table(text = character(0L))
            if (length(allfix) < 1L) {
                next
            }

            ## Split all the fixes needed for this child
            newfix   <- read.table(
                text = allfix,
                sep = ",",
                as.is = TRUE,
                fill = TRUE,
                row.names=NULL,
                col.names = c("1","2","3","4","5","6","7","8")
            )

            ## For each row select the position of the variable that needs fixing
            newfix   <- newfix[cbind(seq_along(position), position)]
            ## Remove weird spaces on the edges
            newfix   <- gsub("^[[:space:]]+|[[:space:]]+$", "", newfix)

            ## Substitute the values with the corrected ones
            if (length(DB[DB[, UNIQUEID] %in% fixIDs, what]) > 0) {
                DB[DB[, UNIQUEID] %in%  fixIDs, what] <- newfix
            }
        } ## Closing 'what' loop
        DATA[[data]] <- DB

        # Dropping any blank (all NA) rows
        DATA[[data]] <- DATA[[data]][apply(DATA[[data]], 1L, function(x) !all(is.na(x))), ]

        if (!is.null(OUTPATH)) {
            ### Temporary chunk for workshop - data security ----------------------------------------
            ### -------------------------------------------------------------------------------------
            # namevars        <- names(DATA[[data]])[grepl("NAME|INITIAL",names(DATA[[data]]),ignore.case = T)]
            # if(length(namevars)>0){
            #     DATA[[data]]         <- encrypt(DATA[[data]],namevars,public_key_path=file.path(folder,"Code/id_workshop.pub"))
            # }
            ### -------------------------------------------------------------------------------------
            ### -------------------------------------------------------------------------------------
            
            write.csv(DATA[[data]],paste0(OUTPATH,"JUST_",names(DATA)[[data]],"_VERIFIED.csv"),row.names = FALSE)
        }
    } ## Closing 'data' loop

    ## Print note to avoid freaking out people who are not in charge of debugging code:
    cat(crayon::bgCyan(black("NOTE: Unless you are the person that is debugging the code in this function, 
                             you should only pay attention to the text that was printed in colors 🌈(if there was any).")))
                             cat("\n")
                             cat(crayon::bgYellow(black("NOTE: Unless you are the person that is debugging the code in this function, 
                                                        you should only pay attention to the text that was printed in colors 🌈(if there was any).")))
                                                        cat("\n")
                                                        cat(crayon::bgMagenta(white("NOTE: Unless you are the person that is debugging the code in this function, 
                                                                                    you should only pay attention to the text that was printed in colors 🌈(if there was any).")))
                                                                                    cat("\n")

                                                                                    ## Output of the function
                                                                                    return(DATA)
} ## Closing function


