########## STANDARD VERIFICATION FUNCTION WHEN ONLY ONE ASSESSMENT IS AVAILABLE ########
###################################################
###### **Note: ID variables should be standardized 
###### before applying this function, (you can use 
###### the basic cleaning function, if your ID variables contain "ID)**
###################################################
###################################################

########## Exports basic problematic cases into excel files for manual  verification
########## It works with a simple comparison against one reference file (e.g. Masterlist, Randomization file, etc)

########## Exports duplicate IDs
########## Exports cases where IDs don't match the standard ID form


########## DATA:    Database that need to be verified
########## REFERENCE: Administrtive database, previously wave of data collection, or any other file that can be used as a reference for verification.
########## ID:      ID variable name used 
################### e.g. ID="ST_ID"
########## IDPATT:  ID format structure. Specified by the user as a list, where the "Fixed" portion of the IDs is indicated between quotes, if there's any 
################### subset of characters that may change by region it should be specified in the "Changing" portion, the range of digits that are are allowed in
################### in the IDs (if 3 to 4 digits allowed then 3:4, if only 4 digits allowed then 4, and the structure of the ID is indicated in quotes,
################### with X representing the fixed portion of the ID, C the changing characters and N the position of the numbers in relation to the letter portions.
################### E.g. if IDs are of the form AREM1734, BREM5643 then         IDPATT=list(Fixed="REM", Changing="(A|B)", Numbers=4, structure= "CXN") 
###################      if IDs are of the form TATT134, TBTT345, TATT4956 then IDPATT=list(Fixed="T", Changing="(A|B)", Numbers=3:4, structure= "XCXXN") 
###################      if IDs are of the form ST134, ST67895  then            IDPATT=list(Fixed="ST", Changing="", Numbers=3:5, structure= "XN") 
###################      if IDs are of the form A134 and B678  then             IDPATT=list(Fixed="", Changing="(A|B)", Numbers=3, structure= "CN") 
########## UNIQUEID: Variable that if combined with the ID variable can function as a unique identifier of each ASSESSMENT in the data (not person)
################### **Note: it doesn't matter if the same person took it twice we want one unique identifier per survey entry.
################### (the only cases in which it is OK for the the identifier+ID combination not to be unique is if the system re-sent the 
################### same entry twice, in which case there would be no way to uniquely distinguish two surveys, and
#### those two would share the same "unique" ID) These are the cases classified as "IDENTICAL" and this function will address them, 
#### but they don't often happen.  The function won't actually use it but the argument is there to remind the user that the UNIQUE ID should be created before
################### the verification output is given to the workers who will take charge of the manual verifications.
########## OUTPATH: Path to the directory where the issues should be saved
################### e.g. OUTPATH = paste0("/Users/",user,"/Box Sync/Box 3EA Team Folder/Data Management/2016-17 3EA Niger Data Cleaning/Merges Pre-Imputation/ACROSS-WAVE MERGES/ISSUES/")
########## FILE:    Name root for the output files including the country letters, the year, the assessment, and the wave information
################### These should depart from the content of each one of the dataframes specified in "DATA" (in the same order, please).
################### e.g. FILE = "LBY2_EGRA_BASELINE" or FILE = "NGY1_TOCA_MIDLINE" or FILE=c("LBY2_EGRA_BASELINE","LBY2_EGMA_BASELINE","LBY2_ODK_BASELINE")

oneset.issues <- function(
    DATA,
    ID,
    IDPATT,
    UNIQUEID,
    OUTPATH,
    FILE,
    FUN_DIR,
    REFERENCE = NULL,
    FIX_FORMAT = FALSE,
    ROW_NAMES = FALSE,
    EXTRA_COLUMNS = NULL
) {
    ## Check and install any required packages
    source(paste0(FUN_DIR, "packages_check.R"))
    REQUIREMENTS <- c("crayon","encryptr")
    check.and.install(p = REQUIREMENTS)

    # Check if OUTPATH exists. Make directory if it doesn't exist.
    if (!dir.exists(OUTPATH)) {
        message("Created output directory: ", file.path(OUTPATH))
        dir.create(OUTPATH, recursive = TRUE)
    }    

    ## Duplicates
    DATA <- DATA[order(DATA[,ID]),]
    DUPS <- DATA[duplicated(DATA[,ID])|duplicated(DATA[,ID],fromLast = T),]
    ## Cases with weird IDs
    X  <- IDPATT[[1]]
    C  <- IDPATT[[2]]
    N  <- IDPATT[[3]]
    S  <- unlist(strsplit(IDPATT[[4]],split="|"))
    ORDER <- rep(NA,length(S))
    ORDER[which(S==bquote(C))] <- C
    ORDER[which(S==bquote(X))] <- X
    stringfinal                <- ""
    string                     <- rep(NA,length(N))

    for(n in 1:length(N)){
        string[n]   <- paste0(replicate(N[n],"[0-9]"),collapse = "")
        stringfinal <- paste0(stringfinal,string[n],sep = "$|") 
    }

    ORDER[which(S==bquote(N))] <- paste0("(",substr(stringfinal,1,nchar(stringfinal)-1),")",collapse = "|") ## Remove only the last character
    ORDER[1]                   <- paste0("^", ORDER[1])
    ## "ORDER" CONTAINS THE STRUCTURE THAT THE IDS SHOULD HAVE

    ## Now we look into the data for IDs that don't match the given structure, and save those cases into "WEIRD"
    WEIRD           <- DATA[!grepl(paste0(ORDER,collapse = ""),DATA[,ID]),]
    ## Make sure that none of the weird cases are already in the duplicates
    "%not_in%"      <- function(x,table){match(x,table,nomatch = 0)==0 }
    WEIRD           <- WEIRD[WEIRD[,ID] %not_in% DUPS[,ID],]

    ## Add an indicator for the type of issue
    WEIRD           <- cbind(rep("WEIRD ID",nrow(WEIRD)),WEIRD)
    names(WEIRD)[1] <- "ISSUE"
    DUPS            <- cbind(rep("DUPLICATED ID",nrow(DUPS)),DUPS)
    names(DUPS)[1]  <- "ISSUE"

    ## Append weird cases and duplicates
    ISSUES          <- rbind(WEIRD,DUPS)
    namevars        <- names(DATA)[grepl("NAME|INITIAL",names(DATA),ignore.case = T)]

    if (nrow(ISSUES) > 0) {
        if (isTRUE(FIX_FORMAT)) {
            ISSUES <- .add_fix_format(ISSUES, id_col = ID, uid_col = UNIQUEID, verification_col_order = EXTRA_COLUMNS)
        }
        ### Temporary chunk for workshop - data security ----------------------------------------
        ### -------------------------------------------------------------------------------------
        # if(length(namevars)>0){
        #     ISSUES        <- encrypt(ISSUES,namevars,public_key_path=file.path(folder,"Code/id_workshop.pub"))
        # }
        ### -------------------------------------------------------------------------------------
        
        write.csv(ISSUES, paste0(OUTPATH, FILE, "_DUPLICATES&ISSUES.csv"), row.names = ROW_NAMES)
    }else{
        cat(crayon::magenta(paste0("HURRAY!! ", FILE, " HAD NO DUPLICATES OR ALIEN IDS!!")))
        cat("\n")
    }

    ## Unmatched
    if(!is.null(REFERENCE)) {
        onlyindat <- DATA[DATA[[ID]] %not_in% REFERENCE[[ID]], ]
        onlyinref <- REFERENCE[REFERENCE[[ID]] %not_in% DATA[[ID]], ]

        if (isTRUE(FIX_FORMAT)) {
            onlyindat <- .add_fix_format(onlyindat, id_col = ID, uid_col = UNIQUEID, verification_col_order = EXTRA_COLUMNS)
            onlyinref <- .add_fix_format(onlyinref, id_col = ID, uid_col = UNIQUEID, verification_col_order = EXTRA_COLUMNS)
        }
        
        ### Temporary chunk for workshop - data security ----------------------------------------
        ### -------------------------------------------------------------------------------------
        # if(length(namevars)>0){
        #     onlyindat     <- encrypt(onlyindat,namevars,public_key_path=file.path(folder,"Code/id_workshop.pub"))
        #     refnames      <- names(onlyinref)[grepl("NAME|INITIAL",names(onlyinref),ignore.case = T)]
        #     onlyinref     <- encrypt(onlyinref,refnames,public_key_path=file.path(folder,"Code/id_workshop.pub"))
        # }
        ### -------------------------------------------------------------------------------------
        ### -------------------------------------------------------------------------------------

        write.csv(onlyindat, paste0(OUTPATH, FILE, "_ONLYINDATA.csv"), row.names = ROW_NAMES)
        write.csv(onlyinref, paste0(OUTPATH, FILE, "_ONLYINREFERENCE.csv"), row.names = ROW_NAMES)
    }
}

# New version of oneset.issues() that's more flexible
oneset_issues <- function(
    database,
    id_col,
    unique_id_col,
    database_name,
    output_directory = NULL,
    reference = NULL,
    column_structure = NULL,
    custom_checks = NULL,
    extra_cols = NULL
) {
    stopifnot(inherits(database, "data.frame"))

    if (!requireNamespace("data.table", quietly = TRUE)) {
        stop("oneset_issues() makes extensive use of data.table. Please install it.", call. = FALSE)
    }

    DT <- data.table::as.data.table(database)

    # ID duplicates
    .handle_issue(DT, "duplicated_id", function(dat) {
        dat[, duplicated(dat[[id_col]]) | duplicated(dat[[id_col]], fromLast = TRUE)]
    })

    # Column pattern structure checks -- defaults to ID check
    if (!is.null(column_structure)) {
        if (is.list(column_structure[[1L]])) {
            # Check selected columns' structure
            stopifnot(!is.null(names(column_structure)))

            for (col in names(column_structure)) {
                .handle_issue(DT, paste0("bad_", col), function(dat) {
                    dat[, !grepl(.structure_regex(column_structure[[..col]]), dat[[..col]])]
                })
            }
        } else {
            .handle_issue(DT, paste0("bad_", id_col), function(dat) {
                dat[, !grepl(.structure_regex(..column_structure), dat[[..id_col]])]
            })
        }
    }

    # Only in data checks
    if (!is.null(reference)) {
        stopifnot(inherits(reference, "data.frame"))
        RF <- data.table::as.data.table(reference)
        
        .handle_issue(DT, "dataonly", function(dat) {
            dat[, apply(.SD, 1L, function(x) !x %in% RF[[..id_col]]), .SDcols = id_col]        
        })
    }

    # Custom checks
    if (!is.null(custom_checks)) {
        stopifnot(is.list(custom_checks), !is.null(names(custom_checks)))

        for (check in names(custom_checks)) {
            .handle_issue(DT, check, custom_checks[[check]])
        }
    }

    # Convert issue cols to one column
    issue_cols <- names(DT)[grepl("^_.*_issue$", names(DT))]

    for (issue_col in issue_cols) {
        query <- bquote(.(as.name(issue_col)) := as.character(.(as.name(issue_col))))
        DT[, eval(query)]

        query <- bquote(.(as.name(issue_col)) := NA_character_)
        DT[DT[[issue_col]] != "TRUE", eval(query)]

        query <- bquote(.(as.name(issue_col)) := {
            flagname <- gsub("^_", "", issue_col)

            gsub("_issue$", "", flagname)
        })
        DT[DT[[issue_col]] == "TRUE", eval(query)]
    }

    DT[, ISSUE := apply(.SD, 1L, function(x) paste0(x[!is.na(x)], collapse = ", ")), .SDcols = issue_cols]
    
    query <- bquote(order(.(as.name(id_col))))
    DT <- DT[eval(query)]

    output <- .add_fix_format(as.data.frame(DT), id_col = id_col, uid_col = unique_id_col, verification_col_order = extra_cols)
    output <- output[!(is.na(output$ISSUE) | grepl("^\\s*$", output$ISSUE)), ]

    if (!is.null(output_directory) && nrow(output) > 0L) {
        data.table::fwrite(output, file = file.path(output_directory, paste0(database_name, ".csv")))
    } else if (nrow(output) == 0L) {
        message("No issues detected in ", database_name, "!")
    }

    output
}

.handle_issue <- function(dat, flag_name, condition) {
    stopifnot(!data.table::is.data.table(dt))

    query <- bquote(.(as.name(paste0("_", flag_name, "_issue"))) := condition(dat))
    dat[, eval(query)]
}

.structure_regex <- function(idpatt) {
    stopifnot(length(setdiff(names(idpatt), c("Fixed", "Numbers", "Changing", "structure"))) == 0L)

    working_pattern <- idpatt[["structure"]]

    # Handling the `Numbers = x:y` syntax
    if (length(idpatt[["Numbers"]]) > 1) {
        idpatt <- paste0(idpatt[["Numbers"]][1], ",", idpatt[["Numbers"]][length(idpatt[["Numbers"]])])
    }

    working_pattern <- gsub("N", paste0("\\\\d{", idpatt[["Numbers"]], "}"), working_pattern)
    working_pattern <- gsub("C", idpatt[["Changing"]], working_pattern)
    working_pattern <- gsub("X", gsub("(\\W)", "\\\\\\1", idpatt[["Fixed"]]), working_pattern)

    working_pattern
}

.add_fix_format <- function(dat, id_col = "", uid_col = "", verification_col_order = NULL) {
    stopifnot(is.data.frame(dat))

    format_cols <- c("Verifier", "Problem",  "WHAT", "change.from", "change.to", "Note")
    collided_vars <- format_cols[format_cols %in% names(dat)]

    if (length(collided_vars) > 0L) {
        warning("Could not safely add single set fix format because of variable name conflict: ", paste0(collided_vars, collapse = ", "), ". Please compensate for these", call. = FALSE, immediate. = TRUE)
        return(dat)
    }

    for (column in format_cols) {
        dat[, column] <- rep("", nrow(dat))
    }

    format_col_front <- c(id_col, uid_col, format_cols, "ISSUE")
    format_col_front <- format_col_front[format_col_front != "" & format_col_front %in% names(dat)]

    total_order <- if (!is.null(verification_col_order)) {
        ver_cols <- setdiff(verification_col_order, format_col_front)
        ver_cols <- ver_cols[ver_cols %in% names(dat)]

        c(format_col_front, ver_cols, setdiff(names(dat), c(format_col_front, ver_cols)))
    } else {
        c(format_col_front, setdiff(names(dat), format_col_front))
    }

    dat[, total_order]
}
