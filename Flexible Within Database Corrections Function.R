#### DATA MISTAKES CORRECTIONS FUNCTION ####

###############
### Make sure that the input format that contains the fixes has the following columns:
### UNIQUEID       :a unique identifier for each survey entered in the system, could be the START time or a combination of variables that ensures uniqueness 
#### (it doesn't matter if the same person took it twice we want one unique identifier per survey entry)
#### (unless the system re-sent the same entry twice, in which case there would be no way to uniquely distinguish two surveys, and
#### those two would share the same "unique" ID) These are the cases classified as "IDENTICAL" and this function will address them, 
#### but they don't often happen.  
### ST_ID or TCH_ID: Depending on the survey in question, the ID column in the format should have the same ID name as in the database that is being fixed.
### WHAT           : Name of the variables that need to be fixed for each case (should match with the names in the Database)
### change.from    : Value with error in the data for the variable that needs fixing.
### change.to      : Correct value that should substitute the mistaken one.

###############
## Write as a function
correct<-function(corrections,DATA,UNIQUEID,ID){
  ### Do some pre-processing
  corrections           <- as.data.frame(corrections)
  corrections$change.to <- as.character(corrections$change.to)
  # **Convert blank cells to missing values**
  if(nrow(corrections)>1){
    corrections<-as.data.frame(sapply(corrections,function(x){gsub("^[[:space:]]+$|^$",replacement=NA,x)}))
  }else{corrections[grepl("^[[:space:]]+$|^$",corrections)] <- NA}
  
  ## Delete empty observations
  empty<-as.vector(apply(corrections,1,function(x){sum(is.na(x))}))==length(corrections)
  corrections<-corrections[!empty,]
  
  corrections[,paste0(UNIQUEID)] <- as.character(corrections[,paste0(UNIQUEID)])
  corrections$change.from        <- as.character(corrections$change.from)
  corrections$change.to          <- as.character(corrections$change.to)
  
  #Capitalize letters in IDs
  corrections[,"change.to"]<-toupper(corrections[,"change.to"])
  
  corrections$change.to[grepl("^ST|^AREM|^BREM|^enq",corrections$change.to,ignore.case = TRUE)]<-
    gsub("[[:space:]]","",corrections$change.to[grepl("^ST|^AREM|^BREM|^enq",corrections$change.to,ignore.case = TRUE)])
  
  ## Make sure there arent any spaces around the names of the variables that need to be changed
  corrections$WHAT <- gsub("^[[:space:]]+|[[:space:]]+$","",corrections$WHAT)
  
  ## Delete empty observations
  corrections<-corrections[!is.na(corrections$WHAT),]
  empty<-as.vector(apply(corrections,1,function(x){sum(is.na(x))}))==length(corrections)
  corrections<-corrections[!empty,]
  
  ## Make sure that after all previous changes, things didn't go back to factor
  corrections[,paste0(UNIQUEID)]     <-as.character(corrections[,paste0(UNIQUEID)])
  corrections$change.from            <-as.character(corrections$change.from)
  corrections$change.to              <- as.character(corrections$change.to)
  
  ### MAKE CORRECTIONS ###
  ########################
  
  #######################################################################################
  ### Automatized fixes: ################################################################
  #######################################################################################
  
  for(i in 1:nrow(corrections)){
    unique         <- as.character(corrections[,paste0(UNIQUEID)])[i]
    what           <- as.character(corrections$WHAT)[i]
    print(paste0(i,"-",unique," Fix: ",what))
    p_id           <- unique(corrections[!is.na(corrections[,paste0(UNIQUEID)]) & corrections[,paste0(UNIQUEID)] == unique,ID])
    #what<-substr(what, 1,4) ## Which variable to change
    
    if(sum(DATA[DATA[,ID] == p_id, UNIQUEID] %in% corrections[grepl("identical",corrections$WHAT,ignore.case = T) & corrections[,ID]==p_id,UNIQUEID])>1 ## Check if there are identical duplicates that haven't been fixed in a previous loop)
       ){
      print(paste0("Identical entry's IDs:",p_id))
      print(paste0("# Rows before:", nrow(DATA)))
      ## Identify the unique IDs for all the identical entries under that participat's ID (p_id)
      u_ids <- corrections[grepl("identical",corrections$WHAT,ignore.case = T) & corrections[,ID]==p_id,paste0(UNIQUEID)]
      ## Identify the rows where these identical entries are in the final data
      r_rows<- which(DATA[,paste0(UNIQUEID)] %in% u_ids)
      ## Randomly select which ones to drop, and which one to keep
      randompick <- sample(r_rows,length(r_rows)-1,replace=F)
      ## Drop
      DATA       <- DATA[-randompick,]
      print(paste0("# Rows after:", nrow(DATA)))
      print(paste0("i=",i))
      }else{
        if(grepl("Whol",what,ignore.case = TRUE)){ 
          ## The fixes sheet indicates that all of these assessments should be deleted
          if(!is.na(unique)){
          DATA   <- DATA[   !(DATA[,paste0(UNIQUEID)] == unique & 
                            !is.na(DATA[,paste0(UNIQUEID)])),]
          }else{
            DATA <- DATA[ !(is.na(DATA[,paste0(UNIQUEID)]) & 
                            DATA[,paste0(UNIQUEID)]==unique),]}
          }else{## For all other cases we just need to change a variable's value
          ## Here all changes are at the student level
          DATA[!is.na(DATA[,paste0(UNIQUEID)]) & 
                 DATA[,paste0(UNIQUEID)]==unique ,
               grep(paste0(what,"$"),names(DATA))] <- corrections$change.to[i]
          }## Close variable changes
        } ## Close if not "identical
    } ## Close i loop
  row.names(DATA) <- NULL
  return(DATA)
  }

correct_single <- function(corrections, database, unique_id_col, id_col, debug = FALSE, force_fix = FALSE) {
    log_db <- function(...) {
        if (isTRUE(debug)) cat(..., "\n", sep = "", file = stderr())

        invisible(NULL)
    }

    if (!requireNamespace("data.table", quietly = TRUE)) {
        stop("correct_single uses data.table to make fast changes to databases. Please install it", call. = FALSE)
    }

    if (!inherits(database, "data.frame")) {
        stop("`database` must be a data.frame or similar object (e.g. a tbl_df or data.table).", call. = FALSE)
    }

    if (is.character(corrections)) {
        log_db("Loading corrections from path")

        # Treat as a path
        stopifnot(length(corrections) == 1L)

        if (!file.exists(corrections)) {
            stop("`corrections` file not found: ", corrections, call. = FALSE)
        }

        corrections <- if (grepl("\\.csv$", corrections)) {
            read.csv(corrections, stringsAsFactors = FALSE, na.strings = c("", "NA"))
        } else if (grepl("\\.xlsx", corrections)) {
            openxlsx::read.xlsx(corrections, na.strings = c("", "NA"))
        } else {
            NULL
        }

        if (is.null(corrections)) {
            stop("Could not read `corrections` file", call. = FALSE)
        }

        log_db("Successfully loaded corrections")
    } else {
        if (!inherits(corrections, "data.frame")) {
            stop("`corrections` must be a data.frame or similar object OR a path to the corrections file", call. = FALSE)
        }
    }

    DT  <- data.table::as.data.table(database)
    CORRECT <- data.table::as.data.table(corrections)

    CORRECT <- if (isTRUE(attr(CORRECT, "verified_fixes"))) {
        CORRECT[, c("database", "UNIQUE_ID", "ID", "WHAT", "change.from", "change.to", "state", "fixhash"), with = FALSE]
    } else {
        tmp <- CORRECT[, c(unique_id_col, id_col, "WHAT", "change.from", "change.to"), with = FALSE]
        data.table::setnames(tmp, c(unique_id_col, id_col), c("UNIQUE_ID", "ID"))
        tmp
    }

    # Assert that the change.to column has character data
    CORRECT[, change.to := as.character(change.to)]

    # Assert that the ID column in the database has character data
    query <- bquote(.(as.name(id_col)) := as.character(.(as.name(id_col))))
    DT[, eval(query)]

    CORRECT <- CORRECT[!is.na(WHAT)]

    # If UNIQUE_IDs are character information, ensure their capitalization matches prior to verification
    if (is.character(CORRECT$UNIQUE_ID)) {
        if (!is.character(DT[[unique_id_col]])) {
            query <- bquote(.(as.name(unique_id_col)) := as.character(.(as.name(unique_id_col))))
            DT[, eval(query)]
        }

        query <- bquote(.(as.name(unique_id_col)) := toupper(.(as.name(unique_id_col))))
        DT[, eval(query)]

        CORRECT[, UNIQUE_ID := toupper(UNIQUE_ID)]
    }

    # Verify fixes before going ahead with corrections to filter out bad fix requests
    if (!isTRUE(attr(CORRECT, "verified_fixes"))) {
        .verify_fixes(CORRECT, DT, unique_id_col, id_col)
    }

    ACCEPTED <- if (!isTRUE(force_fix)) {
        if ("state" %in% names(CORRECT)) {
            CORRECT[state != "rejected"]
        } else {
            CORRECT[any_issue == FALSE]
        }
    } else {
        CORRECT[]
    }

    # Records marked to delete have "Whole Observation" in WHAT
    deletions <- ACCEPTED[grepl("^whole obs", WHAT, ignore.case = TRUE), UNIQUE_ID]
    query <- bquote(!.(as.name(unique_id_col)) %in% deletions)
    DT <- DT[eval(query)]

    # Identical cases are those where the recorded data is approximately identical. In this situation,
    # randomly select which record to keep, grouped by ID
    if (nrow(ACCEPTED[grepl("^identical$", WHAT, ignore.case = TRUE)]) > 0L) {
        reject_rows <- ACCEPTED[grepl("^identical$", WHAT, ignore.case = TRUE), .(.rows = sample(.I, .N - 1)), by = ID][, .rows]
        reject_uids <- ACCEPTED[reject_rows, UNIQUE_ID]
        query <- bquote(!.(as.name(unique_id_col)) %in% reject_uids)
        DT <- DT[eval(query)]
    }

    other_vars <- ACCEPTED[, unique(WHAT[!grepl("^whole obs|^identical$", WHAT, ignore.case = TRUE)])]

    for (ov in other_vars) {
        ovhashes <- ACCEPTED[WHAT == ov, fixhash]
        ovsym <- as.name(ov)
        
        if (ACCEPTED[, typeof(change.to)] != DT[, typeof(ov)]) {
            message("Converted ", ov, " to a character vector. Please compensate for this!")
            query <- bquote(.(ovsym) := as.character(.ovsym))
            DT[, eval(query)]
        }

        for (ovhash in ovhashes) {
            ovuid <- ACCEPTED[fixhash == ovhash, UNIQUE_ID]

            q_filter <- bquote(.(as.name(unique_id_col)) == ovuid)
            q_change <- bquote(.(ovsym) := ACCEPTED[fixhash == ovhash, change.to])

            current <- DT[eval(q_filter)][[ov]]
            change  <- ACCEPTED[fixhash == ovhash, change.to]

            log_db("(", ovhash, ") ", ovuid, " @ ", ov, ": ", current, " -> ", change)
            DT[eval(q_filter), eval(q_change)]
        }
    }

    # Send the list of bad IDs to console for now
    if ("state" %in% names(CORRECT)) {
        CORRECT[, any_issue := state == "rejected"]
    }

    if (nrow(CORRECT[any_issue == TRUE]) > 0L) {
        if (isTRUE(force_fix)) {
            warning("Bad fixes were applied! Look out for these.", call. = FALSE, immediate. = TRUE)
        }

        print("The following are bad fix requests:")
        print(CORRECT[any_issue == TRUE])
    }

    as.data.frame(DT)
}

.verify_fixes <- function(corrections, database, uid_col, id_col) {
    if ("database" %in% names(corrections)) {
        corrections[, fixhash := apply(.SD, 1L, digest::digest), .SDcols = c("database", "UNIQUE_ID", "WHAT", "change.from")]
    } else {
        corrections[, fixhash := apply(.SD, 1L, digest::digest), .SDcols = c("UNIQUE_ID", "WHAT", "change.from")]
    }
    corrections[, missing_uid := FALSE]
    corrections[, duplicate_changes := FALSE]
    corrections[, multiple_conclusions := FALSE]
    corrections[, what_not_found := FALSE]
    corrections[, existing_id := FALSE]
    corrections[, nonexistent_id_removed := FALSE]
    corrections[, uid_count := .N, by = "UNIQUE_ID"]

    corrections[!UNIQUE_ID %in% database[[uid_col]], missing_uid := TRUE]

    corrections[, incomplete_record := FALSE]
    corrections[!grepl("^whole obs|^identical$", WHAT, ignore.case = TRUE), incomplete_record := is.na(change.to)]

    corrections[uid_count > 1L, duplicate_changes := any(duplicated(fixhash)), by = "UNIQUE_ID"]
    corrections[duplicate_changes == TRUE, multiple_conclusions := length(unique(change.to)) > 1L, by = "UNIQUE_ID"]

    corrections[!(is.na(WHAT) | grepl("^whole obs|^identical$", WHAT, ignore.case = TRUE)), what_not_found := !WHAT %in% names(database)]

    idcolsym <- as.name(id_col)
    q1 <- bquote(database[, unique(.(idcolsym))])
    id_pool <- database[, .(Count = .N), by = id_col]
    data.table::setnames(id_pool, id_col, "ID")

    query <- bquote(id_change := WHAT == .(id_col))
    corrections[, eval(query)]
    corrections[, delete_record := grepl("^whole obs", WHAT, ignore.case = TRUE)]

    id_change_from <- corrections[missing_uid == FALSE & id_change == TRUE & incomplete_record == FALSE, .(ID = change.from, Count = -1)]
    del_record <- corrections[missing_uid == FALSE & delete_record == TRUE, .(ID = ID, Count = -1)]
    id_change_to <- corrections[missing_uid == FALSE & id_change == TRUE & incomplete_record == FALSE, .(ID = change.to, Count = 1)]

    id_pool <- data.table::rbindlist(list(id_pool, id_change_from, id_change_to, del_record), use.names = TRUE)
    id_pool <- id_pool[, .(Count = sum(Count)), by = ID]

    existing_ids <- id_pool[Count > 1L, ID]
    removed_nonexistent_ids <- id_pool[Count < 0L, ID]

    corrections[change.to %in% existing_ids, existing_id := TRUE]
    corrections[change.to %in% existing_ids, conflicting_id := as.character(change.to)]

    corrections[change.from %in% removed_nonexistent_ids, nonexistent_id_removed := TRUE]
    corrections[change.from %in% removed_nonexistent_ids, conflicting_id := as.character(change.from)]

    err_cols <- c(
        "missing_uid",
        "incomplete_record",
        "duplicate_changes",
        "multiple_conclusions",
        "what_not_found",
        "existing_id",
        "nonexistent_id_removed"
    )

    corrections[, any_issue := apply(.SD, 1L, any, na.rm = TRUE), .SDcols = err_cols]

    invisible(NULL)
}
