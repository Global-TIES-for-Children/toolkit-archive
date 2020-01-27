### IDENTIFY NAME MISMATCHES by PAIRS
### An attempt at filtering name mismatches for revisions will also be done based on letter changes

## If more than x% of characters need to be changed for the names to match AND one name is not contained in the other, then categorize as a name mismatch.
Name.Missmatch <- function(NAME_A,NAME_B,DATA,PROPORTION,NAME_ROOT = NULL){
  Perc                  <- rep(NA,nrow(DATA)) 
  DATA$NAMEnotCONTAINED <- rep(FALSE,nrow(DATA))
  ISSUE                 <- rep(FALSE,nrow(DATA))
  DATA$BNOTinA          <- rep(FALSE,nrow(DATA))
  DATA$ANOTinB          <- rep(FALSE,nrow(DATA))
  ## Only run the content of the following loop for the rows for which both NAME_A and NAME_B
  ## are NOT NA (otherwise it just increases computing burden). We already know these cases
  ## should NOT be flagged for revision.
  
  not.nas <- which(!grepl("^NA$|^[[:space:]]+NA[[:space:]]+$|^[[:space:]]+N[[:space:]]+A[[:space:]]+$",DATA[,NAME_A]) &
                     !grepl("^NA$|^[[:space:]]+NA[[:space:]]+$|^[[:space:]]+N[[:space:]]+A[[:space:]]+$",DATA[,NAME_B]) &
                     !is.na(DATA[,NAME_A]) & !is.na(DATA[,NAME_B]))
  
  if(length(not.nas)>0){ ## Only run the name checks if both of the vectors have, at least, some names in them
    
    for(i in not.nas){ # change
      nameA <- DATA[,NAME_A][i]
      nameB <- DATA[,NAME_B][i]
      print(paste0("Looking for name missmatches in entry:", i," Name_A:",DATA[,NAME_A][i], "Name_B:",DATA[,NAME_A][i]))
      ## Pair 1
      DATA$BNOTinA[i]      <-  !grepl(nameB,nameA,ignore.case = T)
      DATA$ANOTinB[i]      <-  !grepl(nameA,nameB,ignore.case = T)
      
      # Allow flexible margins for name checking:
      # If PROPORTION is a list, check if NAME_ROOT or NAME_A is in the names of PROPORTION
      # If so, use that value in PROPORTION. If NAME_A is **not** in the names of PROPORTION,
      # yet ".others" is, use that value. Otherwise, raise an error. If PROPORTION is not a list,
      # use PROPORTION as the value.
      prop <- if (is.list(PROPORTION)) {
        name <- if (!is.null(NAME_ROOT)) {
          NAME_ROOT
        } else {
          NAME_A
        }

        if (name %in% names(PROPORTION)) {
          stopifnot(is.numeric(PROPORTION[[name]]))

          PROPORTION[[name]]
        } else if (".others" %in% names(PROPORTION)) {
          stopifnot(is.numeric(PROPORTION[[".others"]]))

          PROPORTION[[".others"]]
        } else {
          stop("Unspecified proportion to use. Please provide NAME_A as an entry in PROPORTION or '.others' as a default case", call. = FALSE)
        }
      } else {
        PROPORTION
      }

      #       Perc[i]              <- ceiling(max(nchar(nameB),nchar(nameA),na.rm = T)*prop)
      Perc[i] <- prop
    }
    
    ## By pair (write a more succint function later)
    DATA$NAMEnotCONTAINED <- DATA$BNOTinA==T & DATA$ANOTinB==T
    
    ##If more than x% of the letters don't match, flag it
    #     ISSUE[not.nas] <- (RecordLinkage::levenshteinDist(gsub("[[:space:]]+","",DATA[not.nas,NAME_B]),
    #                                                gsub("[[:space:]]+","",DATA[not.nas,NAME_A])) > Perc) & DATA$NAMEnotCONTAINED1[not.nas]==T
    ISSUE[not.nas] <- (stringdist::stringsim(
        gsub("\\s+", "", DATA[not.nas, NAME_A]),
        gsub("\\s+", "", DATA[not.nas, NAME_B]),
    ) < (1 - Perc)) & DATA$NAMEnotCONTAINED[not.nas] == TRUE
    
  }                 
  
  ISSUE[is.na(ISSUE)] <- FALSE
  return(ISSUE)
}

notsim_chr <- function(col_a, col_b, database, proportion, varname = NULL) {
    a <- gsub("\\s+", "", as.character(database[[col_a]]))
    a[!is.na(a) & a == "NA"] <- NA
    b <- gsub("\\s+", "", as.character(database[[col_b]]))
    b[!is.na(b) & b == "NA"] <- NA

    notnas <- !(is.na(a) | is.na(b)) 
    nsim <- rep(FALSE, length(a))

    ainb <- purrr::map2_lgl(a, b, function(.x, .y) as.logical(grepl(.y, .x, ignore.case = TRUE)))
    bina <- purrr::map2_lgl(a, b, function(.x, .y) as.logical(grepl(.x, .y, ignore.case = TRUE)))

    prop <- if (is.list(proportion)) {
        name <- if (!is.null(varname)) {
            varname
        } else {
            col_a
        }

        if (name %in% names(proportion)) {
            stopifnot(is.numeric(proportion[[name]]))

            proportion[[name]]
        } else if (".others" %in% names(proportion)) {
            stopifnot(is.numeric(proportion[[".others"]]))

            proportion[[".others"]]
        } else {
            stop("Unspecified proportion to use. Please provide col_a as an entry in proportion or '.others' as a default case", call. = FALSE)
        } 
    } else {
        proportion
    }

    asimb <- stringdist::stringsim(a, b) >= (1 - prop)

    nsim[notnas] <- !(ainb[notnas] | bina[notnas] | asimb[notnas])
    nsim[is.na(nsim)] <- FALSE  # Treat NAs as OK for now

    nsim
}
