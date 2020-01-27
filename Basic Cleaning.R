### BASIC & GENERAL CLEANING ###


##Old code kept for reference

CLEAN <- function(DATA,DATEVAR=NULL,DATEFORMAT=NULL,BEGINDATE=NULL){
  ## Turn all non-numeric variables into character class variables:
  ###################################################################

  ## Create a function that applies the character conversion (sapply is not working well for this)
  convert.char<- function(Data,Types){
    for (i in 1:length(Data)){
      FUN <- switch(Types[i],character = as.character,
                    numeric = as.numeric,
                    factor = as.factor)
      Data[,i] <- FUN(Data[,i])
    }
    Data
  }
  ###################################################################################################
  #Do some general cleaning:
  ###################################################################################################


  ## Remove -99s & spaces from ID variables
  if( class(DATA[,grepl("ID$|ID..",names(DATA))])=="data.frame"){

    DATA[,grepl("ID|ID..",names(DATA))]<-as.data.frame(sapply(DATA[,grepl("ID|ID..",names(DATA))],function(x){
      gsub("^-99$|^99$|^[[:space:]]+-99[[:space:]]+$|^[[:space:]]+-99[[:space:]]+$|^[[:space:]]+-99$|^[[:space:]]+99$|^-99[[:space:]]+$|^99[[:space:]]+$",NA,x)
    }))

    DATA[,grepl("ID$|ID..",names(DATA))]<-as.data.frame(sapply(DATA[,grepl("ID$|ID..",names(DATA))],function(x){
      gsub("[[:space:]]+","",x)}))
  }else{
    DATA[,grepl("ID$|ID..",names(DATA))] <- gsub("[[:space:]]+","",DATA[,grepl("ID$|ID..",names(DATA))])
    DATA[,grepl("ID$|ID..",names(DATA))] <- gsub("^-99$|^99$|^[[:space:]]+-99[[:space:]]+$|^[[:space:]]+-99[[:space:]]+$|^[[:space:]]+-99$|^[[:space:]]+99$|^-99[[:space:]]+$|^99[[:space:]]+$",NA,DATA[,grepl("ID$|ID..",names(DATA))])
    }


  #Remove spaces
  DATA<-as.data.frame(sapply(DATA,function(x){gsub("^[[:space:]]+$|^[[:space:]]+|[[:space:]]+$","",x)}))
  ##**change character NAs to missing in order to be able to keep the levels in a numeric format.**
  DATA<-as.data.frame(sapply(DATA,function(x){gsub("^Na$|^NA$|^N A$|n/a|#N/A|#DIV/0!|#VALUE!",NA,x)}))

  #Delete empty variables
  ## First, identify the empty variables
  Empty       <- sapply(DATA,function(x){sum(is.na(x)|grepl("^$|^[[:space:]]+$",x))==length(x)})
  ## Keep only the non-empty variables
  DATA <- DATA[,!Empty]

  ## Delete empty observations
  empty       <- as.vector(apply(DATA,1,function(x){sum(is.na(x)|grepl("^$|^[[:space:]]+$",x))}))==length(DATA)
  DATA <- DATA[!empty,]

  ## Turn variables that have only numeric values to numeric class
  #################################################################

  ## If all levels of a variable are numeric
  for(v in 1:length(DATA)){
    if (sum(!is.na(as.integer(names(table(DATA[,v])))))==length(names(table(DATA[,v])))){
      #Then turn the variable into numeric type:
      DATA[,v]<-as.numeric(as.character(DATA[,v]))
    }
  }

  ## If more than one string variable in the dataset:
  strings <- sapply(DATA,function(x){is.character(x)|is.factor(x)})
  if(sum(strings)>1){
    ## If character variable, then turn it to upper case
    DATA[,strings] <- as.data.frame(sapply(DATA[,strings],toupper))
    ## Apply the function that converts factors into character to the factor subset:
    DATA[,strings]<-convert.char(DATA[,strings],rep("character",length(DATA[,strings])))
    }else{
      if(sum(strings)==1){
        ## If character variable, then turn it to upper case
        DATA[,strings] <- toupper(DATA[,strings])
        ## Apply the function that converts factors into character to the factor subset:
        DATA[,strings] <- as.character(DATA[,strings])
        }
      }
   ## If a start of data collection date was specified then delete observations
   if(all(c(!is.null(DATEVAR), !is.null(DATEFORMAT), !is.null(BEGINDATE)))){
   	   DATES <- as.POSIXlt(DATA [,DATEVAR], format=DATEFORMAT)
  	   DATA  <- DATA[DATES>=BEGINDATE,]
  	   }else{
  	   	if(sum(c(!is.null(DATEVAR), !is.null(DATEFORMAT), !is.null(BEGINDATE)))>0){
  	   		print(paste0("You didn't specify ", which(c(is.null(DATEVAR), is.null(DATEFORMAT), is.null(BEGINDATE)))))
  	   }}

  ## Delete empty observations
  empty       <- as.vector(apply(DATA,1,function(x){sum(is.na(x))}))==length(DATA)
  DATA <- DATA[!empty,]

  return(DATA)

}

#' Perform an initial cleaning pass
#'
#' This removes simple artifacts commonly found in raw data (-99 flags, strange NA values, and factors). Some features may already be handled by data import, but this attempts to catch the rest. Furthermore, all character data gets capitalized; this is known to clash with some multibyte Unicode strings. Inspect your raw data before using this!
#'
#' @param dat A `data.frame` to be cleaned
#' @param remove_accents If you wish accented characters to be "down-converted" to their Latin base, set this to `TRUE`.
#' @param date_cols Any columns whose data are considered dates that could be coerced to POSIXct objects
#' @param date_format A character vector (length 1 or `length(date_cols)`) that contains the desired date format for `date_cols`
#' @param begin_date A `POSIXct` date that is a reference date. If `date_cols` is length 1, all rows with a date less
#' than `begin_date` will be dropped.
#' @param factors_to_chars If `TRUE`, then all factors will be converted to character vectors
#' @return A cleaned data.frame
#'
#' @export
basic_clean <- function(dat,
                        remove_accents   = TRUE,
                        date_cols        = NULL,
                        date_format      = NULL,
                        begin_date       = NULL,
                        factors_to_chars = TRUE,
                        drop_id_ws       = TRUE,
                        id_cols          = NULL) {
    stopifnot(inherits(dat, "data.frame"))

    datl             <- as.list(dat)
    empty_cols       <- rep(FALSE, length(datl))
    detected_id_cols <- rep(FALSE, length(datl))

    for (j in seq_along(datl)) {
        current_class <- class(datl[[j]])

        if ("factor" %in% current_class && isTRUE(factors_to_chars)) {
            datl[[j]] <- as.character(datl[[j]])
        }

        if ("character" %in% current_class) {
            datl[[j]] <- tryCatch({
                toupper(datl[[j]])
            }, error = function(e) {
                warning(paste0("Multibyte string failed to make column all uppercase: ", names(datl)[[j]]), immediate. = TRUE, call. = FALSE)
                datl[[j]]
            })

            datl[[j]] <- cleanenv$clear_artifacts(datl[[j]])
            datl[[j]] <- cleanenv$coerce_class(datl[[j]])

            current_class <- class(datl[[j]])
        }

        if (!is.null(date_cols) && names(datl)[[j]] %in% date_cols) {
            if (is.null(date_format)) {
                stop("Date format must be defined", call. = FALSE)
            } else if (!length(date_format) %in% c(1L, length(date_cols))) {
                warning("Used the first date format provided. This may not be correct. Ensure the number of formats matches the number of date columns", call. = FALSE, immediate. = TRUE)

                date_format <- date_format[[1L]]
            }

            if (length(date_format) == 1L) {
                date_format <- rep(date_format, length(date_cols))
            }

            datl[[j]] <- unlist(Map(cleanenv$to_date, datl[[j]], date_format))
        }

        if ("character" %in% current_class) {
            # Final pass to get rid of dummy 99 and -99s in non-numeric data
            datl[[j]][grepl("^\\s*-?99\\s*$", datl[[j]])] <- NA

            if (isTRUE(remove_accents)) {
                datl[[j]] <- cleanenv$remove_accents(datl[[j]])
            }

            if (isTRUE(drop_id_ws)) {
                if (!is.null(id_cols) && names(datl)[[j]] %in% id_cols) {
                    datl[[j]] <- vapply(datl[[j]], function(x) gsub("\\s+", "", x), character(1L))
                } else if (is.null(id_cols) & grepl(cleanenv$ID_WS, names(datl)[[j]])) {
                    detected_id_cols[[j]] <- TRUE
                    datl[[j]] <- vapply(datl[[j]], function(x) gsub("\\s+", "", x), character(1L))
                }
            }
        }

        empty_cols[[j]] <- all(is.na(datl[[j]]) | datl[[j]] == "")
    }

    if (isTRUE(drop_id_ws)) {
        columns <- names(datl)[detected_id_cols]

        if (!is.null(id_cols)) {
            columns <- c(id_cols, columns)
        }

        message(paste0("ALL whitespace removed in ID columns: ", paste0(columns, collapse = ", ")))
    }

    datl <- datl[!empty_cols]
    datl <- cleanenv$quick_df(datl)

    if (length(date_cols) == 1L && !is.null(begin_date)) {
        datl <- datl[datl[[date_cols]] >= begin_date, ]
    }

    empty_rows <- unlist(apply(datl, 1, function(dat_row) all(is.na(dat_row) | dat_row == "")))

    datl[!empty_rows, ]
}

# Function name to be deprecated once the cleaning toolkit is converted to a package

# Private variable scoping
cleanenv <- new.env()

# Using an environment to take advantage of the hashmap implementation of environments to drop the O(N) behavior
# of lookup to O(1). chartr was being slow for some reason. This implementation is now just kinda slow.
cleanenv$BAD_CHARS <- list('a', 'A', 'a', 'A', 'a', 'A', 'a', 'A', 'a', 'A', 'a', 'A', 'a', 'A', 'c', 'C', 'o', 'e', 'E', 'e', 'E', 'e', 'E', 'e', 'E', 'i', 'I', 'i', 'I', 'i', 'I', 'i', 'I', 'n', 'N', 'o', 'O', 'o', 'O', 'o', 'O', 'o', 'O', 'o', 'O', 'o', 'O', 's', 'S', 'S', 'u', 'U', 'u', 'U', 'u', 'U', 'U', 'y', 'y', 'Y', 'y', 'z', 'Z', 'b', 'B')
names(cleanenv$BAD_CHARS) <- c('á', 'Á', 'à', 'À', 'â', 'Â', 'å', 'Å', 'ä', 'Ä', 'ã', 'Ã', 'æ', 'Æ', 'ç', 'Ç', 'ð', 'é', 'É', 'è', 'È', 'ê', 'Ê', 'ë', 'Ë', 'í', 'Í', 'ì', 'Ì', 'î', 'Î', 'ï', 'Ï', 'ñ', 'Ñ', 'ó', 'Ó', 'ò', 'Ò', 'ô', 'Ô', 'ö', 'Ö', 'õ', 'Õ', 'ø', 'Ø', 'š', 'Š', 'ß', 'ú', 'Ú', 'ù', 'Ù', 'û', 'Û', 'Ü', 'ý', 'ý', 'Ý', 'ÿ', 'ž', 'Ž', 'þ', 'Þ')

cleanenv$BAD_CHARS <- list2env(cleanenv$BAD_CHARS)

# Pattern to test if column is ID. Preferably, supply ID columns via id_cols
cleanenv$ID_WS <- "\\w+ID[^[:alpha:]]|ID$|ID.."

#' Quickly convert a list to a dataframe
#'
#' For lists that have a structure like a `data.frame` (constant number of rows in each column and each column is an atomic vector), add the necessary metadata to the list to transform to a `data.frame` without any checks and change the class. This is a much quicker form of `as.data.frame` when reasonable assumptions of the input list's structure are in place. [Written by Hadley Wickham](http://adv-r.had.co.nz/Profiling.html#be-lazy)
#'
#' @param l A list whose structure mirrors that of a `data.frame`
#' @return A `data.frame` form of the list
#'
#' @name cleanenv$quick_df
#' @keywords internal
cleanenv$quick_df <- function(l) {
    class(l) <- "data.frame"
    attr(l, "row.names") <- .set_row_names(length(l[[1]]))

    l
}

#' Remove whitespace and common NA values
#'
#' If not already taken care of during file reading using \code{\link[data.table]{fread}}, \code{\link[readr]{read_csv}} (and relatives), or \code{\link{read.table}} (and relatives), get rid of excess whitespace in cells and convert extra common "NA" strings to `NA`. To prevent excess class coercion, this only works for character vectors.
#'
#' @param vec A character vector
#' @return A cleared character vector
#'
#' @name cleanenv$clear_artifacts
#' @keywords internal
cleanenv$clear_artifacts <- function(vec) {
    stopifnot(is.character(vec))

    not_na     <- !is.na(vec)
    ws_pattern <- "^\\s+|\\s+$"
    na_pattern <- "^N\\s?\\/?A$|^#N/A$|^#DIV/0!$|^#VALUE!$"

    ws_match <- not_na & grepl(ws_pattern, vec)
    na_match <- not_na & grepl(na_pattern, vec, ignore.case = T)

    vec[ws_match] <- gsub(ws_pattern, "", vec[ws_match])
    vec[na_match] <- gsub(na_pattern, "", vec[na_match], ignore.case = T)

    vec[not_na & (vec == "")] <- NA

    vec
}

#' Safely coerce character vectors into other classes
#'
#' Converts a character vector to other classes. This tries to coerce each element to a `integer`, `double`, or `complex` (in that order), and if all non-`NA` elements can be coerced, it will execute the coercion.
#'
#' @param vec A character vector to be coerced
#' @return The possibly-coerced vector
#'
#' @name cleanenv$coerce_class
#' @keywords internal
cleanenv$coerce_class <- function(vec) {
    stopifnot(is.character(vec))

    tryCatch({
        as.integer(vec)
    }, warning = function(warn) {
        tryCatch({
            as.double(vec)
        }, warning = function(warn) {
            tryCatch({
                as.complex(vec)
            }, warning = function(warn) {
                vec
            })
        })
    }, error = function(e) {
        vec
    })
}

#' Convert integral or character data to dates
#'
#' Essentially, this is a wrapper to as.POSIXct with known pre-defined parameters because as.POSIXct is a pain.
#'
#' @param vec A vector that represents temporal information
#' @return A `POSIXct` vector
#'
#' @name cleanenv$to_date
#' @keywords internal
cleanenv$to_date <- function(vec, format) {
    if (is.numeric(vec)) {
        as.POSIXct(vec, format = format, origin = "1960-01-01")
    } else {
        as.POSIXct(vec, format = format)
    }
}

#' Drop accented characters
#'
#' Maps known accented characters to their English counterparts.
#'
#' If `stringi` is installed, this will use stringi::stri_trans_general() convert to Latin-ASCII.
#' Otherwise, it will use a hard-coded transformation (please install stringi).
#'
#' @param vec A character vector which may have "accented" characters (Latin/Germanic)
#' @return A character vector without accented characters
#'
#' @name cleanenv$remove_accents
#' @keywords internal
cleanenv$remove_accents <- function(vec) {
    stopifnot(is.character(vec))

    if (requireNamespace("stringi", quietly = TRUE)) {
        return(stringi::stri_trans_general(vec, id = "Latin-ASCII"))
    }

    message("Using the hard-coded character translations to remove \"accented\" characters. Please install `stringi` to use a better engine.")

    vec[!is.na(vec)] <- vapply(vec[!is.na(vec)], function(x) {
        tryCatch({
            chars <- strsplit(x, "")[[1L]]

            for (.c in chars) {
                if (!is.null(cleanenv$BAD_CHARS[[.c]])) {
                    x <- gsub(.c, cleanenv$BAD_CHARS[[.c]], x)
                }
            }

            x
        }, error = function(e) {
            browser()

            warning(paste0("Bad string encountered. Leaving as-is in accent removal..."), call. = FALSE, immediate. = TRUE)
            x
        })
    }, character(1L))

    vec
}
