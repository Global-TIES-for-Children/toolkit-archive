library(rlang)
library(purrr)
library(dplyr)

# ------ Constants ------
DEFAULT_MAPPING_COLSPEC <- list(
    group     = "Group",
    name1     = "HomogenizedName",
    coding    = "FinalCoding",
    subgroup1 = "SubgroupDB3",
    reduce1   = "ReductionDB3",
    subgroup2 = "SubgroupDB3M",
    reduce2   = "ReductionDB3M",
    name2     = "FinalNameDB3",
    subgroup3 = "SubgroupDB4",
    reduce3   = "ReductionDB4",
    name3     = "FinalNameDB4"
)

ESCAPE_PREFIX <- "___VARMAPPED___"

# ------ Helpers ------
`%||%` <- function(x, y) if (is.null(x)) y else x

`%|%` <- function(x, y) {
    out <- x | y

    # Override missing algebra
    out[is.na(out) & is.na(x) & !is.na(y)] <- FALSE
    out[is.na(out) & !is.na(x) & is.na(y)] <- FALSE

    out
}

DEBUG <- function(string) if (getOption("tiestk.debug", default = FALSE)) cat(string, "\n", file = stderr(), sep = "")

FLATTEN <- function(...) {
    if (missing(..1)) {
        stop("At least one value must be supplied to FLATTEN")
    }
    
    x <- list(...)
    
    if (length(x) == 1L) {
        x <- x[[1L]]
        
        if (is.data.frame(x)) {
            return(do.call(FLATTEN, as.list(x)))
        } else if (length(x) < 2L) {
            return(x)
        }
    }
    
    y    <- x[[1L]]
    take <- x[-1]
    
    for (j in seq_along(take)) {
        stopifnot(exprs = {
            length(take[[j]]) == length(y) || length(take[[j]]) == 1L
            typeof(take[[j]]) == typeof(y)
        })
        
        if (length(take[[j]]) == 1L) {
            take[[j]] <- rep(take[[j]], length(y))
        }
        
        y[is.na(y)] <- take[[j]][is.na(y)]
        
        if (length(y) < 2L && all(!is.na(y))) break # Don't waste clock cycles
    }
    
    y
}

CONSOLIDATE <- function(x, columns = NULL, pattern = NULL, consolidation_func = NULL, ...) {
    pickfunc <- function(r) {
        ur <- unique(r)

        if (length(ur[!is.na(ur)]) > 0L) {
            ur <- ur[!is.na(ur)]
        }

        if (length(ur[!is.na(ur)]) > 1L && !is.null(consolidation_func)) {
            if (!is.null(consolidation_func)) {
                ur <- consolidation_func(ur, ...)

                if (length(ur) > 1L) {
                    stop("Consolidation function must collapse all values to one", call. = FALSE)
                }
            } else {
                stop("Multiple unique values caught with no consolidation function: ", r, call. = FALSE)
            }
        }

        ur
    }

    if (inherits(x, "data.frame")) {
        DT <- data.table::as.data.table(x)
        colname <- "__GENCOL__"


        if (is.null(columns) && is.null(pattern)) {
            # Consolidate all columns into one
            DT[, c(colname) := apply(DT, 1L, pickfunc)]
        } else if (!is.null(columns)) {
            DT[, c(colname) := apply(.SD, 1L, pickfunc, .SDcols = columns)]
        } else if (!is.null(pattern)) {
            cols <- names(DT)[grepl(pattern, names(DT))]

            DT[, c(colname) := apply(.SD, 1L, pickfunc), .SDcols = cols]
        }

        if (!colname %in% names(DT)) {
            stop("Screwy error happened. Somehow consolidation did not properly create the target variable.")
        }

        return(DT[[colname]])
    } else if (!is.recursive(x)) {
        return(pickfunc(x))
    } else {
        stop("CONSOLIDATE only defined for vectors and data.frame-like objects.", call. = FALSE)
    }
}

SUM <- function(df) {
    rowSums(df, na.rm = TRUE)
}

DEDUMMIFY <- function(df, coding_vec) {
    stopifnot(length(df) == length(coding_vec), inherits(df, "data.frame"))

    dt <- data.table::as.data.table(df)
    varnames <- data.table::copy(names(dt))

    if (is.null(names(coding_vec))) {
        names(coding_vec) <- varnames
    } else {
        stopifnot(setequal(names(coding_vec), varnames))
    }

    # Check to see if df *can* be dedummified
    dt[, dedummifiable := apply(.SD, 1L, function(x) {
        x <- tryCatch(as.integer(x), warning = function(w) stop("Cannot coerce dummies to integers", call. = FALSE))
        
        sum(x, na.rm = TRUE) <= 1
    })]

    if (dt[, !all(dedummifiable)]) {
        stop("Multiple choice matrix encountered. Cannot dedummify safely.", call. = FALSE)
    }

    for (vn in varnames) {
        dt[, (vn) := ifelse(!is.na(dt[[vn]]) & dt[[vn]] == 1, coding_vec[vn], NA)] 
    }

    dt[, data.table::fcoalesce(.SD), .SDcols = varnames]
}

COALESCE <- function(dat, cols, master_col = NULL, id_col = "MERGE_STID", time_col = ".TIME", ranges = list(y1 = 1:3, y2 = 4:5), priority = NULL) {
    dt <- data.table::as.data.table(dat)
    selected <- names(dt)[names(dt) %in% c(id_col, time_col, cols)]

    dt <- dt[, selected, with = FALSE]
    data.table::setnames(dt, c(id_col, time_col), c(".id", ".time"))

    coalesced <- lapply(ranges, .coalesce_times, dt, cols, priority)
    names(coalesced) <- names(ranges)

    master_col <- if (is.null(master_col)) {
        "master_col"
    } else {
        master_col
    }

    for (range in names(ranges)) {
        dt[.time %in% ranges[[range]], (master_col) := coalesced[[range]][.SD, variable, on = ".id"]]
    }

    dt[, master_col, with = FALSE][[1L]]
}

coalesce_long <- COALESCE

.coalesce_times <- function(range, dt, cols, priority) {
    sub_dt <- dt[.time %in% range]

    if (!is.null(priority)) {
        data.table::setcolorder(sub_dt, c(".id", ".time", priority))
    }

    dt_wide <- data.table::dcast(sub_dt, .id ~ .time, value.var = cols)

    # Drop all NA cols
    empty_cols <- dt_wide[, vapply(.SD, function(x) all(is.na(x)), logical(1L))]
    dt_wide <- dt_wide[, names(empty_cols[!empty_cols]), with = FALSE]

    dt_wide[, variable := data.table::fcoalesce(.SD), .SDcols = names(dt_wide)[names(dt_wide) != ".id"]]

    dt_wide
}

typecast_char <- function(charvec) {
    vec <- charvec[!is.na(charvec)]

    bools <- "^T$|^F$|^[tT]rue$|^TRUE$|^[fF]alse$|^FALSE$"
    uints <- "\\d+"
    fracs <- "\\.\\d*"
    exps  <- "e[+-]?\\d+"

    ints     <- paste0("^-?", uints, "$")
    numerics <- paste0(uints, "(", fracs, ")?(", exps, ")?")
    dbls     <- paste0("^-?", numerics, "$")
    cmplxes  <- paste0("^-?", numerics, "[+-]", numerics, "i$")

    if (all(grepl(bools, vec))) {
        charvec <- as.logical(charvec)
    } else if (all(grepl(ints, vec))) {
        charvec <- as.integer(charvec)
    } else if (all(grepl(dbls, vec, ignore.case = TRUE))) {
        charvec <- as.double(charvec)
    } else if (all(grepl(cmplxes, vec, ignore.case = TRUE))) {
        charvec <- as.complex(charvec)
    }

    charvec
}

str_escape <- function(string) stringr::str_replace_all(string, "(\\W)", "\\\\\\1")

all.na <- function(vec) {
    length(vec[is.na(vec)]) == length(vec)
}

.pw_char <- function(num, pw_max = 32L) {
    # A little spinner for some user feedback while HomogenizeWaveVariables and ApplyCodebook are working
    if (pw_max < 4L) {
        # Idiot check
        pw_max <- 4L
    }
    
    pw_step <- pw_max %/% 4L
    pw_range <- function(point) {
        seq(from = (point * pw_step), to = ((point * pw_step) + (pw_step - 1L)))
    }
    
    if (pw_max < 8L) {
        if (num %% 4L == 0L) {
            "-"
        } else if (num %% 4L == 1L) {
            "/"
        } else if (num %% 4L == 2L) {
            "|"
        } else {
            "\\"
        }
    } else {
        if ((num %% pw_max) %in% pw_range(0L)) {
            "-"
        } else if ((num %% pw_max) %in% pw_range(1L)) {
            "/"
        } else if ((num %% pw_max) %in% pw_range(2L)) {
            "|"
        } else {
            "\\"
        }
    }
}

`%not_in%` <- function(x, y) !(x %in% y)

MODE <- function(..., na.rm = FALSE) {
    # Shamelessly lifted from Stack Overflow because oddly mode is not already built into a statistical analysis language?
    # Also uses varargs for Reduction DSL parsing
    vec <- list(...)

    if (length(vec) == 1L) {
        vec <- vec[[1L]]
    }

    unique_entries <- if (na.rm == TRUE) {
        u <- unique(vec)
        u[!is.na(u)]
    } else {
        unique(vec)
    }
    
    unlist(unique_entries[which.max(tabulate(match(vec, unique_entries)))])
}

.make_env_bundle <- function(list_assessments) {
    if (all(grepl("^T\\d+$", names(list_assessments)))) {
        # Post database merge
        merge_env <- new.env()
        
        for (t in names(list_assessments)) {
            merge_env[[t]] <- list_assessments[[t]]
        }
        
        return(merge_env)
    }
    
    env_bundle <- lapply(names(list_assessments), function(name) new.env())
    names(env_bundle) <- names(list_assessments)
    
    for (name in names(list_assessments)) {
        env_bundle[[name]]$group <- name

        # Pre-intragroup merge
        if (is(list_assessments[[name]], "list")) {
            for (i in seq_along(list_assessments[[name]])) {
                env_bundle[[name]][[paste0("T", i)]] <- list_assessments[[name]][[i]]
            }
        } else { # Post-intragroup merge
            env_bundle[[name]]$merged <- list_assessments[[name]]
        }
        
    }
    
    env_bundle
}

.recombine_list_with_errors <- function(env_bundle) {
    errors  <- list()
    warns   <- list()
    mapping <- list()
    
    if (is.list(env_bundle)) {
        recombined <- as.list(rep(NA, length(env_bundle)))
        names(recombined) <- names(env_bundle)
        
        for (df_env in env_bundle) {
            errors[[length(errors) + 1]] <- df_env$err
            warns[[length(warns) + 1]]   <- df_env$warn
            
            if (exists("mapping", envir = df_env)) {
                mapping[[length(mapping) + 1]] <- df_env$mapping
            }
            
            # Grab all of the time points and assemble into a list for output
            time_points_names <- keep(names(df_env), function(name) grepl("^T", name))
            time_points <- map(time_points_names, function(name) df_env[[name]])
            names(time_points) <- time_points_names
            
            time_points_order <- time_points_names %>%
                stringr::str_extract("\\d+$") %>%
                as.integer() %>%
                order()
            
            recombined[[df_env$group]] <- time_points[time_points_order]
        }
        
        errors <- reduce(errors, rbind)
        warns  <- reduce(warns, rbind)
        
        if (length(mapping) > 0) {
            mapping <- arrange(reduce(mapping, rbind), CODE_ID)
            
            list(output = recombined, err = errors, warn = warns, map = mapping)
        }  else {
            list(output = recombined, err = errors, warn = warns)
        }
    } else {
        errors[[length(errors) + 1]] <- env_bundle$err
        warns[[length(warns) + 1]]   <- env_bundle$warn
        
        # Grab all of the time points and assemble into a list for output
        time_points_names <- keep(names(env_bundle), function(name) grepl("^T", name))
        time_points <- map(time_points_names, function(name) env_bundle[[name]])
        names(time_points) <- time_points_names
        
        time_points_order <- time_points_names %>%
            stringr::str_extract("\\d+$") %>%
            as.integer() %>%
            order()
        
        recombined <- time_points[time_points_order]
        
        errors <- reduce(errors, rbind)
        warns  <- reduce(warns, rbind)
        
        if (length(mapping) > 0) {
            mapping <- arrange(reduce(mapping, rbind), CODE_ID)
            
            list(output = recombined, err = errors, warn = warns, map = mapping)
        }  else {
            list(output = recombined, err = errors, warn = warns)
        }
    }
}

.print_list_assessments <- function(list_assessments, output_directory, file_prefix = "T", variable_prefix = "___VARMAPPED___") {
    for (group in names(list_assessments)) {
        cwd <- file.path(output_directory, group)

        if (!dir.exists(cwd)) {
            dir.create(cwd, recursive = TRUE)
        }
        
        not_na <- list_assessments[[group]][!is.na(list_assessments[[group]])]

        for (i in seq_along(not_na)) {
            if (!is.na(variable_prefix)) {
                # Clear any marker prefixes

                cleared <- map_chr(names(not_na[[i]]), function(name) gsub(variable_prefix, "", name))
                names(not_na[[i]]) <- cleared
            }

            file_path <- file.path(cwd, paste0(file_prefix, "_", .homogenize$wave_encode(i, .homogenize$USER_WAVES_OVERRIDE), ".csv"))
            write.csv(not_na[[i]], file = file_path, row.names = F)
        }
    }
}

# ------ Exported functions ------

#' Homogenize variables across waves
#'
#' Given a list of base, mid, and end assessments, homogenize (and capitalize) the variables based on the base variable name.
#' `mapping` must have Baseline and Endline at the minimum. If some year has 3 waves of assessments, the input `mapping` **must** have a filled-in `NameMidline` column as well.
HomogenizeWaveVariables <- function(mapping, 
                                    list_assessments, 
                                    waves = NULL, 
                                    report_errors = F, 
                                    report_directory = NA, 
                                    export_assessments = F, 
                                    export_directory = NA) {

    .homogenize$USER_WAVES_OVERRIDE <- waves

    # Check to see if time points are explicitly defined -- logical test avoids deep is.na call
    if (!is.null(.homogenize$USER_WAVES_OVERRIDE)) {
        # Ensure that the number of time points per each assessment grouping does not exceed the number of waves
        stopifnot({
            !any(map_lgl(list_assessments, function(group) length(group) > length(waves)))
        })
    }
    
    mapping <- .homogenize$mapping_clean(mapping)
    checked_mapping <- .homogenize$mapping_error_check(mapping)
    
    if (nrow(checked_mapping$err) > 0) {
        warning("Some errors are present in the mapping file. These mappings were ignored.", call. = F)
        
        if (report_errors) {
            err_report_file  <- paste0(report_directory, "/", "mapping-errs.csv")
            warning(paste0("Please see \"", err_report_file, "\" for the list of variables that have some errors."), call. = F)
            write.csv(checked_mapping$err, file = err_report_file, row.names = F)
        }
    }
    
    env_bundle <- .homogenize$process_chunks(checked_mapping$map, list_assessments)
    
    recombined <- .recombine_list_with_errors(env_bundle)
    
    if (nrow(recombined$err) > 0 || nrow(recombined$warn) > 0) {
        warning("Some errors occurred in the homogenization process. These erroneous variables were ignored.", call. = F)
        
        if (report_errors) {
            err_report_file <- paste0(report_directory, "/homogenization-errs.csv")
            warn_report_file <- paste0(report_directory, "/homogenization-warns.csv")
            warning(paste0("Please see \"", err_report_file, "\" for the list of variables that have some errors."), call. = F)
            warning(paste0("Please see \"", warn_report_file, "\" for the list of variables that have some warnings."), call. = F)

            write.csv(recombined$err,  file = err_report_file,  row.names = F)
            write.csv(recombined$warn, file = warn_report_file, row.names = F)
        }
    }

    if (export_assessments) {
        .print_list_assessments(recombined$output, export_directory)
    }
    
    # Mark for garbage collection
    rm(env_bundle)
    
    list(mapping = recombined$map, data = recombined$output)
}

#' Merge homogenized assessments
#' 
#' Merge a set of assessments from `HomogenizeWaveVariables` into a single dataframe. Leverages `data.table` to make the merges use pass-by-reference. Uses `id` as the merging column (more often than not, this should be a student ID).
#' Note: The `id` columns **must not** have any duplicates or NA values. This is an assumption since the input assessments should already by verified.
#' 
#' @param list_assessments A list of assessment groupings. Should be the output of `HomogenizeWaveVariables`
#' @param id The column by which the assessments merge
#' @param output_file If not NA, this is the path for the CSV of the merged dataframe
#' @return The merged dataframe
MergeDatabases <- function(list_assessments, 
                           id,
                           times_col     = ".TIME",
                           merge_col     = ".MERGE",
                           prefix_select = ESCAPE_PREFIX,
                           output_file   = NULL) {
    merge_env  <- new.env(parent = emptyenv())
    merge_env$pw <- 0
    
    time_point_length <- length(list_assessments[[1L]])
    stopifnot(all(vapply(list_assessments, length, integer(1L)) == time_point_length))
    
    time_points <- 1:time_point_length
    dbkeys      <- paste0("T", time_points)

    .list_to_merge_env(merge_env, list_assessments, dbkeys)
    
    content_summary <- .data_content_summary(list_assessments)
    variable_collisions <- unique(content_summary[, .(variable, database)])
    variable_collisions[, has_collision := .N > 1, by = variable]
    variable_collisions[, output_name := variable]
    variable_collisions[has_collision == TRUE, output_name := paste0(variable, "_", database)]

    # Set up merging column
    merge_id <- id
    
    if (length(id) > 1L) {
        if (is.null(merge_col)) stop("Merge column name must be specified", call. = FALSE)
        
        merge_id <- merge_col
    }

    # Drop merge_id from collisions
    variable_collisions <- variable_collisions[variable != merge_id]

    # Alert about variable collisions
    cat("Encountered conflicting variables:\n", file = stderr(), sep = "")
    cat(paste0(paste0("  - ", variable_collisions[has_collision == TRUE, unique(variable)]), collapse = "\n"), "\n", file = stderr(), sep = "")
    cat('All suffixed with _[database]\n\n', file = stderr(), sep = "")
    
    purrr::walk(time_points, function(t) {
        dbkey  <- dbkeys[[t]]
        ok_dbs <- which(vapply(merge_env[[dbkey]], function(db) !is(db, "logical"), logical(1L)))
        
        if (length(ok_dbs) > 1L) {
            for (db in names(merge_env[[dbkey]][ok_dbs])) {
                # Prefix-select variables
                if (!is.null(prefix_select)) {
                    if (any(grepl(prefix_select, names(merge_env[[dbkey]][[db]])))) {
                        merge_env[[dbkey]][[db]] <- dplyr::select(merge_env[[dbkey]][[db]], dplyr::starts_with(prefix_select))
                        names_x <- names(merge_env[[dbkey]][[db]])
                        names(names_x) <- vapply(names_x, function(n) gsub(prefix_select, "", n), character(1L))
                        
                        merge_env[[dbkey]][[db]] <- dplyr::rename(merge_env[[dbkey]][[db]], !!!names_x)
                    }
                }
                
                # Generate MERGE_ID if needed
                if (length(id) > 1L) {
                    notindb <- id[!id %in% names(merge_env[[dbkey]][[db]])]
                    
                    if (length(notindb) == length(id)) stop(paste0("At least one merge column must be present in ", db, "$", dbkey), call. = FALSE)
                    
                    merge_env[[dbkey]][[db]][, notindb] <- NA
                    merge_env[[dbkey]][[db]][[merge_col]] <- FLATTEN(merge_env[[dbkey]][[db]][, id])
                }

                # Check for duplicates in merge_id
                if (any(duplicated(merge_env[[dbkey]][[db]][[merge_id]]))) {
                    dupes <- duplicated(merge_env[[dbkey]][[db]][[merge_id]])

                    warning(paste0("Duplicated IDs encountered in ", db, " database merge: ", 
                            paste0(unique(merge_env[[dbkey]][[db]][[merge_id]][dupes]), collapse = ", "),
                            ". These were removed for merging. Please fix!"),
                            call. = FALSE,
                            immediate. = TRUE)

                    merge_env[[dbkey]][[db]] <- merge_env[[dbkey]][[db]][!dupes, ]
                }
            }

            renames <- list()

            for (db in names(merge_env[[dbkey]][ok_dbs])) {
                needs_rename <- variable_collisions[has_collision == TRUE & database == db, variable]
                names(needs_rename) <- variable_collisions[has_collision == TRUE & database == db, output_name]

                renames[[db]] <- as.list(needs_rename)
            }

            
            for (db in names(renames)) {
                # Filter which renames are valid
                time_renames <- renames[[db]]
                time_renames <- time_renames[time_renames %in% names(merge_env[[dbkey]][[db]])]

                merge_env[[dbkey]][[db]] <- dplyr::rename(merge_env[[dbkey]][[db]], !!!time_renames)
            }
            
            merge_env[[dbkey]] <- purrr::reduce(merge_env[[dbkey]][ok_dbs], function(x, y) {
                dplyr::full_join(x, y, by = merge_id) 
            })
            
            
            merge_env[[dbkey]][[times_col]] <- t
        } else if (length(ok_dbs) == 1L) {
            merge_env[[dbkey]] <- merge_env[[dbkey]][[ok_dbs]]
            
            if (!is.null(prefix_select)) {
                if (any(grepl(prefix_select, names(merge_env[[dbkey]])))) {
                    merge_env[[dbkey]] <- dplyr::select(merge_env[[dbkey]], dplyr::starts_with(prefix_select))
                    names_x <- names(merge_env[[dbkey]])
                    names(names_x) <- vapply(names_x, function(n) gsub(prefix_select, "", n), character(1L))
                    
                    merge_env[[dbkey]] <- dplyr::rename(merge_env[[dbkey]], !!!names_x)
                }
            }

            # Variable collisions
            db <- names(ok_dbs)
            needs_rename <- variable_collisions[has_collision == TRUE & database == db, variable]
            names(needs_rename) <- variable_collisions[has_collision == TRUE & database == db, output_name]
            renames <- as.list(needs_rename)
            
            # Filter out ones that only exist in working database
            renames <- renames[renames %in% names(merge_env[[dbkey]])]
            merge_env[[dbkey]] <- dplyr::rename(merge_env[[dbkey]], !!!renames)

            merge_env[[dbkey]][[times_col]] <- t
        } else {
            merge_env[[dbkey]] <- NA
        }
    })
    
    env_l <- as.list(merge_env)
    rm(merge_env)
    
    env_l[grepl("^T\\d+$", names(env_l))]
}

.list_to_merge_env <- function(merge_env, list_assessments, dbkeys) {
    for (group in names(list_assessments)) {
        names(list_assessments[[group]]) <- dbkeys
    }

    for (dbk in dbkeys) {
        merge_env[[dbk]] <- list()

        for (group in names(list_assessments)) {
            if (is.data.frame(list_assessments[[group]][[dbk]])) {
                merge_env[[dbk]][[group]] <- list_assessments[[group]][[dbk]]
            }
        }
    }

    invisible(NULL)
}

.data_content_summary <- function(list_assessments) {
    datalist <- lapply(names(list_assessments), function(group) {
        dbkeys <- paste0("T", seq_along(list_assessments[[group]]))
        names(list_assessments[[group]]) <- dbkeys

        ok_dbs <- vapply(dbkeys, function(dbk) is.data.frame(list_assessments[[group]][[dbk]]), logical(1L))

        content_dbs <- lapply(dbkeys[ok_dbs], function(dbk) {
            data.table::data.table(variable = names(list_assessments[[group]][[dbk]]), time = dbk, database = group)
        })

        data.table::rbindlist(content_dbs)
    })

    data.table::rbindlist(datalist)
}

MergeTimes <- function(merged_list,
                       id,
                       times_col     = ".TIME",
                       prefix_select = NULL,
                       mapping       = NULL,
                       map_colspec   = NULL,
                       safe_coerce   = TRUE) {
    ok_times <- which(!is.na(merged_list))
    
    if (length(ok_times) > 1L) {
        merged <- purrr::reduce(merged_list[ok_times], function(x, y) { 
            if (!is.null(prefix_select)) {
                if (any(grepl(prefix_select, names(x)))) {
                    x <- dplyr::select(x, dplyr::starts_with(prefix_select))
                    names_x <- names(x)
                    names(names_x) <- vapply(names_x, function(n) gsub(prefix_select, "", n), character(1L))
                    
                    x <- dplyr::rename(x, !!!names_x)
                }
                
                y <- dplyr::select(y, dplyr::starts_with(prefix_select))
                names_y <- names(y)
                names(names_y) <- vapply(names_y, function(n) gsub(prefix_select, "", n), character(1L))
                
                y <- dplyr::rename(y, !!!names_y)
            }
            
            notinx <- setdiff(names(y), names(x))
            notiny <- setdiff(names(x), names(y))
            
            x[, notinx] <- NA
            y[, notiny] <- NA
            
            cols_x <- setdiff(names(x), c(id, times_col))
            cols_y <- setdiff(names(y), c(id, times_col))
            order_cols_x <- c(id, times_col, cols_x[order(cols_x)])
            order_cols_y <- c(id, times_col, cols_y[order(cols_y)])
            
            x <- x[, order_cols_x]
            y <- y[, order_cols_y]
            
            rbind(x, y)
        })
        
        merged <- dplyr::as_tibble(merged)
        
        if (!is.null(mapping)) {
            if (is.null(map_colspec)) stop("A mapping column specification in the form `list(name = \"...\", group = \"...\")` must be provided", call. = FALSE)
            
            col_order <- unique(mapping[[map_colspec$name]])
            col_order <- col_order[!is.na(col_order)]
            
            restored_order <- unlist(lapply(col_order, function(co) {
                databases <- unique(mapping[[map_colspec$group]])
                databases <- databases[!is.na(databases)]
                
                group_pattern <- paste0(paste0("^", co, "_", databases, "$"), collapse = "|")
                
                names(merged)[grepl(paste0("^", co, "$|", group_pattern), names(merged))]
            }))
            
            missing_cols <- setdiff(names(merged), restored_order)
            
            if (length(missing_cols) > 0L) {
                warning("Ordering columns after merging times resulted in some missed columns. These are now at the end of the dataset.", call. = FALSE, immediate. = TRUE)
            }
            
            final_order <- setdiff(c(restored_order, missing_cols), c(id, times_col))
            
            merged <- merged[, c(id, times_col, final_order)]
        }
    } else if (length(ok_times) == 1L) {
        merged <- merged_list[[ok_times]]
        
        if (!is.null(prefix_select)) {
            if (any(grepl(prefix_select, names(merged)))) {
                merged <- dplyr::select(merged, dplyr::starts_with(prefix_select))
                names_x <- names(merged)
                names(names_x) <- vapply(names_x, function(n) gsub(prefix_select, "", n), character(1L))
                
                merged <- dplyr::rename(merged, !!!names_x)
            }
        }
    } else {
        merged <- NA
    }
    
    merged
}

#' Spread tidy data to wide for for longitudinal analysis
#' 
#' By inferring the "unitedness" of variables, spread out the other variables by time.
#' 
#' @param df A data.frame
#' @param id The desired ID column
#' @param times_col An integer-valued column which acts as the observation time. Defaults to ".TIME"
#' @param req_united_vars If any variables should be united but may be split, include them in this vector. An error will
#' be generated if the variable cannot be united. In this situation, a function may need to be applied to the variable
#' using `ApplyCodebook`.
#' @param req_split_vars If any variables ought to be split but may be inferred to be united, include them in this vector.
#' 
#' @return `df` but in wide format
SpreadTimes <- function(df, 
                        id, 
                        times_col         = ".TIME",
                        spread_all        = FALSE,
                        autocompute_split = TRUE,
                        drop_empty        = TRUE,
                        mode_unite        = NULL,
                        req_united_vars   = NULL,
                        req_split_vars    = NULL) {
    stopifnot(inherits(df, "data.frame"), id %in% names(df), times_col %in% names(df))
    
    # To not blow up memory for the ~5000 groupings, use data.table
    DT <- data.table::as.data.table(df)
    original_colorder <- names(DT)
    
    if (spread_all) {
        cat("Spreading ALL variables...",
            "\n", sep = "", file = stdout())
        split_vars <- setdiff(names(DT), c(id, times_col))
    } else {
        if (!is.null(mode_unite)) {
            for (muv in mode_unite) {
                query <- bquote(.(as.name(muv)) := MODE(.(as.name(muv), na.rm = TRUE)))

                DT[, eval(query), by = id]
            }
        }

        if (isTRUE(autocompute_split)) {
            cat("Computing split variables...",
                "\n", sep = "", file = stdout())
            split_vars <- DT[, 
                lapply(.SD, function(x) {
                    ux <- unique(x)
                    length(ux[!is.na(ux)]) > 1L
                }),
                by = id,
                .SDcols = setdiff(names(DT), c(id, times_col))
            ]
            
            not_id <- setdiff(names(split_vars), id)
            
            split_vars <- split_vars[, vapply(.SD, any, logical(1L)), .SDcols = not_id]
            
            # Finally get the names of split variables
            split_vars <- names(split_vars[split_vars])
        } else {
            split_vars <- setdiff(names(DT), c(id, times_col))

            if (!is.null(mode_unite)) {
                split_vars <- setdiff(split_vars, mode_unite)
            }
        }
        
        if (!is.null(req_split_vars)) {
            cat("Including required split variables...",
                "\n", sep = "", file = stdout())
            split_vars <- c(split_vars, req_split_vars[!req_split_vars %in% split_vars])
        }
        
        if (!is.null(req_united_vars)) {
            bad_vars <- req_united_vars[req_united_vars %in% split_vars]
            warning(paste0("Could not unite: ", paste0(bad_vars, collapse = ", ")), immediate. = TRUE, call. = FALSE)
        }
    }
    
    if (!spread_all) {

        united_vars <- setdiff(names(DT), c(id, times_col, split_vars))
        
        cat("Spreading while keeping these variables united:\n", 
            paste0(paste0("  - ", united_vars), collapse = "\n"),
            "\n", sep = "", file = stdout())
        
        unique_vals <- DT[,
            lapply(.SD, function(v) {
                uv <- unique(v)
               
                if (length(uv[!is.na(uv)]) < 1L) uv[is.na(uv)] else uv[!is.na(uv)]
            }),
            by = id,
            .SDcols = united_vars
        ]
        
        pw <- 0
        for (.id in unique(unlist(DT[, ..id]))) {
            if (!getOption("tiestk.debug", FALSE)) {
                cat(.pw_char(pw, pw_max = 64), " Updating united variables...\r", sep = "")
                pw <- pw + 1
            }
            
            row <- as.list(unique_vals[eval(as.name(id)) == .id][, -..id])
            
            data.table::set(
                DT,
                i = which(DT[, ..id] == .id),
                j = names(row),
                value = row
            )
        }
    } else {
        united_vars <- ""
    }
    
    cat("\n")
    
    # United vars, including ID but excluding times_col for dcast call
    if (!spread_all) {
        uv_expr <- rlang::parse_expr(paste(c(id, united_vars), collapse = "+"))
    } else {
        uv_expr <- rlang::parse_expr(id)
    }
    
    formula_expr <- rlang::expr(!!uv_expr ~ !!as.name(times_col))
    
    casted <- data.table::dcast(
        DT,
        eval(formula_expr),
        value.var = split_vars
    )

    if (isTRUE(drop_empty)) {
        empty_cols <- casted[, vapply(.SD, function(x) all(is.na(x)), logical(1L))]

        cat("Dropping empty: ", 
            paste0(names(empty_cols)[empty_cols], collapse = ", "), 
            "\n", sep = "", file = stderr())
        casted <- casted[, !..empty_cols]
    }

    casted
}

SpreadDatabase <- function(
    database,
    id_col,
    times_col = ".TIME",
    autocompute_unite = TRUE,
    required_united_vars = NULL,
    force_unite = NULL,
    drop_empty = TRUE,
    verbose = FALSE
) {
    stopifnot(inherits(database, "data.frame"), id_col %in% names(database), times_col %in% names(database))

    DT <- data.table::as.data.table(database)
    original_colorder <- names(DT)

    .log_db <- function(...) {
        if (isTRUE(verbose)) {
            cat(..., "\n", sep = "")
        }

        invisible(NULL)
    }

    .mode <- function(..., na.rm = TRUE) {
        vec <- list(...)

        if (length(vec) == 1L) {
            vec <- vec[[1L]]
        }

        unique_entries <- if (na.rm == TRUE) {
            u <- unique(vec)
            u[!is.na(u)]
        } else {
            unique(vec)
        }
        
        unlist(unique_entries[which.max(tabulate(match(vec, unique_entries)))])
    }

    if (!is.null(force_unite)) {
        stopifnot(is.character(force_unite))
        .log_db("Force uniting: ", paste0(force_unite, collapse = ", "))

        for (fun in force_unite) {
            query <- bquote(.(as.name(fun)) := .mode(.(as.name(fun)), na.rm = TRUE))

            DT[, eval(query), by = id_col]
        } 
    }

    check_unite_vars <- if (isTRUE(autocompute_unite)) {
        setdiff(names(DT), c(id_col, times_col))
    } else {
        if (!is.null(required_united_vars)) {
            # Ensure no-one tries to unite the ID and times cols
            setdiff(required_united_vars, c(id_col, times_col))
        } else {
            character(0L)
        }
    }

    if (isTRUE(autocompute_unite)) {
        .log_db("Automatically computing united variables...")
    }

    united_vars <- if (length(check_unite_vars) > 0L) {
        unite_compatible <- DT[,
            lapply(.SD, function(x) {
                ux <- unique(x[!is.na(x)])
                length(ux) <= 1L
            }),
            by = id_col,
            .SDcols = check_unite_vars
        ]

        unite_compatible <- unite_compatible[, !grep(id_col, names(unite_compatible)), with = FALSE][, vapply(.SD, all, logical(1L))]

        names(unite_compatible[unite_compatible])
    } else {
        character(0L)
    }

    if (length(united_vars) < 1L) {
        message("No united variables detected")
    } else {
        .log_db("Detected united variables: ", paste0(united_vars, collapse = ", "))
    }

    if (!is.null(required_united_vars) && !all(required_united_vars %in% united_vars)) {
        warning("Required united variables cannot be united: ", paste0(required_united_vars[!required_united_vars %in% united_vars]), call. = FALSE, immediate. = TRUE)
    }

    if (length(united_vars) > 0L) {
        .log_db("Updating united variables...")

        for (uvar in united_vars) {
            query <- bquote(.(as.name(uvar)) := {
                uv <- unique(.(as.name(uvar)))

                if (length(uv[!is.na(uv)]) < 1L) uv[is.na(uv)] else uv[!is.na(uv)]
            })

            DT[, eval(query), by = id_col]
        }
    }

    .log_db("Spreading database...")
    
    uv_expr <- rlang::parse_expr(paste(c(id_col, united_vars), collapse = "+"))
    formula_expr <- bquote(.(uv_expr) ~ .(as.name(times_col)))

    casted <- data.table::dcast(
        DT,
        eval(formula_expr),
        value.var = setdiff(names(DT), c(id_col, times_col, united_vars))
    )

    if (isTRUE(drop_empty)) {
        empty_cols <- casted[, vapply(.SD, function(x) all(is.na(x)), logical(1L))]
        empty_cols <- names(empty_cols[empty_cols])

        if (length(empty_cols) > 0L) {
            .log_db("Dropping empty columns: ", paste0(empty_cols, collapse = ", "))

            casted <- casted[, setdiff(names(casted), empty_cols), with = FALSE]
        }
    }

    casted
}

PermuteSpreadCollisionTags <- function(df, databases = NULL, spread_collided_vars = NULL) {
    stopifnot(is.data.frame(df))
    
    varnames <- names(df)
    default_pattern  <- "_[[:alpha:]]+_\\d+$"
    inverted_pattern <- "_\\d+_[[:alpha:]]+$"
    
    default_pattern_grped  <- "_([[:alpha:]]+)_(\\d+)$"
    inverted_pattern_grped <- "_(\\d+)_([[:alpha:]]+)$"
    
    if (!any(grepl(paste0(default_pattern, "|", inverted_pattern), varnames))) {
        stop("There doesn't appear to be any spread collided variables here", call. = FALSE)
    }
    
    DT <- data.table::as.data.table(df)
    
    get_sc_vars <- function(varset, pattern) {
        sc_vars <- varset[grepl(pattern, varset)]
        
        if (!is.null(spread_collided_vars)) {
            added_vars <- character(0L)
            
            for (scv in spread_collided_vars) {
                if (!grepl(pattern, scv)) {
                    # Assume var is a root
                    vars <- varset[grepl(scv, varset)]
                    
                    if (length(vars) < 1L) {
                        warning(paste0(scv, " not found in database. Could not permute this variable."), 
                                immediate. = TRUE, 
                                call. = FALSE)
                    } else {
                        added_vars <- c(added_vars, vars)
                    }
                } else {
                    if (!any(grepl(scv, varnames))) {
                        warning(paste0(scv, " not found in database. Could not permute this variable."), 
                                immediate. = TRUE, 
                                call. = FALSE)
                    } else {
                        added_vars <- c(added_vars, scv)
                    }
                }
            }
            
            sc_vars <- c(sc_vars, added_vars[!added_vars %in% sc_vars])
        }
        
        if (!is.null(databases)) {
            matches <- vapply(databases, function(db) {
                vapply(sc_vars, function(v) as.integer(grepl(db, v)), integer(1L))
            }, integer(length(sc_vars)))
            
            matches <- as.logical(rowSums(matches))
            
            sc_vars <- sc_vars[matches]
        }
        
        sc_vars
    }
    
    if (any(grepl(default_pattern, varnames))) {
        # Default tagging -> inverted tagging
        sc_vars <- get_sc_vars(varnames, default_pattern)
        
        cat("Permuting _[DB]_[TIME] -> _[TIME]_[DB] for the following variables:\n",
            paste0(paste0("  - ", sc_vars), collapse = "\n"),
            "\n", sep = "", file = stdout())
        
        switched <- vapply(sc_vars, function(scv) {
            gsub(default_pattern_grped, "_\\2_\\1", scv)
        }, character(1L))
        
        data.table::setnames(DT, sc_vars, switched)
    } else if (any(grepl(inverted_pattern, varnames))) {
        # Inverted tagging -> default tagging
        sc_vars <- get_sc_vars(varnames, inverted_pattern)
        
        cat("Permuting _[TIME]_[DB] -> _[DB]_[TIME] for the following variables:\n",
            paste0(paste0("  - ", sc_vars), collapse = "\n"),
            "\n", sep = "", file = stdout())
        
        switched <- vapply(sc_vars, function(scv) {
            gsub(inverted_pattern_grped, "_\\2_\\1", scv)
        }, character(1L))
        
        data.table::setnames(DT, sc_vars, switched)
    }
    
    DT
}

#' Convenience function to convert wide, time-suffixed data to tidy format
#' 
#' Using `data.table::melt.data.table`, efficiently gather time-split variables into single columns with `time_col`
#' as the time variable. The tidy data format is useful for data transformations, but it's not particularly useful for
#' longitudinal analysis. The functional inverse of `GatherTimes` is `SpreadTimes` to convert from tidy format to wide. 
#'
#' @param df A data.frame or data.table to be gathered
#' @param times_col The name of the time variable to be created. Defaults to ".TIME"
#' @return `df` in tidy format
GatherTimes <- function(df,
                        times_col   = ".TIME",
                        united_vars = NULL) {
    stopifnot(is.data.frame(df))
    
    if (!data.table::is.data.table(df)) {
        data.table::setDT(df)
    }
    
    split_vars <- names(df)[grepl("_\\d+$", names(df))]
    
    # Drop any split variables that may be accidentally caught by the _\d+$ pattern, e.g. LOCID_1 which is **not** split
    if (!is.null(united_vars)) {
        split_vars <- split_vars[!split_vars %in% united_vars]
    }
    
    split_vars_roots <- unique(vapply(split_vars, function(var) {
        strsplit(var, "_\\d+$")[[1L]]
    }, character(1L)))
    
    split_vars_cols <- lapply(split_vars_roots, function(root) {
        split_vars[grepl(paste0("^", root, "_\\d$"), split_vars)]
    })
    
    cat("Detected these variables to be split:\n", 
        paste0(paste0("  - ", split_vars_roots), collapse = "\n"),
        "\n", sep = "", file = stdout())
    
    data.table::melt.data.table(
        df,
        measure.vars  = split_vars_cols,
        value.name    = split_vars_roots,
        variable.name = times_col
    )
}

#' Convert known variable names to desired Final_Name
#' 
#' Given a list of assessments (broken into their respective groups), apply a codebook to the VARMAPPED variables
#' to convert the variables to the final form. If a summary function is defined for a matching code, move along for now.
#' 
#' @param list_assessments List of the form `list(group1 = list(... dataframes ordered by time ...), group2 = list(...), ...)`
#' @param codebook A dataframe that contains all the code mappings for final imputation
#' @return A list of the same form as `list_assessments` that contains the coded assessments
ApplyCodebook <- function(assessments, 
                          codebook,
                          initvar_col        = "HomogenizedName",
                          subgroup_col       = "SubgroupDB3",
                          reduction_col      = "ReductionDB3",
                          finalvar_col       = "FinalNameDB3", 
                          timevar_col        = ".TIME",
                          id_col             = "ST_ID",
                          prefix_select      = TRUE,
                          generated_col      = NULL,
                          report_errors      = FALSE, 
                          report_directory   = NULL, 
                          export_assessments = FALSE, 
                          export_directory   = NULL,
                          col_spec           = NULL,
                          parallel           = FALSE,
                          cluster            = NULL) {
    
    colspec <- .mapping$build_colspec(col_spec)
    
    master_context <- list(
        id_col        = id_col,
        initvar_col   = initvar_col,
        subgroup_col  = subgroup_col,
        timevar_col   = timevar_col,
        reduction_col = reduction_col,
        finalvar_col  = finalvar_col,
        generated_col = generated_col,
        prefix_select = prefix_select,
        colspec       = colspec,
        parallel      = parallel,
        cluster       = cluster
    )
    
    if (is.data.frame(assessments)) {
        # Final merged logic
        env_bundle <- .codebook$process_database(assessments, codebook, master_context)

        if (nrow(env_bundle$err) > 0 || nrow(env_bundle$warn) > 0) {
            warning("Some errors occurred in the codebook application process. These erroneous variables were ignored.", call. = F)
            
            if (report_errors) {
                err_file <- file.path(report_directory, "codebook-application-errs.csv")
                warn_file <- file.path(report_directory, "codebook-application-warns.csv")
                warning(paste0("Please see \"", err_file, "\" for the list of variables that have some errors."), call. = F)
                warning(paste0("Please see \"", err_file, "\" for the list of variables that have some warnings."), call. = F)
                write.csv(env_bundle$err, file = err_file, row.names = FALSE, na = "")
                write.csv(env_bundle$warn, file = warn_file, row.names = FALSE, na = "")
            }
        }

        return(env_bundle$merged)

    } else {
        env_bundle <- .make_env_bundle(assessments)
        
        env_bundle <- lapply(env_bundle, .codebook$process_group, codebook, master_context)
        recombined <- .recombine_list_with_errors(env_bundle)
        
        if (nrow(recombined$err) > 0 || nrow(recombined$warn) > 0) {
            warning("Some errors occurred in the codebook application process. These erroneous variables were ignored.", call. = F)
            
            if (report_errors) {
                err_file <- paste0(report_directory, "/", "codebook-application-errs.csv")
                warn_file <- paste0(report_directory, "/", "codebook-application-warns.csv")
                warning(paste0("Please see \"", err_file, "\" for the list of variables that have some errors."), call. = F)
                warning(paste0("Please see \"", err_file, "\" for the list of variables that have some warnings."), call. = F)
                write.csv(recombined$err, file = err_file, row.names = F)
                write.csv(recombined$warn, file = warn_file, row.names = F)
            }
        }
        
        if (export_assessments) {
            .print_list_assessments(recombined$output, export_directory)
        }
        
        # Make resources for garbage collection AND run the garbage collector (may change this in the future after benchmarking the code)
        rm(env_bundle)
        gc()
        
        return(recombined$output)
    }
}

#' Cleaning pipeline metafunction based on Lebanon defaults
#'
#' Runs HomogenizeWaveVariables, Patch99 (if `fn_path` is provided), and ApplyCodebook.
#' Finally, prefix-select the output variables to get rid of dropped variables.
#'
CleanDatabases <- function(
    mapping, 
    data, 
    waves = NULL,
    report_errors = FALSE,
    report_directory = NULL,
    patch99_source_column = "HomogenizedName",
    generated_column = "GeneratedDB3",
    merge_id_col = NULL,
    merge_databases = FALSE,
    spread_times = FALSE,
    output_directory = NULL,
    fn_path = NULL
) {
    homogenized <- HomogenizeWaveVariables(
        mapping, 
        data, 
        waves = waves, 
        report_errors = report_errors, 
        report_directory = report_directory
    )

    applied <- if (!is.null(fn_path)) {
        source(file.path(fn_path, "Patch99.R"))

        patched <- Patch99(homogenized$data, mapping = homogenized$map, map_source_col = patch99_source_column)
        
        ApplyCodebook(
            patched, 
            homogenized$map, 
            generated_col = generated_column,
            report_errors = report_errors,
            report_directory = report_directory
        )
    } else {
        ApplyCodebook(
            homogenized$data, 
            homogenized$map, 
            generated_col = generated_column,
            report_errors = report_errors,
            report_directory = report_directory
        )
        
    }

    selected <- lapply(applied, function(group) {
        lapply(group, function(df) {
            if (is(df, "logical")) return(df)

            df <- df %>% select(starts_with("___VARMAPPED___"))
            names(df) <- gsub("___VARMAPPED___", "", names(df))

            if ("X" %in% names(df)) {
                df$X <- NULL
            }

            df
        })
    })

    if (!is.null(output_directory)) {
        lapply(names(selected), function(group) {
            lapply(names(selected[[group]]), function(t_point) {
                if (is(selected[[group]][[t_point]], "logical")) {
                    return()
                }

                t_df <- selected[[group]][[t_point]]
                key  <- paste0(group, "_", t_point)

                file_path <- file.path(output_directory, paste0(key, "_CLEANED"))
                
                cat(paste0("Exporting database time point file: ", paste0(file_path, ".[csv|dta]"), "\n"))

                write.csv(t_df, file = paste0(file_path, ".csv"), 
                          row.names = FALSE, 
                          fileEncoding = "UTF-8")
                tryCatch(haven::write_dta(t_df, path = paste0(file_path, ".dta")), error = function(e) warning("Could not create STATA file: ", e, immediate. = TRUE, call. = FALSE))
            })
        })
    }

    if (isTRUE(merge_databases)) {
        if (!is.character(merge_id_col)) {
            stop("`merge_id_col` must be the name of a column in all databases to be used to merge databases together")
        }

        sel_merged <- MergeDatabases(selected, merge_id_col)
        selected[["MERGED_DATABASES"]] <- sel_merged

        sel_tmerged <- MergeTimes(sel_merged, merge_id_col, mapping = homogenized$map, map_colspec = list(name = "FinalNameDB3", group = "Group"))
        selected[["MERGED_TIMES"]] <- sel_tmerged

        if (!is.null(output_directory)) {
            file_path <- file.path(output_directory, paste0("ALL_MERGED_LONG"))
            
            cat(paste0("Exporting long-form merged databases: ", paste0(file_path, ".[csv|dta]"), "\n"))

            write.csv(sel_tmerged, file = paste0(file_path, ".csv"), 
                      row.names = FALSE, 
                      fileEncoding = "UTF-8")
            tryCatch(haven::write_dta(sel_tmerged, path = paste0(file_path, ".dta")), error = function(e) warning("Could not create STATA file: ", e, immediate. = TRUE, call. = FALSE))
            
        }

        if (isTRUE(spread_times)) {
            sel_full_spread <- SpreadTimes(sel_tmerged, merge_id_col, spread_all = TRUE)
            selected[["SPREAD_TIMES"]] <- sel_full_spread

            if (!is.null(output_directory)) {
                file_path <- file.path(output_directory, paste0("ALL_MERGED_WIDE"))
                
                cat(paste0("Exporting wide-form merged databases: ", paste0(file_path, ".[csv|dta]"), "\n"))

                write.csv(sel_full_spread, file = paste0(file_path, ".csv"), 
                          row.names = FALSE, 
                          fileEncoding = "UTF-8")
                tryCatch(haven::write_dta(sel_full_spread, path = paste0(file_path, ".dta")), error = function(e) warning("Could not create STATA file: ", e, immediate. = TRUE, call. = FALSE))
            }
        }
    }

    selected
}

# ------ Private variables and environments ------
.mapping    <- new.env()
.homogenize <- new.env()
.codebook   <- new.env()
.merging    <- new.env()

# --------- Mapping-related variables / functions ---------
.mapping$build_colspec <- function(spec = NULL) {
    colspec <- DEFAULT_MAPPING_COLSPEC
    
    if (!is.null(spec)) {
        cols <- names(spec)
        colspec[names(colspec) %in% cols] <- spec[cols]
    }
    
    colspec
}

# --------- Homogenize-related variables ---------
local({
    LETTER_MAPPING <- list(B = 1, M = 2, E = 3)
    MAPPING_KEYWORDS <- c("Name", "Code")
    MASTER_UNIFICATION_COLS <- c(Name = "HomogenizedName", Code = "FinalCode")
    USER_WAVES_OVERRIDE <- NULL
    
    wave_decode <- function(code, waves) {
        if (is.null(waves)) {
            wave <- LETTER_MAPPING[[stringr::str_extract(code, "[BME]")]]
            year <- as.integer(stringr::str_extract(code, "\\d+$")) - 1
            
            wave + (3 * year)
        } else {
            which(waves == code)
        }
    }
    
    wave_encode <- function(num, waves) {
        if (is.null(waves)) {
            num <- num - 1
            
            wave <- names(LETTER_MAPPING)[[(num %% 3) + 1]]
            year <- (num %/% 3) + 1
            
            paste0(wave, "Y", year)
        } else {
            waves[[num]]
        }
    }
    
    mapping_decode <- function(varname) {
        pattern_check <- if (!is.null(USER_WAVES_OVERRIDE)) {
            paste0("(", paste0(USER_WAVES_OVERRIDE, collapse = "|"), ")$")
        } else {
            "[bBmMeE][yY]\\d+$"
        }

        if (!grepl(pattern_check, varname)) {
            return(varname)
        }
        
        code <- toupper(stringr::str_extract(varname, pattern_check))
        num <- wave_decode(code, USER_WAVES_OVERRIDE)
        
        paste0(unlist(strsplit(varname, "_"))[[1]], "___", num)
    }
    
    mapping_encode <- function(varname) {
        if (!grepl("_{3}\\d+$", varname)) {
            return(varname)
        }
        
        num <- as.integer(stringr::str_extract(varname, "\\d+$"))
        code <- wave_encode(num, USER_WAVES_OVERRIDE)
        
        paste0(unlist(strsplit(varname, "___"))[[1]], "_", code)
    }
    
    mapping_clean <- function(mapping) {
        # Decode the columns to locate which time points are missing and need to be filled with NA
        names(mapping) <- map_chr(names(mapping), mapping_decode)
        
        all_missing_vars <- lapply(MAPPING_KEYWORDS, function(known_var) {
            time_points <- mapping %>%
                select(starts_with(paste0(known_var, "___"))) %>%
                names %>%
                stringr::str_extract("_{3}\\d+$") %>%
                map_chr(function(t) gsub("_{3}", "", t)) %>%
                as.integer
            
            # Get the missing integers and encode them with the missing variable's name
            setdiff(min(time_points):max(time_points), time_points) %>%
                map_chr(function(t) paste0(known_var, "___", t))
        }) %>% flatten_chr
        
        missing_cols <- rep(NA, length(all_missing_vars))
        names(missing_cols) <- all_missing_vars

        # If the Generated column doesn't exist, fill it in
        if ("GeneratedDB3" %not_in% names(mapping)) {
            mapping$GeneratedDB3 <- FALSE
        }
        
        mapping <- mapping %>% 
            mutate(!!!missing_cols) %>%
            yesno_to_boolean
        
        chr_cols <- map_lgl(mapping, is.character)
        mapping[, chr_cols] <- lapply(mapping[, chr_cols], function(column) {
            # Trim whitespace
            column[!is.na(column)] <- map_chr(column[!is.na(column)], trimws)
            
            # Convert "NA" to NA
            column[!is.na(column) & grepl("^na$|^n\\/a$|^$", column, ignore.case = T)] <- NA
            
            column
        })

        # Insert row number as primary key for reference
        mapping$CODE_ID <- 1:nrow(mapping)

        # Codebook generating function consistency
        # Swap GROUP to Group
        old_group_col <- names(mapping)[grepl("^group$", names(mapping), ignore.case = T)]

        if (length(old_group_col) < 1) {
            stop("Mapping sheet needs a 'Group' column to link variable names to assessment databases", call. = F)
        } else if (length(old_group_col) > 1) {
            stop("Multiple 'Group' columns found in mapping sheet. Please fix.", call. = F)
        }

        if (old_group_col != "Group") {
            mapping$Group <- mapping[, old_group_col]
            mapping[, old_group_col] <- NULL
        }

        # Drop "subgrouping" of assessments for cleaning; this subgrouping is only relevant for codebook generation
        mapping$Group <- map_chr(mapping$Group, function(vargroup) {
            vargroup <- vargroup %>% 
                strsplit("-") %>% 
                unlist

            gsub("^\\s+|\\s+$", "", vargroup)[[1]]
        })

        # Swap "Coding" to "FinalCode"
        if ("Coding" %in% names(mapping)) {
            mapping$FinalCode <- mapping$Coding
            mapping$Coding    <- NULL
        }
        
        mapping
    }
    
    yesno_to_boolean <- function(dataframe) {
        bool_cols <- map_lgl(dataframe, function(var) all(is.na(var) | grepl("^Y$|^N$", var, ignore.case = T)))
        dataframe[, bool_cols] <- map(dataframe[, bool_cols], function(var) ifelse(var == "Y" || var == "y", T, F))
        dataframe
    }
    
    mapping_error_check <- function(mapping) {
        # Severe errors: if master homogenization columns do not exist, halt
        for (uni in names(MASTER_UNIFICATION_COLS)) {
            
            tryCatch({
                mapping[, MASTER_UNIFICATION_COLS[[uni]]]
            }, error = function(e) {
                stop("Missing master unification column in mapping: ", MASTER_UNIFICATION_COLS[[uni]], "\nHomogenization will not work without this column.", call. = F)
            })
        }
        
        waves <- mapping %>% select(matches("Name_{3}\\d+$")) %>% names
        names(waves) <- map_chr(waves, function(wave) {
            wave %>%
                mapping_encode %>%
                (function(x) gsub("Name_", "error_duplicate_", x))
        })
        
        mapping <- mapping %>% mutate(error_na_group = is.na(Group))
        groups <- unique(mapping[!mapping$error_na_group, "Group"])
        
        for (dup in names(waves)) {
            mapping <- mapping %>% mutate(!!ensym(dup) := F)
        }
        
        for (group in groups) {
            group_filter <- map_lgl(mapping$Group, function(g) !is.na(g) & grepl(g, group))
            
            for (dup in names(waves)) {
                # Select all observations in the current non-NA group and determine if there are any non-NA duplicates WITHIN the group
                mapping[group_filter, ][, dup] <- !is.na(mapping[group_filter, ][, waves[[dup]]]) & (duplicated(mapping[group_filter, ][, waves[[dup]]]) | duplicated(mapping[group_filter, ][, waves[[dup]]], fromLast = T))
            }
        }
        
        errcols <- mapping %>% 
            select(starts_with("error_")) %>% 
            names
        
        mapping <- mapping %>%
            mutate(has_error = eval(parse(text = paste(errcols, collapse = "|"))))
        
        # Makes `errors` which compiles human-readable dataframe with the errors
        errors <- mapping %>%
            filter(has_error) %>%
            select(starts_with("Name"), Group, starts_with("error_"))
        
        errs_readable <- map_chr(errcols, function(ec) toupper(gsub("error_", "", ec)))
        names(errs_readable) <- errcols
        
        for (err in errcols) {
            err_sym <- ensym(err)
            
            # Runs through all of the error columns to convert the boolean flag to the human-readable string before uniting
            errors <- errors %>%
                mutate(!!err_sym := map_chr(!!err_sym, function(val) ifelse(val, errs_readable[[!!err]], "")))
        }
        
        errors <- errors %>% tidyr::unite("Issue", starts_with("error_"), sep = ",")
        errors$Issue <- map_chr(errors$Issue, function(iss) { # Cuts out excess commas for more legibility
            iss <- gsub("^,+|,+$", "", iss)
            iss <- gsub("(?<=\\w),+(?=\\w)", ", ", iss, perl = T)
            iss
        })
        
        list(map = mapping, err = errors)
    }
    
    process_chunks <- function(checked_mapping, list_all_assessments) {
        #' Rearrange chunks into separate environments and compute
        #'
        #' `process_chunks` moves the assessments out of `list_all_assessments`
        #' and places them into separate environments for quicker batch processing with
        #' `unify_across_waves`. After `unify_across_waves` is complete, the entire bundle
        #' of environments is returned.
        #'
        #' @param checked_mapping A cleaned mapping, processed by `mapping_error_check` (the `$map` member, to be specific)
        #' @param list_all_assessments All the desired assessments (same form as `list_assessments` in `HomogenizeWaveVariables`)
        #' @return A list of environments for each group of assessments
        
        mapping <- checked_mapping %>% filter(!has_error)
        
        env_bundle <- .make_env_bundle(list_all_assessments)
        
        env_bundle <- sapply(env_bundle, unify_across_waves, mapping, simplify = F)
        env_bundle
    }
    
    unify_across_waves <- function(df_env, mapping) {
        #' Assume all databases are basically cleaned
        #' 
        #' @param mapping data.frame. Cleaned dataframe that holds all the code mappings across waves
        #' @param df_env env(group = , base = , mid = , end = ). List of dataframes with `group`, `base` and `end` as members. `group` is the ID associated with the assessments in the mapping sheet, and `base` and `end` are the respective assessments from baseline to endline e.g. `new.env(group = "EGRA", base = egra_bl, end = egra_el)` Optional presence of midline.
        #' 
        #' Race conditions? If map is over each unique observation, then I do not think it should be an issue
        
        df_env$err  <- tibble(Group = character(), Wave = character(), Variable = character(), Issue = character())
        df_env$warn <- tibble(Group = character(), Wave = character(), Variable = character(), Issue = character())
        df_env$pw   <- 0 # pinwheel indicator for interaction
        
        group_mapping <- mapping[mapping$Group == df_env$group,]
        df_env$mapping <- group_mapping
        
        order_time_cols <- function(df) {
            cols_order <- df %>%
                names %>% 
                stringr::str_extract("\\d+$") %>% 
                as.integer %>% 
                order
            
            df[, cols_order]
        }
        
        select_keyword_cols <- function(keyword) {
            df_env$mapping %>%
                select(starts_with(paste0(keyword, "___"))) %>% 
                order_time_cols
        }
        
        # Changes column bundles to row bundles for pmap-ing unify_variables
        reorient_columns <- function(col_bundle) pmap(as.list(col_bundle), function(...) as.character(unlist(list(...))))
        
        name_columns <- select_keyword_cols("Name") %>% reorient_columns
        code_columns <- select_keyword_cols("Code") %>% reorient_columns
        
        masters <- lapply(names(MASTER_UNIFICATION_COLS), function(name) df_env$mapping[, MASTER_UNIFICATION_COLS[[name]]])
        
        if (getOption("tiestk.trapcontinue", FALSE)) {
            tryCatch({
                unify_variables(code_ids = group_mapping$CODE_ID,
                                master_names = masters[[1]],
                                master_codes = masters[[2]],
                                name_bundles = name_columns,
                                code_bundles = code_columns,
                                df_env)

                cat("\nCompleted \"", df_env$group, "\"\n", sep = "")
            }, error = function(e) {
                warning("Fault in unifying \"", df_env$group, "\" : \"", e$message, "\" Check homogenization error file for more details.", call. = F, immediate. = T)
            })
        } else {
            unify_variables(code_ids = group_mapping$CODE_ID,
                            master_names = masters[[1]],
                            master_codes = masters[[2]],
                            name_bundles = name_columns,
                            code_bundles = code_columns,
                            df_env)

            cat("\nCompleted \"", df_env$group, "\"\n", sep = "")
        }
        
        df_env
    }

    unify_variables <- function(code_ids, master_names, master_codes, name_bundles, code_bundles, df_env) {
        name_keys <- pmap_chr(list(master_names, name_bundles, code_ids), unify_names, df_env)

        pwalk(list(name_keys, master_codes, code_bundles), unify_codes, df_env)
    }
    
    unify_names <- function(master_name, name_bundle, code_id, df_env) {
        if (!is.null(getOption("DEBUG")) && !getOption("DEBUG")) {
            cat(.pw_char(df_env$pw, pw_max = 128), " Processing naming unifications for \"", df_env$group, "\"... \r", sep = "")
            df_env$pw <- df_env$pw + 1
        }

        key <- ""
        time_set <- which(!is.na(name_bundle))
        
        process_vars <- function(t) {
            dataframe <- paste0("T", t)
            
            # is.na check on dataframe results in a deep check which uses an enormous amount of memory
            if (class(df_env[[dataframe]]) == "logical" || is.null(df_env[[dataframe]])) {
                # Continue
            } else {
                if (grepl("^\\$", name_bundle[[t]])) {
                    # Branching support -- If variable starts with '$', it is assumed that what follows the '$' is
                    # the homogenized name of a previous row. Copy that column into this variable.

                    branch     <- gsub("\\$", "", name_bundle[[t]])
                    branch_key <- paste0(ESCAPE_PREFIX, toupper(branch)) 

                    cat("Branching ", df_env$group, "$", dataframe, "$", toupper(branch), " -> ", 
                        df_env$group, "$", dataframe, "$", gsub(ESCAPE_PREFIX, "", key, fixed = T), "\x1B[K\n", sep = "")

                    df_env[[dataframe]][, key] <- df_env[[dataframe]][, branch_key]
                } else if (name_bundle[[t]] %not_in% names(df_env[[dataframe]])) {
                    df_env$err[nrow(df_env$err) + 1, ] <- list(
                        Group = df_env$group,
                        Wave = wave_encode(t, USER_WAVES_OVERRIDE),
                        Variable = name_bundle[[t]],
                        Issue = "Missing variable which is non-NA in item mapping sheet. Check data?")
                } else {
                    df_env[[dataframe]] <- rename(df_env[[dataframe]], !!!list2(!!ensym(key) := name_bundle[[t]]))
                }
            }
        }
        
        if (is.na(master_name)) {
            if (!all.na(name_bundle)) {
                error_report <- list(
                    Group = df_env$group,
                    Wave = NA,
                    Variable = paste0(name_bundle[!is.na(name_bundle)], collapse = ", "),
                    Issue = "No homogenized name provided. Used first non-NA name as reference."
                )
                
                df_env$warn[nrow(df_env$warn) + 1, ] <- error_report
                
                key <- paste0(ESCAPE_PREFIX, toupper(name_bundle[!is.na(name_bundle)][[1]]))
                
                lapply(time_set, process_vars)
            } # Else, blank row so ignore
        } else {
            key <- paste0(ESCAPE_PREFIX, toupper(master_name))
            
            lapply(time_set, process_vars)
        }

        # Update mapping file with new homogenized name if homogenized name was constructed
        if (key != "") {
            df_env$mapping[df_env$mapping$CODE_ID == code_id, "HomogenizedName"] <- gsub(ESCAPE_PREFIX, "", key, fixed = TRUE)
        }
        
        key
    }

    link_descriptions <- function(master, test) {
        # TODO: Convert to binary search tree, if even possible. The pattern matching ruins the ordered requirement
        desc_key <- "Description"

        for (i in 1:nrow(test)) {
            for (j in 1:nrow(master)) {
                # Firstly, check if master_desc == test_desc (for partial use of &)
                if (grepl(paste0("^", str_escape(master[j, desc_key]), "$"), test[i, desc_key])) {
                   # All good, carry on
                   break
                }

                master_desc_pattern <- master[j, desc_key] %>% 
                    strsplit("&") %>% 
                    unlist %>% 
                    map_chr(function(pat) paste0("^", str_escape(gsub("^\\s+|\\s+$", "", pat)), "$")) %>% 
                    paste0(collapse = "|")

                if (grepl(master_desc_pattern, test[i, desc_key], ignore.case = T)) {
                    test[i, desc_key] <- master[j, desc_key]
                    break
                }
            }
        }

        test %>% group_by(Description) %>% summarise(Value = paste0(Value, collapse = "|"))
    }

    descriptions_match <- function(master, test) {
        desc_key <- "Description"
        counts   <- as.integer(table(unlist(list(master[, desc_key], test[, desc_key]))))

        all(counts == max(counts))
    }

    merge_codings <- function(x, y, df_env, readable_name) {
        if ((length(x) == 2) && ("Value" %in% names(x))) {
            x <- x %>% rename(Value_0 = Value)
        }

        # Check to verify that descriptions are all unique
        if (length(unique(y[, "Description"])) != length(unique(y[, "Value"]))) {
            df_env$err[nrow(df_env$err) + 1, ] <- list(
                Group = df_env$group,
                Wave = wave_encode(df_env$used_times[[1]], USER_WAVES_OVERRIDE),
                Variable = readable_name,
                Issue = "Duplicated descriptions detected. Please check the coding to verify that each coding has a unique description."
            )
            
            # Drop this observation for non-bijection
            y[, "Value"] <- "%BAD_CODE%"
        } else {
            if (!descriptions_match(x, y)) {
                y <- link_descriptions(x, y)

                if (!descriptions_match(x, y)) {
                    if (nrow(y) > nrow(x)) {
                        df_env$err[nrow(df_env$err) + 1, ] <- list(
                            Group = df_env$group,
                            Wave = wave_encode(df_env$used_times[[1]], USER_WAVES_OVERRIDE),
                            Variable = readable_name,
                            Issue = "Coding dimensionality mismatch: Time point with larger scale than master coding could not be resolved into master coding"
                        )

                        # Drop this observation for incompatible dimensionality
                        y[, "Value"] <- "%BAD_CODE%"
                    } else {
                        # Throw warning. Could be okay.
                        df_env$warn[nrow(df_env$warn) + 1, ] <- list(
                            Group = df_env$group,
                            Wave = wave_encode(df_env$used_times[[1]], USER_WAVES_OVERRIDE),
                            Variable = readable_name,
                            Issue = "Coding dimensionality mismatch: Time point has a smaller scale than the master coding"
                        )
                    }
                }
            }    
        }

        if (all(y[, "Value"] == "%BAD_CODE%")) {
            # Move on
            df_env$used_times <- df_env$used_times[-1]

            x
        } else {
            y <- rename(y, !!!list2(!!paste0("Value_", df_env$used_times[[1]]) := "Value"))
            df_env$used_times <- df_env$used_times[-1]
            
            left_join(x, y, by = "Description")
        }
    }
    
    unify_codes <- function(name_key, master_code, code_bundle, df_env) {
        if (!is.null(getOption("DEBUG")) && !getOption("DEBUG")) {
            cat(.pw_char(df_env$pw, pw_max = 128), " Processing coding unifications for \"", df_env$group, "\"... \r", sep = "")
            df_env$pw <- df_env$pw + 1
        }

        readable_name <- gsub(ESCAPE_PREFIX, "", name_key)
        
        render_code <- function(code, wave) {
            coding_df <- NA
            wave_enc <- ifelse(!is.na(wave), wave_encode(wave, USER_WAVES_OVERRIDE), NA)
            
            tryCatch({
                # swap cbind -> data.frame for type stability
                if (grepl("^cbind\\(.*\\)$", code, ignore.case = T)) {
                    arguments <- stringr::str_extract(code, "(?<=^cbind\\().*(?=\\)$)")
                    coding_df <- eval(parse(text = paste0("data.frame(", arguments, ", stringsAsFactors = F)"))) 
                } else {
                    coding_df <- eval(parse(text = code))
                }
                
                names(coding_df) <- c("Value", "Description")
                coding_df$Description <- toupper(coding_df$Description)
            }, error = function(e) {
                # Parse error
                df_env$err[nrow(df_env$err) + 1,] <- list(
                    Group = df_env$group,
                    Wave = wave_enc,
                    Variable = readable_name,
                    Issue = paste0("Error interpreting coding: ", e$message)
                )
                
                coding_df <- NA
            })
            
            coding_df
        }
        
        if (name_key == "") {
            # Blank line, return
            return()
        }
        
        if (is.na(master_code)) {
            if (!all.na(code_bundle)) {
                df_env$err[nrow(df_env$err) + 1,] <- list(
                    Group = df_env$group,
                    Wave = NA,
                    Variable = readable_name,
                    Issue = "No final code provided for comparison")
            } # Else empty coding, blank line or variable does not have coding 
        } else {
            master_code <- render_code(master_code, NA)

            codes <- map2(code_bundle, seq_along(code_bundle), function(code, wave) {
                if (!is.na(code)) {
                    render_code(code, wave)
                } else {
                    NA
                }
            })

            time_set <- which(!is.na(codes))
            df_env$used_times <- time_set
            
            # Merge the rendered codes into one table to be used for data rewriting
            merged_codes <- reduce(list2(master_code, !!!codes[time_set]), merge_codings, df_env, readable_name)

            if ("Value_0" %not_in% names(merged_codes)) {
                # Generated variable with FinalCode filled in for clarity
                cat("Skipped recoding for \"", readable_name, "\". Check errors if not intended.\x1B[K\n", sep = "")
                return()
            } else {
                roots <- merged_codes$Value_0

                if (is.character(roots[!is.na(roots)])) {
                    roots <- typecast_char(roots)
                }

                merged_codes$Value_0 <- roots
            }

            coding_rewrite_data(df_env, name_key, merged_codes)
        }
    }

    coding_rewrite_data <- function(df_env, name_key, coding_table) {
        # Value_0 carries the root value per row
        # Firstly, bundle coding table value columns to rows

        data_cols <- select(coding_table, -Description, -Value_0)
        data_rows <- pmap(as.list(data_cols), function(...) {
            cols <- list(...)

            old_names <- names(cols)
            cols <- as.character(unlist(cols))

            names(cols) <- old_names
            cols
        })

        df_env$rewritten <- list() # Memoized table to see which rows we have overwritten

        rewrite <- function(root, data) {
            names(data) <- map_chr(names(data), function(name) gsub("Value_", "T", name))

            # Filter out those that match the root value and those that had errors
            times_needing_fix <- data[!grepl("%BAD_CODE%", data) & !grepl(paste0("^", root, "$"), as.character(data))]

            if (length(times_needing_fix) > 0) {
                time_points <- names(times_needing_fix)

                walk(time_points, function(t) {
                    if (is.na(times_needing_fix[[t]])) {
                        # Ensure NA is not overwritten in the data
                        return()
                    }

                    cat("Rewriting \"", df_env$group, "$", t, "$", gsub(ESCAPE_PREFIX, "", name_key), "\" : ", times_needing_fix[[t]], " -> ", root, "\x1B[K\n", sep = "")
                    if (is.null(df_env$rewritten[[t]])) {
                        df_env$rewritten[[t]] <- list()
                    }

                    if (name_key %not_in% names(df_env[[t]])) {
                        # Not in database. Flag and move on
                        df_env$warn[nrow(df_env$warn) + 1, ] <- list(
                            Group = df_env$group,
                            Wave  = wave_encode(as.integer(gsub("^T", "", t)), USER_WAVES_OVERRIDE),
                            Variable = gsub(ESCAPE_PREFIX, "", name_key),
                            Issue    = "Missing variable from database, caught while recoding"
                        )

                        invisible(t)
                    } else {
                        if (grepl("\\|", times_needing_fix[[t]])) {
                            changed_rows <- which(map_lgl(df_env[[t]][[name_key]], function(val) grepl(times_needing_fix[[t]], as.character(val))))
                        } else {
                            changed_rows <- which(df_env[[t]][, name_key] == times_needing_fix[[t]])
                        }

                        # Cut rows which are already in the rewritten memo table
                        changed_rows <- setdiff(changed_rows, df_env$rewritten[[t]])

                        if (length(changed_rows) > 0) {
                            # Proceed with overwrites
                            df_env[[t]][changed_rows, name_key] <- root

                            # Log changes
                            df_env$rewritten[[t]] <- append(df_env$rewritten[[t]], changed_rows)
                        }
                    }
                })
            }

            names(times_needing_fix)
        }

        fixed_times <- unique(unlist(map2(as.character(coding_table$Value_0), data_rows, rewrite)))

        # Reset types, if they were changed
        for (t in fixed_times) {
            if (name_key %not_in% names(df_env[[t]])) {
                next
            }

            old_type <- typeof(unlist(df_env[[t]][, name_key]))

            if (old_type == "character") {
                df_env[[t]][, name_key] <- typecast_char(df_env[[t]][[name_key]])
            }
        }

        # Clear the memo table from memory
        rm(rewritten, envir = df_env)
    }
    
}, .homogenize)

# --------- Merging assessments functions / variables ---------
.merging$squash_vartree <- function(dbs, id, conflicts = NULL) {
    var_tree <- list()
    
    for (ndb in names(dbs)) {
        if (!id %in% names(dbs[[ndb]])) stop(paste0("ID column ", id, " not found in ", ndb), call. = FALSE)
        
        for (v in setdiff(names(dbs[[ndb]]), id)) {
            if (!v %in% names(var_tree)) {
                var_tree[[v]] <- ndb
            } else {
                var_tree[[v]] <- c(var_tree[[v]], ndb)
            }
        }
    }
    
    var_tree[vapply(names(var_tree), function(vname) {
        length(var_tree[[vname]]) > 1L | (if (!is.null(conflicts)) vname %in% conflicts else FALSE)
    }, logical(1L))]
}

.merging$spread_vars <- function(vt) {
    out <- list()
    
    for (v in names(vt)) {
        for (db in vt[[v]]) {
            if (!db %in% names(out)) {
                out[[db]] <- list()
            }
            
            out[[db]][[paste0(v, "_", db)]] <- v
        }
    }
    
    out
}

# --------- Codebook-related variables ---------
local({
    process_database <- function(df, codebook, context) {
        if (!is.data.frame(df)) stop("process_database() only defined for data.frames and their derivatives")
        
        db_env <- new.env()
        db_env$merged <- df
        context$dbkeys <- "merged"
        context$singledb <- TRUE

        # Load abstract of current content
        variable_origins <- data.table::data.table(variable = codebook[[context$initvar_col]], root = codebook[[context$initvar_col]], origin = codebook[[context$colspec$group]])
        variable_origins[, variable := dplyr::case_when(
            !variable %in% names(db_env$merged) & paste0(variable, "_", origin) %in% names(db_env$merged) ~ paste0(variable, "_", origin),
            !variable %in% names(db_env$merged) ~ NA_character_,
            TRUE ~ variable
        )]

        context$content_origins <- as.data.frame(variable_origins[!is.na(variable)])
        
        process_environment(db_env, codebook, context)
    }
    
    process_group <- function(group_env, codebook, context) {
        # Filter database grouping and build subgroup table
        if (exists("group", envir = group_env)) {
            codebook <- codebook[codebook[[context$colspec$group]] == group_env$group, ]
        }
        
        time_points <- names(group_env)[grepl("^T\\d+", names(group_env))]
        ok_times <- which(map_lgl(time_points, function(t) !is(group_env[[t]], "logical")))
        
        context$dbkeys <- time_points[ok_times]

        process_environment(group_env, codebook, context)
    }

    inspect_mapping <- function(mapping, group_env, context) {
        stopifnot(!is.null(context$initvar_col), !is.null(context$finalvar_col), !is.null(context$reduction_col))

        map <- data.table::as.data.table(mapping)
        map[grepl("^\\s*$", map[[context$initvar_col]]), c(context$initvar_col) := NA_character_]
        map[grepl("^\\s*$", map[[context$finalvar_col]]), c(context$finalvar_col) := NA_character_]
        map[grepl("^\\s*$", map[[context$reduction_col]]), c(context$reduction_col) := NA_character_]

        map[, c(context$initvar_col) := gsub("^\\s+|\\s+$", "", map[[context$initvar_col]])]
        map[, c(context$finalvar_col) := gsub("^\\s+|\\s+$", "", map[[context$finalvar_col]])]
        map[, c(context$reduction_col) := gsub("^\\s+|\\s+$", "", map[[context$reduction_col]])]

        map[, `_mapping_type` := NA_character_]
        map[, `_mapping_issue` := FALSE]
        map[, `_mapping_issue_reason` := NA_character_]

        map[is.na(map[[context$initvar_col]]) & is.na(map[[context$finalvar_col]]) & is.na(map[[context$reduction_col]]), `_mapping_issue` := TRUE]
        map[is.na(map[[context$initvar_col]]) & is.na(map[[context$finalvar_col]]) & is.na(map[[context$reduction_col]]), `_mapping_issue_reason` := "Empty row. Skipping..."]

        map[map[[context$initvar_col]] != map[[context$finalvar_col]] & is.na(map[[context$reduction_col]]), `_mapping_type` := "rename"]
        map[is.na(map[[context$initvar_col]]) & !is.na(map[[context$finalvar_col]]) & is.na(map[[context$reduction_col]]), `_mapping_issue` := TRUE]
        map[is.na(map[[context$initvar_col]]) & !is.na(map[[context$finalvar_col]]) & is.na(map[[context$reduction_col]]), `_mapping_issue_reason` := "Missing initial name for variable rename"]
        map[!is.na(map[[context$initvar_col]]) & is.na(map[[context$finalvar_col]]) & is.na(map[[context$reduction_col]]), `_mapping_issue` := TRUE]
        map[!is.na(map[[context$initvar_col]]) & is.na(map[[context$finalvar_col]]) & is.na(map[[context$reduction_col]]), `_mapping_issue_reason` := "Missing final name for variable rename. Perhaps intended to be a DROP?"]

        map[!is.na(map[[context$reduction_col]]), `_mapping_type` := "reduction"]
        map[map[[context$reduction_col]] == "DROP", `_mapping_type` := "drop"]

        map
    }

    build_subgroup_table <- function(codebook, context) {
        sgdt <- data.table::as.data.table(codebook[, c(context$initvar_col, context$subgroup_col, context$colspec$group)])
        data.table::setnames(sgdt, c(context$initvar_col, context$subgroup_col, context$colspec$group), c("variable", "subgroup", "group"))
        sgdt <- sgdt[!is.na(subgroup)]

        if (sgdt[, .N] < 1) {
            return(NULL)
        }

        if (!is.null(context$content_origins)) {
            sgdt[paste0(variable, "_", group) %in% context$content_origins[["variable"]], variable := paste0(variable, "_", group)]
        }

        # Squash extra records into one prior to going to long -- shouldn't happen, but always good to check
        sgdt <- unique(sgdt[, subgroup := paste0(subgroup, collapse = ","), by = variable])
        sgdt <- sgdt[, .(subgroup = strsplit(subgroup, "\\,")[[1L]]), by = variable]

        variables <- sgdt[, variable] 
        subgroups <- sgdt[, subgroup]

        tapply(variables, subgroups, list) 
    }
    
    process_environment <- function(group_env, codebook, context) {
        if (is.null(context)) stop("Codebook application context undefined")
        if (is.null(context$dbkeys)) stop("Database reference keys undefined")
        
        group_env$err  <- tibble(Group = character(), Final = character(), Original = character(), Issue = character())
        group_env$warn <- tibble(Group = character(), Final = character(), Original = character(), Issue = character())
        group_env$pw   <- 0
        
        if (!is.null(context$subgroup_col)) {
            context$sgtable <- build_subgroup_table(codebook, context)
        }
        
        group_env$edited_vars <- lapply(context$dbkeys, function(t) list())
        names(group_env$edited_vars) <- context$dbkeys

        group_env$dropped_vars <- lapply(context$dbkeys, function(t) list())
        names(group_env$dropped_vars) <- context$dbkeys
        
        # Clear the VARMAPPED prefix
        if (isTRUE(context$prefix_select)) {
            walk(context$dbkeys, clear_prefixes, ESCAPE_PREFIX, group_env)
        }
        
        # First, compute instructions that require no reduction
        # tryCatch({
        #     walk(time_points[ok_times], map_no_reduction, group_codes[group_codes], context, group_env)
        #     walk(time_points[ok_times], map_no_reduction, filter(group_codes, is.na(ReductionDB3)), context, group_env)
        #     cat("\n")
        # }, error = function(e) {
        #     cat("\n")
        #     warning("Fault performing codebook application for variables with no reduction in \"", group_env$group, "\" : \"", e$message, "\" Check the codebook application error file for more information.", call. = F, immediate. = T)
        # })
        
        # Only perform renames for separated databases
        if (!isTRUE(context$singledb)) {
            walk(context$dbkeys, map_no_reduction, codebook[is.na(codebook[[context$reduction_col]]), ], context, group_env)
            cat("\n")
        }
        
        # Now, attempt to perform reductions on variables that need them
        # tryCatch({
        #     walk(time_points[ok_times], map_reduction, filter(group_codes, !is.na(ReductionDB3)), "ReductionDB3", "HomogenizedName", "SubgroupDB3", "FinalNameDB3", time_points, group_env)
        #     cat("\n")
        # }, error = function(e) {
        #     cat("\n")
        #     warning("Fault performing codebook application for variables with reduction in \"", group_env$group, "\" : \"", e$message, "\" Check the codebook application error file for more information.", call. = F, immediate. = T)
        # })
        
        walk(context$dbkeys, map_reduction, codebook[!is.na(codebook[[context$reduction_col]]), ], context, group_env)
        cat("\n")

        walk(context$dbkeys, drop_variables, group_env)

        walk(context$dbkeys, order_by_codebook, unique(codebook[!is.na(codebook[[context$finalvar_col]]), context$finalvar_col]), group_env, edited_only = context$prefix_select)
        
        # Rename the edited variables to reflect the prefixes
        if (isTRUE(context$prefix_select)) {
            tryCatch({
                walk(context$dbkeys, apply_prefixes, unique(codebook[!is.na(codebook[[context$finalvar_col]]), context$finalvar_col]), group_env)
                cat("\nCompleted codebook application for \"", group_env$group, "\"\n", sep = "")
            }, error = function(e) {
                warning("Fault renaming edited variables to final form: \"", e$message, "\"", call. = F, immediate. = T)
            })
        }
        
        group_env
    }

    order_by_codebook <- function(t, final_vars, group_env, edited_only = TRUE) {
        if (isTRUE(edited_only)) {
            final_vars <- final_vars[final_vars %in% unlist(group_env$edited_vars[[t]])]
        }

        final_vars <- final_vars[final_vars %in% names(group_env[[t]])]
        other_vars <- setdiff(names(group_env[[t]]), final_vars)
        other_vars <- other_vars[order(other_vars)]
        
        group_env[[t]] <- group_env[[t]][, c(final_vars, other_vars)]

        invisible(t)
    }

    drop_variables <- function(t, group_env) {
        drops <- unlist(group_env$dropped_vars[[t]])
        drops <- unique(drops[!is.na(drops)])

        group_env$dropped_vars[[t]] <- drops

        if (length(group_env$dropped_vars[[t]]) > 0L) {
            for (drop in group_env$dropped_vars[[t]]) {
                group_env[[t]][[drop]] <- NULL
            }
        }

        invisible(NULL)
    }
    
    apply_prefixes <- function(t, final_vars, group_env) {
        if (!getOption("tiestk.debug", FALSE)) {
            cat(.pw_char(group_env$pw), " Renaming variables in \"", ifelse(exists("group", envir = group_env), group_env$group, t), "\"...\r", sep = "")
            group_env$pw <- group_env$pw + 1
        }

        names(group_env$edited_vars[[t]]) <- map_chr(group_env$edited_vars[[t]], function(sp_n) paste0(ESCAPE_PREFIX, sp_n))

        group_env[[t]] <- rename(group_env[[t]], !!!group_env$edited_vars[[t]])

        invisible(t)
    }

    clear_prefixes <- function(t, prefixes, group_env, select_prefixed = T) {
        if (length(prefixes) > 1) {
            prefixes <- unlist(prefixes) # Just in case
            
            if (select_prefixed) {
                group_env[[t]] <- group_env[[t]] %>%
                    select(matches(paste0(paste0("^", prefixes), collapse = "|")))
            } 

            remap <- names(group_env[[t]])
            
            for (p in prefixes) {
                names(remap) <- gsub(p, "", remap, fixed = T)
            }

            group_env[[t]] <- dplyr::rename(group_env[[t]], !!!remap)
        } else {
            if (select_prefixed) {
                group_env[[t]] <- group_env[[t]] %>%
                    select(starts_with(prefixes))
            }

            remap <- names(group_env[[t]])
            
            names(remap) <- gsub(prefixes, "", remap, fixed = T)

            group_env[[t]] <- dplyr::rename(group_env[[t]], !!!remap)
        }
        
        invisible(t)
    }
    
    map_no_reduction <- function(t, codes, context, group_env) {
        pmap(list(codes[["CODE_ID"]], codes[[context$initvar_col]], codes[[context$finalvar_col]]), perform_rename, group_env, t, codes)

        invisible(t)
    }

    perform_rename <- function(code_id, init_n, fin_n, group_env, t, codes) {
        if (!getOption("tiestk.debug", FALSE)) {
            cat(.pw_char(group_env$pw, pw_max = 64), " Processing bijections in \"", ifelse(exists("group", envir = group_env), group_env$group, t), "\"...\r", sep = "")
            group_env$pw <- group_env$pw + 1
        }

        x <- init_n
        y <- fin_n

        DEBUG(paste0("Rename requested: ", x, " -> ", y))

        if (is.na(x)) {
            if (is.na(y)) {
                # DROP flag not set. Treat as DROP but raise an error.
                dropped_variables <- codes %>% filter(CODE_ID == code_id) %>% select(starts_with("Name_")) %>% as.list

                group_env$err[nrow(group_env$err) + 1, ] <- list(
                    Group = ifelse(exists("group", envir = group_env), group_env$group, t),
                    Variable = ifelse(all.na(dropped_variables), NA, paste0(dropped_variables, collapse = ", ")),
                    Final = NA,
                    Issue = "Missing initial and final variable names. Treated as DROP."
                )  
            } else {
                # Confounding variable for ApplyCodebook. Raise error
                group_env$warn[nrow(group_env$warn) + 1, ] <- list(
                    Group = ifelse(exists("group", envir = group_env), group_env$group, t),
                    Variable = NA,
                    Final = fin_n,
                    Issue = "Initial variable name undefined. Treated as DROP."
                )
            }
        } else {
            if (is.na(y)) {
                # Treat initial as final. Could be bad! Raise an error in case.
                group_env$warn[nrow(group_env$warn) + 1, ] <- list(
                    Group = ifelse(exists("group", envir = group_env), group_env$group, t),
                    Variable = x,
                    Final = NA,
                    Issue = "Final variable missing. Used initial variable as final, but please fill in the final variable."
                )

               y <- init_n
            } # Else all good. Continue with the processing

            if (x %not_in% names(group_env[[t]])) {
                if (!is.na(codes[codes$CODE_ID == code_id, paste0("Name___", gsub("T", "", t))])) {
                    group_env$err[nrow(group_env$err) + 1, ] <- list(
                        Group = ifelse(exists("group", envir = group_env), group_env$group, t),
                        Variable = x,
                        Final = y,
                        Issue = paste0("Could not change initial to final as it does not exist in time \"", t, "\"")
                    )
                }
            } else {
                DEBUG(paste0("Rename accepted: ", x, " -> ", y))
                group_env[[t]] <- rename(group_env[[t]], !!y := !!x)
                group_env$edited_vars[[t]] <- append(group_env$edited_vars[[t]], y)
            }
        }
    }
    
    map_reduction <- function(t, codes, context, group_env) {
        # Pre-filter variables that aren't present in the current time point. Keep generated variables, even if dependencies don't exist
        generated_variables <- codes %>% 
            select(contains("Name___")) %>% 
            apply(1,  function(x) all.na(x)) %>%
            unlist

        gen_override <- if (!is.null(context$generated_col)) {
            gen <- codes[[context$generated_col]]
            gen[is.na(gen)] <- FALSE
            gen
        } else {
            rep(FALSE, nrow(codes))
        }

        present_variables <- if (isTRUE(context$singledb)) {
            rep(TRUE, nrow(codes))
        } else {
            !is.na(codes[, paste0("Name___", gsub("T", "", t)), drop = TRUE])
        }

        global_context <- list(
            idvar = context$id_col,
            timesvar = context$timevar_col,
            database  = if (exists("group", envir = group_env)) group_env$group else NULL,
            databases = unique(codes[[context$colspec$group]]),
            sgtable   = if (!is.null(context$sgtable)) context$sgtable else NULL,
            parallel = context$parallel,
            cluster = context$cluster,
            content_origins = context$content_origins
        )

        codes <- codes[generated_variables | present_variables | gen_override, ]
        codes <- codes %>% 
            filter(
                is.na(!!as.name(context$subgroup_col)) |
                !(is.na(!!as.name(context$subgroup_col)) | duplicated(!!as.name(context$finalvar_col)))
            )

        # To prevent strange NA or empty strings crashing the system, return here.
        if (nrow(codes) < 1L) {
            return(invisible(t))
        }

        contexts <- lapply(1:nrow(codes), function(i) {
            list(
                dbkey     = t,
                initvar   = codes[i, context$initvar_col],
                finalvar  = codes[i, context$finalvar_col],
                subgroup  = codes[i, context$subgroup_col],
                reduction = codes[i, context$reduction_col],
                idvar     = global_context$idvar,
                timesvar  = global_context$timesvar,
                database  = global_context$database,
                databases = global_context$databases,
                sgtable   = global_context$sgtable,
                parallel  = global_context$parallel,
                cluster   = global_context$cluster,
                content_origins = global_context$content_origins
            )
        })

        purrr::walk(contexts, perform_reduction, codes, group_env)

        invisible(t)
    }
    
    perform_reduction <- function(ctx, codes, group_env) {
        if (!getOption("tiestk.debug", FALSE)) {
            cat(.pw_char(group_env$pw, pw_max = 64), " Processing reductions in \"", ifelse(exists("group", envir = group_env), group_env$group, ctx$dbkey), "\"...\r", sep = "")
            group_env$pw <- group_env$pw + 1
        }
        
        # Aliases for programming reductions
        . <- group_env[[ctx$dbkey]]

        DEBUG(paste0("Reduction requested [", ctx$initvar, " -> ", ctx$finalvar, "]: \"", ctx$reduction, "\""))

        # 99DUMMY ignore
        if (grepl("^99DUMMY$", ctx$reduction)) {
            if (ctx$initvar %in% names(group_env[[ctx$dbkey]])) {
                # Treat as bijected
                DEBUG(paste0("99DUMMY encountered: ", ctx$initvar, " -> ", ctx$finalvar))
                perform_rename(NA, ctx$initvar, ctx$finalvar, group_env, ctx$dbkey, codes)
            }
        } else {
            groupop_mode <- FALSE
            ctx$groupop <- FALSE
            
            if (isTRUE(grepl("^\\$G\\{.*\\}$", ctx$reduction))) {
                groupop_mode <- TRUE
                ctx$groupop <- TRUE
                ctx$reduction <- gsub("^\\$G\\{", "", gsub("\\}$", "", ctx$reduction))     
            }

            r <- parse_reduction(ctx$reduction, ctx)

            if (is.null(r)) {
                # DROP var
                DEBUG(paste0("DROP accepted: ", ctx$initvar))
                group_env$dropped_vars[[ctx$dbkey]] <- append(group_env$dropped_vars[[ctx$dbkey]], {
                    dbvars <- names(group_env[[ctx$dbkey]])
                    out <- NA_character_

                    if (!ctx$initvar %in% dbvars && !is.null(ctx$content_origins)) {
                        if (ctx$initvar %in% ctx$content_origins$root) {
                            co_subset <- ctx$content_origins[ctx$content_origins$root == ctx$initvar, ]
                            out <- co_subset$variable[co_subset$variable %in% dbvars]
                        } else {
                            group_env$err[nrow(group_env$err) + 1, ] <- list(
                                Group = ifelse(exists("group", envir = group_env), ctx$database, ctx$dbkey),
                                Variable = paste0("Time: ", ctx$dbkey, "; Initial variable: ", ctx$initvar),
                                Final = ctx$finalvar,
                                Issue = paste0("Invalid DROP request: variable or related variables not found")
                            )
                        }
                    } else if (!ctx$initvar %in% dbvars && is.null(ctx$content_origins)) {
                        group_env$err[nrow(group_env$err) + 1, ] <- list(
                            Group = ifelse(exists("group", envir = group_env), ctx$database, ctx$dbkey),
                            Variable = paste0("Time: ", ctx$dbkey, "; Initial variable: ", ctx$initvar),
                            Final = ctx$finalvar,
                            Issue = paste0("Invalid DROP request: Missing variable with no content origins specified")
                        )
                    } else {
                        if (!is.null(ctx$content_origins)) {
                            # Mark both for removal
                            co_subset <- ctx$content_origins[ctx$content_origins$root == ctx$initvar, ]
                            out <- co_subset$variable[co_subset$variable %in% dbvars]
                        }
                        out <- c(out, ctx$initvar)
                    }

                    out
                })

            } else if (is.character(r)) {
                # Parse error
                group_env$err[nrow(group_env$err) + 1, ] <- list(
                    Group = ifelse(exists("group", envir = group_env), ctx$database, ctx$dbkey),
                    Variable = paste0("Time: ", ctx$dbkey, "; Initial variable: ", ctx$initvar, "; Subgroup: ", ctx$subgroup),
                    Final = ctx$finalvar,
                    Issue = paste0("Error parsing instruction: ", r)
                )
            } else if (is.na(ctx$finalvar) || ctx$finalvar == "") {
                if (!is.null(r)) {
                    # Bad instruction in codebook. Cannot perform a reduction without a final name
                    group_env$err[nrow(group_env$err) + 1, ] <- list(
                        Group = ifelse(exists("group", envir = group_env), ctx$database, ctx$dbkey),
                        Variable = paste0("Time: ", ctx$dbkey, "; Initial variable: ", ctx$initvar, "; Subgroup: ", ctx$subgroup),
                        Final = ctx$finalvar,
                        Issue = "Cannot perform a reduction without a final name"
                    )
                }

                return()
            } else {
                if (!is.character(r)) {
                    tryCatch({
                        if (isTRUE(groupop_mode)) {
                            DEBUG(paste0("Reduction accepted: ", ctx$initvar, " -> ", ctx$finalvar))
                            apply_grouped_reduction(r, group_env, ctx)
                            group_env$edited_vars[[ctx$dbkey]] <- append(group_env$edited_vars[[ctx$dbkey]], ctx$finalvar)
                        } else {
                            value <- rlang::eval_tidy(r, data = group_env[[ctx$dbkey]])
                            group_env[[ctx$dbkey]][, ctx$finalvar] <- value

                            if (!is.null(value)) {
                                DEBUG(paste0("Reduction accepted: ", ctx$initvar, " -> ", ctx$finalvar))
                                group_env[[ctx$dbkey]][, ctx$finalvar] <- value
                                group_env$edited_vars[[ctx$dbkey]] <- append(group_env$edited_vars[[ctx$dbkey]], ctx$finalvar)
                            } else {
                                DEBUG(paste0("Assignment skipped for ", ctx$dbkey, ": ", ctx$initvar))
                            }
                        }
                    }, error = function(e) {
                        group_env$err[nrow(group_env$err) + 1, ] <- list(
                            Group = ifelse(exists("group", envir = group_env), ctx$database, ctx$dbkey),
                            Variable = paste0("Time: ", ctx$dbkey, "; Initial variable: ", ctx$intvar, "; Subgroup: ", ctx$subgroup),
                            Final = ctx$finalvar,
                            Issue = paste0(e$message)
                        )
                    })
                }
            }
        }
    }

    apply_grouped_reduction <- function(r, owning_env, ctx) {
        parallel_processing <- ctx$parallel %||% FALSE
        numcores            <- ctx$numcores %||% (parallel::detectCores() - 1L)

        DT <- data.table::as.data.table(owning_env[[ctx$dbkey]])

        # Ensure time ordering within subject ID to make guarantees about row order meaning
        order_query <- bquote(order(.(as.name(ctx$idvar)), .(as.name(ctx$timesvar))))
        DT <- DT[eval(order_query)]

        if (isTRUE(parallel_processing)) {
            DT[, `_GROUP_ID` := sample(1:numcores, 1L), by = c(ctx$idvar)]

            dtgroup <- lapply(1:numcores, function(i) DT[`_GROUP_ID` == i])

            # If ctx$cluster is NOT NULL, use that cluster and do NOT release the cluster after use.
            # This strategy will allow ApplyCodebook to more efficiently use resources.
            cl <- ctx$cluster %||% parallel::makeCluster(numcores)
            parallel::clusterExport(cl = cl, varlist = c("CONSOLIDATE", "SUM", "MODE"))
            on.exit(if (is.null(ctx$cluster)) parallel::stopCluster(cl))

            dtgroup <- parallel::parLapply(cl, dtgroup, function(dt) {
                # Don't really know why this is required, but it is. 
                if (!data.table::is.data.table(dt)) {
                    dt <- data.table::as.data.table(dt)
                }

                dt[, c(ctx$finalvar) := eval(r), by = c(ctx$idvar)]

                data.table::copy(dt)
            })

            DT <- data.table::rbindlist(dtgroup)
            DT <- DT[eval(order_query)]
            DT[, `_GROUP_ID` := NULL]
        } else {
            group_count <- DT[, length(unique(DT[[ctx$idvar]]))]

            if (group_count >= 1000) {
                message("[", ctx$initvar, " -> ", ctx$finalvar, "] The number of subject ID groupings exceeds 1000, which can be taxing if the reduction is complicated enough. Consider setting `parallel = TRUE` in the ApplyCodebook parameters.")
            }
            
            DT[, c(ctx$finalvar) := eval(r), by = c(ctx$idvar)]
        }

        owning_env[[ctx$dbkey]] <- as.data.frame(DT)
    }
}, .codebook)

# --------- Reduction DSL parser logic ---------
local({
    # Lifted from [Advanced R v2](https://adv-r.hadley.nz/expressions.html)
    expr_type <- function(x) {
        if (rlang::is_syntactic_literal(x)) {
            "constant"
        } else if (is.symbol(x)) {
            "symbol"
        } else if (is.call(x)) {
            "call"
        } else if (is.pairlist(x)) {
            "pairlist"
        } else {
            typeof(x)
        }
    }
    
    switch_expr <- function(x, ...) {
        switch(expr_type(x),
               ...,
               stop("Unknown type: ", typeof(x), call. = FALSE)
        )
    }
    
    extract_calltree <- function(e) {
        switch_expr(e,
            constant = e,
            symbol   = e,
            call     = {
                l <- list()
                
                if (identical(rlang::call_name(e), "function")) {
                    raw_args     <- rlang::call_args(e)
                    l$fargs      <- raw_args[[1L]]
                    l$args       <- list()
                    l$args[[1L]] <- extract_calltree(raw_args[[2L]])
                } else {
                    l$args  <- lapply(as.list(e[-1]), extract_calltree)
                }
                
                l$head    <- as.symbol(rlang::call_name(e))
                namespace <- rlang::call_ns(e)
                
                if (!is.null(namespace)) {
                    l$ns <- as.symbol(namespace)
                } else {
                    l["ns"] <- list(NULL)
                }
                
                l
            }
        )
    }
    
    squash_calltree <- function(e) {
        if (is.list(e)) {
            if (identical(as.character(e$head), "function")) {
                call("function", e$fargs, squash_calltree(e$args[[1L]]))
            } else {
                squashed <- lapply(e$args, squash_calltree)
                
                if (!is.null(e$ns)) {
                    rlang::expr(`::`(!!e$ns, !!e$head)(!!!squashed))
                } else {
                    rlang::expr((!!e$head)(!!!squashed))
                }
            }
        } else {
            rlang::enexpr(e)
        }
    }

    parse_reduction <- function(instruction, context) {
        stopifnot(is.list(context))
        
        if (grepl("^\\$R\\{.*\\}$", instruction)) {
            # Legacy support
            return(parse_reduction(stringr::str_extract(instruction, "(?<=\\$R\\{).*(?=\\})"), context))
        }
        
        if (grepl("^DROP$", instruction)) {
           instruction  <- "NULL"
        }
        
        parsed <- tryCatch({
            rlang::parse_expr(instruction)
        }, error = function(e) {
            e$message
        })
        
        if (is.character(parsed)) {
            return(parsed)
        }
        
        squash_calltree(translate_calltree(extract_calltree(parsed), context))
    }

    expand_columns <- function(cols, list_mode = FALSE) {
        colsyms <- lapply(cols, as.name)
        trunk   <- rlang::expr(tibble::tibble(!!!colsyms))

        extract_calltree(trunk)
    }

    translate_calltree <- function(e, context) {
        if (is.null(e) || is.pairlist(e)) {
            # Ignore as to not infuriate the R gods
            return(e)
        }

        if (is.list(e)) {
            if (macro_is_indexed_call(e)) {
                if (as.character(e$args[[1L]]$head) %in% names(registered_macros)) {
                    return(safe_macro(e$args[[1L]]$head, "indexed_call", e, context))
                }
            }

            if (macro_is_indexed(e)) {
                if (as.character(e$args[[1L]]) %in% names(registered_macros)) {
                    return(safe_macro(e$args[[1L]], "indexed", e, context))
                }
            }

            if (macro_is_call(e)) {
                # macro_is_call does the name check already 
                return(safe_macro(e$head, "call", e, context))
            }

            e$args <- lapply(e$args, translate_calltree, context)

            return(e)
        }

        # Symbol or constant
        if (as.character(e) %in% names(registered_macros)) {
            return(safe_macro(e, "variable", e, context, func_only = FALSE))
        }
        
        e
    }

    macro <- function(name, ...) {
        stopifnot(is.character(name))

        m <- list(name = name, variable = NULL, call = NULL, indexed = NULL, indexed_call = NULL)
        arglist <- list(...)

        kw <- names(arglist)[names(arglist) %in% names(m)]
        m[kw] <- arglist[kw]

        for (check in c("call", "indexed", "indexed_call")) {
            if (!is.null(m[[check]]) && !is.function(m[[check]])) stop("'", check, "' for macro '", m$name, "' must be a function f that takes in the current expression branch e and the working context ctx.", call. = FALSE)
        }

        structure(m, class = "reduction_macro")
    }

    macro_is_indexed <- function(e) {
        identical(as.character(e$head), "[") &&
            !is.list(e$args[[1L]])
    }

    macro_is_indexed_call <- function(e) {
        identical(as.character(e$head), "[") && 
            is.list(e$args[[1L]]) &&
            !is.null(e$args[[1L]]$head)
    }

    macro_is_call <- function(e) {
        !is.null(e$head) &&
            as.character(e$head) %in% names(registered_macros)
    }
    
    safe_macro <- function(root, type, e, context, func_only = TRUE) {
        expr_chr <- as.character(root)
        macro_call <- registered_macros[[expr_chr]][[type]]

        if (is.null(macro_call)) stop("Undefined '", type, "' for macro '", expr_chr, "'", call. = FALSE)

        if (!is.function(macro_call) && isTRUE(func_only)) stop("Macro '", expr_chr, "' action has to be a function for '", type, "'", call. = FALSE)

        if (is.function(macro_call)) {
            return(macro_call(e, context))
        } else {
            macro_call
        }
    }

    variable_subgroup <- function(e, ctx) {
        if (is.null(ctx$subgroup)) {
            stop("Variable subgroup undefined -- Cannot be undefined for use of .SUBGROUP alone", call. = FALSE)
        } else if (is.null(ctx$sgtable)) {
            stop("Subgroup table not found", call. = FALSE)
        }
        
        expand_columns(unlist(strsplit(ctx$sgtable[[ctx$subgroup]], "\\|")), list_mode = ctx$groupop)
    }

    indexed_subgroup <- function(e, ctx) {
        if (is.null(ctx$sgtable)) {
            stop("Subgroup table not found", call. = FALSE)
        }
        
        keys <- vapply(e$args[-1], function(x) if (!is.character(x)) as.character(x) else x, character(1L))
        columns <- unique(unlist(lapply(keys, function(key) {
            if (!key %in% names(ctx$sgtable)) {
                NA
            } else {
                ctx$sgtable[[key]]
            }
        })))

        expand_columns(columns[!is.na(columns)], list_mode = ctx$groupop)
    }

    variable_sgvarnames <- function(e, ctx) {
        if (is.null(ctx$subgroup)) {
            stop("Variable subgroup undefined -- Cannot be undefined for use of .SUBGROUP alone", call. = FALSE)
        } else if (is.null(ctx$sgtable)) {
            stop("Subgroup table not found", call. = FALSE)
        }
        
        extract_calltree(rlang::call2("c", !!!unlist(strsplit(ctx$sgtable[[ctx$subgroup]], "\\|"))))
    }

    indexed_sgvarnames <- function(e, ctx) {
        if (is.null(ctx$sgtable)) {
            stop("Subgroup table not found", call. = FALSE)
        }
        
        keys <- vapply(e$args[-1], function(x) if (!is.character(x)) as.character(x) else x, character(1L))
        columns <- unique(unlist(lapply(keys, function(key) {
            if (!key %in% names(ctx$sgtable)) {
                NA
            } else {
                ctx$sgtable[[key]]
            }
        })))

        extract_calltree(rlang::call2("c", !!!columns[!is.na(columns)]))
    }

    variable_database <- function(e, ctx) {
        if (is.null(ctx$databases)) {
            stop("Data groups list not found", call. = FALSE)
        } else if (is.null(ctx$initvar)) {
            stop("Initial variable stem not defined for .DATABASES call", call. = FALSE)
        }

        expand_columns(paste0(ctx$initvar, "_", ctx$databases))
    }

    indexed_database <- function(e, ctx) {
        if (is.null(ctx$databases)) {
            stop("Database groups list not found", call. = FALSE)
        } else if (is.null(ctx$initvar)) {
            stop("Initial variable stem not defined for .DATABASES call", call. = FALSE)
        }

        keys <- vapply(e$args[-1], function(x) if (!is.character(x)) as.character(x) else x, character(1L))
        databases <- vapply(keys, function(key) {
            if (!key %in% ctx$databases) {
                warning(paste0("Requested database \"", key, "\" not found"), call. = FALSE, immediate. = TRUE)
                NA_character_
            } else {
                key
            }
        }, character(1L))

        expand_columns(paste0(ctx$initvar, "_", databases[!is.na(databases)]))
    }

    indexed_call_database <- function(e, ctx) {
        if (is.null(ctx$databases)) {
            stop("Database groups list not found", call. = FALSE)
        }

        keys <- vapply(e$args[-1], function(x) if (!is.character(x)) as.character(x) else x, character(1L))
        databases <- vapply(keys, function(key) {
            if (!key %in% ctx$databases) {
                warning(paste0("Requested database \"", key, "\" not found"), call. = FALSE, immediate. = TRUE)
                NA_character_
            } else {
                key
            }
        }, character(1L))

        varsym <- {
            .var <- translate_calltree(e$args[[1L]]$args[[1L]], ctx)

            if (is.list(.var)) stop("Argument to .DATABASES must be a string or symbol (including `.THIS`)", call. = FALSE)

            as.character(.var)
        }


        expand_columns(paste0(varsym, "_", databases[!is.na(databases)]))
    }

    indexed_group <- function(e, ctx) {
        if (is.null(ctx$content_origins)) {
            stop("Content origins undefined. Will not be able to determine the variables associated with a database group.", call. = FALSE)
        }

        keys <- vapply(e$args[-1], function(x) if (!is.character(x)) as.character(x) else x, character(1L))
        groupvars <- unlist(lapply(keys, function(key) {
            if (!key %in% ctx$content_origins$origin) {
                warning(paste0("Requested database \"", key, "\" not found"), call. = FALSE, immediate. = TRUE)
                NA_character_
            } else {
                ctx$content_origins[ctx$content_origins$origin == key, "variable"]
            }
        }))

        expand_columns(unique(groupvars[!is.na(groupvars)]))
    }

    variable_this <- function(e, ctx) {
        if (is.null(ctx$initvar)) {
            stop("Initial variable not defined for .THIS call", call. = FALSE)
        }

        as.name(ctx$initvar)
    }

    variable_thisname <- function(e, ctx) {
        if (is.null(ctx$initvar)) {
            stop("Initial variable not defined for .THISNAME call", call. = FALSE)
        }

        as.character(ctx$initvar)
    }

    indexed_this <- function(e, ctx) {
        if (is.null(ctx$initvar)) {
            stop("Initial variable not defined for .THIS call", call. = FALSE)
        }

        e$args[[1L]] <- as.name(as.character(ctx$initvar))
        e$args[[2L]] <- translate_calltree(e$args[[2L]], ctx)

        e
    }

    call_dummify <- function(e, ctx) {
        na_rm <- if ("na.rm" %in% names(e$args)) {
            e$args$na.rm
        } else {
            FALSE
        }

        var <- as.name(e$args[[1L]])
        val <- e$args[[2L]]
        
        new_e <- if (isTRUE(na_rm)) {
            bquote({ .(var)[.(var) == .(val)] <- 1L; .(var)[is.na(.(var)) | .(var) != .(val)] <- 0L; .(var) })
        } else {
            bquote({ .(var)[.(var) == .(val)] <- 1L; .(var)[.(var) != .(val)] <- 0L; .(var) })
        }
         
        translate_calltree(extract_calltree(new_e), ctx)
    }

    variable_key <- function(e, ctx) {
        if (is.null(ctx$dbkey)) {
            stop("Database key not defined", call. = FALSE)
        }
        
        ctx$dbkey
    }

    indexed_times <- function(e, ctx) {
        if (is.null(ctx$timesvar)) {
            stop("Time variable not defined for .TIMES call",  call. = FALSE) 
        } else if (is.null(ctx$initvar)) {
            stop("Initial variable not defined", call. = FALSE)
        }

        vals_tree <- list()
        vals_tree$head <- "c"
        vals_tree["ns"] <- list(NULL)
        vals_tree$args <- e$args[2:length(e$args)]

        expr_c <- squash_calltree(vals_tree)
        varsym <- as.name(ctx$initvar)

        full_expr <- bquote(.(varsym)[.(as.name(ctx$timesvar)) %in% .(expr_c)])

        translate_calltree(extract_calltree(full_expr), ctx)
    }

    indexed_call_times <- function(e, ctx) {
        if (is.null(ctx$timesvar)) {
            stop("Time variable not defined for .TIMES call",  call. = FALSE) 
        } 

        vals_tree <- list()
        vals_tree$head <- "c"
        vals_tree["ns"] <- list(NULL)
        vals_tree$args <- e$args[2:length(e$args)]

        expr_c <- squash_calltree(vals_tree)
        varsym <- as.name(e$args[[1L]]$args[[1L]])

        full_expr <- bquote(.(varsym)[.(as.name(ctx$timesvar)) %in% .(expr_c)])

        translate_calltree(extract_calltree(full_expr), ctx)
    }

    load_call <- function(e, ctx) {
        file_path <- rlang::eval_tidy(squash_calltree(e$args[[1L]]))

        expr <- parse(file = file_path)

        # This assumes that the target file is wrapped in a code block and returns a SINGLE value
        translate_calltree(extract_calltree(as.list(expr)[[1L]]), ctx)
    }

    registered_macros <- list(
        macro(
            name = ".DATABASES",
            variable = variable_database,
            indexed = indexed_database,
            indexed_call = indexed_call_database
        ),
        macro(
            name = ".GROUPS",
            indexed = indexed_group
        ),
        macro(
            name = ".DUMMIFY",
            call = call_dummify
        ),
        macro(
            name = ".KEY",
            variable = variable_key
        ),
        macro(
            name = ".SUBGROUP",
            variable = variable_subgroup,
            indexed = indexed_subgroup
        ),
        macro(
            name = ".SGVARNAMES",
            variable = variable_sgvarnames,
            indexed = indexed_sgvarnames
        ),
        macro(
            name = ".THIS",
            variable = variable_this,
            indexed = indexed_this
        ),
        macro(
            name = ".THISNAME",
            variable = variable_thisname
        ),
        macro(
            name = ".TIMES",
            indexed = indexed_times,
            indexed_call = indexed_call_times
        ),
        macro(
            name = ".LOAD",
            call = load_call
        )
    )

    names(registered_macros) <- lapply(registered_macros, function(m) m$name)
}, .codebook)
