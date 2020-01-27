long.format.fix <- function(
    corrections,
    database,
    db_root,
    id_col,
    time_col,
    unique_id_col,
    what_vars,
    fn_path) {
    
    if (!inherits(database, "data.frame")) {
        stop("`database` must be a data.frame or similar object (e.g. a tbl_df or data.table).", call. = FALSE)
    }

    if (is.character(corrections)) {
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
    } else {
        if (!inherits(corrections, "data.frame")) {
            stop("`corrections` must be a data.frame or similar object OR a path to the corrections file", call. = FALSE)
        }
    }

    stopifnot(is.character(db_root), length(db_root) == 1L)

    if (data.table::is.data.table(database)) {
        # Multiple.fix() is not compatible with data.tables currently
        # TODO: Make Multiple.fix() compatible with data.tables
        database <- as.data.frame(database)
    }

    if (data.table::is.data.table(corrections)) {
        corrections <- as.data.frame(corrections)
    }

    times <- unique(database[[time_col]])
    times <- times[order(times)]

    split_databases <- lapply(times, function(.t) database[database[[time_col]] == .t, ])
    names(split_databases) <- paste0(db_root, "_", times)

    source(file.path(fn_path, "Basic Cleaning.R"))
    source(file.path(fn_path, "Flexible Across Database Corrections Function.R"))
    
    fixed_split <- Multiple.fix(
        CORRECT = corrections, 
        DATA = split_databases,
        WHAT = what_vars,
        ID = rep(id_col, length(split_databases)),
        UNIQUEIDS = rep(unique_id_col, length(split_databases)),
        OUTPATH = NULL,
        FUN_DIR = fn_path,
        CLEANED = TRUE
    )

    recombined <- data.table::rbindlist(fixed_split, use.names = TRUE, fill = TRUE)
    recombined <- recombined[!is.na(recombined[[unique_id_col]])]

    as.data.frame(recombined[order(recombined[[id_col]], recombined[[time_col]])])
}
