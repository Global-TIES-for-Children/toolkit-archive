#' Export issues for a long format database
#'
#' Sometimes, data is not entirely in a wide format, which most of the toolkit is constructed to handle.
#' This function splits a long format database (multiple observations per observed entity, e.g. a teacher or student, per database)
#' into several smaller databases, each corresponding to a single time point. Then, all of the databases, tagged with corresponding value
#' in `time_col`, are shuttled to the multiple set standard verification function `export.issues()`.
#'
#' @param database The long database in question
#' @param db_root A string that is the root for the output SVF files (e.g. if your `database` is called "TEACHER", it might be useful to assign `db_root` to "TEACHER")
#' @param id_col The name of a character-valued column in `database` containing the IDs for the observed entities (e.g. student IDs for students)
#' @param time_col The name of a column in `database` that distinguishes the time point for each session. For example, if the input database is a survey of students and some students had multiple survey sessions, then the column that stores the session number, say, "SessionNumber" should be assigned to `time_col`. This function uses `time_col` to split apart `database` into smaller sub-databases to pass into `export.issues()`
#' @param unique_id_col Like `time_col`, this is the name of a column that uniquely identifies each observation (i.e. row). This parameter gets passed to `export.issues()`. Please read its documentation, specifically about the parameter `UNIQUEID`, to know more about its specifications.
#' @param reference_db A database that acts as a reference for `database` for any verifiable information (e.g. demographic data)
#' @param name_vars See `NAMES` in `export.issues()`
#' @param numeric_vars See `VARS` in `export.issues()`
#' @param name_margin See `NMARGIN` in `export.issues()`
#' @param numeric_upper_margin See `Vuppermargins` in `export.issues()`
#' @param numeric_lower_margin See `Vlowermargins` in `export.issues()`
#' @param output_directory See `OUTPATH` in `export.issues()`
#' @param fn_path Where the other "General Functions" are located.
long.oneset.issues <- function(
    database,
    db_root,
    id_col,
    time_col,
    unique_id_col,
    reference_db,
    name_vars,
    numeric_vars,
    name_margin,
    numeric_upper_margin,
    numeric_lower_margin,
    output_directory,
    fn_path) {

    if (!inherits(database, "data.frame")) {
        stop("`database` must be a data.frame or similar object (e.g. a tbl_df or data.table).", call. = FALSE)
    }

    stopifnot(is.character(db_root), length(db_root) == 1L)

    if (requireNamespace("data.table", quietly = TRUE)) {
        if (data.table::is.data.table(database)) {
            # export.issues() is not compatible with data.tables currently
            # TODO: Make export.issues() compatible with data.tables
            database <- as.data.frame(database)
        }
    }

    if (!dir.exists(output_directory)) {
        message("Created missing directory: ", output_directory)
        dir.create(output_directory, recursive = TRUE)
    }

    times <- unique(database[[time_col]])

    split_databases <- lapply(times, function(.t) database[database[[time_col]] == .t, ])
    names(split_databases) <- paste0(db_root, "_", times)

    source(file.path(fn_path, "Standard Verification Function.R"))
    export.issues(
        DATA          = split_databases,
        ID            = id_col,
        UNIQUEID      = rep(unique_id_col, length(split_databases)),
        REFERENCE     = reference_db,
        NAMES         = name_vars,
        VARS          = numeric_vars,
        NMARGIN       = name_margin,
        Vlowermargins = numeric_lower_margin,
        Vuppermargins = numeric_upper_margin,
        OUTPATH       = output_directory,
        FILE          = names(split_databases),
        FNPATH        = fn_path
    )
}
