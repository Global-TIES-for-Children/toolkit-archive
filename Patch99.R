`%not_in%` <- function(x, y) !(x %in% y)

Patch99 <- function(list_assessments, mapping = NA, variable_roots = NA, map_source_col = "FinalNameDB3", prefix = "___VARMAPPED___") {
    #' Patching databases with abuse of the non-response column
    #' 
    #'
    #' @param list_assessments List of databases of the form `list(GROUP1 = list(time1, time2, ...), GROUP2 = list(time1, time2, ...), ...)`
    #' @param mapping A cleaned item mapping spreadsheet (see the specifications in `HomogenizeWaveVariables`). Will not be used if variable_roots is not `NA`
    #' @param variable_roots A character vector that contains all the items that could have the _99 issue.
    #' @param map_source_col If `mapping` is not `NA`, then this will be the source of variable names
    #' @param prefix After running parts of the cleaning toolkit, some variables have a prefix. To compensate, define that prefix here
    #' @return list of assessments with the same form as `list_assessments`

    if (!is.na(variable_roots)) {
        out <- lapply(list_assessments, function(group){
            lapply(list_assessments[[group]], function(time_point) .patch_time_point, variable_roots, prefix)
        })

        names(out) <- names(list_assessments)
        out
    } else if (!is(mapping, "logical")) { # Avoid the deep check on is.na against dataframes
        out <- lapply(names(list_assessments), function(group) {
            final_vars <- mapping[mapping$Group == group, map_source_col]

            bad_vars <- vapply(final_vars, function(var) {
                if (grepl("_99$", var)) {
                    root <- gsub("_99$", "", var)
                    items_pattern <- paste0(root, "[0-9]+$")

                    if (length(final_vars[grepl(items_pattern, final_vars)]) > 0) {
                        root
                    } else {
                        ""
                    }
                } else {
                    ""
                }
            }, character(1))

            lapply(seq_along(list_assessments[[group]]), .patch_time_point, list_assessments[[group]], group, bad_vars[bad_vars != ""], prefix)
        })

        names(out) <- names(list_assessments)
        out
    } else {
        stop("Either a mapping file or list of variables must be provided")
    }
}

.patch_time_point <- function(tp, time_points, group, roots, prefix) {
    time_point <- time_points[[tp]]

    for (root in roots) {
        bad_var <- paste0(prefix, root, "_99")
        start_anchor <- paste0("^", prefix, root)
        end_anchor   <- "_99$"
        cols <- names(time_point)

        if (bad_var %not_in% cols) {
            next
        }

        cat("Patching ", paste0(group, "$T", tp, "$", root), "...\n", sep = "")

        var_filter <- vapply(cols, function(var) {
            grepl(start_anchor, var) & !grepl(end_anchor, var)
        }, logical(1))

        # Did the surveyed individual respond?
        responded <- apply(time_point[, var_filter], 1, function(x) {
            y <- unlist(lapply(x, function(.x) {
                if (is.character(.x)) {
                    if (grepl("T(RUE)?|F(ALSE)?", .x, ignore.case = T)) {
                        as.logical(.x)
                    } else if (grepl("\\d+(\\.\\d*)?", .x)) {
                        as.numeric(.x)
                    } else {
                        # Can't coerce to numeric/logical
                        NA
                    }
                } else {
                    .x
                }
            }))

            sum(y[!is.na(y)]) > 0
        })

        # For the cases where the individual didn't say YES to any of the options, 
        # and that were flagged as non-response (_99), substitute the zeros for NAs
        no_response_filter <- (time_point[, bad_var] == 1) & !responded
        selected_cols <- vapply(cols, function(column) grepl(start_anchor, column) & !grepl(end_anchor, column), logical(1))
        selected_cols <- names(selected_cols[selected_cols])

        # Fill in the other variables with NA to drop the _99 var
        for (sel_col in selected_cols) {
            time_point[no_response_filter & !is.na(time_point[, sel_col]), sel_col] <- NA
        }

        time_point[, bad_var] <- NULL
    }

    time_point
}
